# revised_cascade_windows_utils.py
from datetime import datetime, timedelta
from collections import defaultdict
import traceback
import pandas as pd

def find_cascade_windows_from_events_v3(events):
    """
    Identifies the time windows of PinDrop, ChestOpen, SwapVote, PreBlockActivity, and PostBlockActivity cascades.
    Each window includes cascade_id, hi_eventType, hiMeta_eventType, BlockType, and time bounds.
    Matching is based on parsed Timestamps for higher precision.
    """
    windows = []
    cascade_id = 0
    start_time, hi_event_type, hi_meta_type, block_type = None, None, None, None

    for event in sorted(events, key=lambda e: safe_parse_timestamp(e.get("Timestamp"))):
        lo_type = event.get("lo_eventType", "")

        # 🚫 Exclude marks entirely
        if "Mark" in lo_type:
            continue

        if lo_type in {
            "PinDrop_Moment", "ChestOpen_Moment", "SwapVote_Moment",
            "PreBlock_CylinderWalk_start", "InterBlock_Idle_start"
        }:
            print(f"➕ Starting cascade: {event['lo_eventType']} @ {event['Timestamp']}")
            cascade_id += 1
            start_time = safe_parse_timestamp(event.get("Timestamp"))
            hi_event_type = lo_type.replace("_Moment", "").replace("_start", "")
            hi_meta_type = "BlockActivity"
            block_type = event.get("BlockType", "Unknown")

        elif lo_type in {
            "CoinValueTextVis_end", "BlockScoreText_end",
            "PreBlock_CylinderWalk_end", "InterBlock_Idle_end"
        } and start_time is not None:
            print(f"🛑 Ending cascade: {event['lo_eventType']} @ {event['Timestamp']}")
            end_time = safe_parse_timestamp(event.get("Timestamp"))
            windows.append({
                "cascade_id": cascade_id,
                "hi_eventType": hi_event_type,
                "hiMeta_eventType": hi_meta_type,
                "BlockType": block_type,
                "start_Timestamp": start_time,
                "end_Timestamp": end_time
            })
            start_time, hi_event_type, hi_meta_type, block_type = None, None, None, None

    from collections import Counter
    summary = Counter(w["hi_eventType"] for w in windows)
    print("🧪 Cascade Summary by hi_eventType:")
    for etype, count in summary.items():
        print(f"  - {etype}: {count} cascade(s)")

    return windows

def match_cascade_window_v2(event, cascade_windows):
    """
    Finds the full cascade window context that matches the event's AppTime.
    Returns full cascade info including cascade_id, hi_eventType, hiMeta_eventType.
    """
    time = safe_parse_timestamp(event.get("Timestamp"))
    lo_type = event.get("lo_eventType", "")

    # 🚫 Marks should not be matched to cascades
    if "Mark" in lo_type:
        return None
    for window in cascade_windows:
        if window["start_Timestamp"] <= time <= window["end_Timestamp"]:
            return window
    return None

def match_cascade_window_v3a(event, cascade_windows):
    """
    Matches an event to its cascade window using its Timestamp.
    Skips Mark-type events and windows with missing timestamps.
    """
    lo = event.get("lo_eventType", "")
    if "Mark" in lo:
        return None

    ts = safe_parse_timestamp(event.get("Timestamp"))
    if ts is None:
        print(f"⚠️ NULL parsed timestamp in event:\n  raw Timestamp: {ts_raw}\n  event: {event}")
        return None

    for window in cascade_windows:
        start = window.get("start_Timestamp")
        end = window.get("end_Timestamp")
        if start is None or end is None:
            continue  # Skip broken window
        if start <= ts <= end:
            return window

    return None

def match_cascade_window_v3(event, cascade_windows):
    lo = event.get("lo_eventType", "")
    if "Mark" in lo:
        return None

    ts = safe_parse_timestamp(event.get("Timestamp"))
    if ts is None:
        print(f"⚠️ NULL parsed timestamp in event:\n  raw Timestamp: {event.get('Timestamp')}\n  event: {event}")
        return None

    matches = []
    for window in cascade_windows:
        start = window.get("start_Timestamp")
        end = window.get("end_Timestamp")
        if start is None or end is None:
            continue

        if window.get("overlap_ok", False):
            if start <= ts <= end:
                matches.append(window)
        else:
            if start <= ts < end:
                matches.append(window)

    if matches:
        return max(matches, key=lambda w: w["start_Timestamp"])
    return None


def find_cascade_windows_from_events_v4a(events):
    """
    Builds cascade windows from sequences of events based on *_Moment and *_start initiators.
    The window ends at the maximum of `end_Timestamp` among its members.
    """
    windows = []
    cascade_id = 0
    cascade_events = []
    current_window_start_event = None

    # Sort all events by Timestamp
    events_sorted = sorted(events, key=lambda e: safe_parse_timestamp(e["Timestamp"]))

    for event in events_sorted:
        lo = event.get("lo_eventType", "")
        ts = safe_parse_timestamp(event["Timestamp"])

        # Detect cascade-starting events
        is_start = any(lo.endswith(suffix) for suffix in ["_Moment", "_start"]) and "Mark" not in lo

        if is_start:
            # Save prior window if one was active
            if current_window_start_event and cascade_events:
                end_ts = max(safe_parse_timestamp(ev.get("end_Timestamp", ev["Timestamp"])) for ev in cascade_events)
                windows.append({
                    "cascade_id": cascade_id,
                    "hi_eventType": current_window_start_event.get("hi_eventType"),
                    "hiMeta_eventType": current_window_start_event.get("hiMeta_eventType"),
                    "BlockType": current_window_start_event.get("BlockType", "Unknown"),
                    "start_Timestamp": safe_parse_timestamp(current_window_start_event["Timestamp"]),
                    "end_Timestamp": end_ts
                })

            # Begin new cascade
            cascade_id += 1
            current_window_start_event = event
            cascade_events = [event]
        elif current_window_start_event:
            cascade_events.append(event)

    # Handle last cascade
    if current_window_start_event and cascade_events:
        end_ts = max(safe_parse_timestamp(ev.get("end_Timestamp", ev["Timestamp"])) for ev in cascade_events)
        windows.append({
            "cascade_id": cascade_id,
            "hi_eventType": current_window_start_event.get("hi_eventType"),
            "hiMeta_eventType": current_window_start_event.get("hiMeta_eventType"),
            "BlockType": current_window_start_event.get("BlockType", "Unknown"),
            "start_Timestamp": safe_parse_timestamp(current_window_start_event["Timestamp"]),
            "end_Timestamp": end_ts
        })

    # Optional: print cascade summary
    from collections import Counter
    summary = Counter(w["hi_eventType"] for w in windows)
    print("🧪 Cascade Summary by hi_eventType:")
    for etype, count in summary.items():
        print(f"  - {etype}: {count} cascade(s)")

    return windows

def find_cascade_windows_from_events_v4b(events):
    """
    Builds cascade windows from sequences of events based on *_Moment and *_start initiators.
    The window ends at the maximum of `end_Timestamp` among its members.
    """
    windows = []
    cascade_id = 0
    cascade_events = []
    current_window_start_event = None

    # Sort all events by Timestamp
    events_sorted = sorted(events, key=lambda e: safe_parse_timestamp(e.get("Timestamp")) or datetime.min)

    for event in events_sorted:
        lo = event.get("lo_eventType", "")
        ts = safe_parse_timestamp(event.get("Timestamp"))

        # Detect cascade-starting events
        is_start = any(lo.endswith(suffix) for suffix in ["_Moment", "_start"]) and "Mark" not in lo

        if is_start:
            # Save prior window if one was active
            if current_window_start_event and cascade_events:
                timestamps = [
                    safe_parse_timestamp(ev.get("end_Timestamp") or ev.get("Timestamp"))
                    for ev in cascade_events
                ]
                timestamps = [t for t in timestamps if t is not None]

                if not timestamps:
                    print("⚠️ No valid timestamps in previous cascade_events:")
                    for ev in cascade_events:
                        print(f"  - {ev}")
                else:
                    end_ts = max(timestamps)
                    windows.append({
                        "cascade_id": cascade_id,
                        "hi_eventType": current_window_start_event.get("hi_eventType"),
                        "hiMeta_eventType": current_window_start_event.get("hiMeta_eventType"),
                        "BlockType": current_window_start_event.get("BlockType", "Unknown"),
                        "start_Timestamp": safe_parse_timestamp(current_window_start_event.get("Timestamp")),
                        "end_Timestamp": end_ts
                    })

            # Begin new cascade
            cascade_id += 1
            current_window_start_event = event
            cascade_events = [event]
        elif current_window_start_event:
            cascade_events.append(event)

    # Handle last cascade
    if current_window_start_event and cascade_events:
        timestamps = [
            safe_parse_timestamp(ev.get("end_Timestamp") or ev.get("Timestamp"))
            for ev in cascade_events
        ]
        timestamps = [t for t in timestamps if t is not None]

        if not timestamps:
            print("⚠️ No valid timestamps in final cascade_events:")
            for ev in cascade_events:
                print(f"  - {ev}")
        else:
            end_ts = max(timestamps)
            windows.append({
                "cascade_id": cascade_id,
                "hi_eventType": current_window_start_event.get("hi_eventType"),
                "hiMeta_eventType": current_window_start_event.get("hiMeta_eventType"),
                "BlockType": current_window_start_event.get("BlockType", "Unknown"),
                "start_Timestamp": safe_parse_timestamp(current_window_start_event.get("Timestamp")),
                "end_Timestamp": end_ts
            })

    # Optional: print cascade summary
    from collections import Counter
    summary = Counter(w["hi_eventType"] for w in windows)
    print("🧪 Cascade Summary by hi_eventType:")
    for etype, count in summary.items():
        print(f"  - {etype}: {count} cascade(s)")

    return windows

def find_cascade_windows_from_events_v4c(events):
    windows = []
    cascade_id = 0
    cascade_events = []
    current_window_start_event = None

    # Sort all events by Timestamp
    events_sorted = sorted(events, key=lambda e: safe_parse_timestamp(e.get("Timestamp")) or datetime.min)

    for event in events_sorted:
        lo = event.get("lo_eventType", "")
        ts = safe_parse_timestamp(event.get("Timestamp"))

        is_start = any(lo.endswith(suffix) for suffix in ["_Moment", "_start"]) and "Mark" not in lo

        if is_start:
            if current_window_start_event and cascade_events:
                timestamps = [
                    safe_parse_timestamp(ev.get("end_Timestamp") or ev.get("Timestamp"))
                    for ev in cascade_events
                ]
                timestamps = [t for t in timestamps if t is not None]
                if timestamps:
                    end_ts = max(timestamps)
                    hi_evt = current_window_start_event.get("hi_eventType")
                    windows.append({
                        "cascade_id": cascade_id,
                        "hi_eventType": hi_evt,
                        "hiMeta_eventType": current_window_start_event.get("hiMeta_eventType"),
                        "BlockType": current_window_start_event.get("BlockType", "Unknown"),
                        "start_Timestamp": safe_parse_timestamp(current_window_start_event.get("Timestamp")),
                        "end_Timestamp": end_ts,
                        "overlap_ok": hi_evt in {"PinDrop", "ChestOpen"}
                    })

            cascade_id += 1
            current_window_start_event = event
            cascade_events = [event]
        elif current_window_start_event:
            cascade_events.append(event)

    if current_window_start_event and cascade_events:
        timestamps = [
            safe_parse_timestamp(ev.get("end_Timestamp") or ev.get("Timestamp"))
            for ev in cascade_events
        ]
        timestamps = [t for t in timestamps if t is not None]
        if timestamps:
            end_ts = max(timestamps)
            hi_evt = current_window_start_event.get("hi_eventType")
            windows.append({
                "cascade_id": cascade_id,
                "hi_eventType": hi_evt,
                "hiMeta_eventType": current_window_start_event.get("hiMeta_eventType"),
                "BlockType": current_window_start_event.get("BlockType", "Unknown"),
                "start_Timestamp": safe_parse_timestamp(current_window_start_event.get("Timestamp")),
                "end_Timestamp": end_ts,
                "overlap_ok": hi_evt in {"PinDrop", "ChestOpen"}
            })

    from collections import Counter
    summary = Counter(w["hi_eventType"] for w in windows)
    print("🧪 Cascade Summary by hi_eventType:")
    for etype, count in summary.items():
        print(f"  - {etype}: {count} cascade(s)")

    return windows

def find_cascade_windows_from_events_v4(events):
    windows = []
    cascade_id = 0
    cascade_events = []
    current_window_start_event = None

    events_sorted = sorted(events, key=lambda e: safe_parse_timestamp(e.get("Timestamp")) or datetime.min)

    for event in events_sorted:
        lo = event.get("lo_eventType", "")
        ts = safe_parse_timestamp(event.get("Timestamp"))

        is_start = any(lo.endswith(suffix) for suffix in ["_Moment", "_start"]) and "Mark" not in lo

        if is_start:
            if current_window_start_event and cascade_events:
                timestamps = [
                    safe_parse_timestamp(ev.get("end_Timestamp") or ev.get("Timestamp"))
                    for ev in cascade_events
                ]
                timestamps = [t for t in timestamps if t is not None]

                if not timestamps:
                    print(f"⚠️ Skipped cascade_id {cascade_id}: no valid timestamps in events: {cascade_events}")
                else:
                    end_ts = max(timestamps)
                    hi_evt = current_window_start_event.get("hi_eventType")
                    windows.append({
                        "cascade_id": cascade_id,
                        "hi_eventType": hi_evt,
                        "hiMeta_eventType": current_window_start_event.get("hiMeta_eventType"),
                        "BlockType": current_window_start_event.get("BlockType", "Unknown"),
                        "start_Timestamp": safe_parse_timestamp(current_window_start_event.get("Timestamp")),
                        "end_Timestamp": end_ts,
                        "overlap_ok": hi_evt in {"PinDrop", "ChestOpen"}
                    })

            cascade_id += 1
            current_window_start_event = event
            cascade_events = [event]
        elif current_window_start_event:
            cascade_events.append(event)

    if current_window_start_event and cascade_events:
        timestamps = [
            safe_parse_timestamp(ev.get("end_Timestamp") or ev.get("Timestamp"))
            for ev in cascade_events
        ]
        timestamps = [t for t in timestamps if t is not None]

        if not timestamps:
            print(f"⚠️ Skipped cascade_id {cascade_id}: no valid timestamps in final cascade_events: {cascade_events}")
        else:
            end_ts = max(timestamps)
            hi_evt = current_window_start_event.get("hi_eventType")
            windows.append({
                "cascade_id": cascade_id,
                "hi_eventType": hi_evt,
                "hiMeta_eventType": current_window_start_event.get("hiMeta_eventType"),
                "BlockType": current_window_start_event.get("BlockType", "Unknown"),
                "start_Timestamp": safe_parse_timestamp(current_window_start_event.get("Timestamp")),
                "end_Timestamp": end_ts,
                "overlap_ok": hi_evt in {"PinDrop", "ChestOpen"}
            })

    from collections import Counter
    summary = Counter(w["hi_eventType"] for w in windows)
    print("🧪 Cascade Summary by hi_eventType:")
    for etype, count in summary.items():
        print(f"  - {etype}: {count} cascade(s)")

    return windows


def extract_walking_periods_with_cascade_ids_v3(df, cascade_windows):
    """
    Creates walking periods and assigns cascade_id and hi_eventType/meta from matching cascade windows.
    These periods are tagged as 'WalkingPeriod' and later refined into Walk2PinDrop/Wait4Feedback.
    """
    walking_periods = []
    seen_rounds = set()
    
    # Safe parse and sort by Timestamp for consistent precision
    df["parsed_Timestamp"] = df["Timestamp"].apply(safe_parse_timestamp)
    df = df.sort_values("parsed_Timestamp").reset_index(drop=True)

    for i, row in df.iterrows():
        msg = row["Message"] if isinstance(row["Message"], str) else ""
        round_key = (row.get("BlockNum"), row.get("RoundNum"), row.get("CoinSetID"))

        if round_key not in seen_rounds:
            seen_rounds.add(round_key)
            trigger_type = "Round start"
        elif "collected" in msg.lower():
            trigger_type = "Post_coin_collect"
        else:
            continue

        start_time = row["parsed_Timestamp"]
        start_app = row.get("AppTime")
        end_time, end_app, matched_window = None, None, None

        for j in range(i + 1, len(df)):
            msg_j = df.at[j, "Message"] if "Message" in df.columns else ""
            if any(term in msg_j.lower() for term in ["dropped a pin", "chest opened", "coin collected"]):
                end_time = df.at[j, "parsed_Timestamp"]
                end_app = df.at[j, "AppTime"]
                matched_window = match_cascade_window_v2({"Timestamp": df.at[j, "Timestamp"], "lo_eventType": row.get("lo_eventType", "")}, cascade_windows)
                break

        if end_time:
            duration_sec = (end_time - start_time).total_seconds()

            walking_periods.append({
                "Timestamp": row["Timestamp"],
                "AppTime": start_app,
                "event_type": "WalkingPeriod",

                "cascade_id": matched_window.get("cascade_id") if matched_window else None,
                "hiMeta_eventType": matched_window.get("hiMeta_eventType") if matched_window else "unclassified",
                "hi_eventType": matched_window.get("hi_eventType") if matched_window else "WalkingPeriod",
                "med_eventType": "UnclassifiedNavigation",
                "lo_eventType": "UnclassifiedNavigation",

                "BlockNum": row.get("BlockNum"),
                "RoundNum": row.get("RoundNum"),
                "CoinSetID": row.get("CoinSetID"),
                "BlockType": row.get("BlockType", "unknown"),
                "BlockStatus": row.get("BlockStatus", "unknown"),
                "chestPin_num": row.get("chestPin_num"),

                "details": {"trigger": trigger_type},
                "source": "synthetic",

                "original_row_start": row.get("original_index", i),
                "original_row_end": j - 1,
                "duration": duration_sec,
                "start_AppTime": start_app,
                "end_AppTime": end_app,
                "start_Timestamp": start_time,
                "end_Timestamp": end_time,
            })

    return walking_periods

def generate_reward_walking_periods_v2a(df, events):
    df = df.set_index("original_index").copy()
    df["parsed_Timestamp"] = df["Timestamp"].apply(safe_parse_timestamp)
    walk_periods = []

    pin_drops = [e for e in events if e["lo_eventType"] == "PinDrop_Moment"]
    coin_collects = [e for e in events if e["lo_eventType"] == "CoinCollect_Moment_PinDrop"]

    pin_drops.sort(key=lambda e: e["AppTime"])
    coin_collects.sort(key=lambda e: e["AppTime"])

    for i, pin in enumerate(pin_drops):
        chest_pin = pin.get("chestPin_num")
        pin_idx = pin.get("original_row_start")
        if pin_idx is None:
            print(f"⚠️ Missing 'original_row_start' in pin: {pin}")
            continue

        feedback = next((c for c in coin_collects if c.get("chestPin_num") == chest_pin), None)
        if feedback:
            feedback_idx = feedback.get("original_row_start")
            if feedback_idx is None:
                print(f"⚠️ Missing 'original_row_start' in feedback: {feedback}")
                continue

            start_row = df.loc[pin_idx]
            end_row = df.loc[feedback_idx]

            duration_sec = (end_row["parsed_Timestamp"] - start_row["parsed_Timestamp"]).total_seconds()
            walk_periods.append({
                "AppTime": start_row["AppTime"],
                "Timestamp": start_row["Timestamp"],
                "lo_eventType": "Wait4Feedback",
                "med_eventType": "RewardDriven_Navigation",
                "hi_eventType": pin.get("hi_eventType"),
                "hiMeta_eventType": pin.get("hiMeta_eventType"),
                "source": "synthetic",
                "start_AppTime": start_row["AppTime"],
                "end_AppTime": end_row["AppTime"],
                "start_Timestamp": start_row["Timestamp"],
                "end_Timestamp": end_row["Timestamp"],
                "duration": duration_sec,
                "original_row_start": pin_idx,
                "original_row_end": feedback_idx,
                "BlockNum": pin.get("BlockNum"),
                "RoundNum": pin.get("RoundNum"),
                "CoinSetID": pin.get("CoinSetID"),
                "BlockStatus": pin.get("BlockStatus"),
                "BlockType": pin.get("BlockType", "unknown"),
                "chestPin_num": chest_pin,
                "cascade_id": pin.get("cascade_id"),
                "details": {"trigger": "Wait4Feedback"}
            })

        if i == 0:
            pre_idx = pin_idx - 1
        else:
            last_feedback_idx = coin_collects[i-1].get("original_row_start")
            pre_idx = last_feedback_idx + 1

        if pre_idx in df.index:
            start_row = df.loc[pre_idx]
            end_row = df.loc[pin_idx]
            duration_sec = (end_row["parsed_Timestamp"] - start_row["parsed_Timestamp"]).total_seconds()
            walk_periods.append({
                "AppTime": start_row["AppTime"],
                "Timestamp": start_row["Timestamp"],
                "lo_eventType": "Walk2PinDrop",
                "med_eventType": "RewardDriven_Navigation",
                "hi_eventType": pin.get("hi_eventType"),
                "hiMeta_eventType": pin.get("hiMeta_eventType"),
                "source": "synthetic",
                "start_AppTime": start_row["AppTime"],
                "end_AppTime": end_row["AppTime"],
                "start_Timestamp": start_row["Timestamp"],
                "end_Timestamp": end_row["Timestamp"],
                "duration": duration_sec,
                "original_row_start": pre_idx,
                "original_row_end": pin_idx,
                "BlockNum": pin.get("BlockNum"),
                "RoundNum": pin.get("RoundNum"),
                "CoinSetID": pin.get("CoinSetID"),
                "BlockStatus": pin.get("BlockStatus"),
                "BlockType": pin.get("BlockType", "unknown"),
                "chestPin_num": chest_pin,
                "cascade_id": pin.get("cascade_id"),
                "details": {"trigger": "Walk2PinDrop"}
            })

    return walk_periods

# Identify logged events with no hi_eventType within each hiMeta_eventType
def check_unassigned_logged_periods(df):
    try:
        unassigned = df[
            (df['source'] == 'logged') &
            ((df['hi_eventType'].isna()) | (df['hi_eventType'] == '')) &
            (~df['hiMeta_eventType'].isna())
        ]
        grouped = unassigned.groupby('hiMeta_eventType').size().reset_index(name='unassigned_count')
        #return grouped, unassigned[['AppTime', 'Timestamp', 'lo_eventType', 'med_eventType', 'details', 'hiMeta_eventType', 'original_row_start']]
        columns_to_return = ['AppTime', 'Timestamp', 'lo_eventType', 'med_eventType', 'details', 'hiMeta_eventType']
        if 'original_row_start' in unassigned.columns:
            columns_to_return.append('original_row_start')
        return grouped, unassigned[columns_to_return]
    except KeyError as e:
        print(f"⚠️ KeyError: {e} in event: {event}")
        raise

def synthesize_reward_driven_walking_periods_v3(df, reward_walks):
    synthesized = []
    df_sorted = pd.DataFrame(reward_walks).sort_values("original_row_start").reset_index(drop=True)

    pin_drops = df_sorted[df_sorted['lo_eventType'] == 'PinDrop_Moment']
    coin_collects = df_sorted[df_sorted['lo_eventType'] == 'CoinCollect_Moment_PinDrop']

    for i, pin_row in pin_drops.iterrows():
        cascade_id = pin_row.get('cascade_id')
        chest_pin_num = pin_row.get('chestPin_num')
        blocknum = pin_row.get('BlockNum', None)
        roundnum = pin_row.get('RoundNum', None)

        # Walk2PinDrop: look for the last coin collect before this pin drop
        prev_collect = coin_collects[
            (coin_collects['original_row_start'] < pin_row['original_row_start']) &
            (coin_collects['chestPin_num'] == chest_pin_num)
        ]
        if not prev_collect.empty:
            last_collect_row = prev_collect.iloc[-1]
            synthesized.append({
                "AppTime": last_collect_row["AppTime"],
                "Timestamp": last_collect_row["Timestamp"],
                "lo_eventType": "Walk2PinDrop",
                "med_eventType": "RewardDriven_Navigation",
                "hi_eventType": "PinDrop",
                "hiMeta_eventType": "BlockActivity",
                "event_type": "WalkingPeriod",
                "source": "synthetic",
                "cascade_id": cascade_id,
                "BlockNum": blocknum,
                "RoundNum": roundnum,
                "CoinSetID": pin_row.get("CoinSetID"),
                "BlockStatus": pin_row.get("BlockStatus", "unknown"),
                "BlockType": pin_row.get("BlockType", "unknown"),
                "chestPin_num": chest_pin_num,
                "original_row_start": last_collect_row["original_row_start"],
                "original_row_end": pin_row["original_row_start"] - 1,
                "start_AppTime": last_collect_row["AppTime"],
                "end_AppTime": pin_row["AppTime"],
                "duration": pin_row["AppTime"] - last_collect_row["AppTime"],
                "details": {"from": "CoinCollect_Moment_PinDrop", "to": "PinDrop_Moment"}
            })

        # Wait4Feedback: look for the next coin collect after this pin drop
        matching_collect = coin_collects[
            (coin_collects['original_row_start'] > pin_row['original_row_start']) &
            (coin_collects['chestPin_num'] == chest_pin_num)
        ]
        if not matching_collect.empty:
            next_collect_row = matching_collect.iloc[0]
            synthesized.append({
                "AppTime": pin_row["AppTime"],
                "Timestamp": pin_row["Timestamp"],
                "lo_eventType": "Wait4Feedback",
                "med_eventType": "RewardDriven_Navigation",
                "hi_eventType": "PinDrop",
                "hiMeta_eventType": "BlockActivity",
                "event_type": "WalkingPeriod",
                "source": "synthetic",
                "cascade_id": cascade_id,
                "BlockNum": blocknum,
                "RoundNum": roundnum,
                "CoinSetID": pin_row.get("CoinSetID"),
                "BlockStatus": pin_row.get("BlockStatus", "unknown"),
                "BlockType": pin_row.get("BlockType", "unknown"),
                "chestPin_num": chest_pin_num,
                "original_row_start": pin_row["original_row_start"],
                "original_row_end": next_collect_row["original_row_start"] - 1,
                "start_AppTime": pin_row["AppTime"],
                "end_AppTime": next_collect_row["AppTime"],
                "duration": next_collect_row["AppTime"] - pin_row["AppTime"],
                "details": {"from": "PinDrop_Moment", "to": "CoinCollect_Moment_PinDrop"}
            })
    return pd.DataFrame(reward_walks)
    #return pd.DataFrame(synthesized)

def refine_reward_walking_periods_v2(events):
    """
    Reclassify 'WalkingPeriod' events into either:
        - Walk2PinDrop / Walk2ChestOpen
        - Wait4Feedback (between drop and coin collection)
    based on high-resolution timing in the same cascade.

    Returns:
        List[dict]: Updated events with refined lo/med eventTypes
    """
    cascades = defaultdict(list)
    for e in events:
        cid = e.get("cascade_id")
        if cid is not None:
            cascades[cid].append(e)

    updated = []
    for event in events:
        if event.get("event_type") != "WalkingPeriod":
            updated.append(event)
            continue

        cid = event.get("cascade_id")
        if cid not in cascades:
            updated.append(event)
            continue

        cascade_events = cascades[cid]
        hi_type = event.get("hi_eventType")
        meta = event.get("hiMeta_eventType", "unknown")

        # Parse timestamps for accurate comparisons
        drop_time = None
        collect_time = None

        for e in cascade_events:
            lo = e.get("lo_eventType")
            ts = safe_parse_timestamp(e.get("Timestamp"))
            if lo in {"PinDrop_Moment", "ChestOpen_Moment"}:
                drop_time = ts
            elif lo in {"CoinCollect_Moment_PinDrop", "CoinCollect_Moment_Chest"}:
                collect_time = ts

        start_ts = safe_parse_timestamp(event.get("start_Timestamp"))
        end_ts = safe_parse_timestamp(event.get("end_Timestamp"))

        if drop_time and end_ts <= drop_time:
            event["lo_eventType"] = "Walk2PinDrop" if hi_type == "PinDrop" else "Walk2ChestOpen"
            event["med_eventType"] = "RewardDriven_Navigation"
        elif drop_time and collect_time and start_ts >= drop_time and end_ts <= collect_time:
            event["lo_eventType"] = "Wait4Feedback"
            event["med_eventType"] = "RewardDriven_Navigation"

        updated.append(event)

    return updated

def generate_reward_walking_periods_v2b(df, events):
    df = df.set_index("original_row_start").copy()
    df["parsed_Timestamp"] = df["Timestamp"].apply(safe_parse_timestamp)
    walk_periods = []

    reward_types = [
        {
            "anchor_lo": "PinDrop_Moment",
            "collect_lo": "CoinCollect_Moment_PinDrop",
            "hi_eventType": "PinDrop",
            "hiMeta_eventType": "BlockActivity",
            "walk_label": "Walk2PinDrop",
            "wait_label": "Wait4Feedback"
        },
        {
            "anchor_lo": "ChestOpen_Moment",
            "collect_lo": "CoinCollect_Moment_Chest",
            "hi_eventType": "ChestOpen",
            "hiMeta_eventType": "BlockActivity",
            "walk_label": "Walk2ChestOpen",
            "wait_label": "Wait4CoinVis"
        }
    ]

    for reward in reward_types:
        anchors = [e for e in events if e["lo_eventType"] == reward["anchor_lo"]]
        collects = [e for e in events if e["lo_eventType"] == reward["collect_lo"]]

        anchors.sort(key=lambda e: e["AppTime"])
        collects.sort(key=lambda e: e["AppTime"])

        for i, anchor in enumerate(anchors):
            chest_pin = anchor.get("chestPin_num")
            anchor_idx = anchor.get("original_row_start")
            if chest_pin is None or anchor_idx is None:
                continue

            feedback = next((c for c in collects if c.get("chestPin_num") == chest_pin), None)
            if feedback:
                feedback_idx = feedback.get("original_row_start")
                if feedback_idx is None:
                    continue

                start_row = df.loc[anchor_idx]
                end_row = df.loc[feedback_idx]
                duration_sec = (end_row["parsed_Timestamp"] - start_row["parsed_Timestamp"]).total_seconds()

                walk_periods.append({
                    "AppTime": start_row["AppTime"],
                    "Timestamp": start_row["Timestamp"],
                    "lo_eventType": reward["wait_label"],
                    "med_eventType": "RewardDriven_Navigation",
                    "hi_eventType": reward["hi_eventType"],
                    "hiMeta_eventType": reward["hiMeta_eventType"],
                    "source": "synthetic",
                    "start_AppTime": start_row["AppTime"],
                    "end_AppTime": end_row["AppTime"],
                    "start_Timestamp": start_row["Timestamp"],
                    "end_Timestamp": end_row["Timestamp"],
                    "duration": duration_sec,
                    "original_row_start": anchor_idx,
                    "original_row_end": feedback_idx,
                    "BlockNum": anchor.get("BlockNum"),
                    "RoundNum": anchor.get("RoundNum"),
                    "CoinSetID": anchor.get("CoinSetID"),
                    "BlockStatus": anchor.get("BlockStatus"),
                    "BlockType": anchor.get("BlockType", "unknown"),
                    "chestPin_num": chest_pin,
                    "details": {"trigger": reward["wait_label"]}
                })

            if i == 0:
                pre_idx = anchor_idx - 1
            else:
                last_feedback_idx = collects[i-1].get("original_row_start")
                pre_idx = last_feedback_idx + 1 if last_feedback_idx is not None else anchor_idx - 1

            if pre_idx in df.index:
                start_row = df.loc[pre_idx]
                end_row = df.loc[anchor_idx]
                duration_sec = (end_row["parsed_Timestamp"] - start_row["parsed_Timestamp"]).total_seconds()

                walk_periods.append({
                    "AppTime": start_row["AppTime"],
                    "Timestamp": start_row["Timestamp"],
                    "lo_eventType": reward["walk_label"],
                    "med_eventType": "RewardDriven_Navigation",
                    "hi_eventType": reward["hi_eventType"],
                    "hiMeta_eventType": reward["hiMeta_eventType"],
                    "source": "synthetic",
                    "start_AppTime": start_row["AppTime"],
                    "end_AppTime": end_row["AppTime"],
                    "start_Timestamp": start_row["Timestamp"],
                    "end_Timestamp": end_row["Timestamp"],
                    "duration": duration_sec,
                    "original_row_start": pre_idx,
                    "original_row_end": anchor_idx,
                    "BlockNum": anchor.get("BlockNum"),
                    "RoundNum": anchor.get("RoundNum"),
                    "CoinSetID": anchor.get("CoinSetID"),
                    "BlockStatus": anchor.get("BlockStatus"),
                    "BlockType": anchor.get("BlockType", "unknown"),
                    "chestPin_num": chest_pin,
                    "details": {"trigger": reward["walk_label"]}
                })

    return walk_periods

def generate_reward_walking_periods_v2(df, events):
    df = df.copy()
    df["parsed_Timestamp"] = df["Timestamp"].apply(safe_parse_timestamp)

    # Use original_index for lookup since df does not contain original_row_start
    df.set_index("original_index", inplace=True)

    walk_periods = []

    reward_types = [
        {
            "anchor_lo": "PinDrop_Moment",
            "collect_lo": "CoinCollect_Moment_PinDrop",
            "hi_eventType": "PinDrop",
            "hiMeta_eventType": "BlockActivity",
            "walk_label": "Walk2PinDrop",
            "wait_label": "Wait4Feedback"
        },
        {
            "anchor_lo": "ChestOpen_Moment",
            "collect_lo": "CoinCollect_Moment_Chest",
            "hi_eventType": "ChestOpen",
            "hiMeta_eventType": "BlockActivity",
            "walk_label": "Walk2ChestOpen",
            "wait_label": "Wait4CoinVis"
        }
    ]

    for reward in reward_types:
        anchors = [e for e in events if e["lo_eventType"] == reward["anchor_lo"]]
        collects = [e for e in events if e["lo_eventType"] == reward["collect_lo"]]

        anchors.sort(key=lambda e: e["AppTime"])
        collects.sort(key=lambda e: e["AppTime"])

        for i, anchor in enumerate(anchors):
            chest_pin = anchor.get("chestPin_num")
            anchor_idx = anchor.get("original_row_start")
            if chest_pin is None or anchor_idx is None:
                continue

            feedback = next((c for c in collects if c.get("chestPin_num") == chest_pin), None)
            if feedback:
                feedback_idx = feedback.get("original_row_start")
                if feedback_idx is None:
                    continue

                if anchor_idx not in df.index or feedback_idx not in df.index:
                    continue

                start_row = df.loc[anchor_idx]
                end_row = df.loc[feedback_idx]
                duration_sec = (end_row["parsed_Timestamp"] - start_row["parsed_Timestamp"]).total_seconds()

                walk_periods.append({
                    "AppTime": start_row["AppTime"],
                    "Timestamp": start_row["Timestamp"],
                    "lo_eventType": reward["wait_label"],
                    "med_eventType": "RewardDriven_Navigation",
                    "hi_eventType": reward["hi_eventType"],
                    "hiMeta_eventType": reward["hiMeta_eventType"],
                    "source": "synthetic",
                    "start_AppTime": start_row["AppTime"],
                    "end_AppTime": end_row["AppTime"],
                    "start_Timestamp": start_row["Timestamp"],
                    "end_Timestamp": end_row["Timestamp"],
                    "duration": duration_sec,
                    "original_row_start": anchor_idx,
                    "original_row_end": feedback_idx,
                    "BlockNum": anchor.get("BlockNum"),
                    "RoundNum": anchor.get("RoundNum"),
                    "CoinSetID": anchor.get("CoinSetID"),
                    "BlockStatus": anchor.get("BlockStatus"),
                    "BlockType": anchor.get("BlockType", "unknown"),
                    "chestPin_num": chest_pin,
                    "details": {"trigger": reward["wait_label"]}
                })

            if i == 0:
                pre_idx = anchor_idx - 1
            else:
                last_feedback_idx = collects[i-1].get("original_row_start")
                pre_idx = last_feedback_idx + 1 if last_feedback_idx is not None else anchor_idx - 1

            if pre_idx in df.index and anchor_idx in df.index:
                start_row = df.loc[pre_idx]
                end_row = df.loc[anchor_idx]
                duration_sec = (end_row["parsed_Timestamp"] - start_row["parsed_Timestamp"]).total_seconds()

                walk_periods.append({
                    "AppTime": start_row["AppTime"],
                    "Timestamp": start_row["Timestamp"],
                    "lo_eventType": reward["walk_label"],
                    "med_eventType": "RewardDriven_Navigation",
                    "hi_eventType": reward["hi_eventType"],
                    "hiMeta_eventType": reward["hiMeta_eventType"],
                    "source": "synthetic",
                    "start_AppTime": start_row["AppTime"],
                    "end_AppTime": end_row["AppTime"],
                    "start_Timestamp": start_row["Timestamp"],
                    "end_Timestamp": end_row["Timestamp"],
                    "duration": duration_sec,
                    "original_row_start": pre_idx,
                    "original_row_end": anchor_idx,
                    "BlockNum": anchor.get("BlockNum"),
                    "RoundNum": anchor.get("RoundNum"),
                    "CoinSetID": anchor.get("CoinSetID"),
                    "BlockStatus": anchor.get("BlockStatus"),
                    "BlockType": anchor.get("BlockType", "unknown"),
                    "chestPin_num": chest_pin,
                    "details": {"trigger": reward["walk_label"]}
                })

    return walk_periods

def build_common_event_fields(row, index=None):
    idx = index if index is not None else row.name

    #print(f"🔎 Assigning original_row_start from row: {row.to_dict()}")
    return {
        "BlockNum": row.get("BlockNum", None),
        "RoundNum": row.get("RoundNum", None),
        "CoinSetID": row.get("CoinSetID", None),
        "BlockStatus": row.get("BlockStatus", "unknown"),
        "BlockType": row.get("BlockType", "unknown"),
        "chestPin_num": row.get("chestPin_num", None),
        "original_row_start": row.get("original_index", idx),
        "original_row_end": row.get("original_index", idx),
        "cascade_id": None
    }

def generate_synthetic_events_v2(base_time, timestamp_str, timed_events, base_info, event_meta):
    synthetic_events = []
    try:
        base_timestamp = safe_parse_timestamp(timestamp_str)
        if base_timestamp is None:
            print(f"⚠️ base_timestamp is None for input: {timestamp_str} with base_info: {base_info}")
        for evt_name, offset, duration in timed_events:
            start_time = base_time + offset
            start_ts = (base_timestamp + timedelta(seconds=offset)).strftime('%H:%M:%S:%f') if base_timestamp else None
            end_time = start_time + duration if duration else None
            end_ts = (base_timestamp + timedelta(seconds=offset + duration)).strftime('%H:%M:%S:%f') if duration and base_timestamp else None

            synthetic_events.append({
                "AppTime": start_time,
                "Timestamp": start_ts,
                "start_AppTime": start_time,
                "end_AppTime": end_time,
                "start_Timestamp": start_ts,
                "end_Timestamp": end_ts,
                "lo_eventType": evt_name,
                "details": {},
                "source": "synthetic",
                "original_row_start": base_info.get("original_row_start", -1),
                "original_row_end": base_info.get("original_row_end", -1),
                **event_meta,
                **base_info
            })
    except Exception as e:
        print(f"⚠️ Failed to create synthetic event at {timestamp_str}: {e}")
    return synthetic_events

# def safe_parse_timestamp_v1(ts):
#     try:
#         return datetime.strptime(ts, '%H:%M:%S:%f')
#     except Exception:
#         return None

# def safe_parse_timestamp(ts):
#     try:
#         # Normalize timestamp to have 6 digits of microseconds
#         if isinstance(ts, str) and ts.count(':') == 3:
#             parts = ts.split(':')
#             if len(parts) == 4:
#                 hh, mm, ss, ms = parts
#                 ms = (ms + '000')[:6]  # pad or truncate to 6 digits
#                 new_ts = f"{hh}:{mm}:{ss}:{ms}"
#                 return datetime.strptime(new_ts, '%H:%M:%S:%f')
#         return datetime.strptime(ts, '%H:%M:%S:%f')
#     except Exception as e:
#         print(f"⚠️ Failed to parse timestamp '{ts}': {e}")
#         return None





# Re-import necessary libraries and redefine backfill function after reset
import pandas as pd
from datetime import datetime

def safe_parse_timestamp(ts):
    try:
        if isinstance(ts, str) and ts.count(':') == 3:
            hh, mm, ss, ms = ts.split(':')
            ms = (ms + '000')[:6]
            return datetime.strptime(f"{hh}:{mm}:{ss}:{ms}", "%H:%M:%S:%f")
        return datetime.strptime(ts, "%H:%M:%S:%f")
    except Exception:
        return None

def backfill_approx_row_indices(events, df):
    """
    For each synthetic event, assign `original_row_start` and `original_row_end` by matching
    closest preceding row in the original DataFrame based on AppTime or Timestamp.
    """
    df = df.reset_index().copy()
    df["parsed_Timestamp"] = df["Timestamp"].apply(safe_parse_timestamp)
    df_sorted = df.sort_values("AppTime").reset_index(drop=True)

    for event in events:
        if event.get("source") != "synthetic":
            continue

        app_time = event.get("AppTime")
        if app_time is not None:
            candidates = df_sorted[df_sorted["AppTime"] <= app_time]
            if not candidates.empty:
                matched_row = candidates.iloc[-1]
                event["original_row_start"] = matched_row["index"]
                event["original_row_end"] = matched_row["index"]
            continue

        ts = safe_parse_timestamp(event.get("Timestamp"))
        if ts is not None:
            candidates = df_sorted[df_sorted["parsed_Timestamp"] <= ts]
            if not candidates.empty:
                matched_row = candidates.iloc[-1]
                event["original_row_start"] = matched_row["index"]
                event["original_row_end"] = matched_row["index"]

    return events


def build_common_event_fields(row, index=None):
    idx = index if index is not None else row.name

    #print(f"🔎 Assigning original_row_start from row: {row.to_dict()}")
    return {
        "BlockNum": row.get("BlockNum", None),
        "RoundNum": row.get("RoundNum", None),
        "CoinSetID": row.get("CoinSetID", None),
        "BlockStatus": row.get("BlockStatus", "unknown"),
        "BlockType": row.get("BlockType", "unknown"),
        "chestPin_num": row.get("chestPin_num", None),
        "original_row_start": row.get("original_index", idx),
        "original_row_end": row.get("original_index", idx),
        "cascade_id": None
    }

def generate_synthetic_events_v2(base_time, timestamp_str, timed_events, base_info, event_meta):
    synthetic_events = []
    try:
        base_timestamp = safe_parse_timestamp(timestamp_str)
        if base_timestamp is None:
            print(f"⚠️ base_timestamp is None for input: {timestamp_str} with base_info: {base_info}")
        for evt_name, offset, duration in timed_events:
            start_time = base_time + offset
            start_ts = (base_timestamp + timedelta(seconds=offset)).strftime('%H:%M:%S:%f') if base_timestamp else None
            end_time = start_time + duration if duration else None
            end_ts = (base_timestamp + timedelta(seconds=offset + duration)).strftime('%H:%M:%S:%f') if duration and base_timestamp else None

            synthetic_events.append({
                "AppTime": start_time,
                "Timestamp": start_ts,
                "start_AppTime": start_time,
                "end_AppTime": end_time,
                "start_Timestamp": start_ts,
                "end_Timestamp": end_ts,
                "lo_eventType": evt_name,
                "details": {},
                "source": "synthetic",
                "original_row_start": base_info.get("original_row_start", -1),
                "original_row_end": base_info.get("original_row_end", -1),
                **event_meta,
                **base_info
            })
    except Exception as e:
        print(f"⚠️ Failed to create synthetic event at {timestamp_str}: {e}")
    return synthetic_events
