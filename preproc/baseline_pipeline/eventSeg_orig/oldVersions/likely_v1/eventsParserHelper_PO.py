# eventsParserHelper.py
import re
import os
from datetime import datetime, timedelta
from io import StringIO
import traceback

# Bare Bones Events Handling 

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
        "original_row_end": row.get("original_index", idx)
        #"cascade_id": None
    }

#def generate_synthetic_events_v2(base_time, timestamp_str, alignTimestamp, timed_events, base_info, event_meta):
def generate_synthetic_events_v2(base_time, timestamp_str, timed_events, base_info, event_meta):
    synthetic_events = []
    try:
        base_timestamp = safe_parse_timestamp(timestamp_str)
        #base_atimestamp = safe_parse_timestamp(alignTimestamp)
        if base_timestamp is None:
            print(f"⚠️ base_timestamp is None for input: {timestamp_str} with base_info: {base_info}")
        for evt_name, offset, duration in timed_events:
            start_time = base_time + offset
            start_ts_dt = base_timestamp + timedelta(seconds=offset)
            start_ts = start_ts_dt.time().strftime('%H:%M:%S:%f') if base_timestamp else None
            #astart_ts_dt = safe_parse_timestamp(alignTimestamp)
            #astart_ts = astart_ts_dt.time().strftime('%H:%M:%S:%f') if astart_ts_dt else None
            #start_ts = (base_timestamp + timedelta(seconds=offset)).strftime('%H:%M:%S:%f') if base_timestamp else None
            end_time = start_time + duration if duration else None
            end_ts_dt = base_timestamp + timedelta(seconds=offset + duration)
            end_ts = start_ts_dt.time().strftime('%H:%M:%S:%f') if base_timestamp else None
            
            #aend_ts_dt = base_atimestamp + timedelta(seconds=offset + duration)
            #aend_ts = astart_ts_dt.time().strftime('%H:%M:%S:%f') if base_atimestamp else None
            #end_ts = (base_timestamp + timedelta(seconds=offset + duration)).strftime('%H:%M:%S:%f') if duration and base_timestamp else None

            synthetic_events.append({
                "AppTime": start_time,
                "Timestamp": start_ts,
                #"AlignedTimestamp": astart_ts,
                "start_AppTime": start_time,
                "end_AppTime": end_time,
                "start_Timestamp": start_ts,
                "end_Timestamp": end_ts,
                #"start_AlignedTimestamp": astart_ts,
                #"end_AlignedTimestamp": aend_ts,
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


def build_segment_event(start_row, end_row, event_type):
    return {
        "AppTime": start_row["AppTime"],
        "Timestamp": start_row["Timestamp"],
        #"AlignedTimestamp": start_row.get("AdjustedTimestamp", None),
        "start_AppTime": start_row["AppTime"],
        "end_AppTime": end_row["AppTime"],
        "start_Timestamp": start_row["Timestamp"],
        "end_Timestamp": end_row["Timestamp"],
        #"start_AlignedTimestamp": start_row.get("AdjustedTimestamp", None),
        #"end_AlignedTimestamp": end_row.get("AdjustedTimestamp", None),
        "lo_eventType": event_type,
        "med_eventType": f"{event_type}_Transition",
        "hi_eventType": "BlockStructure",
        "hiMeta_eventType": "Infrastructure",
        "source": "synthetic",
        "BlockNum": start_row.get("BlockNum"),
        "RoundNum": start_row.get("RoundNum"),
        "CoinSetID": start_row.get("CoinSetID"),
        "BlockStatus": start_row.get("BlockStatus"),
        "BlockType": start_row.get("BlockType"),
        "chestPin_num": start_row.get("chestPin_num"),
        "original_row_start": start_row.name,
        "original_row_end": end_row.name
    }

def process_marks(df, allowed_statuses, role, cascade_windows=None):
    if role not in ("AN", "PO"):
        raise ValueError(f"Invalid role '{role}'. Expected 'AN' or 'PO'.")

    events = []
    for idx, row in df.iterrows():
        if isinstance(row.Message, str) and "Sending Headset mark" in row.Message:
            common_info = build_common_event_fields(row, idx)
            matched = match_cascade_window(row, cascade_windows) if cascade_windows else None

            start_time = row["AppTime"]
            timestamp = row["Timestamp"]

            start_ts_dt = safe_parse_timestamp(timestamp)
            start_ts = start_ts_dt.time().strftime('%H:%M:%S:%f') if start_ts_dt else None

            #atimestamp = row["AdjustedTimestamp"]

            #astart_ts_dt = safe_parse_timestamp(atimestamp)
            #astart_ts = astart_ts_dt.time().strftime('%H:%M:%S:%f') if astart_ts_dt else None

            if start_ts is None:
                print(f"⚠️ base_timestamp is None for input: {timestamp} with base_info: {common_info}")

            details = {"mark": "A"} if role == "AN" else {"mark": "B"}

            events.append({
                "AppTime": start_time,
                "Timestamp": start_ts,
                #"AlignedTimestamp": astart_ts,
                "start_AppTime": start_time,
                "end_AppTime": start_time,
                "start_Timestamp": start_ts,
                "end_Timestamp": start_ts,
                #"start_AlignedTimestamp": astart_ts,
                #"end_AlignedTimestamp": astart_ts,
                "lo_eventType": "Mark",
                "med_eventType": "ReferencePoint",
                "hi_eventType": "SystemEvent",
                "hiMeta_eventType": "Infrastructure",
                "details": details,
                "source": "logged",
                **common_info
            })

    return events


def process_true_round_segments(df, allowed_statuses):
    events = []
    df = df.reset_index(drop=True)
    
    excluded_rounds = {0, 7777, 8888, 9999}
    prev_round = None
    round_start_idx = None

    for idx, row in df.iterrows():
        block_status = row.get("BlockStatus", "unknown")
        if block_status not in allowed_statuses:
            continue

        curr_round = row.get("RoundNum")
        if curr_round in excluded_rounds:
            continue

        # On round change, emit RoundEnd for previous and RoundStart for current
        if curr_round != prev_round:
            if prev_round is not None and round_start_idx is not None:
                end_row = df.iloc[idx - 1]
                start_row = df.iloc[round_start_idx]
                events.append(build_segment_event(start_row, start_row, "RoundStart"))
                events.append(build_segment_event(end_row, end_row, "RoundEnd"))

            round_start_idx = idx
            prev_round = curr_round

    # Emit final round's start and end if still pending
    if round_start_idx is not None and prev_round is not None:
        start_row = df.iloc[round_start_idx]
        end_row = df.iloc[-1]
        events.append(build_segment_event(start_row, start_row, "RoundStart"))
        events.append(build_segment_event(end_row, end_row, "RoundEnd"))

    return events

def process_special_round_segments(df, allowed_statuses):
    """
    Scans the DataFrame row-by-row to find uninterrupted spans of special RoundNums
    [0, 7777, 8888, 9999] and emits a single synthetic event for each span.
    """
    special_round_map = {
        0: ("PreBlock_CylinderWalk", "PreBlockActivity"),
        7777: ("InterRound_CylinderWalk", "BlockActivity"),
        8888: ("InterRound_PostCylinderWalk", "BlockActivity"),
        9999: ("InterBlock_Idle", "PostBlockActivity")
    }

    special_rounds = set(special_round_map.keys())
    events = []
    current_segment = []

    for idx, row in df.iterrows():
        block_status = row.get("BlockStatus", "unknown")
        round_num = row.get("RoundNum")

        if round_num in special_rounds and block_status in allowed_statuses:
            if not current_segment or current_segment[-1][1].get("RoundNum") == round_num:
                current_segment.append((idx, row))
            else:
                # Segment ended, process it
                start_i = current_segment[0][0]
                end_i = current_segment[-1][0]
                first_row = current_segment[0][1]
                last_row = current_segment[-1][1]
                lo_event, hi_meta = special_round_map[first_row["RoundNum"]]
                events.append({
                    "AppTime": first_row["AppTime"],
                    "Timestamp": first_row["Timestamp"],
                    #"AlignedTimestamp": first_row.get("AdjustedTimestamp", None),
                    "start_AppTime": first_row["AppTime"],
                    "end_AppTime": last_row["AppTime"],
                    "start_Timestamp": first_row["Timestamp"],
                    "end_Timestamp": last_row["Timestamp"],
                    #"start_AlignedTimestamp": first_row.get("AdjustedTimestamp", None),
                    #"end_AlignedTimestamp": last_row.get("AdjustedTimestamp", None),
                    "lo_eventType": f"{lo_event}_segment",
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod",
                    "hiMeta_eventType": hi_meta,
                    "source": "synthetic",
                    "BlockNum": first_row.get("BlockNum"),
                    "RoundNum": first_row.get("RoundNum"),
                    "CoinSetID": first_row.get("CoinSetID"),
                    "BlockStatus": first_row.get("BlockStatus"),
                    "BlockType": first_row.get("BlockType"),
                    "chestPin_num": first_row.get("chestPin_num"),
                    "original_row_start": start_i,
                    "original_row_end": end_i
                })
                current_segment = [(idx, row)]
        else:
            if current_segment:
                # Segment ended, process it
                start_i = current_segment[0][0]
                end_i = current_segment[-1][0]
                first_row = current_segment[0][1]
                last_row = current_segment[-1][1]
                lo_event, hi_meta = special_round_map[first_row["RoundNum"]]
                events.append({
                    "AppTime": first_row["AppTime"],
                    "Timestamp": first_row["Timestamp"],
                    #"AlignedTimestamp": first_row.get("AdjustedTimestamp", None),
                    "start_AppTime": first_row["AppTime"],
                    "end_AppTime": last_row["AppTime"],
                    "start_Timestamp": first_row["Timestamp"],
                    "end_Timestamp": last_row["Timestamp"],
                    #"start_AlignedTimestamp": first_row.get("AdjustedTimestamp", None),
                    #"end_AlignedTimestamp": last_row.get("AdjustedTimestamp", None),
                    "lo_eventType": f"{lo_event}_segment",
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod",
                    "hiMeta_eventType": hi_meta,
                    "source": "synthetic",
                    "BlockNum": first_row.get("BlockNum"),
                    "RoundNum": first_row.get("RoundNum"),
                    "CoinSetID": first_row.get("CoinSetID"),
                    "BlockStatus": first_row.get("BlockStatus"),
                    "BlockType": first_row.get("BlockType"),
                    "chestPin_num": first_row.get("chestPin_num"),
                    "original_row_start": start_i,
                    "original_row_end": end_i
                })
                current_segment = []

    # Final flush
    if current_segment:
        start_i = current_segment[0][0]
        end_i = current_segment[-1][0]
        first_row = current_segment[0][1]
        last_row = current_segment[-1][1]
        lo_event, hi_meta = special_round_map[first_row["RoundNum"]]
        events.append({
            "AppTime": first_row["AppTime"],
            "Timestamp": first_row["Timestamp"],
            #"AlignedTimestamp": first_row.get("AdjustedTimestamp", None),
            "start_AppTime": first_row["AppTime"],
            "end_AppTime": last_row["AppTime"],
            "start_Timestamp": first_row["Timestamp"],
            "end_Timestamp": last_row["Timestamp"],
            #"start_AlignedTimestamp": first_row.get("AdjustedTimestamp", None),
            #"end_AlignedTimestamp": last_row.get("AdjustedTimestamp", None),
            "lo_eventType": f"{lo_event}_segment",
            "med_eventType": "NonRewardDrivenNavigation",
            "hi_eventType": "WalkingPeriod",
            "hiMeta_eventType": hi_meta,
            "source": "synthetic",
            "BlockNum": first_row.get("BlockNum"),
            "RoundNum": first_row.get("RoundNum"),
            "CoinSetID": first_row.get("CoinSetID"),
            "BlockStatus": first_row.get("BlockStatus"),
            "BlockType": first_row.get("BlockType"),
            "chestPin_num": first_row.get("chestPin_num"),
            "original_row_start": start_i,
            "original_row_end": end_i
        })

    return events

def process_block_segments(df, allowed_statuses):
    events = []
    df = df.reset_index(drop=True)
    
    prev_block = None
    block_start_idx = None

    for idx, row in df.iterrows():
        block_status = row.get("BlockStatus", "unknown")
        if block_status not in allowed_statuses:
            continue

        curr_block = row.get("BlockNum")

        # On block change, emit BlockEnd for previous block and BlockStart for current
        if curr_block != prev_block:
            if prev_block is not None and block_start_idx is not None:
                end_row = df.iloc[idx - 1]
                start_row = df.iloc[block_start_idx]
                events.append(build_segment_event(start_row, start_row, "BlockStart"))
                events.append(build_segment_event(end_row, end_row, "BlockEnd"))

            block_start_idx = idx
            prev_block = curr_block

    # Emit final block's start and end if still pending
    if block_start_idx is not None and prev_block is not None:
        start_row = df.iloc[block_start_idx]
        end_row = df.iloc[-1]
        events.append(build_segment_event(start_row, start_row, "BlockStart"))
        events.append(build_segment_event(end_row, end_row, "BlockEnd"))

    return events

def process_block_periods_v4(df, allowed_statuses):
    events = []
    df = df.reset_index(drop=True)

    round_event_map = {
        0: ("PreBlock_CylinderWalk", "PreBlockActivity"),
        7777: ("InterRound_CylinderWalk", "BlockActivity"),
        8888: ("InterRound_PostCylinderWalk", "BlockActivity"),
        9999: ("InterBlock_Idle", "PostBlockActivity")
    }

    grouped = df.groupby("RoundNum")

    for round_code, (lo_event, hi_meta) in round_event_map.items():
        if round_code not in grouped.groups:
            continue

        rows = df.loc[grouped.groups[round_code]]
        start_row = rows.iloc[0]
        end_row = rows.iloc[-1]

        start_time = start_row.AppTime
        end_time = end_row.AppTime
        start_ts = start_row.Timestamp
        end_ts = end_row.Timestamp
        #astart_ts = start_row.AdjustedTimestamp
        #aend_ts = end_row.AdjustedTimestamp
        duration = end_time - start_time

        common_info = build_common_event_fields(start_row, start_row.name)
        common_info.update({
            "start_AppTime": start_time,
            "end_AppTime": end_time,
            "start_Timestamp": start_ts,
            "end_Timestamp": end_ts
            #"start_AlignedTimestamp": astart_ts,
            #"end_AlignedTimestamp": aend_ts,
        })

        synthetic = generate_synthetic_events_v2(
            start_time,
            start_ts,
            [
                (f"{lo_event}_start", 0.0, 0.0),
                (f"{lo_event}_end", duration, 0.0)
            ],
            common_info,
            {
                "med_eventType": "NonRewardDrivenNavigation",
                "hi_eventType": "WalkingPeriod",
                "hiMeta_eventType": hi_meta
            }
        )
        events.extend(synthetic)

    return events

def process_TrueBlocks(df, allowed_statuses, cascade_windows=None):

    events = []
    for idx, row in df.iterrows():
        if isinstance(row.Message, str) and (
            row.Message.startswith("Started collecting.") or
            row.Message.startswith("Started pindropping.") or
            row.Message.startswith("Started watching other participant's collecting.") or
            row.Message.startswith("Started watching other participant's pin dropping.")
        ):
            common_info = build_common_event_fields(row, idx)
            matched = match_cascade_window(row, cascade_windows) if cascade_windows else None

            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            #alignTimestamp = row["AlignedTimestamp"]
            #alignTimestamp = row["AdjustedTimestamp"]
            start_ts_dt = safe_parse_timestamp(timestamp)
            start_ts = start_ts_dt.time().strftime('%H:%M:%S:%f') if start_ts_dt else None
            #astart_ts_dt = safe_parse_timestamp(alignTimestamp)
            #astart_ts = astart_ts_dt.time().strftime('%H:%M:%S:%f') if astart_ts_dt else None
            if start_ts is None:
                print(f"⚠️ base_timestamp is None for input: {timestamp} with base_info: {common_info}")

            block_start_event = {
                **common_info,
                "AppTime": start_time,
                "Timestamp": start_ts,
                #"AlignedTimestamp": astart_ts,
                "start_AppTime": start_time,
                "end_AppTime": start_time,
                "start_Timestamp": start_ts,
                "end_Timestamp": start_ts,
                #"start_AlignedTimestamp": astart_ts,
                #"end_AlignedTimestamp": astart_ts,
                "lo_eventType": "TrueBlockStart",
                "med_eventType": "ReferencePoint",
                "hi_eventType": "SystemEvent",
                "hiMeta_eventType": "Infrastructure",
                "details": {},
                "source": "logged",
            }
            events.append(block_start_event)


        elif isinstance(row.Message, str) and "finished current task" in row.Message:
            common_info = build_common_event_fields(row, idx)
            matched = match_cascade_window(row, cascade_windows) if cascade_windows else None

            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            #alignTimestamp = row["AdjustedTimestamp"]
            start_ts_dt = safe_parse_timestamp(timestamp)
            start_ts = start_ts_dt.time().strftime('%H:%M:%S:%f') if start_ts_dt else None
            #astart_ts_dt = safe_parse_timestamp(alignTimestamp)
            #astart_ts = astart_ts_dt.time().strftime('%H:%M:%S:%f') if astart_ts_dt else None
            if start_ts is None:
                print(f"⚠️ base_timestamp is None for input: {timestamp} with base_info: {common_info}")

            #details = {"mark": "A"} if role == "AN" else {"mark": "B"}

            events.append({
                "AppTime": start_time,
                "Timestamp": start_ts,
                #"AlignedTimestamp": astart_ts,
                "start_AppTime": start_time,
                "end_AppTime": start_time,
                "start_Timestamp": start_ts,
                "end_Timestamp": start_ts,
                #"start_AlignedTimestamp": astart_ts,
                #"end_AlignedTimestamp": astart_ts,
                "lo_eventType": "TrueBlockEnd",
                "med_eventType": "ReferencePoint",
                "hi_eventType": "SystemEvent",
                "hiMeta_eventType": "Infrastructure",
                "details": {},
                "source": "logged",
                **common_info
            })

    return events

def add_elapsed_time_columns(df):
    """
    Adds SessionElapsedTime, BlockElapsedTime, and RoundElapsedTime columns.
    """
    if 'Timestamp' not in df.columns:
        print("⚠️ Timestamp column missing, skipping elapsed time calculations.")
        return df

    df['ParsedTimestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    df = df[df['ParsedTimestamp'].notna()].copy()
    df.sort_values(by='ParsedTimestamp', inplace=True)
    df.reset_index(drop=True, inplace=True)

    session_start_time = df['ParsedTimestamp'].iloc[0]
    df['SessionElapsedTime'] = (df['ParsedTimestamp'] - session_start_time).dt.total_seconds()

    if 'BlockNum' in df.columns:
        df['BlockElapsedTime'] = df.groupby('BlockNum')['ParsedTimestamp'].transform(lambda x: (x - x.iloc[0]).dt.total_seconds())
    else:
        df['BlockElapsedTime'] = pd.NA

    if 'RoundNum' in df.columns:
        df['RoundElapsedTime'] = df.groupby('RoundNum')['ParsedTimestamp'].transform(lambda x: (x - x.iloc[0]).dt.total_seconds())
    else:
        df['RoundElapsedTime'] = pd.NA

    df.drop(columns=['ParsedTimestamp'], inplace=True)
    return df
