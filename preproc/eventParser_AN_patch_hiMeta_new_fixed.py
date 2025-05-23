# eventParser_AN_patch_hiMeta.py

import pandas as pd
import re
import os
from datetime import datetime, timedelta
from io import StringIO
import traceback

# from revised_cascade_windows_utils_new import (find_cascade_windows_from_events_v4,
#                                     match_cascade_window_v3,
#                                     # assign_cascade_id, 
#                                     extract_walking_periods_with_cascade_ids_v3, 
#                                     generate_reward_walking_periods_v2,
#                                     synthesize_reward_driven_walking_periods_v3,
#                                     refine_reward_walking_periods_v2,
#                                     build_common_event_fields,
#                                     generate_synthetic_events_v2,
#                                     safe_parse_timestamp,
#                                     backfill_approx_row_indices)

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

###########################

def process_swap_votes_v4(df, allowed_statuses):
    events = []
    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str):
            match = re.match(r"Active Navigator says it was an?\s+(.*)\.", row["Message"])
            if match:
                swapvote = match.group(1).strip().upper()
                try:
                    start_time = row["AppTime"]
                    timestamp = row["Timestamp"]

                    base_timestamp = safe_parse_timestamp(start_time)

                    common_info = build_common_event_fields(row, i)

                    events.append({
                        "AppTime": start_time,
                        "Timestamp": start_ts,
                        "start_AppTime": start_time,
                        "end_AppTime": start_time,
                        "start_Timestamp": start_ts,
                        "end_Timestamp": start_ts,
                        "lo_eventType": "SwapVoteMoment",
                        "med_eventType": "SwapVote",
                        "hi_eventType": "SwapVote",
                        "hiMeta_eventType": "SwapVote",
                        "details": {"SwapVote": swapvote},
                        "source": "logged",
                        **common_info
                    })

                    # offsets_events = [
                    #     (0.000, "SwapVoteText_end"),
                    #     (0.000, "BlockScoreText_start"),
                    #     (2.000, "BlockScoreText_end")
                    # ]

                    offsets_events = [
                        ("SwapVoteText_end", 0.000, 0.000),
                        ("BlockScoreText_start", 0.000, 2.000)
                    ]

                    # how to plan for marking the beginning of the swapVoteText_start window
                    event_meta = {
                        "med_eventType": "FullPostSwapVoteEvents",
                        "hi_eventType": "SwapVote",
                        "hiMeta_eventType": "SwapVote"
                    }
                    synthetic = generate_synthetic_events_v2(start_time, timestamp, offsets_events, common_info, event_meta)
                    events.extend(synthetic)

                except Exception as e:
                    print(f"⚠️ Failed to process swap vote at row {i}: {e}")

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
        duration = end_time - start_time

        common_info = build_common_event_fields(start_row, start_row.name)
        common_info.update({
            "start_AppTime": start_time,
            "end_AppTime": end_time,
            "start_Timestamp": start_ts,
            "end_Timestamp": end_ts
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

def process_marks_v2(df, allowed_statuses, cascade_windows=None):
    events = []
    for idx, row in df.iterrows():
        if isinstance(row.Message, str) and "Sending Headset mark" in row.Message:
            common_info = build_common_event_fields(row, idx)
            matched = match_cascade_window(row, cascade_windows) if cascade_windows else None


            start_time = row["AppTime"]
            timestamp = row["Timestamp"]

            base_timestamp = safe_parse_timestamp(timestamp_str)
            if base_timestamp is None:
                print(f"⚠️ base_timestamp is None for input: {timestamp_str} with base_info: {base_info}")
            for evt_name, offset, duration in timed_events:
                start_time = base_time + offset
                start_ts = (base_timestamp + timedelta(seconds=offset)).strftime('%H:%M:%S:%f') if base_timestamp else None

            events.append({
                "AppTime": start_time,
                "Timestamp": start_ts,
                "start_AppTime": start_time,
                "end_AppTime": start_time,
                "start_Timestamp": start_ts,
                "end_Timestamp": start_ts,
                "lo_eventType": "Mark",
                "med_eventType": "ReferencePoint",
                "hi_eventType": "SystemEvent",
                "hiMeta_eventType": "Infrastructure",
                "details": {"mark": "A"},
                "source": "logged",
                **common_info
            })
    return events

# Myra Patched
# -- Pin Dropping Events

def process_pin_drop_v5(df,allowed_statuses):
    events = []
    i = 0

    while i < len(df):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        # ✅ Skip if block is not marked complete
        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            i += 1
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and "Just dropped a pin" in row["Message"]:
            try:
                common_info = build_common_event_fields(row, i)

                start_time = row["AppTime"]
                timestamp = row["Timestamp"]

                base_timestamp = safe_parse_timestamp(timestamp_str)
                if base_timestamp is None:
                    print(f"⚠️ base_timestamp is None for input: {timestamp_str} with base_info: {base_info}")
                for evt_name, offset, duration in timed_events:
                    start_time = base_time + offset
                    start_ts = (base_timestamp + timedelta(seconds=offset)).strftime('%H:%M:%S:%f') if base_timestamp else None

                event = {
                    "AppTime": start_time,
                    "Timestamp": start_ts,
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_Timestamp": start_ts,
                    "end_Timestamp": start_ts,
                    "lo_eventType": "PinDrop_Moment",
                    "med_eventType": "PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                    "details": {},
                    "source": "logged",
                    **common_info
                }
                # events.append(event)
                #     "Timestamp": start_ts,
                #     "start_AppTime": start_time,
                #     "end_AppTime": start_time,
                #     "start_Timestamp": start_ts,
                #     "end_Timestamp": start_ts,
                #     "lo_eventType": "PinDrop_Moment",
                #     "med_eventType": "PinDrop",
                #     "hi_eventType": "PinDrop",
                #     "hiMeta_eventType": "BlockActivity",
                #     "details": {},
                #     "source": "logged",
                #     **common_info
                # })

                # --- Parsing Pin Drop Information with Regex --- 
                
                j = i + 1
                # j = i + 1 Loop: purpose is to gather messages tied to the current pin drop.
                while j < len(df):
                    next_row = df.iloc[j]
                    if next_row["Type"] != "Event" or not isinstance(next_row["Message"], str):
                        break
                    msg = next_row["Message"]
                    
                    # --- Pin Drop Location --- 
                    # Example line: "Dropped a new pin at 1.311 -1.517 -1.755 localpos: -0.350 0.000 -5.840"
                    
                    if "Dropped a new pin at" in msg:
                        match = re.search(
                            # r'at ([\d\.\-]+) ([\d\.\-]+) ([\d\.\-]+) localpos: ([\d\.\-]+) ([\d\.\-]+) ([\d\.\-]+)', # apparently this line is less robust & more brittle for future scripts
                            # apparently the line below is a lot more defensive coding than the previous line 
                            r'at\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)\s+localpos:\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)', 

                            msg
                        )
                        if match:
                            try:
                                event["details"].update({
                                    "pinLocal_x": float(match.group(4)),
                                    "pinLocal_y": float(match.group(5)),
                                    "pinLocal_z": float(match.group(6)),
                                })
                            except ValueError:
                                print(f"⚠️ Float conversion failed in pin location at row {j}: {msg}")
                        else:
                            print(f"⚠️ Regex mismatch for pin location at row {j}: {msg}")

                    elif "Closest location was" in msg:
                        match = re.search(
                            # r"Closest location was:\s*([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s*\|\s*actual distance:\s*([-\d.]+)\s*\|\s*(good|bad) drop\s*\|\s*coinValue:\s*([-\d.]+)",
                            # apparently the line below is a lot more defensive coding than the previous line 
                            r"Closest location was:\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)\s*\|\s*actual distance:\s+([-+]?[0-9]*\.?[0-9]+)\s*\|\s*(good|bad) drop\s*\|\s*coinValue:\s+([-+]?[0-9]*\.?[0-9]+)",
                            msg
                        )
                        if match:
                            try:
                                event["details"].update({
                                    "coinPos_x": float(match.group(1)),
                                    "coinPos_y": float(match.group(2)),
                                    "coinPos_z": float(match.group(3)),
                                    "dropDist": float(match.group(4)),
                                    "dropQual": match.group(5),
                                    "coinValue": float(match.group(6)),
                                })
                            except ValueError:
                                print(f"⚠️ Drop analysis parsing error at row {j}: {msg}")
                        else:
                            print(f"⚠️ Regex mismatch in drop analysis at row {j}: {msg}")
                    # --- Current Round Number, Current Perfect Round Number, Running Round Total, Running Grand Total ---
                    # Example line: "Dropped a bad pin|0|0|0.00|0.00"
                    
                    elif "Dropped a good pin" in msg or "Dropped a bad pin" in msg:
                        parts = msg.split("|")
                        if len(parts) == 5:
                            try:
                                event["details"].update({
                                    "currRoundNum": int(parts[1]),
                                    "currPerfRoundNum": int(parts[2]),
                                    "runningBlockTotal": float(parts[3]),
                                    "currGrandTotal": float(parts[4]),
                                })
                            except ValueError:
                                print(f"⚠️ Score part conversion failed at row {j}: {msg}")
                        else:
                            print(f"⚠️ Unexpected parts format in score line at row {j}: {msg}")

                    j += 1

                events.append(event)

                # --- Pin Drop Synthetic Events ---
                # What happens immediately after the triggering line "Just dropped a pin"
                
                offsets_events = [
                    ("PinDropSound", 0.000, 0.403),
                    ("GrayPinVis", 0.000, 2.000),
                    ("Feedback_Sound", 2.000, 0.182),
                    ("FeedbackTextVis", 2.000, 1.000),
                    ("FeedbackPinColor", 2.000, 1.000),
                    ("CoinVis_start", 3.000, 0.000),
                    ("CoinPresentSound", 3.000, 0.650),
                    ("CoinLocked", 3.000, 1.000)
                    ]
                    
                event_meta = {
                    "med_eventType": "PinDrop_Animation",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity"
                    }

                synthetic = generate_synthetic_events_v2(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process pin drop at row {i}: {e}")

            i = j
        else:
            i += 1

    return events

def process_feedback_collect_v5(df, allowed_statuses):
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Collected feedback coin:"):
            common_info = build_common_event_fields(row, i)
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]

            base_timestamp = safe_parse_timestamp(timestamp_str)
            if base_timestamp is None:
                print(f"⚠️ base_timestamp is None for input: {timestamp_str} with base_info: {base_info}")
            for evt_name, offset, duration in timed_events:
                start_time = base_time + offset
                start_ts = (base_timestamp + timedelta(seconds=offset)).strftime('%H:%M:%S:%f') if base_timestamp else None

            #msg_body = row.Message.replace("Collected pin feedback coin:", "").replace(" round reward", "")
            msg_body = row.Message.replace("Collected feedback coin:", "").replace(" round reward", "")
            parts = msg_body.split(":")

            if len(parts) != 2:
                print(f"⚠️ Unexpected feedback format at row {i}: {row['Message']}")
                continue

            try:
                value_earned = float(parts[0].strip())
                round_total = float(parts[1].strip())

                details = {
                    "valueEarned": value_earned,
                    "runningRoundTotal": round_total,
                }

                # Logged event
                event = {
                    "AppTime": start_time,
                    "Timestamp": start_ts,
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_Timestamp": start_ts,
                    "end_Timestamp": start_ts,
                    "lo_eventType": "PinDrop_Moment",
                    "med_eventType": "PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                    "details": {},
                    "source": "logged",
                    **common_info
                }
                # events.append(event)
                #     "Timestamp": start_ts,
                #     "start_AppTime": start_time,
                #     "end_AppTime": start_time,
                #     "start_Timestamp": start_ts,
                #     "end_Timestamp": start_ts,
                #     "lo_eventType": "CoinCollect_Moment_PinDrop",
                #     "med_eventType": "CoinCollect_PinDrop",
                #     "hi_eventType": "PinDrop",
                #     "hiMeta_eventType": "BlockActivity",
                #     "details": details,
                #     "source": "logged",
                #     **common_info
                # })

                # Synthetic follow-ups
                offsets_events = [
                    ("CoinVis_end", 0.000, 0.000),
                    ("CoinValueTextVis", 0.000, 2.000),
                    ("CoinCollectSound_start", 0.000, 0.654)
                    ]
                
                event_meta = {
                    "med_eventType": "CoinCollect_Animation_PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity"
                    }

                synthetic = generate_synthetic_events_v2(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to parse feedback values at row {i}: {e}")

    return events

# -- Chest Opening Events

def process_chest_opened_v4(df, allowed_statuses):
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Chest opened:"):
            try:
                common_info = build_common_event_fields(row, i)
                coin_id = int(row.Message.replace("Chest opened: ", "").strip())
                start_time = row["AppTime"]
                timestamp = row["Timestamp"]

                base_timestamp = safe_parse_timestamp(timestamp_str)
                if base_timestamp is None:
                    print(f"⚠️ base_timestamp is None for input: {timestamp_str} with base_info: {base_info}")
                for evt_name, offset, duration in timed_events:
                    start_time = base_time + offset
                    start_ts = (base_timestamp + timedelta(seconds=offset)).strftime('%H:%M:%S:%f') if base_timestamp else None
                event = {
                    "AppTime": start_time,
                    "Timestamp": start_ts,
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_Timestamp": start_ts,
                    "end_Timestamp": start_ts,
                    "lo_eventType": "PinDrop_Moment",
                    "med_eventType": "PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                    "details": {},
                    "source": "logged",
                    **common_info
                }
                # events.append(event)
                #     "Timestamp": start_ts,
                #     "start_AppTime": start_time,
                #     "end_AppTime": start_time,
                #     "start_Timestamp": start_ts,
                #     "end_Timestamp": start_ts,
                #     "lo_eventType": "ChestOpen_Moment",
                #     "med_eventType": "ChestOpen",
                #     "hi_eventType": "ChestOpen",
                #     "hiMeta_eventType": "BlockActivity",
                #     "details": {"idvCoinID": coin_id},
                #     "source": "logged",
                #     **common_info
                # })

                # offsets_events = [
                #     (0.000, "ChestOpenAnimation_start"),
                #     (0.000, "ChestOpenSound_start"),
                #     (0.400, "ChestOpenAnimation_end"),
                #     (0.400, "ChestOpenSound_end"),
                #     (0.400, "ChestOpenEmpty_start"),
                #     (2.000, "ChestOpenEmpty_end"),
                #     (2.000, "CoinVis_start"),
                #     (2.000, "CoinPresentSound_start"),
                #     (2.650, "CoinPresentSound_end"),
                #     (3.000, "Coin_Released")
                #     ]
                offsets_events = [
                    ("ChestOpenAnimation", 0.000, 0.400),
                    ("ChestOpenSound", 0.000, 0.400),
                    ("ChestOpenEmpty", 0.400, 1.600),
                    ("CoinVis_start", 2.000, 0.000),
                    ("CoinPresentSound", 2.000, 0.650),
                    ("CoinLocked", 2.000, 1.000)
                    ]

                event_meta = {
                    "med_eventType": "ChestOpen_Animation",
                    "hi_eventType": "ChestOpen",
                    "hiMeta_eventType": "BlockActivity"
                    }

                synthetic = generate_synthetic_events_v2(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process chest open at row {i}: {e}")

    return events

def process_chest_collect_v3(df, allowed_statuses):
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("coin collected"):
            try:
                common_info = build_common_event_fields(row, i)
                start_time = row["AppTime"]
                timestamp = row["Timestamp"]

                base_timestamp = safe_parse_timestamp(timestamp_str)
                if base_timestamp is None:
                    print(f"⚠️ base_timestamp is None for input: {timestamp_str} with base_info: {base_info}")
                for evt_name, offset, duration in timed_events:
                    start_time = base_time + offset
                    start_ts = (base_timestamp + timedelta(seconds=offset)).strftime('%H:%M:%S:%f') if base_timestamp else None
                event = {
                    "AppTime": start_time,
                    "Timestamp": start_ts,
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_Timestamp": start_ts,
                    "end_Timestamp": start_ts,
                    "lo_eventType": "PinDrop_Moment",
                    "med_eventType": "PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                    "details": {},
                    "source": "logged",
                    **common_info
                }
                # events.append(event)
                #     "Timestamp": start_ts,
                #     "start_AppTime": start_time,
                #     "end_AppTime": start_time,
                #     "start_Timestamp": start_ts,
                #     "end_Timestamp": start_ts,
                #     "lo_eventType": "CoinCollect_Moment_Chest",
                #     "med_eventType": "CoinCollect_Chest",
                #     "hi_eventType": "ChestOpen",
                #     "hiMeta_eventType": "BlockActivity",
                #     "details": {},
                #     "source": "logged",
                #     **common_info
                # })

                # offsets_events = [
                #     (0.000, "CoinVis_end"),
                #     (0.000, "ChestVis_end"),
                #     (0.000, "CoinCollectSound_start"),
                #     (0.000, "CoinValueTextVis_start"),
                #     (0.000, "NextChestVisible"),
                #     (0.654, "CoinCollectSound_end"),
                #     (2.000, "CoinValueTextVis_end")
                # ]
                offsets_events = [

                    ("CoinVis_end", 0.000, 0.000),
                    ("CurrChestVis_end", 0.000, 0.000),
                    ("NextChestVis_start", 0.000, 0.000),
                    ("CoinValueTextVis", 0.000, 2.000),
                    ("CoinCollectSound", 0.000, 0.654)
                ]
                event_meta = {
                    "med_eventType": "CoinCollect_Animation_Chest",
                    "hi_eventType": "ChestOpen",
                    "hiMeta_eventType": "BlockActivity"
                }
                synthetic = generate_synthetic_events_v2(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process chest coin collect at row {i}: {e}")

    return events

# -- Putting Everything Together

# def buildEvents_AN_v4a(df, allowed_statuses):
#     try:
#         # 1. Build cascades from all known sources
#         cascades = (
#             process_pin_drop_v5(df, allowed_statuses) +
#             process_feedback_collect_v5(df, allowed_statuses) +
#             process_chest_opened_v4(df, allowed_statuses) +
#             process_chest_collect_v3(df, allowed_statuses) +
#             process_marks_v2(df, allowed_statuses) +
#             process_swap_votes_v4(df, allowed_statuses) +
#             process_block_periods_v4(df, allowed_statuses)
#         )

#         # 2. Generate cascade windows from them
#         cascade_windows = find_cascade_windows_from_events_v4(cascades)

#         # 3. Assign cascade metadata to each event
#         cascades_updated = []
#         for e in cascades:
#             matched = match_cascade_window_v3(e, cascade_windows)
#             if matched:
#                 e.update(matched)
#             else:
#                 e["cascade_id"] = None
#             cascades_updated.append(e)
#         cascades = cascades_updated

#         # 4. Generate and refine synthetic reward navigation events
#         reward_walks = generate_reward_walking_periods_v2(df, cascades)
#         for rw in reward_walks:
#             print(rw["lo_eventType"], rw.get("original_row_start"), rw.get("cascade_id"))
#         print(f"Generated {len(reward_walks)} reward walks")

#         refined_walks_df = synthesize_reward_driven_walking_periods_v3(df, reward_walks)
#         print(f"Generated {len(refined_walks_df)} refined walks df")
#         refined_walks = refine_reward_walking_periods_v2(refined_walks_df.to_dict("records"))
#         print(f"Refined {len(refined_walks)} walking periods")
#         # 5. Combine cascades + synthetic walking periods
#         all_events = cascades + refined_walks

#         # 6. Clean internal-use-only fields
#         for e in all_events:
#             e.pop("event_type", None)

#         return all_events

#     except KeyError as e:
#         print(f"🔥 KEY ERROR: {e}")
#         traceback.print_exc()
#         raise

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
                    "start_AppTime": first_row["AppTime"],
                    "end_AppTime": last_row["AppTime"],
                    "start_Timestamp": first_row["Timestamp"],
                    "end_Timestamp": last_row["Timestamp"],
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
                    "start_AppTime": first_row["AppTime"],
                    "end_AppTime": last_row["AppTime"],
                    "start_Timestamp": first_row["Timestamp"],
                    "end_Timestamp": last_row["Timestamp"],
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
            "start_AppTime": first_row["AppTime"],
            "end_AppTime": last_row["AppTime"],
            "start_Timestamp": first_row["Timestamp"],
            "end_Timestamp": last_row["Timestamp"],
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


def buildEvents_AN_v4(df, allowed_statuses):
    try:
        # 1. Build cascades from all known sources
        cascades = (
            #process_block_periods_v4(df, allowed_statuses) +
            process_special_round_segments(df, allowed_statuses) +
            process_pin_drop_v5(df, allowed_statuses) +
            process_feedback_collect_v5(df, allowed_statuses) +
            process_chest_opened_v4(df, allowed_statuses) +
            process_chest_collect_v3(df, allowed_statuses) +
            process_marks_v2(df, allowed_statuses) +
            process_swap_votes_v4(df, allowed_statuses)
            #process_block_periods_v4(df, allowed_statuses)
        )

        # 2. Generate cascade windows from them
        all_events = backfill_approx_row_indices(cascades, df)

        return all_events

    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise
