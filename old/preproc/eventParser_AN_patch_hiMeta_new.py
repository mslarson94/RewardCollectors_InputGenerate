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
