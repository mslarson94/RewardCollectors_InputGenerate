# glia_eventsParserHelper.py

'''
Author: Myra Sarai Larson   08/18/2025
   This helper script is designed to be the glue of the foundational events (like glia in the brain holding all non-neuronal 
   activity together through their various support roles!)

   This script should be limited to the fundamental parsing of those foundational events - when Blocks & Round Numbers
   change, and when Marks happen. 

   As of right now, I need to figure out if any of these functions rely on any "mLTimestamp_orig" or "ParsedmLTimestamp_orig" columns
   and if so, swap those out for the "RobustmLTimestamp_orig" column. Also, completely nix the safeParsemLTimestamp_orig function use if it used. 
   
   Also, figure out the usage for the process_ functions (do we even need it?), and whether the backfill_approx_row_indices 
   makes sense to be here.

   Additionally, I should figure out if these helper functions are specific to AN processing, or if it can be used for PO processing as well. 

'''
import re
import os
from datetime import datetime, timedelta
from io import StringIO
import traceback
import pandas as pd
from schwannCells_eventsParserHelper_PO import (build_common_event_fields_noTime, build_common_event_fields_bony, 
                                        build_common_event_fields_full, backfill_approx_row_indices_v2,
                                        build_segment_event, generate_synthetic_events_v3)

# Bare Bones Events Handling 

def process_marks(df, allowed_statuses, role):
    if role not in ("AN", "PO"):
        raise ValueError(f"Invalid role '{role}'. Expected 'AN' or 'PO'.")

    events = []
    for idx, row in df.iterrows():
        if isinstance(row.Message, str) and "Sending Headset mark" in row.Message:
            common_info = build_common_event_fields_noTime(row, idx) ### Fix Me

            start_time = row["AppTime"]
            start_ts = row["mLTimestamp_orig"]

            details = {"mark": "A"} if role == "AN" else {"mark": "B"}

            events.append({
                "AppTime": start_time,
                "mLTimestamp_orig": start_ts,
                "start_AppTime": start_time,
                "end_AppTime": start_time,
                "start_mLT_orig": start_ts,
                "end_mLT_orig": start_ts,
                "mLTimestamp_raw": row["mLTimestamp_raw"],
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
                    "mLTimestamp_orig": first_row["mLTimestamp_orig"],
                    "start_AppTime": first_row["AppTime"],
                    "end_AppTime": last_row["AppTime"],
                    "start_mLT_orig": first_row["mLTimestamp_orig"],
                    "end_mLT_orig": last_row["mLTimestamp_orig"],
                    "mLTimestamp_raw": first_row["mLTimestamp_raw"],
                    "lo_eventType": f"{lo_event}_segment",
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod",
                    "hiMeta_eventType": hi_meta,
                    "source": "synthetic",
                    "BlockNum": first_row.get("BlockNum"),
                    "BlockInstance": first_row.get("BlockInstance"),
                    "RoundNum": first_row.get("RoundNum"),
                    "CoinSetID": first_row.get("CoinSetID"),
                    "BlockStatus": first_row.get("BlockStatus"),
                    "BlockType": first_row.get("BlockType"),
                    "chestPin_num": first_row.get("chestPin_num"),
                    "origRow_start": start_i,
                    "origRow_end": end_i
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
                    "mLTimestamp_orig": first_row["mLTimestamp_orig"],
                    "start_AppTime": first_row["AppTime"],
                    "end_AppTime": last_row["AppTime"],
                    "start_mLT_orig": first_row["mLTimestamp_orig"],
                    "end_mLT_orig": last_row["mLTimestamp_orig"],
                    "mLTimestamp_raw": first_row["mLTimestamp_raw"],
                    "lo_eventType": f"{lo_event}_segment",
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod",
                    "hiMeta_eventType": hi_meta,
                    "source": "synthetic",
                    "BlockNum": first_row.get("BlockNum"),
                    "RoundNum": first_row.get("RoundNum"),
                    "CoinSetID": first_row.get("CoinSetID"),
                    "BlockInstance": first_row.get("BlockInstance"),
                    "BlockStatus": first_row.get("BlockStatus"),
                    "BlockType": first_row.get("BlockType"),
                    "chestPin_num": first_row.get("chestPin_num"),
                    "origRow_start": start_i,
                    "origRow_end": end_i
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
            "mLTimestamp_orig": first_row["mLTimestamp_orig"],
            "start_AppTime": first_row["AppTime"],
            "end_AppTime": last_row["AppTime"],
            "start_mLT_orig": first_row["mLTimestamp_orig"],
            "end_mLT_orig": last_row["mLTimestamp_orig"],
            "mLTimestamp_raw": first_row["mLTimestamp_raw"],
            "lo_eventType": f"{lo_event}_segment",
            "med_eventType": "NonRewardDrivenNavigation",
            "hi_eventType": "WalkingPeriod",
            "hiMeta_eventType": hi_meta,
            "source": "synthetic",
            "BlockNum": first_row.get("BlockNum"),
            "BlockInstance": first_row.get("BlockInstance"),
            "RoundNum": first_row.get("RoundNum"),
            "CoinSetID": first_row.get("CoinSetID"),
            "BlockStatus": first_row.get("BlockStatus"),
            "BlockType": first_row.get("BlockType"),
            "chestPin_num": first_row.get("chestPin_num"),
            "origRow_start": start_i,
            "origRow_end": end_i
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
        start_ts = start_row.mLTimestamp_orig
        end_ts = end_row.mLTimestamp_orig
        duration = end_time - start_time

        common_info = build_common_event_fields_full(start_row, start_row.name) ### Fix Me
        common_info.update({
            "start_AppTime": start_time,
            "end_AppTime": end_time,
            "start_mLT_orig": start_ts,
            "end_mLT_orig": end_ts
        })

        synthetic = generate_synthetic_events_v3(
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

def process_TrueContent(df, allowed_statuses):

    events = []
    for idx, row in df.iterrows():
        if isinstance(row.Message, str) and (
            row.Message.startswith("Started collecting.") or
            row.Message.startswith("Started pindropping.") or
            row.Message.startswith("Started watching other participant's collecting.") or
            row.Message.startswith("Started watching other participant's pin dropping.")
        ):
            common_info = build_common_event_fields_noTime(row, idx) ### Fix Me

            start_time = row["AppTime"]
            start_ts = row["mLTimestamp_orig"]

            round_start_event = {
                **common_info,
                "mLTimestamp_orig": start_ts,
                "AppTime": start_time,
                
                "start_AppTime": start_time,
                "end_AppTime": start_time,
                "start_mLT_orig": start_ts,
                "end_mLT_orig": start_ts,
                "mLTimestamp_raw": row["mLTimestamp_raw"],

                "lo_eventType": "TrueContentStart",
                "med_eventType": "ReferencePoint",
                "hi_eventType": "SystemEvent",
                "hiMeta_eventType": "Infrastructure",
                "details": {},
                "source": "logged",
            }
            events.append(round_start_event)


        elif isinstance(row.Message, str) and "finished current task" in row.Message:
            common_info = build_common_event_fields_noTime(row, idx) ### Fix Me

            start_time = row["AppTime"]
            start_ts = row["mLTimestamp_orig"]

            #details = {"mark": "A"} if role == "AN" else {"mark": "B"}

            events.append({
                "AppTime": start_time,
                "mLTimestamp_orig": start_ts,

                "start_AppTime": start_time,
                "end_AppTime": start_time,
                "start_mLT_orig": start_ts,
                "end_mLT_orig": start_ts,
                "mLTimestamp_raw": row["mLTimestamp_raw"],

                "lo_eventType": "TrueContentEnd",
                "med_eventType": "ReferencePoint",
                "hi_eventType": "SystemEvent",
                "hiMeta_eventType": "Infrastructure",
                "details": {},
                "source": "logged",
                **common_info
            })

    return events


def buildGliaEvents_PO(df, role, allowed_statuses):
    return (
        process_TrueContent(df, allowed_statuses) +
        process_marks(df, allowed_statuses, role) +
        process_true_round_segments(df, allowed_statuses) +
        process_special_round_segments(df, allowed_statuses) +
        process_block_segments(df, allowed_statuses) +
        process_block_periods_v4(df, allowed_statuses)
    )