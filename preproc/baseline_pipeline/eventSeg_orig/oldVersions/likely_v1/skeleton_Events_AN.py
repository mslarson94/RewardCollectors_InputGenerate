# skeleton_Events.py

'''
Author: Myra Sarai Larson   08/18/2025
   This script that just generates a skeleton dataframe where the only events that are tagged are the "TrueBlockStart", "TrueBlockEnd", 
   and "Marks". 

   I need to decide how bare bones this skeleton dataframe is (mostly because I want to reclaim the bare_bones name from the other 
   version), and whether its prudent to expand this script to also do some Round Tagging. 

   As of right now, I need to figure out if any of these functions rely on any "Timestamp" or "ParsedTimestamp" columns
   and if so, swap those out for the "RobustTimestamp" column. Also, completely nix the safeParseTimestamp function use if it used. 
   
   I also need to figure out what helper functions are being called (because I've separated my original eventParserHelper_AN.py 
   script into my glia_ & schwannCell_ variants.)

   There's also this weird usage of "cascade_windows" that I'd like to track down - I think it's a relic of an older incarnation of 
   the script. Also - does this script completely overlap with the glia_eventsParserHelper_AN script? Can this script be used in 
   a way that is agnostic to participant roles?

'''

import pandas as pd
import re
import traceback
from metadata_and_manifest_utils import attach_metadata_to_events



def build_common_event_fields_bony(row, index=None):
    idx = index if index is not None else row.name

    #print(f"🔎 Assigning original_row_start from row: {row.to_dict()}")
    return {
        "ParsedTimestamp": row.get("ParsedTimestamp", None),
        "BlockInstance": row.get("BlockInstance", None),
        "original_row_start": row.get("original_index", idx),
        "BlockNum": row.get("BlockNum", None),
        "RoundNum": row.get("RoundNum", None),
        "BlockStatus": row.get("BlockStatus", "unknown"),
        "BlockType": row.get("BlockType", "unknown")
    }

def process_TrueBlocks(df):
    events = []
    for idx, row in df.iterrows():
        if isinstance(row.Message, str) and (
            row.Message.startswith("Started collecting.") or
            row.Message.startswith("Started pindropping.") or
            row.Message.startswith("Started watching other participant's collecting.") or
            row.Message.startswith("Started watching other participant's pin dropping.")):
            
            common_info = build_common_event_fields(row, idx)
            block_start_event = {
                **common_info,
                "lo_eventType": "TrueBlockStart",
                "details": {}
            }
            events.append(block_start_event)

        elif isinstance(row.Message, str) and "finished current task" in row.Message:
            common_info = build_common_event_fields(row, idx)
            events.append({
            	**common_info,
                "lo_eventType": "TrueBlockEnd",
                "details": {}
            })
    return events

def process_marks(df, role):
    if role not in ("AN", "PO"):
        raise ValueError(f"Invalid role '{role}'. Expected 'AN' or 'PO'.")
    events = []
    for idx, row in df.iterrows():

        if isinstance(row.Message, str) and "Sending Headset mark" in row.Message:
            common_info = build_common_event_fields(row, idx)
            details = {"mark": "A"} if role == "AN" else {"mark": "B"}

            events.append({
                **common_info,
                "lo_eventType": "Mark",
                "details": details,
            })

    return events


def buildEvents_skeleton(df, allowed_statuses, role):
    try:
        cascades = (
            process_TrueBlocks(df) +
            process_marks(df, allowed_statuses, role) + 
            process_block_segments(df, allowed_statuses) +
            process_true_round_segments(df, allowed_statuses) +
            process_special_round_segments(df, allowed_statuses) 
            )
        return cascades

    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise

def buildEvents_AN_v4(df, allowed_statuses):
    try:
        # 1. Build cascades from all known sources
        cascades = (
            process_block_segments(df, allowed_statuses) +
            process_true_round_segments(df, allowed_statuses) +
            process_TrueBlocks(df, allowed_statuses) +
            process_special_round_segments(df, allowed_statuses) +
            process_marks(df, allowed_statuses, role = 'AN') +
        )

        # 2. Generate cascade windows from them
        all_events = backfill_approx_row_indices(cascades, df)

        return all_events

    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise
