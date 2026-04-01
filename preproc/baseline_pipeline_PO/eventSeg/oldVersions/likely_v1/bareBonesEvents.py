# bareBonesEvents.py
# script that just generates a super bare bones events dataframe 
# only events that are tagged are the "TrueBlockStart", "TrueBlockEnd", and "Marks"
# only fields kept are 
import pandas as pd
import re
import traceback
from metadata_and_manifest_utils import attach_metadata_to_events



def build_common_event_fields(row, index=None):
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


def buildBareBonesEvents(df, role):
    try:
        cascades = (
            process_TrueBlocks(df = df) +
            process_marks(df = df, role = role))
        return cascades

    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise

