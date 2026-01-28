# schwannCells_eventsParserHelper_PO.py

'''

Author: Myra Sarai Larson   08/18/2025
   This helper script is designed to do the supporting the innervation of the muscle events we are building 
   in the muscles_eventParser_AN script - (like real life Schwann Cells that insulate the long axons of 
   the Peripheral Nervous System!). 

   It is only supposed to be used to support the muscles_eventParser_AN script to build up those synthetic 
   events by building common event fields, building segmented events, having the scaffold used
   to generate synthetic events. 
   
   As of right now, I need to figure out when to use which of my build_common_event_fields variations, 
   switch all "Timestamp" pulls to "RobustTimestamp" (and completely nix the safeParseTimestamp function use)
   and figure out the usage for the process_ functions. 

   Also figure out if the backfill_approx_row_indices makes sense to be here.

'''

import re
import os
from datetime import datetime, timedelta
from io import StringIO
import traceback
import pandas as pd 


def backfill_approx_row_indices_v2(events, df):
    """
    For each synthetic event, assign `original_row_start` and `original_row_end` by matching
    closest preceding row in the original DataFrame based on AppTime or RobustParsedTimestamp.
    """
    timestamp_col = "eMLT_orig"

    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")

    df = df.reset_index().copy()
    df_sorted = df.sort_values("AppTime").reset_index(drop=True)

    for event in events:
        if event.get("source") != "synthetic":
            continue

        app_time = event.get("AppTime")
        if app_time is not None:
            candidates = df_sorted[df_sorted["AppTime"] <= app_time]
            if not candidates.empty:
                matched_row = candidates.iloc[-1]
                event["origRow_start"] = matched_row["origRow"] if "origRow" in matched_row else matched_row["index"] ## More robust to pitfalls associated with using matched_row["index"] 1/25/2026
                event["origRow_end"]   = matched_row["origRow"] if "origRow" in matched_row else matched_row["index"] ## More robust to pitfalls associated with using matched_row["index"] 1/25/2026
            continue

        ts = event.get(timestamp_col)
        if ts is not None:
            if not isinstance(ts, pd.Timestamp):
                ts = pd.to_datetime(ts, errors='coerce')
            if pd.notnull(ts):
                candidates = df_sorted[df_sorted[timestamp_col] <= ts]
                if not candidates.empty:
                    matched_row = candidates.iloc[-1]
                    event["origRow_start"] = matched_row["origRow"] if "origRow" in matched_row else matched_row["index"] ## More robust to pitfalls associated with using matched_row["index"] 1/25/2026
                    event["origRow_end"]   = matched_row["origRow"] if "origRow" in matched_row else matched_row["index"] ## More robust to pitfalls associated with using matched_row["index"] 1/25/2026
    return events

def build_common_event_fields_noTime(row, index=None):
    idx = index if index is not None else row.name
    return {
        "BlockNum": row.get("BlockNum", None),
        "RoundNum": row.get("RoundNum", None),
        "CoinSetID": row.get("CoinSetID", None),
        "BlockStatus": row.get("BlockStatus", "unknown"),
        "BlockInstance": row.get("BlockInstance", None),
        "BlockType": row.get("BlockType", "unknown"),
        "chestPin_num": row.get("chestPin_num", None),
        "origRow_start": row.get("origRow", idx),
        "origRow_end": row.get("origRow", idx),
        "mLT_raw": row.get("mLT_raw", None),
        "mLT_orig": row.get("mLT_orig", None)
    }


def build_common_event_fields_full(row, index=None):
    timestamp_col = "eMLT_orig"

    # if isinstance(row[timestamp_col], str):
    #     row[timestamp_col] = pd.to_datetime(row[timestamp_col], errors="coerce")
    idx = index if index is not None else row.name

    return {
        "eMLT_orig": row.get("eMLT_orig", None),
        "mLT_orig": row.get("mLT_orig", None),
        "mLT_raw": row.get("mLT_raw", None),
        "BlockInstance": row.get("BlockInstance", None),
        "BlockNum": row.get("BlockNum", None),
        "RoundNum": row.get("RoundNum", None),
        "CoinSetID": row.get("CoinSetID", None),
        "BlockStatus": row.get("BlockStatus", "unknown"),
        "BlockType": row.get("BlockType", "unknown"),
        "chestPin_num": row.get("chestPin_num", None),
        "origRow_start": row.get("origRow", idx),
        "origRow_end": row.get("origRow", idx)
    }

def build_segment_event(start_row, end_row, event_type):
    timestamp_col = "eMLT_orig"

    return {
        "AppTime": start_row["AppTime"],
        "mLT_orig": start_row.get("mLT_orig", None),
        "eMLT_orig": start_row.get("eMLT_orig", None),
        "mLT_raw": start_row["mLT_raw"],

        "start_AppTime": start_row["AppTime"],
        "end_AppTime": end_row["AppTime"],

        "start_eMLT_orig": start_row.get("eMLT_orig", None),
        "end_eMLT_orig": end_row.get("eMLT_orig", None),

        "lo_eventType": event_type,
        "med_eventType": f"{event_type}_Transition",
        "hi_eventType": "BlockStructure",
        "hiMeta_eventType": "Infrastructure",
        "source": "synthetic",
        "BlockNum": start_row.get("BlockNum"),
        "RoundNum": start_row.get("RoundNum"),
        "CoinSetID": start_row.get("CoinSetID"),
        "BlockInstance": start_row.get("BlockInstance", None),
        "BlockStatus": start_row.get("BlockStatus"),
        "BlockType": start_row.get("BlockType"),
        "chestPin_num": start_row.get("chestPin_num"),
        "origRow_start": start_row.get("origRow", start_row.name), ## More robust to pitfalls associated with using start_row.name 1/25/2026
        "origRow_end": end_row.get("origRow", end_row.name), ## More robust to pitfalls associated with using end_row.name 1/25/2026
    }

def generate_synthetic_events_v3(base_time, appTime, timed_events, base_info, event_meta):
    """
    Generate synthetic events based on a base datetime and list of (name, offset, duration).
    Assumes base_time is a valid datetime object.
    """

    timestamp_col = "eMLT_orig"

    synthetic_events = []
    if isinstance(base_time, str):
        base_time = pd.to_datetime(base_time, errors="raise")
    for evt_name, offset, duration in timed_events:
        try:
            # Coerce offset and duration to float (seconds)
            if isinstance(offset, pd.Timedelta):
                offset = offset.total_seconds()
            elif pd.isnull(offset):
                print(f"⚠️ Skipping '{evt_name}' due to NaT offset.")
                continue
            else:
                offset = float(offset)

            if isinstance(duration, pd.Timedelta):
                duration = duration.total_seconds()
            elif pd.isnull(duration):
                duration = 0.0
            else:
                duration = float(duration)

            start_time = base_time + timedelta(seconds=offset)
            end_time = start_time + timedelta(seconds=duration) if duration else None

            synthetic_events.append({
                "AppTime": appTime,
                "eMLT_orig": start_time.isoformat(sep=' '),
                "start_AppTime": offset + appTime,
                "end_AppTime": (offset + duration + appTime) if duration else None,
                "start_eMLT_orig": start_time.isoformat(sep=' '),
                "end_eMLT_orig": end_time.isoformat(sep=' ') if end_time else None,
                "lo_eventType": evt_name,
                "details": {},
                "source": "synthetic",
                "origRow_start": base_info.get("origRow_start", -1),
                "origRow_end": base_info.get("origRow_end", -1),
                **event_meta,
                **base_info
            })

        except Exception as e:
            print(f"⚠️ Failed to create synthetic event '{evt_name}' from base_time {base_time}: {e}")
    return synthetic_events
    
def process_block_segments(df, allowed_statuses):

    events = []
    # if isinstance(df[timestamp_col], str):
    #     df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")
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
