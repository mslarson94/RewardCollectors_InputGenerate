# eventParser_PO_patch.py

import pandas as pd
import re
import os
from datetime import datetime, timedelta
from io import StringIO
import traceback

from schwannCells_eventsParserHelper_PO import (build_common_event_fields_noTime,
                                                backfill_approx_row_indices_v2, 
                                                generate_synthetic_events_v3)
# do we need build_segment_event?
###########################

def process_swap_votes_v4(df, allowed_statuses, segmentType):
    events = []
    if segmentType == "glia":
        timestamp_col = "mLTimestamp_orig"
        start_time = "start_mLT_orig"
        end_time = "end_mLT_orig"
    else:
        timestamp_col =  "mLTimestamp"
        start_time = "start_mLT"
        end_time = "end_mLT"

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str):
            match = re.match(r"Observer says it was an?\s+(.*)\.", row["Message"])
            if match:
                swapvote = match.group(1).strip().upper()
                try:
                    mLTimestamp_raw = row["mLTimestamp_raw"]
                    appTime = row["AppTime"]
                    start_ts = row[timestamp_col]

                    common_info = build_common_event_fields_noTime(row, i, segmentType)

                    # Determine correctness of vote
                    coinset = row.get("CoinSetID")
                    if coinset in [1, 4]:
                        correct_answer = "OLD ROUND"
                    elif coinset in [2, 3, 5]:
                        correct_answer = "NEW ROUND"
                    else:
                        correct_answer = None  # or raise a warning

                    if correct_answer is not None:
                        score = "Correct" if swapvote == correct_answer else "Incorrect"
                    else:
                        score = "Unknown"


                    events.append({
                        "AppTime": appTime,
                        timestamp_col: start_ts,
                        "start_AppTime": appTime,
                        "end_AppTime": appTime,
                        start_time: start_ts,
                        end_time: start_ts,

                        "mLTimestamp_raw": mLTimestamp_raw,

                        "lo_eventType": "SwapVote_Moment",
                        "med_eventType": "SwapVote",
                        "hi_eventType": "SwapVote",
                        "hiMeta_eventType": "SwapVote",
                        "details": {
                            "SwapVote": swapvote,
                            "SwapVoteScore": score
                        },
                        "source": "logged",
                        **common_info
                    })

                    offsets_events = [
                        ("SwapVoteText_Vis_end", 0.000, 0.000),
                        ("BlockScoreText_Vis_start", 0.000, 2.000)
                    ]

                    event_meta = {
                        "med_eventType": "PostSwapVoteEvents",
                        "hi_eventType": "SwapVote",
                        "hiMeta_eventType": "SwapVote"
                    }
                    synthetic = generate_synthetic_events_v3(start_ts, appTime, offsets_events, common_info, event_meta, segmentType)
                    events.extend(synthetic)
                except Exception as e:
                    print(f"⚠️ Failed to process swap vote at row {i}: {e}")

    return events

# -- Pin Dropping Events

def process_pin_drop_v5(df, allowed_statuses, segmentType):
    print("🧪 Columns:", df.columns.tolist())
    if segmentType == "glia":
        timestamp_col = "mLTimestamp_orig"
        start_time = "start_mLT_orig"
        end_time = "end_mLT_orig"
    else:
        timestamp_col =  "mLTimestamp"
        start_time = "start_mLT"
        end_time = "end_mLT"
    # if isinstance(df[timestamp_col], str):
    #     df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")
    events = []
    i = 0
    while i < len(df):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            i += 1
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and "Other participant just dropped a new pin at" in row["Message"]:
            try:
                common_info = build_common_event_fields_noTime(row, i, segmentType)

                mLTimestamp_raw = row["mLTimestamp_raw"]
                appTime = row["AppTime"]
                start_ts = row[timestamp_col]

                event = {
                    "AppTime": appTime,
                    timestamp_col: start_ts,
                    "start_AppTime": appTime,
                    "end_AppTime": appTime,
                    start_time: start_ts,
                    end_time: start_ts,
                    "mLTimestamp_raw": mLTimestamp_raw,

                    "lo_eventType": "PinDrop_Moment",
                    "med_eventType": "PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                    "details": {},
                    "source": "logged",
                    **common_info
                }

                j = i + 1
                while j < len(df):
                    next_row = df.iloc[j]

                    # If it's not an Event row or doesn't contain a string Message, skip and continue
                    if next_row["Type"] != "Event" or not isinstance(next_row["Message"], str):
                        j += 1
                        continue

                    msg = next_row["Message"]

                    # Stop parsing if a new block or drop starts
                    if next_row.get("BlockNum") != row.get("BlockNum"):
                        break

                    if "Other participant just dropped a new pin at " in msg:
                        match = re.search(r'at\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)', msg)
                        if match:
                            try:
                                event["details"].update({
                                    "pinLocal_x": float(match.group(1)),
                                    "pinLocal_y": float(match.group(2)),
                                    "pinLocal_z": float(match.group(3)),
                                })
                            except ValueError:
                                print(f"⚠️ Float conversion failed in pin location at row {j}: {msg}")

                    elif "Dropped pin was dropped at " in msg:
                        match = re.search(
                            r"Dropped pin was dropped at (?P<dropDist>\d+\.\d{2}) from chest (?P<idvCoinID>\d+) originally at \((?P<coinPos_x>-?\d+\.\d{2}), (?P<coinPos_y>-?\d+\.\d{2}), (?P<coinPos_z>-?\d+\.\d{2})\):(?P<dropQual>CORRECT|INCORRECT)",
                            msg
                        )
                        if match:
                            try:
                                parsed = match.groupdict()
                                event["details"].update({
                                    "dropDist": float(parsed["dropDist"]),
                                    "coinPos_x": float(parsed["coinPos_x"]),
                                    "coinPos_y": float(parsed["coinPos_y"]),
                                    "coinPos_z": float(parsed["coinPos_z"]),
                                    "dropQual": parsed["dropQual"]
                                })
                            except ValueError:
                                print(f"⚠️ Drop analysis parsing error at row {j}: {msg}")

                    elif "Observer chose" in msg and "pindrop from the navigator" in msg:
                        match = re.search(r"Observer chose (?P<pinDropVote>CORRECT|INCORRECT) for this pindrop from the navigator", msg)
                        if match:
                            event["details"].update(match.groupdict())

                    j += 1

                events.append(event)

                offsets_events = [
                    ("PinDropSound", 0.000, 0.403),
                    ("GrayPinVis", 0.000, 2.000),
                    ("VotingWindow", 0.000, 2.000),
                    ("VoteInstrText_Vis", 0.000, 2.000),
                    ("Feedback_Sound", 2.000, 0.182),
                    ("FeedbackTextVis", 2.000, 1.000),
                    ("FeedbackPinColor", 2.000, 1.000),
                    ("CoinVis_start", 3.000, 0.000),
                    ("CoinPresentSound", 3.000, 0.650)
                ]
                event_meta = {
                    "med_eventType": "PinDrop_Animation",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity"
                }
                synthetic = generate_synthetic_events_v3(start_ts, appTime, offsets_events, common_info, event_meta, segmentType)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process pin drop at row {i}: {e}")
            i = j
        else:
            i += 1
    return events

def process_feedback_collect_v5(df, allowed_statuses, segmentType):
    if segmentType == "glia":
        timestamp_col = "mLTimestamp_orig"
        start_time = "start_mLT_orig"
        end_time = "end_mLT_orig"
    else:
        timestamp_col =  "mLTimestamp"
        start_time = "start_mLT"
        end_time = "end_mLT"

    # if isinstance(df[timestamp_col], str):
    #     df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row["Message"].startswith("A.N. collected coin:"):
            try:
                common_info = build_common_event_fields_noTime(row, i, segmentType)

                appTime = row["AppTime"]
                start_ts = row[timestamp_col]
                mLTimestamp_raw = row["mLTimestamp_raw"]

                event = {
                    "AppTime": appTime,
                    timestamp_col: start_ts,
                    "start_AppTime": appTime,
                    "end_AppTime": appTime,
                    start_time: start_ts,
                    end_time: start_ts,
                    "mLTimestamp_raw": mLTimestamp_raw,

                    "lo_eventType": "CoinCollect_Moment_PinDrop",
                    "med_eventType": "CoinCollect_PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                    "details": {},
                    "source": "logged",
                    **common_info
                }

                match = re.search(
                    r"A.N. collected coin:(?P<idvCoinID>\d+) round reward: (?P<currRoundTotal>-?\d+\.\d{2})",
                    row["Message"]
                )
                if match:
                    event["details"].update(match.groupdict())
                else:
                    print(f"⚠️ Unexpected parts format in score line at row {i}: {row['Message']}")

                events.append(event)
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

                synthetic = generate_synthetic_events_v3(start_ts, appTime, offsets_events, common_info, event_meta, segmentType)
                events.extend(synthetic)
                

            except Exception as e:
                print(f"⚠️ Failed to parse feedback values at row {i}: {e}")

    return events


# -- Chest Opening Events

def process_chest_collect_v3(df, allowed_statuses, segmentType):
    if segmentType == "glia":
        timestamp_col = "mLTimestamp_orig"
        start_time = "start_mLT_orig"
        end_time = "end_mLT_orig"
    else:
        timestamp_col =  "mLTimestamp"
        start_time = "start_mLT"
        end_time = "end_mLT"

    # if isinstance(df[timestamp_col], str):
    #     df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row["Message"].startswith("Other participant just collected coin: "):
            try:

                coin_id = int(row["Message"].replace("Other participant just collected coin: ", "").strip())
                common_info = build_common_event_fields_noTime(row, i, segmentType)

                appTime = row["AppTime"]
                start_ts = row[timestamp_col]
                mLTimestamp_raw = row["mLTimestamp_raw"]

                event = {
                    "AppTime": appTime,
                    timestamp_col: start_ts,
                    "start_AppTime": appTime,
                    "end_AppTime": appTime,
                    start_time: start_ts,
                    end_time: start_ts,
                    "mLTimestamp_raw": mLTimestamp_raw,
                    "lo_eventType": "CoinCollect_Moment_Chest",
                    "med_eventType": "CoinCollect_Chest",
                    "hi_eventType": "ChestOpen",
                    "hiMeta_eventType": "BlockActivity",
                    "details": {"idvCoinID": coin_id},
                    "source": "logged",
                    **common_info
                }

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

                synthetic = generate_synthetic_events_v3(start_ts, appTime, offsets_events, common_info, event_meta, segmentType)
                events.extend(synthetic)
                events.append(event)

            except Exception as e:
                print(f"⚠️ Failed to process chest open at row {i}: {e}")

    return events


def buildEvents_PO(df, allowed_statuses, segmentType):
    try:
        # 1. Build cascades from all known sources
        cascades = (
            process_pin_drop_v5(df, allowed_statuses, segmentType) +
            process_feedback_collect_v5(df, allowed_statuses, segmentType) +
            process_chest_collect_v3(df, allowed_statuses, segmentType) +
            process_swap_votes_v4(df, allowed_statuses, segmentType)
        )

        # 2. Generate cascade windows from them
        all_events = backfill_approx_row_indices_v2(cascades, df, segmentType)
        print('yay')
        return all_events

    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise
