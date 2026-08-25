# eventParser_PO_patch.py

import pandas as pd
import re
import os
from datetime import datetime, timedelta
from io import StringIO
import traceback
from schwannCells_eventsParserHelper_AN import (build_common_event_fields_noTime, build_common_event_fields_bony, 
                                        build_common_event_fields_full, backfill_approx_row_indices_v2,
                                        build_segment_event, generate_synthetic_events_v3)

###########################

def process_swap_votes_v4(df, allowed_statuses):
    events = []
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
                    start_time = row["AppTime"]
                    Timestamp_raw = row["Timestamp_raw"]
                    start_ts = row["RPTimestamp_orig"]
                    common_info = build_common_event_fields_full(row, i)

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
                        "RPTimestamp_orig": start_ts,
                        "AppTime": start_time,
                        "Timestamp_raw": Timestamp_raw,

                        "start_AppTime": start_time,
                        "end_AppTime": start_time,
                        "start_RPT_orig": start_ts,
                        "end_RPT_orig": start_ts,
                        #"start_AlignedTimestamp": #astart_ts,
                        #"end_AlignedTimestamp": #astart_ts,
                        "lo_eventType": "SwapVoteMoment",
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
                        ("SwapVoteText_end", 0.000, 0.000),
                        ("BlockScoreText_start", 0.000, 2.000)
                    ]

                    event_meta = {
                        "med_eventType": "FullPostSwapVoteEvents",
                        "hi_eventType": "SwapVote",
                        "hiMeta_eventType": "SwapVote"
                    }
                    synthetic = generate_synthetic_events_v3(start_ts, offsets_events, common_info, event_meta)
                    events.extend(synthetic)
                except Exception as e:
                    print(f"⚠️ Failed to process swap vote at row {i}: {e}")

    return events


# Myra Patched
# -- Pin Dropping Events

def process_pin_drop_v5a(df, allowed_statuses):
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
                common_info = build_common_event_fields_full(row, i)

                start_time = row["AppTime"]
                start_ts = row["RPTimestamp_orig"]
                Timestamp_raw = row["Timestamp_raw"]

                event = {
                    "RPTimestamp_orig": start_ts,
                    "AppTime": start_time,
                    "Timestamp_raw": Timestamp_raw,

                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_RPT_orig": start_ts,
                    "end_RPT_orig": start_ts,
                    #"start_AlignedTimestamp": #astart_ts,
                    #"end_AlignedTimestamp": #astart_ts,
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
                    if next_row["Type"] != "Event" or not isinstance(next_row["Message"], str):
                        break
                    msg = next_row["Message"]

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
                        # Observer chose CORRECT for this pindrop from the navigator 
                        # Observer chose INCORRECT for this pindrop from the navigator 
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
                synthetic = generate_synthetic_events_v3(start_ts, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process pin drop at row {i}: {e}")
            i = j
        else:
            i += 1
    return events

def process_pin_drop_v5(df, allowed_statuses):
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
                common_info = build_common_event_fields_full(row, i)

                start_time = row["AppTime"]
                start_ts = row["RPTimestamp_orig"]
                Timestamp_raw = row["Timestamp_raw"]

                event = {
                    "AppTime": start_time,
                    "RPTimestamp_orig": start_ts,
                    "AlignedTimestamp": astart_ts,
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_RPT_orig": start_ts,
                    "end_RPT_orig": start_ts,
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
                synthetic = generate_synthetic_events_v3(start_ts, offsets_events, common_info, event_meta)
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

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row["Message"].startswith("A.N. collected coin:"):
            try:
                common_info = build_common_event_fields_full(row, i)

                start_time = row["AppTime"]
                start_ts = row["RPTimestamp_orig"]
                Timestamp_raw = row["Timestamp_raw"]

                event = {
                    "AppTime": start_time,
                    "RPTimestamp_orig": start_ts,
                    #"AlignedTimestamp": #astart_ts,
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_RPT_orig": start_ts,
                    "end_RPT_orig": start_ts,
                    #"start_AlignedTimestamp": #astart_ts,
                    #"end_AlignedTimestamp": #astart_ts,
                    "lo_eventType": "CoinCollectMoment_PinDrop",
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

                synthetic = generate_synthetic_events_v3(start_ts, offsets_events, common_info, event_meta)
                events.extend(synthetic)
                

            except Exception as e:
                print(f"⚠️ Failed to parse feedback values at row {i}: {e}")

    return events


# -- Chest Opening Events

def process_chest_collect_v3(df, allowed_statuses):
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row["Message"].startswith("Other participant just collected coin: "):
            try:

                coin_id = int(row["Message"].replace("Other participant just collected coin: ", "").strip())
                common_info = build_common_event_fields_full(row, i)

                start_time = row["AppTime"]
                start_ts = row["RPTimestamp_orig"]
                Timestamp_raw = row["Timestamp_raw"]

                event = {
                    "AppTime": start_time,
                    "RPTimestamp_orig": start_ts,
                    ##"AlignedTimestamp": #astart_ts,
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_RPT_orig": start_ts,
                    "end_RPT_orig": start_ts,
                    ##"start_AlignedTimestamp": #astart_ts,
                    ##"end_AlignedTimestamp": #astart_ts,
                    "lo_eventType": "CoinCollectMoment_Chest",
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

                synthetic = generate_synthetic_events_v3(start_ts, offsets_events, common_info, event_meta)
                events.extend(synthetic)
                events.append(event)

            except Exception as e:
                print(f"⚠️ Failed to process chest open at row {i}: {e}")

    return events


def buildEvents_PO(df, allowed_statuses):
    try:
        # 1. Build cascades from all known sources
        cascades = (
            #process_block_periods_v4(df, allowed_statuses) +
            process_block_segments(df, allowed_statuses) +
            process_true_round_segments(df, allowed_statuses) +
            process_special_round_segments(df, allowed_statuses) +
            process_TrueBlocks(df, allowed_statuses) +
            process_pin_drop_v5(df, allowed_statuses) +
            process_feedback_collect_v5(df, allowed_statuses) +
            #process_chest_opened_v4(df, allowed_statuses) +
            process_chest_collect_v3(df, allowed_statuses) +
            process_marks(df, allowed_statuses, role='PO') +
            process_swap_votes_v4(df, allowed_statuses)
            #process_block_periods_v4(df, allowed_statuses)
        )

        # 2. Generate cascade windows from them
        all_events = backfill_approx_row_indices(cascades, df)
        print('yay')
        return all_events

    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise
