# ---- START OF eventParser_AN_proposed_newSyntheticTools.py ----
# eventParser_AN_proposed_newSyntheticTools.py

import pandas as pd
import re
import os
from datetime import datetime, timedelta
from io import StringIO

from cascade_windows_utils import find_cascade_windows_from_events, assign_cascade_id, extract_walking_periods_with_cascade_ids, build_common_event_fields
from coin_utils import classify_coin_type, classify_swap_vote, process_marks


def safe_parse_timestamp(ts):
    try:
        return datetime.strptime(ts, '%H:%M:%S:%f')
    except Exception:
        return None

# --- Pin Drops and Coin Collection Events  ---

def process_pin_drop_v3(df,allowed_statuses):
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
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            common_info = build_common_event_fields(row, i)
            event = {
                "AppTime": start_time,
                "Timestamp": timestamp,
                "lo_eventType": "PinDropMoment",
                "mid_eventType": "PinDrop",
                "hi_eventType": "PinDrop",
                "details": {},
                "source": "logged",
                **common_info
            }

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
                        r"Closest location was:\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)\s*\|\s*actual distance:\s+([-+]?[0-9]*\.?[0-9]+)\s*\|\s*(good|bad) drop\s*\|\s*coinValue:\s+([-+]?[0-9]*\.?[0-9]+)"
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
                (0.000, "PinDropSound_start"),
                (0.000, "GrayPinVis_start"),
                (0.654, "PinDropSound_end"),
                (2.000, "GrayPinVis_end"),
                (2.000, "Feedback_Sound_start"),
                (2.000, "FeedbackTextVis_start"),
                (2.000, "FeedbackPinColor_start"),
                (3.000, "FeedbackTextVis_end"),
                (3.000, "FeedbackPinColor_end"),
                (3.000, "CoinVis_start"),
                (3.000, "CoinPresentSound_start"),
                (3.650, "CoinPresentSound_end"),
                (4.000, "Coin_Released")

                ]
                
            event_meta = {
                "mid_eventType": "FullFeedbackAnimation",
                "hi_eventType": "PinDrop"
                }

            synthetic = generate_synthetic_events(start_time, timestamp, offsets_events, common_info, event_meta)
            events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process chest open at row {i}: {e}")

            i = j
        else:
            i += 1

    return events

def process_feedback_collect_v4(df, allowed_statuses):
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Collected pin feedback coin:"):
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            common_info = build_common_event_fields(row, i)

            msg_body = row.Message.replace("Collected pin feedback coin:", "").replace(" round reward", "")
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
                events.append({
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "FeedbackCoinCollectMoment",
                    "med_eventType": "FeedbackCoinCollect",
                    "hi_eventType": "PinDrop",
                    "details": details,
                    "source": "logged",
                    **common_info
                })

                # Synthetic follow-ups
                offsets_events = [
                    (0.000, "CoinVis_end"),
                    (0.000, "CoinValueTextVis_start"),
                    (0.000, "CoinCollectSound_start"),
                    (0.654, "CoinCollectSound_end"),
                    (2.000, "CoinValueTextVis_end")
                    ]
                
                event_meta = {
                    "med_eventType": "FullFeedbackCoinCollect_Animation",
                    "hi_eventType": "PinDrop"
                    }

                synthetic = generate_synthetic_events(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to parse feedback values at row {i}: {e}")

    return events

def process_chest_opened_v3(df, allowed_statuses):
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Chest opened:"):
            try:
                start_time = row["AppTime"]
                timestamp = row["Timestamp"]
                common_info = build_common_event_fields(row, i)
                coin_id = int(row.Message.replace("Chest opened: ", "").strip())

                events.append({
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "ChestOpenMoment",
                    "med_eventType": "ChestOpen",
                    "hi_eventType": "ChestOpen",
                    "details": {"idvCoinID": coin_id},
                    "source": "logged",
                    **common_info
                })

                offsets_events = [
                    (0.000, "ChestOpenAnimation_start"),
                    (0.000, "ChestOpenSound_start"),
                    (0.400, "ChestOpenAnimation_end"),
                    (0.400, "ChestOpenSound_end"),
                    (0.400, "ChestOpenEmpty_start"),
                    (2.000, "ChestOpenEmpty_end"),
                    (2.000, "CoinVis_start"),
                    (2.000, "CoinPresentSound_start"),
                    (2.650, "CoinPresentSound_end"),
                    (3.000, "Coin_Released")
                    ]

                event_meta = {
                    "med_eventType": "FullChestOpenAnimation",
                    "hi_eventType": "ChestOpen"
                    }

                synthetic = generate_synthetic_events(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process chest open at row {i}: {e}")

    return events

def process_chest_collect_v2(df, allowed_statuses):
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("coin collected"):
            try:
                start_time = row["AppTime"]
                timestamp = row["Timestamp"]
                common_info = build_common_event_fields(row, i)

                events.append({
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "CoinCollectMoment_IE",
                    "med_eventType": "CoinCollect_IE",
                    "hi_eventType": "ChestOpen",
                    "details": {},
                    "source": "logged",
                    **common_info
                })

                offsets_events = [
                    (0.000, "CoinVis_end"),
                    (0.000, "ChestVis_end"),
                    (0.000, "CoinCollectSound_start"),
                    (0.000, "CoinValueTextVis_start"),
                    (0.000, "NextChestVisible"),
                    (0.654, "CoinCollectSound_end"),
                    (2.000, "CoinValueTextVis_end")
                ]
                event_meta = {
                    "med_eventType": "FullCoinCollectIE_Animation",
                    "hi_eventType": "ChestOpen"
                }
                synthetic = generate_synthetic_events(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process chest open at row {i}: {e}")

    return events

# --- Swap Votes ---

def process_swap_votes_v3(df, allowed_statuses):
    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Active Navigator says it was a "):
            swapvote = row.Message.replace("Active Navigator says it was a ", "").strip().upper()
            try:
                start_time = row["AppTime"]
                timestamp = row["Timestamp"]
                common_info = build_common_event_fields(row, i)

                events.append({
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "SwapVoteMoment",
                    "med_eventType": "SwapVote",
                    "hi_eventType": "SwapVote",
                    "details": {"SwapVote": swapvote},
                    "source": "logged",
                    **common_info
                })
                offsets_events = [
                    (0.000, "SwapVoteText_end"),
                    (0.000, "BlockScoreText_start"),
                    (2.000, "BlockScoreText_start")
                ]
                event_meta = {
                    "med_eventType": "FullPostSwapVoteEvents",
                    "hi_eventType": "SwapVote"
                }
                synthetic = generate_synthetic_events(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process chest open at row {i}: {e}")

    return events

# --- Walking Periods ---
def process_block_periods_v2(df, allowed_statuses):
    events = []
    df = df.reset_index(drop=True)

    # Mapping of round codes to event cascade tags
    round_event_map = {
        0: "PreBlock_CylinderWalk",
        7777: "InterRound_CylinderWalk",
        8888: "InterRound_PostCylinderWalk",
        9999: "InterBlock_Idle"
    }

    previous_round = None
    start_idx = None

    for idx, row in df.iterrows():
        round_code = row.get("RoundNum")
        msg = row.Message if isinstance(row.Message, str) else ""

        # Track and emit events for round-based mini-cascades
        if round_code in round_event_map:
            if round_code != previous_round:
                # End previous temporal cascade
                if previous_round in round_event_map and start_idx is not None:
                    events.append({
                        #"event_type": f"{round_event_map[previous_round]}_end",
                        "AppTime": df.at[idx - 1, "AppTime"],
                        "Timestamp": df.at[idx - 1, "Timestamp"],
                        "lo_eventType": f"{round_event_map[previous_round]}_end",
                        "med_eventType": "NonRewardDrivenNavigation",
                        "hi_eventType": "WalkingPeriod",
                        "details": {},
                        "source": "synthetic",
                        "original_row_start": df.at[idx - 1, "original_index"],
                        "original_row_end": df.at[idx - 1, "original_index"],
                        "BlockNum": df.at[idx - 1, "BlockNum"],
                        "RoundNum": df.at[idx - 1, "RoundNum"],
                        "CoinSetID": df.at[idx - 1, "CoinSetID"],
                        "BlockStatus": row.get("BlockStatus", "unknown"),
                        "chestPin_num": None,
                        "cascade_id": None
                    })
                # Start new one
                start_idx = idx
                events.append({
                    #"event_type": f"{round_event_map[round_code]}_start",
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "lo_eventType": f"{round_event_map[previous_round]}_start",
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod",
                    "details": {},
                    "source": "synthetic",
                    "original_row_start": df.at[idx, "original_index"],
                    "original_row_end": df.at[idx, "original_index"],
                    "BlockNum": row.BlockNum,
                    "RoundNum": row.RoundNum,
                    "CoinSetID": row.CoinSetID,
                    "BlockStatus": row.get("BlockStatus", "unknown"),
                    "chestPin_num": None,
                    "cascade_id": None
                })

        # End the final cascade if we're at the last row
        if idx == len(df) - 1 and round_code in round_event_map and start_idx is not None:
            events.append({
                #"event_type": f"{round_event_map[round_code]}_end",
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "lo_eventType": f"{round_event_map[previous_round]}_end",
                "med_eventType": "NonRewardDrivenNavigation",
                "hi_eventType": "WalkingPeriod",
                "details": {},
                "source": "synthetic",
                "original_row_start": df.at[idx, "original_index"],
                "original_row_end": df.at[idx, "original_index"],
                "BlockNum": row.BlockNum,
                "RoundNum": row.RoundNum,
                "CoinSetID": row.CoinSetID,
                "BlockStatus": row.get("BlockStatus", "unknown"),
                "chestPin_num": None,
                "cascade_id": None
            })

        previous_round = round_code

        # Original static structural events
        shared_fields = {
            "BlockNum": row.get("BlockNum", None),
            "RoundNum": row.get("RoundNum", None),
            "CoinSetID": row.get("CoinSetID", None),
            "BlockStatus": row.get("BlockStatus", "unknown"),
            "chestPin_num": None,
            "original_row_start": row.get("original_index", idx),
            "original_row_end": row.get("original_index", idx),
            "cascade_id": None
        }

        if msg == "Mark should happen if checked on terminal.":
            events.append({
                #"event_type": "PreBlock_BlueCylinderVisible_start",
                #"details": {},
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "lo_eventType": "PreBlock_BlueCylinderVisible_start",
                "med_eventType": "NonRewardDrivenNavigation",
                "hi_eventType": "WalkingPeriod",
                "details": {},
                **shared_fields
            })

        elif msg == "Repositioned and ready to start block or round":
            events.extend([
                {
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "lo_eventType": "PreBlock_BlueCylinderVisible_end",
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod",
                    "details": {},
                    **shared_fields
                },
                {
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "lo_eventType": "StartRoundText_visible_start",
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod",
                    "details": {},
                    **shared_fields
                }
            ])

        elif msg.startswith("Started"):
            events.extend([
                {
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "lo_eventType": "StartRoundText_visible_end",
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod",
                    "details": {},
                    **shared_fields
                },
                {
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "lo_eventType": "RoundInstructionText_visible_start",
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod",
                    "details": {},
                    **shared_fields
                },
                {
                    "AppTime": row.AppTime + 2.0,
                    "Timestamp": row.Timestamp,
                    "lo_eventType": "RoundInstructionText_visible_end",
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod",

                    "details": {},
                    "source": "synthetic",
                    "BlockNum": row.get("BlockNum", None),
                    "RoundNum": row.get("RoundNum", None),
                    "CoinSetID": row.get("CoinSetID", None),
                    "BlockStatus": row.get("BlockStatus", "unknown"),
                    "chestPin_num": None,
                    "original_row_start": row.get("original_index", idx),
                    "original_row_end": row.get("original_index", idx),
                    "cascade_id": None
                    **shared_fields
                }
            ])

    return events

# -- New Function

def buildEvents_AN_v3(df, allowed_statuses):
    cascades = (
        process_pin_drop_v3(df, allowed_statuses) +
        process_feedback_collect_v4(df, allowed_statuses) +
        process_chest_opened_v3(df, allowed_statuses) +
        process_chest_collect_v2(df, allowed_statuses) +
        process_marks(df, allowed_statuses) +
        process_swap_votes_v3(df, allowed_statuses) +
        process_block_periods_v2(df, allowed_statuses)
    )
    cascade_windows = find_cascade_windows_from_events(cascades)
    cascades = [assign_cascade_id(e, cascade_windows, debug=True) for e in cascades]
    walking_periods = extract_walking_periods_with_cascade_ids(df, cascade_windows)
    all_events = cascades + walking_periods
    return all_events
# ---- END OF eventParser_AN_proposed_newSyntheticTools.py ----

# ---- START OF AN_Parser_functions_that_are_preserved.py ----
def build_common_event_fields(row, index=None):
    """
    Constructs a standardized dictionary of shared event fields from a row.
    
    Parameters:
        row (pd.Series): A row from the dataframe representing an event.
        index (int or None): The row index for original_row_start/end. If None, infer from row name.
    
    Returns:
        dict: Common fields used across event definitions.
    """
    idx = index if index is not None else row.name
    return {
        "BlockNum": row.get("BlockNum", None),
        "RoundNum": row.get("RoundNum", None),
        "CoinSetID": row.get("CoinSetID", None),
        "BlockStatus": row.get("BlockStatus", "unknown"),
        "chestPin_num": row.get("chestPin_num", None),
        "original_row_start": row.get("original_index", idx),
        "original_row_end": row.get("original_index", idx),
        "cascade_id": None
    }

def generate_synthetic_events(base_time, timestamp_str, offsets_events, base_info, event_meta):
    """
    Generate synthetic events with specified time offsets.

    Parameters:
        base_time (float): Base AppTime from original event.
        timestamp_str (str): Timestamp string (e.g., '13:45:23:123').
        offsets_events (list of tuples): (offset_seconds, lo_eventType) pairs.
        base_info (dict): Shared event data (e.g., common_info).
        event_meta (dict): Keys like 'med_eventType', 'hi_eventType'.

    Returns:
        List[dict]: List of synthetic events.
    """
    synthetic_events = []
    try:
        base_timestamp = safe_parse_timestamp(timestamp_str)
        for offset, lo_evt in offsets_events:
            synthetic_time = base_timestamp + timedelta(seconds=offset)
            synthetic_events.append({
                "AppTime": base_time + offset,
                "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                "lo_eventType": lo_evt,
                "details": {},
                "source": "synthetic",
                **event_meta,
                **base_info
            })
    except Exception as e:
        print(f"⚠️ Failed to create synthetic event at {timestamp_str}: {e}")
    return synthetic_events


def safe_parse_timestamp(ts):
    try:
        return datetime.strptime(ts, '%H:%M:%S:%f')
    except Exception:
        return None




def process_swap_votes_v3(df, allowed_statuses):
    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Active Navigator says it was a "):
            swapvote = row.Message.replace("Active Navigator says it was a ", "").strip().upper()
            try:
                start_time = row["AppTime"]
                timestamp = row["Timestamp"]
                common_info = build_common_event_fields(row, i)

                events.append({
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "SwapVoteMoment",
                    "med_eventType": "SwapVote",
                    "hi_eventType": "SwapVote",
                    "details": {"SwapVote": swapvote},
                    "source": "logged",
                    **common_info
                })

                offsets_events = [
                    (0.000, "SwapVoteText_end"),
                    (0.000, "BlockScoreText_start"),
                    (2.000, "BlockScoreText_start")
                ]
                event_meta = {
                    "med_eventType": "FullPostSwapVoteEvents",
                    "hi_eventType": "SwapVote"
                }
                synthetic = generate_synthetic_events(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process swap vote at row {i}: {e}")

    return events




def process_block_periods_v2(df, allowed_statuses):
    events = []
    df = df.reset_index(drop=True)

    round_event_map = {
        0: "PreBlock_CylinderWalk",
        7777: "InterRound_CylinderWalk",
        8888: "InterRound_PostCylinderWalk",
        9999: "InterBlock_Idle"
    }

    previous_round = None

    for idx, row in df.iterrows():
        round_code = row.get("RoundNum")

        if round_code in round_event_map and round_code != previous_round:
            common_info = build_common_event_fields(row, idx)
            synthetic = generate_synthetic_events(
                row.AppTime,
                row.Timestamp,
                [(0.0, f"{round_event_map[round_code]}_start")],
                common_info,
                {
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod"
                }
            )
            events.extend(synthetic)

        previous_round = round_code

def process_marks(df, allowed_statuses):
    events = []
    for idx, row in df.iterrows():
        if isinstance(row.Message, str) and "Sending Headset mark" in row.Message:
            common_info = build_common_event_fields(row, idx)
            events.append({
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "lo_eventType": "Mark",  # Aligning naming convention with other event types
                "med_eventType": "SystemSignal",
                "hi_eventType": "Infrastructure",
                "details": {"mark": "A"},
                "source": "logged",
                **common_info
            })
    return events


    return events

# ---- END OF AN_Parser_functions_that_are_preserved.py ----

# ---- START OF eventParser_AN_proposed_v2.py ----
# eventParser_AN_proposed.py

import pandas as pd
import re
import os
from datetime import datetime, timedelta
from io import StringIO

from cascade_windows_utils import (find_cascade_windows_from_events, 
                                    assign_cascade_id, 
                                    extract_walking_periods_with_cascade_ids, 
                                    build_common_event_fields,
                                    safe_parse_timestamp,
                                    load_filtered_df,
                                    generate_synthetic_events)

from coin_utils import classify_coin_type, classify_swap_vote

# --- Pin Drops and Coin Collection Events  ---
def process_pin_drop_v2(df,allowed_statuses):
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
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            common_info = build_common_event_fields(row, i)
            event = {
                "AppTime": start_time,
                "Timestamp": timestamp,
                "lo_eventType": "PinDropMoment",
                "mid_eventType": "PinDrop",
                "hi_eventType": "PinDrop",
                "details": {},
                "source": "logged",
                **common_info
            }

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
            
            for offset, evt in [
                (0.000, "PinDropSound_start"),
                (0.000, "GrayPinVis_start"),
                (0.654, "PinDropSound_end"),
                (2.000, "GrayPinVis_end"),
                (2.000, "Feedback_Sound_start"),
                (2.000, "FeedbackTextVis_start"),
                (2.000, "FeedbackPinColor_start"),
                (3.000, "FeedbackTextVis_end"),
                (3.000, "FeedbackPinColor_end"),
                (3.000, "CoinVis_start"),
                (3.000, "CoinPresentSound_start"),
                (3.650, "CoinPresentSound_end"),
                (4.000, "Coin_Released")
            ]:
                try:
                    #synthetic_time = datetime.strptime(timestamp, '%H:%M:%S:%f') + timedelta(seconds=offset)
                    synthetic_time = safe_parse_timestamp(timestamp) + timedelta(seconds=offset)
                    events.append({
                        "AppTime": start_time + offset,
                        "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                        "lo_eventType": evt,
                        "mid_eventType": "FullFeedbackAnimation",
                        "hi_eventType": "PinDrop",
                        "details": {},
                        "source": "synthetic",
                        **common_info
                    })
                except Exception as e:
                    print(f"⚠️ Failed to create synthetic event {evt} at row {i}: {e}")

            i = j
        else:
            i += 1

    return events

def process_feedback_collect_v3(df, allowed_statuses):
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Collected pin feedback coin:"):
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            common_info = build_common_event_fields(row, i)

            msg_body = row.Message.replace("Collected pin feedback coin:", "").replace(" round reward", "")
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
                events.append({
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "FeedbackCoinCollectMoment",
                    "med_eventType": "FeedbackCoinCollect",
                    "hi_eventType": "PinDrop",
                    "details": details,
                    "source": "logged",
                    **common_info
                })

                # Synthetic follow-ups
                for offset, evt in [
                    (0.000, "CoinVis_end"),
                    (0.000, "CoinValueTextVis_start"),
                    (0.000, "CoinCollectSound_start"),
                    (0.654, "CoinCollectSound_end"),
                    (2.000, "CoinValueTextVis_end")
                ]:
                    try:
                        #synthetic_time = datetime.strptime(timestamp, '%H:%M:%S:%f') + timedelta(seconds=offset)
                        synthetic_time = safe_parse_timestamp(timestamp) + timedelta(seconds=offset)
                        events.append({
                            "AppTime": start_time + offset,
                            "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                            "lo_eventType": evt,
                            "med_eventType": "FullFeedbackCoinCollect_Animation",
                            "hi_eventType": "PinDrop",
                            "details": {},
                            "source": "synthetic",
                            **common_info
                        })
                    except Exception as e:
                        print(f"⚠️ Failed to create synthetic event {evt} at row {i}: {e}")

            except Exception as e:
                print(f"⚠️ Failed to parse feedback values at row {i}: {e}")

    return events

def process_chest_opened_v2(df, allowed_statuses):
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Chest opened:"):
            try:
                start_time = row["AppTime"]
                timestamp = row["Timestamp"]
                common_info = build_common_event_fields(row, i)
                coin_id = int(row.Message.replace("Chest opened: ", "").strip())

                events.append({
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "ChestOpenMoment",
                    "med_eventType": "ChestOpen",
                    "hi_eventType": "ChestOpen",
                    "details": {"idvCoinID": coin_id},
                    "source": "logged",
                    **common_info
                })

                for offset, evt in [
                    (0.000, "ChestOpenAnimation_start"),
                    (0.000, "ChestOpenSound_start"),
                    (0.400, "ChestOpenAnimation_end"),
                    (0.400, "ChestOpenSound_end"),
                    (0.400, "ChestOpenEmpty_start"),
                    (2.000, "ChestOpenEmpty_end"),
                    (2.000, "CoinVis_start"),
                    (2.000, "CoinPresentSound_start"),
                    (2.650, "CoinPresentSound_end"),
                    (3.000, "Coin_Released")
                ]:
                    synthetic_time = safe_parse_timestamp(timestamp) + timedelta(seconds=offset)
                    events.append({
                        "AppTime": start_time + offset,
                        "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                        "lo_eventType": evt,
                        "med_eventType": "FullChestOpenAnimation",
                        "hi_eventType": "ChestOpen",
                        "details": {},
                        "source": "synthetic",
                        **common_info
                    })

            except Exception as e:
                print(f"⚠️ Failed to process chest open at row {i}: {e}")

    return events

def process_chest_collect(df, allowed_statuses):
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("coin collected"):
            try:
                start_time = row["AppTime"]
                timestamp = row["Timestamp"]
                common_info = build_common_event_fields(row, i)

                events.append({
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "CoinCollectMoment_IE",
                    "med_eventType": "CoinCollect_IE",
                    "hi_eventType": "ChestOpen",
                    "details": {},
                    "source": "logged",
                    **common_info
                })

                for offset, evt in [
                    (0.000, "CoinVis_end"),
                    (0.000, "ChestVis_end"),
                    (0.000, "CoinCollectSound_start"),
                    (0.000, "CoinValueTextVis_start"),
                    (0.000, "NextChestVisible"),
                    (0.654, "CoinCollectSound_end"),
                    (2.000, "CoinValueTextVis_end")
                ]:
                    synthetic_time = safe_parse_timestamp(timestamp) + timedelta(seconds=offset)
                    events.append({
                        "AppTime": start_time + offset,
                        "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                        "lo_eventType": evt,
                        "med_eventType": "FullCoinCollectIE_Animation",
                        "hi_eventType": "ChestOpen",
                        "details": {},
                        "source": "synthetic",
                        **common_info
                    })

            except Exception as e:
                print(f"⚠️ Failed to process chest coin collect at row {i}: {e}")

    return events

# --- Swap Votes ---

def process_swap_votes_v2(df, allowed_statuses):
    events = []
    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Active Navigator says it was a "):
            swapvote = row.Message.replace("Active Navigator says it was a ", "").strip().upper()
            try:
                start_time = row["AppTime"]
                timestamp = row["Timestamp"]
                common_info = build_common_event_fields(row, i)

                events.append({
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "SwapVoteMoment",
                    "med_eventType": "SwapVote",
                    "hi_eventType": "SwapVote",
                    "details": {"SwapVote": swapvote},
                    "source": "logged",
                    **common_info
                })
                for offset, evt in [
                        (0.000, "SwapVoteText_end"),
                        (0.000, "BlockScoreText_start"),
                        (2.000, "BlockScoreText_start")
                    ]:
                        synthetic_time = safe_parse_timestamp(timestamp) + timedelta(seconds=offset)
                        events.append({
                            "AppTime": start_time + offset,
                            "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                            "lo_eventType": evt,
                            "med_eventType": "FullPostSwapVoteEvents",
                            "hi_eventType": "SwapVote",
                            "details": {},
                            "source": "synthetic",
                            **common_info
                        })

            except Exception as e:
                print(f"⚠️ Failed to process swap vote at row {i}: {e}")

    return events

# --- Walking Periods ---
# def process_block_periods_v2(df, allowed_statuses):
#     events = []
#     df = df.reset_index(drop=True)

#     # Mapping of round codes to event cascade tags
#     round_event_map = {
#         0: "PreBlock_CylinderWalk",
#         7777: "InterRound_CylinderWalk",
#         8888: "InterRound_PostCylinderWalk",
#         9999: "InterBlock_Idle"
#     }

#     previous_round = None
#     start_idx = None

#     for idx, row in df.iterrows():
#         round_code = row.get("RoundNum")
#         msg = row.Message if isinstance(row.Message, str) else ""

#         # Track and emit events for round-based mini-cascades
#         if round_code in round_event_map:
#             if round_code != previous_round:
#                 # End previous temporal cascade - Why am I doing this again? 
#                 if previous_round in round_event_map and start_idx is not None:
#                     events.append({
#                         "AppTime": df.at[idx - 1, "AppTime"],
#                         "Timestamp": df.at[idx - 1, "Timestamp"],
#                         "lo_eventType": f"{round_event_map[previous_round]}_end",
#                         "med_eventType": "NonRewardDrivenNavigation",
#                         "hi_eventType": "WalkingPeriod",
#                         "details": {},
#                         "source": "synthetic",
#                         "original_row_start": df.at[idx - 1, "original_index"],
#                         "original_row_end": df.at[idx - 1, "original_index"],
#                         "BlockNum": df.at[idx - 1, "BlockNum"],
#                         "RoundNum": df.at[idx - 1, "RoundNum"],
#                         "CoinSetID": df.at[idx - 1, "CoinSetID"],
#                         "BlockStatus": row.get("BlockStatus", "unknown"),
#                         "chestPin_num": None,
#                         "cascade_id": None
#                     })
#                 # Start new one
#                 start_idx = idx
#                 events.append({
#                     #"event_type": f"{round_event_map[round_code]}_start",
#                     "AppTime": row.AppTime,
#                     "Timestamp": row.Timestamp,
#                     "lo_eventType": f"{round_event_map[round_code]}_start",
#                     "med_eventType": "NonRewardDrivenNavigation",
#                     "hi_eventType": "WalkingPeriod",
#                     "details": {},
#                     "source": "synthetic",
#                     "original_row_start": df.at[idx, "original_index"],
#                     "original_row_end": df.at[idx, "original_index"],
#                     "BlockNum": row.BlockNum,
#                     "RoundNum": row.RoundNum,
#                     "CoinSetID": row.CoinSetID,
#                     "BlockStatus": row.get("BlockStatus", "unknown"),
#                     "chestPin_num": None,
#                     "cascade_id": None
#                 })

#         # End the final cascade if we're at the last row
#         if idx == len(df) - 1 and round_code in round_event_map and start_idx is not None:
#             events.append({
#                 "AppTime": row.AppTime,
#                 "Timestamp": row.Timestamp,
#                 "lo_eventType": f"{round_event_map[previous_round]}_end",
#                 "med_eventType": "PostTaskIdle", # unsure if I should even be editing the XX_eventType fields
#                 "hi_eventType": "WalkingPeriod",
#                 "details": {},
#                 "source": "synthetic",
#                 "original_row_start": df.at[idx, "original_index"],
#                 "original_row_end": df.at[idx, "original_index"],
#                 "BlockNum": row.BlockNum,
#                 "RoundNum": row.RoundNum,
#                 "CoinSetID": row.CoinSetID,
#                 "BlockStatus": row.get("BlockStatus", "unknown"),
#                 "chestPin_num": None,
#                 "cascade_id": None
#             })

#         previous_round = round_code

#         # Original static structural events
#         shared_fields = {
#             "BlockNum": row.get("BlockNum", None),
#             "RoundNum": row.get("RoundNum", None),
#             "CoinSetID": row.get("CoinSetID", None),
#             "BlockStatus": row.get("BlockStatus", "unknown"),
#             "chestPin_num": None,
#             "original_row_start": row.get("original_index", idx),
#             "original_row_end": row.get("original_index", idx),
#             "cascade_id": None
#         }

#         if msg == "Mark should happen if checked on terminal.":
#             # Technical beginning of the block, a pre-task idle period. 
#             # Also a period where experimenters need to keep the participants in a holding pattern for 
#             # actual marks to be sent to both headsets
#             events.append({
#                 "AppTime": row.AppTime,
#                 "Timestamp": row.Timestamp,
#                 "lo_eventType": "PreBlock_BlueCylinderVisible_start",
#                 "med_eventType": "PreTaskIdle",
#                 "hi_eventType": "WalkingPeriod",
#                 "details": {},
#                 **shared_fields
#             })

#         elif msg == "Repositioned and ready to start block or round":
#             # Beginning of the actual round after the participant has finished their non-rewarded goal-driven navigation to their cylinder
#             # Marking the beginning of reward driven navigation
#             events.extend([
#                 {
#                     "AppTime": row.AppTime,
#                     "Timestamp": row.Timestamp,
#                     "lo_eventType": "PreBlock_BlueCylinderVisible_end",
#                     "med_eventType": "NonRewardDrivenNavigation",
#                     "hi_eventType": "WalkingPeriod",
#                     "details": {},
#                     **shared_fields
#                 },
#                 {
#                     "AppTime": row.AppTime,
#                     "Timestamp": row.Timestamp,
#                     "lo_eventType": "StartRoundText_visible_start",
#                     "med_eventType": "NonRewardDrivenNavigation",
#                     "hi_eventType": "WalkingPeriod",
#                     "details": {},
#                     **shared_fields
#                 }
#             ])

#         elif msg.startswith("Started"):
#             # Beginning of the actual task, beginning of non-rewarded goal-driven navigation
#             events.extend([
#                 {
#                     "AppTime": row.AppTime,
#                     "Timestamp": row.Timestamp,
#                     "lo_eventType": "StartRoundText_visible_end",
#                     "med_eventType": "NonRewardGoalDrivenNavigation",
#                     "hi_eventType": "WalkingPeriod",
#                     "details": {},
#                     **shared_fields
#                 },
#                 {
#                     "AppTime": row.AppTime,
#                     "Timestamp": row.Timestamp,
#                     "lo_eventType": "RoundInstructionText_visible_start",
#                     "med_eventType": "NonRewardGoalDrivenNavigation",
#                     "hi_eventType": "WalkingPeriod",
#                     "details": {},
#                     **shared_fields
#                 },
#                 {
#                     "AppTime": row.AppTime + 2.0,
#                     "Timestamp": row.Timestamp,
#                     "lo_eventType": "RoundInstructionText_visible_end",
#                     "med_eventType": "NonRewardGoalDrivenNavigation",
#                     "hi_eventType": "WalkingPeriod",

#                     "details": {},
#                     "source": "synthetic",
#                     "BlockNum": row.get("BlockNum", None),
#                     "RoundNum": row.get("RoundNum", None),
#                     "CoinSetID": row.get("CoinSetID", None),
#                     "BlockStatus": row.get("BlockStatus", "unknown"),
#                     "chestPin_num": None,
#                     "original_row_start": row.get("original_index", idx),
#                     "original_row_end": row.get("original_index", idx),
#                     "cascade_id": None
#                     # **shared_fields
#                 }
#             ])

#     return events

def process_block_periods_v3(df, allowed_statuses):
    events = []
    df = df.reset_index(drop=True)

    round_event_map = {
        0: "PreBlock_CylinderWalk",
        7777: "InterRound_CylinderWalk",
        8888: "InterRound_PostCylinderWalk",
        9999: "InterBlock_Idle"
    }

    previous_round = None

    for idx, row in df.iterrows():
        round_code = row.get("RoundNum")

        if round_code in round_event_map and round_code != previous_round:
            common_info = build_common_event_fields(row, idx)
            synthetic = generate_synthetic_events(
                row.AppTime,
                row.Timestamp,
                [(0.0, f"{round_event_map[round_code]}_start")],
                common_info,
                {
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod"
                }
            )
            events.extend(synthetic)

        previous_round = round_code

    return events

def process_marks(df, allowed_statuses):
    events = []
    for idx, row in df.iterrows():
        if isinstance(row.Message, str) and "Sending Headset mark" in row.Message:
            common_info = build_common_event_fields(row, idx)
            events.append({
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "lo_eventType": "Mark",  # Aligning naming convention with other event types
                "med_eventType": "SystemSignal",
                "hi_eventType": "Infrastructure",
                "details": {"mark": "A"},
                "source": "logged",
                **common_info
            })
    return events


# -- New Function

def buildEvents_AN_v2(df, allowed_statuses):
    cascades = (
        process_chest_opened_v2(df, allowed_statuses) +
        process_chest_collect(df, allowed_statuses) +
        process_pin_drop_v2(df, allowed_statuses) +
        process_feedback_collect_v3(df, allowed_statuses) +
        process_marks(df, allowed_statuses) +
        process_swap_votes_v2(df, allowed_statuses) +
        process_block_periods_v3(df, allowed_statuses)
    )

    cascade_windows = find_cascade_windows_from_events(cascades)
    cascades = [assign_cascade_id(e, cascade_windows, debug=True) for e in cascades]
    walking_periods = extract_walking_periods_with_cascade_ids(df, cascade_windows)
    all_events = cascades + walking_periods
    return all_events

# ---- END OF eventParser_AN_proposed_v2.py ----

