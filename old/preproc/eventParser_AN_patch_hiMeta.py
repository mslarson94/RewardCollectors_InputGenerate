# eventParser_AN_patch_hiMeta.py

import pandas as pd
import re
import os
from datetime import datetime, timedelta
from io import StringIO
import traceback

from revised_cascade_windows_utils import (find_cascade_windows_from_events_v2,
                                    match_cascade_window_v2,
                                    # assign_cascade_id, 
                                    extract_walking_periods_with_cascade_ids_v2, 
                                    generate_reward_walking_periods_v2,
                                    synthesize_reward_driven_walking_periods_v3,
                                    refine_reward_walking_periods_v2,
                                    build_common_event_fields,
                                    generate_synthetic_events_v2,
                                    safe_parse_timestamp)

# -- hiMeta_eventType patched 

def process_swap_votes_v3(df, allowed_statuses):
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
                    "hiMeta_eventType": "SwapVote",
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
                    "hi_eventType": "SwapVote",
                    "hiMeta_eventType": "SwapVote"
                }
                synthetic = generate_synthetic_events_v2(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process swap vote at row {i}: {e}")

    return events

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
                    common_info = build_common_event_fields(row, i)

                    events.append({
                        "AppTime": start_time,
                        "Timestamp": timestamp,
                        "lo_eventType": "SwapVoteMoment",
                        "med_eventType": "SwapVote",
                        "hi_eventType": "SwapVote",
                        "hiMeta_eventType": "SwapVote",
                        "details": {"SwapVote": swapvote},
                        "source": "logged",
                        **common_info
                    })

                    offsets_events = [
                        (0.000, "SwapVoteText_end"),
                        (0.000, "BlockScoreText_start"),
                        (2.000, "BlockScoreText_end")
                    ]
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


def process_block_periods_v2(df, allowed_statuses):
    events = []
    df = df.reset_index(drop=True)

    round_event_map = {
        0: ("PreBlock_CylinderWalk", "PreBlockActivity"),
        7777: ("InterRound_CylinderWalk", "BlockActivity"),
        8888: ("InterRound_PostCylinderWalk", "BlockActivity"),
        9999: ("InterBlock_Idle", "PostBlockActivity")
    }

    previous_round = None

    for idx, row in df.iterrows():
        round_code = row.get("RoundNum")

        if round_code in round_event_map and round_code != previous_round:
            lo_event, hi_meta = round_event_map[round_code]
            common_info = build_common_event_fields(row, idx)
            synthetic = generate_synthetic_events_v2(
                row.AppTime,
                row.Timestamp,
                [(0.0, f"{lo_event}_start")],
                common_info,
                {
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod",
                    "hiMeta_eventType": hi_meta
                }
            )
            events.extend(synthetic)

        previous_round = round_code

    return events

def process_block_periods_v3(df, allowed_statuses):
    events = []
    df = df.reset_index(drop=True)

    round_event_map = {
        0: ("PreBlock_CylinderWalk", "PreBlockActivity"),
        7777: ("InterRound_CylinderWalk", "BlockActivity"),
        8888: ("InterRound_PostCylinderWalk", "BlockActivity"),
        9999: ("InterBlock_Idle", "PostBlockActivity")
    }

    # Pre-group all rows by round code
    grouped = df.groupby("RoundNum")

    for round_code, (lo_event, hi_meta) in round_event_map.items():
        if round_code not in grouped.groups:
            continue

        rows = df.loc[grouped.groups[round_code]]

        # Start from first row in group
        start_row = rows.iloc[0]
        end_row = rows.iloc[-1]

        common_info = build_common_event_fields(start_row, start_row.name)

        # Generate start + end synthetic events
        synthetic = generate_synthetic_events_v2(
            start_row.AppTime,
            start_row.Timestamp,
            [
                (0.0, f"{lo_event}_start"),
                (end_row.AppTime - start_row.AppTime, f"{lo_event}_end")
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

def process_block_periods_v4(df, allowed_statuses):
    events = []
    df = df.reset_index(drop=True)

    round_event_map = {
        0: ("PreBlock_CylinderWalk", "PreBlockActivity"),
        7777: ("InterRound_CylinderWalk", "BlockActivity"),
        8888: ("InterRound_PostCylinderWalk", "BlockActivity"),
        9999: ("InterBlock_Idle", "PostBlockActivity")
    }

    # Pre-group all rows by round code
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

        common_info = build_common_event_fields(start_row, start_row.name)

        # Extend common_info to carry explicit timing
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
                (0.0, f"{lo_event}_start"),
                (end_time - start_time, f"{lo_event}_end")
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
            events.append({
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "lo_eventType": "Mark",
                "med_eventType": "ReferencePoint",
                "hi_eventType": "SystemEvent",
                "hiMeta_eventType": "Infrastructure",
                "details": {"mark": "A"},
                "start_AppTime": row.AppTime,
                "end_AppTime": row.AppTime,
                "start_Timestamp": row.Timestamp,
                "end_Timestamp": row.Timestamp,
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
                start_time = row["AppTime"]
                timestamp = row["Timestamp"]
                common_info = build_common_event_fields(row, i)
                event = {
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "PinDrop_Moment",
                    "med_eventType": "PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
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
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            common_info = build_common_event_fields(row, i)

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
                events.append({
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "CoinCollect_Moment_PinDrop",
                    "med_eventType": "CoinCollect_PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
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
                start_time = row["AppTime"]
                timestamp = row["Timestamp"]
                common_info = build_common_event_fields(row, i)
                coin_id = int(row.Message.replace("Chest opened: ", "").strip())

                events.append({
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "ChestOpen_Moment",
                    "med_eventType": "ChestOpen",
                    "hi_eventType": "ChestOpen",
                    "hiMeta_eventType": "BlockActivity",
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
                start_time = row["AppTime"]
                timestamp = row["Timestamp"]
                common_info = build_common_event_fields(row, i)

                events.append({
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "CoinCollect_Moment_Chest",
                    "med_eventType": "CoinCollect_Chest",
                    "hi_eventType": "ChestOpen",
                    "hiMeta_eventType": "BlockActivity",
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

def buildEvents_AN_v3_older(df, allowed_statuses):
    cascades = (
        process_pin_drop_v5(df, allowed_statuses) +
        process_feedback_collect_v5(df, allowed_statuses) +
        process_chest_opened_v4(df, allowed_statuses) +
        process_chest_collect_v3(df, allowed_statuses) +
        process_marks_v2(df, allowed_statuses) +
        process_swap_votes_v4(df, allowed_statuses) +
        process_block_periods_v3(df, allowed_statuses)
    )
    cascade_windows = find_cascade_windows_from_events_v2(cascades)

    # cascades = [
    #     {**e, **match_cascade_window(e, cascade_windows)} if match_cascade_window(e, cascade_windows) else e
    #     for e in cascades
    #     ] # previous version 

    updated_cascades = []
    for e in cascades:
        matched = match_cascade_window(e, cascade_windows)
        if matched:
            e.update(matched)
        else:
            e['cascade_id'] = None  # Ensure the key is present even if no match
        updated_cascades.append(e)
    cascades = updated_cascades


    walking_periods = extract_walking_periods_with_cascade_ids(df, cascade_windows)
    refinedWalkingPeriods = refine_reward_walking_periods(walking_periods)
    all_events = cascades + refinedWalkingPeriods
    return all_events

def buildEvents_AN_v3_old(df, allowed_statuses):
    cascades = (
        process_pin_drop_v5(df, allowed_statuses) +
        process_feedback_collect_v5(df, allowed_statuses) +
        process_chest_opened_v4(df, allowed_statuses) +
        process_chest_collect_v3(df, allowed_statuses) +
        process_marks_v2(df, allowed_statuses) +
        process_swap_votes_v4(df, allowed_statuses) +
        process_block_periods_v2(df, allowed_statuses)
    )
    cascade_windows = find_cascade_windows_from_events_v2(cascades)

    # cascades = [
    #     {**e, **match_cascade_window(e, cascade_windows)} if match_cascade_window(e, cascade_windows) else e
    #     for e in cascades
    #     ] # previous version 

    updated_cascades = []
    for e in cascades:
        matched = match_cascade_window(e, cascade_windows)
        if matched:
            e.update(matched)
        else:
            e['cascade_id'] = None  # Ensure the key is present even if no match
        updated_cascades.append(e)
    cascades = updated_cascades
    
    walking_periods = extract_walking_periods_with_cascade_ids(df, cascade_windows)
    walking_periods_gen = generate_reward_walking_periods(df, cascade_windows)
    # Ensure all walking periods get matched window data
    refined_walking_periods = []
    for wp in walking_periods_gen:
        matched = match_cascade_window(wp, cascade_windows)
        if matched:
            wp.update(matched)
        else:
            wp['cascade_id'] = None
        refined_walking_periods.append(wp)

    refinedWalkingPeriods = refine_reward_walking_periods(refined_walking_periods)
    refinedWalkingPeriods_gen = synthesize_reward_driven_walking_periods(refined_walking_periods)
    all_events = cascades + refinedWalkingPeriods_gen
    return all_events


def buildEvents_AN_v3_1(df, allowed_statuses):
    try: 
        cascades = (
            process_pin_drop_v5(df, allowed_statuses) +
            process_feedback_collect_v5(df, allowed_statuses) +
            process_chest_opened_v4(df, allowed_statuses) +
            process_chest_collect_v3(df, allowed_statuses) +
            process_marks_v2(df, allowed_statuses) +
            process_swap_votes_v4(df, allowed_statuses) +
            process_block_periods_v2(df, allowed_statuses)
        )
        cascade_windows = find_cascade_windows_from_events_v2(cascades)

        # Apply cascade metadata to all cascade events
        updated_cascades = []
        for e in cascades:
            matched = match_cascade_window(e, cascade_windows)
            if matched:
                e.update(matched)
            else:
                e['cascade_id'] = None
            updated_cascades.append(e)
        cascades = updated_cascades

        # Generate and synthesize walking periods
        walking_periods_gen = pd.DataFrame(generate_reward_walking_periods(df, cascades))
        refinedWalkingPeriods_gen = synthesize_reward_driven_walking_periods(walking_periods_gen)

        all_events = cascades + refinedWalkingPeriods_gen.to_dict("records")
        return all_events
    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise


def buildEvents_AN_v3_0516(df, allowed_statuses):
    # Collect all task-related event cascades
    try: 
        cascades = (
            process_pin_drop_v5(df, allowed_statuses) +
            process_feedback_collect_v5(df, allowed_statuses) +
            process_chest_opened_v4(df, allowed_statuses) +
            process_chest_collect_v3(df, allowed_statuses) +
            process_marks_v2(df, allowed_statuses) +
            process_swap_votes_v4(df, allowed_statuses) +
            process_block_periods_v2(df, allowed_statuses)
        )

        # Identify cascade windows from collected events
        cascade_windows = find_cascade_windows_from_events_v2(cascades)

        # Safely assign cascade IDs and meta info to all cascade events
        cascades_updated = []
        for e in cascades:
            matched = match_cascade_window(e, cascade_windows)
            if matched:
                e.update(matched)
            else:
                e['cascade_id'] = None
            cascades_updated.append(e)
        cascades = cascades_updated

        # Generate synthetic reward walking periods from logged transitions
        reward_walks = generate_reward_walking_periods(df, cascades)
        # refined_walks = synthesize_reward_driven_walking_periods(reward_walks, cascade_windows)
        refined_walks = synthesize_reward_driven_walking_periods(df, all_events)

        all_events = cascades + refined_walks
        return all_events
    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise



def buildEvents_AN_v3a(df, allowed_statuses):
    # Collect all task-related event cascades
    try:
        cascades = (
            process_pin_drop_v5(df, allowed_statuses) +
            process_feedback_collect_v5(df, allowed_statuses) +
            process_chest_opened_v4(df, allowed_statuses) +
            process_chest_collect_v3(df, allowed_statuses) +
            process_marks_v2(df, allowed_statuses) +
            process_swap_votes_v4(df, allowed_statuses) +
            process_block_periods_v2(df, allowed_statuses)
        )

        # Identify cascade windows from collected events
        cascade_windows = find_cascade_windows_from_events_v2(cascades)

        # Assign cascade IDs to events
        cascades_updated = []
        for e in cascades:
            matched = match_cascade_window(e, cascade_windows)
            if matched:
                e.update(matched)
            else:
                e["cascade_id"] = None
            cascades_updated.append(e)
        cascades = cascades_updated

        # Use cascade-tagged events as base for walking logic
        # reward_walks = generate_reward_walking_periods(df, cascades)
        # all_events = cascades + reward_walks  # define this FIRST
        # # refined_walks = synthesize_reward_driven_walking_periods(df, all_events)
        # refined_walks = synthesize_reward_driven_walking_periods(df, all_events).to_dict('records')
        # all_events = cascades + refined_walks
        # return all_events
        # After cascade detection
        reward_walks = generate_reward_walking_periods(df, cascades)
        refined_walks_df = synthesize_reward_driven_walking_periods(df, reward_walks)

        # Optional refinement (if using event_type to tag only)
        refined_walks = refine_reward_walking_periods(refined_walks_df.to_dict("records"))

        # Merge and drop internal-use field
        all_events = cascades + refined_walks
        for e in all_events:
            e.pop("event_type", None)

        return all_events


    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise


def buildEvents_AN_v3(df, allowed_statuses):
    try:
        # 1. Build cascades from all known sources
        cascades = (
            process_pin_drop_v5(df, allowed_statuses) +
            process_feedback_collect_v5(df, allowed_statuses) +
            process_chest_opened_v4(df, allowed_statuses) +
            process_chest_collect_v3(df, allowed_statuses) +
            process_marks_v2(df, allowed_statuses) +
            process_swap_votes_v4(df, allowed_statuses) +
            process_block_periods_v4(df, allowed_statuses)
        )

        # 2. Generate cascade windows from them
        cascade_windows = find_cascade_windows_from_events_v2(cascades)

        # 3. Assign cascade metadata to each event
        cascades_updated = []
        for e in cascades:
            matched = match_cascade_window(e, cascade_windows)
            if matched:
                e.update(matched)
            else:
                e["cascade_id"] = None
            cascades_updated.append(e)
        cascades = cascades_updated

        # 4. Generate and refine synthetic reward navigation events
        reward_walks = generate_reward_walking_periods(df, cascades)
        for rw in reward_walks:
            print(rw["lo_eventType"], rw.get("original_row_start"), rw.get("cascade_id"))
        print(f"Generated {len(reward_walks)} reward walks")

        refined_walks_df = synthesize_reward_driven_walking_periods(df, reward_walks)
        print(f"Generated {len(refined_walks_df)} refined walks df")
        refined_walks = refine_reward_walking_periods(refined_walks_df.to_dict("records"))
        print(f"Refined {len(refined_walks)} walking periods")
        # 5. Combine cascades + synthetic walking periods
        all_events = cascades + refined_walks

        # 6. Clean internal-use-only fields
        for e in all_events:
            e.pop("event_type", None)

        return all_events

    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise



def buildEvents_AN_v4(df, allowed_statuses):
    try:
        # 1. Build cascades from all known sources
        cascades = (
            process_pin_drop_v5(df, allowed_statuses) +
            process_feedback_collect_v5(df, allowed_statuses) +
            process_chest_opened_v4(df, allowed_statuses) +
            process_chest_collect_v3(df, allowed_statuses) +
            process_marks_v2(df, allowed_statuses) +
            process_swap_votes_v4(df, allowed_statuses) +
            process_block_periods_v4(df, allowed_statuses)
        )

        # 2. Generate cascade windows from them
        cascade_windows = find_cascade_windows_from_events_v3(cascades)

        # 3. Assign cascade metadata to each event
        cascades_updated = []
        for e in cascades:
            matched = match_cascade_window_v2(e, cascade_windows)
            if matched:
                e.update(matched)
            else:
                e["cascade_id"] = None
            cascades_updated.append(e)
        cascades = cascades_updated

        # 4. Generate and refine synthetic reward navigation events
        reward_walks = generate_reward_walking_periods_v2(df, cascades)
        for rw in reward_walks:
            print(rw["lo_eventType"], rw.get("original_row_start"), rw.get("cascade_id"))
        print(f"Generated {len(reward_walks)} reward walks")

        refined_walks_df = synthesize_reward_driven_walking_periods_v3(df, reward_walks)
        print(f"Generated {len(refined_walks_df)} refined walks df")
        refined_walks = refine_reward_walking_periods_v2(refined_walks_df.to_dict("records"))
        print(f"Refined {len(refined_walks)} walking periods")
        # 5. Combine cascades + synthetic walking periods
        all_events = cascades + refined_walks

        # 6. Clean internal-use-only fields
        for e in all_events:
            e.pop("event_type", None)

        return all_events

    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise
