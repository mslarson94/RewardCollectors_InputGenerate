## muscles_eventParser_AN.py 

'''
Author: Myra Sarai Larson   08/18/2025
   This is adding a good chunk of event parsing that adds the bulk of the synthetic events we will create
   (synthetic events relating to in task events that aren't overtly declared in log files). There is a ton of 
   heavy lifting needed here (hence the muscles name), because this script is telling us things like : When
   did the Active Navigator see the chest open? When did they drop their 2nd pin in round 53? When did they submit
   their swap vote? etc... 

   As of right now, I need to figure out if any of these functions rely on any "Timestamp" or "ParsedTimestamp" columns
   and if so, swap those out for the "RobustTimestamp" column. Also, completely nix the safeParseTimestamp function use if it used. 
   
   I also need to figure out what helper functions are being called (because I've separated my original eventParserHelper_AN.py 
   script into my glia_ & schwannCell_ variants.)

'''
import pandas as pd
import re
import os
from datetime import datetime, timedelta
from io import StringIO
import traceback


from schwannCells_eventsParserHelper_AN import (build_common_event_fields_noTime,
                                                backfill_approx_row_indices_v2, 
                                                generate_synthetic_events_v3)
# do we need build_segment_event?

###########################

def process_swap_votes_v4(df, allowed_statuses):
    #print('starting swap votes')
    events = []
    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str):
            match = re.match(r"Active Navigator says it was an? (\w+) round\.", row["Message"])
            if match:
                swapvote = match.group(1).strip().upper()
                try:
                    appTime = row["AppTime"]
                    mLTimestamp_raw = row["mLTimestamp_raw"]
                    start_ts = row["mLTimestamp"]
                    common_info = build_common_event_fields_noTime(row, i)

                    # Determine correctness of vote
                    coinset = row.get("CoinSetID")
                    if coinset in [1, 4]:
                        correct_answer = "OLD"
                    elif coinset in [2, 3, 5]:
                        correct_answer = "NEW"
                    else:
                        correct_answer = None  # or raise a warning

                    if correct_answer is not None:
                        score = "Correct" if swapvote.upper() == correct_answer else "Incorrect"
                    else:
                        score = "Unknown"


                    events.append({
                        "mLTimestamp": start_ts,
                        "AppTime": appTime,
                        "mLTimestamp_raw": mLTimestamp_raw,

                        "start_AppTime": appTime,
                        "end_AppTime": appTime,
                        "start_mLT": start_ts,
                        "end_mLT": start_ts,

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
                    #events.append(event)
                    offsets_events = [
                        ("SwapVoteText_Vis_end", 0.000, 0.000),
                        ("BlockScoreText_Vis_start", 0.000, 2.000)
                    ]

                    event_meta = {
                        "med_eventType": "PostSwapVoteEvents",
                        "hi_eventType": "SwapVote",
                        "hiMeta_eventType": "SwapVote"
                    }
                    synthetic = generate_synthetic_events_v3(start_ts, appTime, offsets_events, common_info, event_meta)
                    events.extend(synthetic)

                except Exception as e:
                    print(f"⚠️ Failed to process swap vote at row {i}: {e}")
    return events

# -- Pin Dropping Events

def process_pin_drop_v5(df,allowed_statuses):
    #print('starting process pin drop')
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
                common_info = build_common_event_fields_noTime(row, i)

                start_time = row["AppTime"]
                start_ts = row["mLTimestamp"]
                mLTimestamp_raw = row["mLTimestamp_raw"]


                event = {
                    "mLTimestamp": start_ts,
                    "AppTime": start_time,
                    "mLTimestamp_raw": mLTimestamp_raw,
                    
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_mLT": start_ts,
                    "end_mLT": start_ts,

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
                # j = i + 1 Loop: pumLose is to gather messages tied to the current pin drop.
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
                                    "curmLerfRoundNum": int(parts[2]),
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

                synthetic = generate_synthetic_events_v3(start_ts, start_time, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process pin drop at row {i}: {e}")

            i = j
        else:
            i += 1
    #print('ending process pin drop')
    return events

def process_feedback_collect_v5(df, allowed_statuses):
    #print('starting process feedback collect')
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue
            #Collected pin feedback coin: 2
            #Collected feedback coin:0.00 round reward: 0.00
        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Collected feedback coin:"):
            common_info = build_common_event_fields_noTime(row, i)
            start_time = row["AppTime"]
            start_ts = row["mLTimestamp"]
            mLTimestamp_raw = row["mLTimestamp_raw"]


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
                    "mLTimestamp": start_ts,
                    "AppTime": start_time,
                    "mLTimestamp_raw": mLTimestamp_raw,
                    
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_mLT": start_ts,
                    "end_mLT": start_ts,

                    "lo_eventType": "CoinCollect_Moment_PinDrop",
                    "med_eventType": "CoinCollect_PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                    "details": {},
                    "source": "logged",
                    **common_info
                }
                events.append(event)
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

                synthetic = generate_synthetic_events_v3(start_ts, start_time, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to parse feedback values at row {i}: {e}")
    #print('ending process feedback collect')
    return events

# -- Chest Opening Events

def process_chest_opened_v4(df, allowed_statuses):
    #print('starting process chest opened')
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Chest opened:"):
            try:
                common_info = build_common_event_fields_noTime(row, i)
                coin_id = int(row.Message.replace("Chest opened: ", "").strip())
                start_time = row["AppTime"]
                start_ts = row["mLTimestamp"]
                mLTimestamp_raw = row["mLTimestamp_raw"]


                event = {
                    "mLTimestamp": start_ts,
                    "AppTime": start_time,
                    "mLTimestamp_raw": mLTimestamp_raw,
                    
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_mLT": start_ts,
                    "end_mLT": start_ts,

                    "lo_eventType": "ChestOpen_Moment",
                    "med_eventType": "ChestOpen",
                    "hi_eventType": "ChestOpen",
                    "hiMeta_eventType": "BlockActivity",
                    "details": {},
                    "source": "logged",
                    **common_info
                }
                events.append(event)
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

                synthetic = generate_synthetic_events_v3(start_ts, start_time, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process chest open at row {i}: {e}")
    #print('ending process chest opened')
    return events

def process_chest_collect_v3(df, allowed_statuses):
    #print('starting process chest collect')
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("coin collected"):
            try:
                common_info = build_common_event_fields_noTime(row, i)
                start_time = float(row["AppTime"])
                start_ts = row["mLTimestamp"]
                mLTimestamp_raw = row["mLTimestamp_raw"]

 
                event = {
                    "mLTimestamp": start_ts,
                    "AppTime": start_time,
                    "mLTimestamp_raw": mLTimestamp_raw,
                    
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_mLT": start_ts,
                    "end_mLT": start_ts,

                    "lo_eventType": "CoinCollect_Moment_Chest",
                    "med_eventType": "CoinCollect_Chest",
                    "hi_eventType": "ChestOpen",
                    "hiMeta_eventType": "BlockActivity",
                    "details": {},
                    "source": "logged",
                    **common_info
                }
                events.append(event)
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
                synthetic = generate_synthetic_events_v3(start_ts, start_time, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process chest coin collect at row {i}: {e}")
    #print('ending process chest collect')
    return events

def buildEvents_AN_v4(df, allowed_statuses):
    try:
        # 1. Build cascades from all known sources
        cascades = (
            process_pin_drop_v5(df, allowed_statuses) +
            process_feedback_collect_v5(df, allowed_statuses) +
            process_chest_opened_v4(df, allowed_statuses) +
            process_chest_collect_v3(df, allowed_statuses) +
            process_swap_votes_v4(df, allowed_statuses)
        )

        # 2. Generate cascade windows from them
        all_events = backfill_approx_row_indices_v2(cascades, df)

        return all_events

    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise
