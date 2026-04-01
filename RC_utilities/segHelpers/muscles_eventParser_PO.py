## muscles_eventParser_PO.py 

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

## schwannCells_eventsParserHelper_AN
from RC_utilities.segHelpers.schwannCells_eventParserHelper import (
    build_common_event_fields_noTime,
    backfill_approx_row_indices_v2, 
    generate_synthetic_events_v3)


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
            match = re.match(r"Observer says it was an?\s+(.*)\.", row["Message"])
            if match:
                swapvote = match.group(1).strip().upper()
                try:
                    appTime = row["AppTime"]
                    mLT_raw = row["mLT_raw"]
                    start_ts = row["eMLT_orig"]
                    mLT_orig = row["mLT_orig"]
                    common_info = build_common_event_fields_noTime(row, i)

                    # Determine correctness of vote
                    coinset = row.get("CoinSetID")
                    if coinset in [1, 4]:
                        correct_answer = "OLD ROUND"
                    elif coinset in [2, 3, 5]:
                        correct_answer = "NEW ROUND"
                    else:
                        correct_answer = None  # or raise a warning

                    if correct_answer is not None:
                        score = "Correct" if swapvote.upper() == correct_answer else "Incorrect"
                    else:
                        score = "Unknown"


                    events.append({
                        "eMLT_orig": start_ts,
                        "AppTime": appTime,
                        "mLT_raw": mLT_raw,
                        "mLT_orig": mLT_orig,


                        "start_AppTime": appTime,
                        "end_AppTime": appTime,
                        "start_eMLT_orig": start_ts,
                        "end_eMLT_orig": start_ts,

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

        if row["Type"] == "Event" and isinstance(row["Message"], str) and "Other participant just dropped a new pin at" in row["Message"]:
            try:
                common_info = build_common_event_fields_noTime(row, i)

                start_time = row["AppTime"]
                start_ts = row["eMLT_orig"]
                mLT_raw = row["mLT_raw"]
                mLT_orig = row["mLT_orig"]


                event = {
                    "eMLT_orig": start_ts,
                    "AppTime": start_time,
                    "mLT_raw": mLT_raw,
                    "mLT_orig": mLT_orig,
                    
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_eMLT_orig": start_ts,
                    "end_eMLT_orig": start_ts,

                    "lo_eventType": "PinDrop_Moment",
                    "med_eventType": "PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                    "source": "logged",
                    "details": {},
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
                        else:
                            print(f"⚠️ Regex mismatch for pin location at row {j}: {msg}")
                    
                    elif "Dropped pin was dropped at " in msg:
                        match = re.search(
                            r"Dropped pin was dropped at (?P<dropDist>\d+\.\d{2}) from chest (?P<idvCoinID>\d+) originally at \((?P<coinPos_x>-?\d+\.\d{2}),(?P<coinPos_y>-?\d+\.\d{2}),(?P<coinPos_z>-?\d+\.\d{2})\):(?P<dropQual>CORRECT|INCORRECT)",
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

                    # --- Current Round Number, Current Perfect Round Number, Running Round Total, Running Grand Total ---
                    # Example line: "Dropped a bad pin|0|0|0.00|0.00"
                    
                    elif "for this pindrop from the navigator" in msg and "Observer" in msg:
                        # Three possibilities:
                        # 1) Observer did not vote for this pindrop from the navigator
                        # 2) Observer chose CORRECT for this pindrop from the navigator
                        # 3) Observer chose INCORRECT for this pindrop from the navigator

                        m_vote = re.search(
                            r"Observer chose (?P<pinDropVote>CORRECT|INCORRECT) for this pindrop from the navigator",
                            msg
                        )
                        if m_vote:
                            event["details"].update(m_vote.groupdict())
                            break  # terminal line for this pin drop

                        if "Observer did not vote for this pindrop from the navigator" in msg:
                            event["details"]["pinDropVote"] = "DID_NOT_VOTE"
                            break  # terminal line for this pin drop

                    j += 1

                events.append(event)
                # --- Pin Drop Synthetic Events ---
                # What happens immediately after the triggering line "Just dropped a pin"
                
                offsets_events = [
                    ("PinDropSound", 0.000, 0.403),
                    ("GrayPinVis", 0.000, 2.000),
                    ("VotingWindow", 0.000, 2.000),
                    ("VoteInstrText_Vis_start", 0.000, 0.000),
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


def process_pin_drop_v6(df, allowed_statuses):
    events = []
    i = 0

    def _is_event_type(v) -> bool:
        return isinstance(v, str) and v.strip().lower() == "event"

    while i < len(df):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            i += 1
            continue

        msg0 = row.get("Message")
        if _is_event_type(row.get("Type")) and isinstance(msg0, str) and "Other participant just dropped a new pin at" in msg0:
            try:
                common_info = build_common_event_fields_noTime(row, i)

                start_time = row["AppTime"]
                start_ts = row["eMLT_orig"]

                event = {
                    "eMLT_orig": start_ts,
                    "AppTime": start_time,
                    "mLT_raw": row["mLT_raw"],
                    "mLT_orig": row["mLT_orig"],
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_eMLT_orig": start_ts,
                    "end_eMLT_orig": start_ts,
                    "lo_eventType": "PinDrop_Moment",
                    "med_eventType": "PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                    "source": "logged",
                    "details": {},
                    **common_info,
                }

                # ✅ parse the triggering line (row i)
                m0 = re.search(
                    r"at\s+([-+]?\d*\.?\d+)\s+([-+]?\d*\.?\d+)\s+([-+]?\d*\.?\d+)",
                    msg0,
                )
                if m0:
                    event["details"].update({
                        "pinLocal_x": float(m0.group(1)),
                        "pinLocal_y": float(m0.group(2)),
                        "pinLocal_z": float(m0.group(3)),
                    })

                # ✅ now scan forward INCLUDING i+1 and don't break on type casing
                j = i + 1
                while j < len(df):
                    next_row = df.iloc[j]
                    if not _is_event_type(next_row.get("Type")) or not isinstance(next_row.get("Message"), str):
                        break

                    msg = next_row["Message"]

                    if "Dropped pin was dropped at " in msg:
                        m = re.search(
                            r"Dropped pin was dropped at (?P<dropDist>\d+\.\d{2}) from chest (?P<idvCoinID>\d+)"
                            r" originally at \((?P<coinPos_x>-?\d+\.\d{2}),\s*(?P<coinPos_y>-?\d+\.\d{2}),\s*(?P<coinPos_z>-?\d+\.\d{2})\)"
                            r":(?P<dropQual>CORRECT|INCORRECT)",
                            msg,
                        )
                        if m:
                            d = m.groupdict()
                            event["details"].update({
                                "dropDist": float(d["dropDist"]),
                                "idvCoinID": int(d["idvCoinID"]),
                                "coinPos_x": float(d["coinPos_x"]),
                                "coinPos_y": float(d["coinPos_y"]),
                                "coinPos_z": float(d["coinPos_z"]),
                                "dropQual": d["dropQual"],
                            })

                    elif "for this pindrop from the navigator" in msg and "Observer" in msg:
                        mv = re.search(r"Observer chose (?P<pinDropVote>CORRECT|INCORRECT) for this pindrop from the navigator", msg)
                        if mv:
                            event["details"].update(mv.groupdict())
                            break
                        if "Observer did not vote for this pindrop from the navigator" in msg:
                            event["details"]["pinDropVote"] = "DID_NOT_VOTE"
                            break

                    j += 1

                events.append(event)

                offsets_events = [
                    ("PinDropSound", 0.000, 0.403),
                    ("GrayPinVis", 0.000, 2.000),
                    ("VotingWindow", 0.000, 2.000),
                    ("VoteInstrText_Vis_start", 0.000, 0.000),
                    ("Feedback_Sound", 2.000, 0.182),
                    ("FeedbackTextVis", 2.000, 1.000),
                    ("FeedbackPinColor", 2.000, 1.000),
                    ("CoinVis_start", 3.000, 0.000),
                    ("CoinPresentSound", 3.000, 0.650),
                    ("CoinLocked", 3.000, 1.000),
                ]
                event_meta = {
                    "med_eventType": "PinDrop_Animation",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                }
                synthetic = generate_synthetic_events_v3(start_ts, start_time, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process pin drop at row {i}: {e}")

            i = j
        else:
            i += 1

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
        if row["Type"] == "Event" and isinstance(row["Message"], str) and row["Message"].startswith("A.N. collected coin:"):
            common_info = build_common_event_fields_noTime(row, i)
            start_time = row["AppTime"]
            start_ts = row["eMLT_orig"]
            mLT_raw = row["mLT_raw"]
            mLT_orig = row["mLT_orig"]

            try:
                # Logged event
                event = {
                    "eMLT_orig": start_ts,
                    "AppTime": start_time,
                    "mLT_raw": mLT_raw,
                    "mLT_orig": mLT_orig,
                    
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_eMLT_orig": start_ts,
                    "end_eMLT_orig": start_ts,

                    "lo_eventType": "CoinCollect_Moment_PinDrop",
                    "med_eventType": "CoinCollect_PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                    "source": "logged",
                    "details": {},
                    **common_info
                }

                match = re.search(
                    r"A\.N\.\s*collected coin:\s*(?P<coinValue>\d+)\s*round reward:\s*(?P<currRoundTotal>-?\d+\.\d{2})",
                    str(row["Message"]),
                )
                if not match:
                    print(f"⚠️ Unexpected feedback format at row {i}: {row['Message']}")
                    continue

                event["details"].update(match.groupdict())
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

def process_chest_collect_v3(df, allowed_statuses):
    #print('starting process chest collect')
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row["Message"].startswith("Other participant just collected coin: "):
            try:
                common_info = build_common_event_fields_noTime(row, i)
                coin_id = int(row["Message"].replace("Other participant just collected coin: ", "").strip())
                start_time = float(row["AppTime"])
                start_ts = row["eMLT_orig"]
                mLT_raw = row["mLT_raw"]
                mLT_orig = row["mLT_orig"]

 
                event = {
                    "eMLT_orig": start_ts,
                    "AppTime": start_time,
                    "mLT_raw": mLT_raw,
                    "mLT_orig": mLT_orig,
                    
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_eMLT_orig": start_ts,
                    "end_eMLT_orig": start_ts,

                    "lo_eventType": "CoinCollect_Moment_Chest",
                    "med_eventType": "CoinCollect_Chest",
                    "hi_eventType": "ChestOpen",
                    "hiMeta_eventType": "BlockActivity",
                    "source": "logged",
                    "details": {"coin_id": coin_id},
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

def process_roundSummary(df, allowed_statuses):
    #print('starting process chest collect')
    events = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row["Message"].startswith("A.N. finished a perfect dropround with:"):
            try:
                
                common_info = build_common_event_fields_noTime(row, i)
                start_time = float(row["AppTime"])
                start_ts = row["eMLT_orig"]
                mLT_raw = row["mLT_raw"]
                mLT_orig = row["mLT_orig"]

 
                event = {
                    "eMLT_orig": start_ts,
                    "AppTime": start_time,
                    "mLT_raw": mLT_raw,
                    "mLT_orig": mLT_orig,
                    
                    "start_AppTime": start_time,
                    "end_AppTime": start_time,
                    "start_eMLT_orig": start_ts,
                    "end_eMLT_orig": start_ts,

                    "lo_eventType": "RoundEnd_Report",
                    "med_eventType": "RoundEnd_Transition",
                    "hi_eventType": "BlockStructure",
                    "hiMeta_eventType": "Meta_Infrastructure",
                    "source": "logged",
                    "details": {},
                    **common_info
                }
                match = re.search(
                    r"A\.N\. finished a perfect dropround with:(?P<currRoundTotal>-?\d+\.\d{2})\s+total reward:\s+(?P<currGrandTotal>-?\d+\.\d{2})",
                    row["Message"],
                )
                if not match:
                    print(f"⚠️ Unexpected feedback format at row {i}: {row['Message']}")
                    continue

                event["details"].update(match.groupdict())
                events.append(event) 
            except Exception as e:
                    print(f"⚠️ Failed to parse round summary values at row {i}: {e}")
 
    return events

def buildEvents_PO(df, allowed_statuses):
    try:
        # 1. Build cascades from all known sources
        cascades = (
            process_pin_drop_v6(df, allowed_statuses) +
            process_feedback_collect_v5(df, allowed_statuses) +
            process_chest_collect_v3(df, allowed_statuses) +
            process_swap_votes_v4(df, allowed_statuses) +
            process_roundSummary(df, allowed_statuses)
        )

        # 2. Generate cascade windows from them
        all_events = backfill_approx_row_indices_v2(cascades, df)

        return all_events

    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise
