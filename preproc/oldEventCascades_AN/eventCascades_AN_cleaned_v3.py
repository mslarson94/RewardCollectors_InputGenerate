# eventCascades_AN_cleaned_v2.py

import pandas as pd
import re
import os
from datetime import datetime, timedelta

# --- Utility: Coin and Swap Vote Classification ---
def classify_coin_type(CoinSetID, idvCoinID):
    if CoinSetID == 2 and idvCoinID == 2:
        return "PPE"
    elif CoinSetID == 3 and idvCoinID == 0:
        return "NPE"
    elif CoinSetID == 1 or (CoinSetID in [2, 3] and idvCoinID in [0, 1]):
        return "Normal"
    elif CoinSetID == 4 or (CoinSetID == 5 and idvCoinID == 1):
        return "TutorialNorm"
    elif CoinSetID == 5 and idvCoinID in [0, 2]:
        return "TutorialRPE"
    return "Unknown"

def classify_swap_vote(CoinSetID, swapvote):
    if CoinSetID in [2, 3] and swapvote == "NEW":
        return "Correct"
    elif CoinSetID == 1 and swapvote == "OLD":
        return "Correct"
    elif CoinSetID == 1 and swapvote == "NEW":
        return "Incorrect"
    elif CoinSetID in [2, 3] and swapvote == "OLD":
        return "Incorrect"
    return "Unknown"

# --- Pin Drops and Coin Collection Events  ---
def process_pin_drop(df,allowed_statuses):
    events = []
    cascade_id = 0
    i = 0

    while i < len(df):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        # ✅ Skip if block is not marked complete
        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            i += 1
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and "Just dropped a pin" in row["Message"]:
            cascade_id += 1
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            common_info = {
                "BlockNum": row.get("BlockNum", None),
                "RoundNum": row.get("RoundNum", None),
                "CoinSetID": row.get("CoinSetID", None),
                "BlockStatus": block_status,
                "original_row_start": df.at[i + 1 , "original_index"],
                "original_row_end": df.at[i + 1, "original_index"]
            }

            event = {
                "AppTime": start_time,
                "Timestamp": timestamp,
                "cascade_id": cascade_id,
                "event_type": "PinDrop",
                "details": {},
                "source": "logged",
                **common_info
            }

            # --- Parsing Pin Drop Information with Regex --- 
            
            j = i + 1
            while j < len(df):
                next_row = df.iloc[j]
                if next_row["Type"] != "Event" or not isinstance(next_row["Message"], str):
                    break
                msg = next_row["Message"]
                
                # --- Pin Drop Location --- 
                # Example line: "Dropped a new pin at 1.311 -1.517 -1.755 localpos: -0.350 0.000 -5.840"
                
                if "Dropped a new pin at" in msg:
                    match = re.search(
                        r'at ([\d\.\-]+) ([\d\.\-]+) ([\d\.\-]+) localpos: ([\d\.\-]+) ([\d\.\-]+) ([\d\.\-]+)',
                        msg
                    )
                    if match:
                        try:
                            event["details"].update({
                                "pin_local_x": float(match.group(4)),
                                "pin_local_y": float(match.group(5)),
                                "pin_local_z": float(match.group(6)),
                            })
                        except ValueError:
                            print(f"⚠️ Float conversion failed in pin location at row {j}: {msg}")
                    else:
                        print(f"⚠️ Regex mismatch for pin location at row {j}: {msg}")

                
                # --- Closest Coin, Drop Quality, Coin Value ---
                # Example line: "Closest location was: {-1.4000.000-2.670} | actual distance: 0.119 | good drop | coinValue: 20.00"
                
                elif "Closest location was" in msg:
                    match = re.search(
                        r'distance: ([\d\.]+) \| (good|bad) drop \| coinValue: ([\d\.]+)', msg
                    )
                    if match:
                        try:
                            event["details"].update({
                                "drop_distance": float(match.group(1)),
                                "drop_quality": match.group(2),
                                "coin_value": float(match.group(3))
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
                (0.000, "PinDrop_Sound_start"),
                (0.000, "GrayPin_Visible_start"),
                (0.654, "PinDrop_Sound_end"),
                (2.000, "GrayPin_Visible_end"),
                (2.000, "Feedback_Sound_start"),
                (2.000, "Feedback_textNcolor_Visible_start"),
                (3.000, "Feedback_textNcolor_Visible_end"),
                (3.000, "Coin_Visible_start"),
                (4.000, "Coin_Released")
            ]:
                try:
                    synthetic_time = datetime.strptime(timestamp, '%H:%M:%S:%f') + timedelta(seconds=offset)
                    events.append({
                        "AppTime": start_time + offset,
                        "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                        "cascade_id": cascade_id,
                        "event_type": evt,
                        "details": {},
                        "source": "synthetic",
                        **{k: event[k] for k in ("BlockNum", "RoundNum", "CoinSetID", "BlockStatus")}
                    })
                except Exception as e:
                    print(f"⚠️ Failed to create synthetic event {evt} at row {i}: {e}")

            i = j
        else:
            i += 1

    return events

def process_feedback_collect(df, allowed_statuses):
    events = []
    cached_idvCoinID = {}

    for row in df.itertuples():
        # Cache individual coin ID
        block_status = getattr(row, "BlockStatus", "unknown")
        if pd.notna(row.BlockNum) and block_status not in allowed_statuses:
            continue

        # --- Grabbing cached coin information based on individual coin ID (idvCoinID)
        # Example line: "Collected pin feedback coin: 1"
        if isinstance(row.Message, str) and row.Message.startswith("Collected pin feedback coin:"):
            id_str = row.Message.replace("Collected pin feedback coin: ", "").strip()
            if id_str.isdigit():
                cached_idvCoinID[(row.BlockNum, row.RoundNum, row.CoinSetID)] = int(id_str)

        # --- Parsing Feedback Coin Collection with Regex --- 
        # Example line: "Collected feedback coin:0.00 round reward: 0.00"

        elif isinstance(row.Message, str) and row.Message.startswith("Collected feedback coin:"):
            msg_body = row.Message.replace("Collected feedback coin:", "").replace(" round reward", "")
            parts = msg_body.split(":")
            if len(parts) == 2:
                try:
                    value_earned = float(parts[0].strip())
                    round_total = float(parts[1].strip())
                    idv_id = cached_idvCoinID.get((row.BlockNum, row.RoundNum, row.CoinSetID))
                    coin_type = classify_coin_type(row.CoinSetID, idv_id) if idv_id is not None else "Unknown"

                    # Attempt to attach to most recent PinDrop
                    cascade_id = None
                    for e in reversed(events):
                        if e["event_type"] == "PinDrop" and all(
                            e.get(k) == getattr(row, k, None) for k in ("BlockNum", "RoundNum", "CoinSetID")
                        ):
                            cascade_id = e["cascade_id"]
                            break

                    details = {
                        "valueEarned": value_earned,
                        "runningRoundTotal": round_total,
                        "idvCoinID": idv_id,
                        "CoinType": coin_type
                    }
                    
                    # --- Feedback Coin Collection Synthetic Events ---
                    # What happens immediately after the triggering line that begins with "Collected pin feedback coin:"
                    
                    event = {
                        "AppTime": row.AppTime,
                        "Timestamp": row.Timestamp,
                        "cascade_id": cascade_id,
                        "event_type": "Feedback_CoinCollect",
                        "details": details,
                        "source": "logged",
                        "original_row_start": df.at[row.Index  + 1, "original_index"],
                        "original_row_end": df.at[row.Index + 1, "original_index"],
                        "BlockNum": getattr(row, "BlockNum", None),
                        "RoundNum": getattr(row, "RoundNum", None),
                        "CoinSetID": getattr(row, "CoinSetID", None)
                    }

                    events.append(event)

                except ValueError:
                    print(f"⚠️ Malformed numeric data in Feedback Coin at row {row.Index}: {row.Message}")
                    continue

    return events

# Functional but I want to break it up 
def process_ie_events(df, allowed_statuses):
    events = []
    for row in df.itertuples():
        block_status = getattr(row, "BlockStatus", "unknown")
        if pd.notna(row.BlockNum) and block_status not in allowed_statuses:
            continue
        if isinstance(row.Message, str) and "Chest opened:" in row.Message:
            parts = row.Message.replace("Chest opened: ", "")
            details = {"idvCoinID": int(parts.strip())}

            event = {
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "cascade_id": None,
                "event_type": "IE_ChestOpen",
                "details": details,
                "source": "logged",
                "original_row_start": df.at[row.Index  + 1, "original_index"],
                "original_row_end": df.at[row.Index + 1, "original_index"],
                "BlockNum": getattr(row, "BlockNum", None),
                "RoundNum": getattr(row, "RoundNum", None),
                "CoinSetID": getattr(row, "CoinSetID", None)
            }

            events.append(event)

            cascade_id = None
            for e in reversed(events):
                if e["event_type"] == "IE_ChestOpen" and all(
                    e.get(k) == event.get(k) for k in ("BlockNum", "RoundNum", "CoinSetID")
                ):
                    cascade_id = e["cascade_id"]
                    break

            start_time = row.AppTime
            timestamp = row.Timestamp

            for offset, evt in [
                (0.000, "ChestOpening_visNsound_start"),
                (0.400, "ChestOpening_visNsound_end"),
                (0.400, "ChestOpen_and_empty_start"),
                (2.000, "ChestOpen_and_empty_end"),
                (2.000, "Coin_Visible_start"),
                (2.000, "Coin_soundPres_start"),
                (2.650, "Coin_soundPres_end"),
                (3.000, "Coin_Released")
            ]:
                synthetic_time = datetime.strptime(timestamp, '%H:%M:%S:%f') + timedelta(seconds=offset)
                events.append({
                    "AppTime": start_time + offset,
                    "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                    "cascade_id": cascade_id,
                    "event_type": evt,
                    "details": {},
                    "source": "synthetic",
                    "BlockNum": getattr(row, "BlockNum", None),
                    "RoundNum": getattr(row, "RoundNum", None),
                    "CoinSetID": getattr(row, "CoinSetID", None)
                })

        elif isinstance(row.Message, str) and "coin collected: " in row.Message:
            parts = row.Message.split("coin collected: ")
            if len(parts) == 2 and parts[1].strip().isdigit():
                details = {"idvCoinID": int(parts[1].strip())}

                event = {
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "cascade_id": None,
                    "event_type": "IE_CoinCollected",
                    "details": details,
                    "source": "logged",
                    "original_row_start": row.Index + 1,
                    "original_row_end": row.Index + 1,
                    "BlockNum": getattr(row, "BlockNum", None),
                    "RoundNum": getattr(row, "RoundNum", None),
                    "CoinSetID": getattr(row, "CoinSetID", None)
                }

                events.append(event)

                cascade_id = None
                for e in reversed(events):
                    if e["event_type"] == "IE_ChestOpen" and all(
                        e.get(k) == event.get(k) for k in ("BlockNum", "RoundNum", "CoinSetID")
                    ):
                        cascade_id = e["cascade_id"]
                        break

                start_time = row.AppTime
                timestamp = row.Timestamp

                for offset, evt in [
                    (0.000, "Coin_Visible_end"),
                    (0.000, "Coin_Sound_start"),
                    (0.650, "Coin_Sound_end"),
                    (2.000, "Coin_Released")
                ]:
                    synthetic_time = datetime.strptime(timestamp, '%H:%M:%S:%f') + timedelta(seconds=offset)
                    events.append({
                        "AppTime": start_time + offset,
                        "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                        "cascade_id": cascade_id,
                        "event_type": evt,
                        "details": {},
                        "source": "synthetic",
                        "BlockNum": getattr(row, "BlockNum", None),
                        "RoundNum": getattr(row, "RoundNum", None),
                        "CoinSetID": getattr(row, "CoinSetID", None)
                    })
            else:
                print(f"⚠️ Malformed coin collected message at row {row.Index}: {row.Message}")
                continue

    return events

def process_chest_opened(df, allowed_statuses):
    chest_events = []
    for row in df.itertuples():
        if pd.notna(row.BlockNum) and getattr(row, "BlockStatus", "unknown") not in allowed_statuses:
            continue
        if isinstance(row.Message, str) and "Chest opened:" in row.Message:
            try:
                coin_id = int(row.Message.replace("Chest opened: ", "").strip())
                chest_events.append({
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "event_type": "ChestOpened",
                    "idvCoinID": coin_id,
                    "BlockNum": row.BlockNum,
                    "RoundNum": row.RoundNum,
                    "CoinSetID": row.CoinSetID
                })
            except ValueError:
                continue
    return chest_events

def process_IE_coin_collected(df, chest_events, allowed_statuses):
    collected_events = []
    chest_lookup = {(e["BlockNum"], e["RoundNum"]): e for e in chest_events}

    for row in df.itertuples():
        if pd.notna(row.BlockNum) and getattr(row, "BlockStatus", "unknown") not in allowed_statuses:
            continue
        if isinstance(row.Message, str) and ("Collected feedback coin" in row.Message or "coin collected" in row.Message):
            match_key = (row.BlockNum, row.RoundNum)
            linked_chest = chest_lookup.get(match_key)

            event_data = {
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "event_type": "CoinCollect",
                "BlockNum": row.BlockNum,
                "RoundNum": row.RoundNum,
                "CoinSetID": row.CoinSetID
            }

            if linked_chest:
                event_data["idvCoinID"] = linked_chest.get("idvCoinID")

            collected_events.append(event_data)
    return collected_events


# --- Marks and Swap Votes ---
def process_marks(df, allowed_statuses):
    events = []
    for row in df.itertuples():
        if isinstance(row.Message, str) and "Sending Headset mark" in row.Message:
            events.append({
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "cascade_id": None,
                "event_type": "Mark",
                "details": {"mark": "A"},
                "source": "logged",
                "original_row_start": df.at[row.Index + 1, "original_index"],
                "original_row_end": df.at[row.Index + 1, "original_index"],
                "BlockNum": getattr(row, "BlockNum", None),
                "RoundNum": getattr(row, "RoundNum", None),
                "CoinSetID": getattr(row, "CoinSetID", None)
            })
    return events

def process_swap_votes(df, allowed_statuses):
    events = []
    for row in df.itertuples():
        if isinstance(row.Message, str) and row.Message.startswith("Active Navigator says it was a "):
            swapvote = row.Message.replace("Active Navigator says it was a ", "").strip().upper()
            score = classify_swap_vote(row.CoinSetID, swapvote)

            events.append({
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "cascade_id": None,
                "event_type": "SwapVote",
                "details": {
                    "SwapVote": swapvote,
                    "SwapVoteScore": score
                },
                "source": "logged",
                "original_row_start": df.at[row.Index + 1, "original_index"],
                "original_row_end": df.at[row.Index + 1, "original_index"],
                "BlockNum": getattr(row, "BlockNum", None),
                "RoundNum": getattr(row, "RoundNum", None),
                "CoinSetID": getattr(row, "CoinSetID", None)
            })
    return events

# --- Walking Periods ---
def process_block_periods(df, allowed_statuses):
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
                        "event_type": f"{round_event_map[previous_round]}_end",
                        "AppTime": df.at[idx - 1, "AppTime"],
                        "Timestamp": df.at[idx - 1, "Timestamp"],
                        "details": {},
                        "source": "synthetic",
                        "original_row_start": df.at[idx, "original_index"],
                        "original_row_end": df.at[idx, "original_index"],
                        "BlockNum": df.at[idx, "BlockNum"],
                        "RoundNum": df.at[idx, "RoundNum"],
                        "CoinSetID": df.at[idx, "CoinSetID"]
                    })
                # Start new one
                start_idx = idx
                events.append({
                    "event_type": f"{round_event_map[round_code]}_start",
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "details": {},
                    "source": "synthetic",
                    "original_row_start": df.at[idx + 1, "original_index"],
                    "original_row_end": df.at[idx + 1, "original_index"],
                    "BlockNum": row.BlockNum,
                    "RoundNum": row.RoundNum,
                    "CoinSetID": row.CoinSetID
                })

        # End the final cascade if we're at the last row
        if idx == len(df) - 1 and round_code in round_event_map and start_idx is not None:
            events.append({
                "event_type": f"{round_event_map[round_code]}_end",
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "details": {},
                "source": "synthetic",
                "original_row_start": df.at[idx + 1, "original_index"],
                "original_row_end": df.at[idx + 1, "original_index"],
                "BlockNum": row.BlockNum,
                "RoundNum": row.RoundNum,
                "CoinSetID": row.CoinSetID
            })

        previous_round = round_code

        # Original static structural events
        shared_fields = {
            "AppTime": row.AppTime,
            "Timestamp": row.Timestamp,
            "source": "synthetic",
            "original_row_start": df.at[idx + 1, "original_index"],
            "original_row_end": df.at[idx + 1, "original_index"],
            "BlockNum": row.BlockNum,
            "RoundNum": row.RoundNum,
            "CoinSetID": row.CoinSetID
        }

        if msg == "Mark should happen if checked on terminal.":
            events.append({
                "event_type": "PreBlock_BlueCylinderVisible_start",
                "details": {},
                **shared_fields
            })

        elif msg == "Repositioned and ready to start block or round":
            events.extend([
                {
                    "event_type": "PreBlock_BlueCylinderVisible_end",
                    "details": {},
                    **shared_fields
                },
                {
                    "event_type": "StartRoundText_visible_start",
                    "details": {},
                    **shared_fields
                }
            ])

        elif msg.startswith("Started"):
            events.extend([
                {
                    "event_type": "StartRoundText_visible_end",
                    "details": {},
                    **shared_fields
                },
                {
                    "event_type": "RoundInstructionText_visible_start",
                    "details": {},
                    **shared_fields
                },
                {
                    "event_type": "RoundInstructionText_visible_end",
                    "AppTime": row.AppTime + 2.0,
                    "Timestamp": row.Timestamp,
                    "details": {},
                    "source": "synthetic",
                    "original_row_start": df.at[idx + 1, "original_index"],
                    "original_row_end": df.at[idx + 1, "original_index"],
                    "BlockNum": row.BlockNum,
                    "RoundNum": row.RoundNum,
                    "CoinSetID": row.CoinSetID
                }
            ])

    return events

def extract_walking_periods(df, cascade_events, allowed_statuses):
    seen_rounds = set()
    walking_periods = []
    df = df.sort_values("AppTime").reset_index(drop=True)

    pin_cascades = {round(e['AppTime'], 3): e['cascade_id'] for e in cascade_events if e['event_type'] == "PinDrop"}

    trigger_rows = []
    for i, row in df.iterrows():
        msg = row.Message if isinstance(row.Message, str) else ""
        round_key = (row.get("BlockNum"), row.get("RoundNum"), row.get("CoinSetID"))
        if round_key not in seen_rounds:
            seen_rounds.add(round_key)
            trigger_rows.append((i, "Round start"))
        elif isinstance(msg, str) and ("Collected feedback coin" in msg or "coin collected" in msg):
            trigger_rows.append((i, "Post_coin_collect"))

    for idx, trigger_type in trigger_rows:
        row = df.iloc[idx]
        block = row.get("BlockNum")
        if pd.notna(block) and row.get("BlockStatus", "unknown") not in allowed_statuses:
            continue

        start_time = row["AppTime"]
        roundnum = row.get("RoundNum")
        end_time, cascade_id = None, None

        for j in range(idx + 1, len(df)):
            msg = df.at[j, "Message"] if "Message" in df.columns else None
            if isinstance(msg, str) and ("Just dropped a pin" in msg or "Chest opened" in msg):
                end_time = df.at[j, "AppTime"]
                cascade_id = pin_cascades.get(round(end_time, 3))
                break

        if end_time is not None:
            walking_periods.append({
                "AppTime": start_time,
                "Timestamp": row["Timestamp"],
                "event_type": "WalkingPeriod",
                "start_AppTime": start_time,
                "end_AppTime": end_time,
                "duration": end_time - start_time,
                "cascade_id": cascade_id,
                "BlockNum": block,
                "RoundNum": row.get("RoundNum"),
                "CoinSetID": row.get("CoinSetID"),
                "BlockStatus": row.get("BlockStatus"),
                "details": {"trigger": trigger_type},
                "source": "synthetic",
                "original_row_start": df.at[idx + 1, "original_index"],
                "original_row_end": df.at[j, "original_index"],
            })
    return walking_periods