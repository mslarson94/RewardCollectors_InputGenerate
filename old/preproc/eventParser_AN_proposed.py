# eventParser_AN_proposed.py

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

# def process_feedback_collect(df, allowed_statuses):
#     events = []
#     cached_idvCoinID = {}

#     for row in df.itertuples():
#         # Cache individual coin ID
#         block_status = getattr(row, "BlockStatus", "unknown")
#         if pd.notna(row.BlockNum) and block_status not in allowed_statuses:
#             continue

#         # --- Grabbing cached coin information based on individual coin ID (idvCoinID)
#         # Example line: "Collected pin feedback coin: 1"
#         if isinstance(row.Message, str) and row.Message.startswith("Collected pin feedback coin:"):
#             id_str = row.Message.replace("Collected pin feedback coin: ", "").strip()
#             if id_str.isdigit():
#                 cached_idvCoinID[(row.BlockNum, row.RoundNum, row.CoinSetID)] = int(id_str)

#         # --- Parsing Feedback Coin Collection with Regex --- 
#         # Example line: "Collected feedback coin:0.00 round reward: 0.00"

#         elif isinstance(row.Message, str) and row.Message.startswith("Collected feedback coin:"):
#             msg_body = row.Message.replace("Collected feedback coin:", "").replace(" round reward", "")
#             parts = msg_body.split(":")
#             if len(parts) == 2:
#                 try:
#                     value_earned = float(parts[0].strip())
#                     round_total = float(parts[1].strip())
#                     idv_id = cached_idvCoinID.get((row.BlockNum, row.RoundNum, row.CoinSetID))
#                     coin_type = classify_coin_type(row.CoinSetID, idv_id) if idv_id is not None else "Unknown"

#                     # Attempt to attach to most recent PinDrop
#                     cascade_id = None
#                     for e in reversed(events):
#                         if e["event_type"] == "PinDrop" and all(
#                             e.get(k) == getattr(row, k, None) for k in ("BlockNum", "RoundNum", "CoinSetID")
#                         ):
#                             cascade_id = e["cascade_id"]
#                             break

#                     details = {
#                         "valueEarned": value_earned,
#                         "runningRoundTotal": round_total,
#                         "idvCoinID": idv_id,
#                         "CoinType": coin_type
#                     }
                    
#                     # --- Feedback Coin Collection Synthetic Events ---
#                     # What happens immediately after the triggering line that begins with "Collected pin feedback coin:"
                    
#                     event = {
#                         "AppTime": row.AppTime,
#                         "Timestamp": row.Timestamp,
#                         "cascade_id": cascade_id,
#                         "lo_eventType": evt,
#                         "med_eventType": f"FeedbackCoinCollect_{row.get('chestPin_num')}",
#                         "hi_eventType": "PinDrop",
#                         "details": details,
#                         "source": "logged",
#                         "original_row_start": row.original_index,
#                         "original_row_end": row.original_index,
#                         "BlockNum": getattr(row, "BlockNum", None),
#                         "RoundNum": getattr(row, "RoundNum", None),
#                         "CoinSetID": getattr(row, "CoinSetID", None)
#                     }

#                     events.append(event)

#                 except ValueError:
#                     print(f"⚠️ Malformed numeric data in Feedback Coin at row {row.Index}: {row.Message}")
#                     continue

#     return events

# def process_feedback_collect_v2(df, allowed_statuses):
#     events = []
#     cached_idvCoinID = {}
#     i = 0

#     while i < len(df):
#         row = df.iloc[i]
#         block_status = row.get("BlockStatus", "unknown")

#         # ✅ Skip if block is not marked complete
#         if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
#             i += 1
#             continue

#         if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Collected pin feedback coin:"):
#             # --- Parsing Feedback Coin Collection with Regex --- 
#             # Example line: "Collected feedback coin:0.00 round reward: 0.00"
#             cascade_id += 1
#             start_time = row["AppTime"]
#             timestamp = row["Timestamp"]
#             common_info = build_common_event_fields(row, i)
#             id_str = row.Message.replace("Collected pin feedback coin: ", "").strip()

#             msg_body = row.Message.replace("Collected feedback coin:", "").replace(" round reward", "")
#             parts = msg_body.split(":")
#             if len(parts) == 2:
#                 try:
#                     value_earned = float(parts[0].strip())
#                     round_total = float(parts[1].strip())
#                     # Attempt to attach to most recent PinDrop
#                     cascade_id = None
#                     for e in reversed(events):
#                         if e["event_type"] == "PinDrop" and all(
#                             e.get(k) == getattr(row, k, None) for k in ("BlockNum", "RoundNum", "CoinSetID")
#                         ):
#                             cascade_id = e["cascade_id"]
#                             break

#                     details = {
#                         "valueEarned": value_earned,
#                         "runningRoundTotal": round_total,
#                     }
                    
#                     # --- Feedback Coin Collection Synthetic Events ---
#                     # What happens immediately after the triggering line that begins with "Collected pin feedback coin:"
                    
#                     event = {
#                         "AppTime": row.AppTime,
#                         "Timestamp": row.Timestamp,
#                         "cascade_id": cascade_id,
#                         "lo_eventType": evt,
#                         "med_eventType": "FeedbackCoinCollect",
#                         "hi_eventType": "PinDrop",
#                         "details": details,
#                         "source": "logged",
#                         **common_info
#                     }

#                     events.append(event)

#                 for offset, evt in [
#                     (0.000, "CoinVis_start"),
#                     (0.000, "CoinValueTextVis_start"),
#                     (0.000, "CoinCollectSound_start"),
#                     (0.654, "CoinCollectSound_end"),
#                     (2.000, "CoinValueTextVis_end")
#                 ]:
#                     try:
#                         synthetic_time = datetime.strptime(timestamp, '%H:%M:%S:%f') + timedelta(seconds=offset)
#                         events.append({
#                             "AppTime": start_time + offset,
#                             "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
#                             "cascade_id": cascade_id,
#                             "lo_eventType": evt,
#                             "med_eventType": "FeedbackCoinCollect",
#                             "hi_eventType": "PinDrop",
#                             "details": {},
#                             "source": "synthetic",
#                             **common_info
#                         })
#                     except Exception as e:
#                         print(f"⚠️ Failed to create synthetic event {evt} at row {i}: {e}")
#                 i = j
#             else:
#                 i += 1
#     return events

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

# def process_chest_opened(df, allowed_statuses):
#     events = []

#     for i in range(len(df)):
#         row = df.iloc[i]
#         block_status = row.get("BlockStatus", "unknown")

#         if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
#             continue

#         if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Chest opened:"):
#             start_time = row["AppTime"]
#             timestamp = row["Timestamp"]
#             common_info = build_common_event_fields(row, i)
#             coin_id = int(row.Message.replace("Chest opened: ", "").strip())

#             try:
#                 coin_id = int(row.Message.replace("Chest opened: ", "").strip())
#                 events.append({
#                     "AppTime": row.AppTime,
#                     "Timestamp": row.Timestamp,
#                     "lo_eventType": "ChestOpenMoment",
#                     "med_eventType": "ChestOpen",
#                     "hi_eventType": "ChestOpen",
#                     "details": {"idvCoinID": coin_id},
#                     "source": "logged",
#                     **common_info
#                 })

#             # Synthetic follow-ups
#             for offset, evt in [
#                 (0.000, "ChestOpenAnimation_start"),
#                 (0.000, "ChestOpenSound_start"),
#                 (0.400, "ChestOpenAnimation_end"),
#                 (0.400, "ChestOpenSound_end"),
#                 (0.400, "ChestOpenEmpty_start"),
#                 (2.000, "ChestOpenEmpty_end"),
#                 (2.000, "CoinVis_start"),
#                 (2.000, "CoinPresentSound_start"),
#                 (2.650, "CoinPresentSound_end"),
#                 (3.000, "Coin_Released")
#             ]:
#                 try:
#                     #synthetic_time = datetime.strptime(timestamp, '%H:%M:%S:%f') + timedelta(seconds=offset)
#                     synthetic_time = safe_parse_timestamp(timestamp) + timedelta(seconds=offset)
#                     events.append({
#                         "AppTime": start_time + offset,
#                         "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
#                         "lo_eventType": evt,
#                         "med_eventType": "FullChestOpenAnimation",
#                         "hi_eventType": "ChestOpen",
#                         "details": {},
#                         "source": "synthetic",
#                         **common_info
#                     })
#                 except Exception as e:
#                     print(f"⚠️ Failed to create synthetic event {evt} at row {i}: {e}")

#         except Exception as e:
#             print(f"⚠️ Failed to parse feedback values at row {i}: {e}")

#     return events

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

def process_swap_votes_v2(df, allowed_statuses):
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
                print(f"⚠️ Failed to process chest open at row {i}: {e}")

    return events

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

# def buildEvents_AN(df, allowed_statuses):
#     chest_events = process_chest_opened(df, allowed_statuses)
#     cascades = (
#         process_pin_drop(df, allowed_statuses) +
#         process_feedback_collect(df, allowed_statuses) +
#         chest_events +
#         process_IE_coin_collected(df, chest_events, allowed_statuses) +
#         process_marks(df, allowed_statuses) +
#         process_swap_votes(df, allowed_statuses) +
#         process_block_periods(df, allowed_statuses)
#     )
#     cascade_windows = find_cascade_windows_from_events(cascades)
#     cascades = [assign_cascade_id(e, cascade_windows, debug=True) for e in cascades]
#     walking_periods = extract_walking_periods_with_cascade_ids(df, cascade_windows)
#     all_events = cascades + walking_periods
#     # walking_periods = extract_walking_periods(df, cascades, allowed_statuses)
#     # all_events = cascades + walking_periods
#     return all_events

def buildEvents_AN_v2(df, allowed_statuses):
    cascades = (
        process_pin_drop_v2(df, allowed_statuses) +
        process_feedback_collect_v3(df, allowed_statuses) +
        process_chest_opened_v2(df, allowed_statuses) +
        process_chest_collect(df, allowed_statuses) +
        process_marks(df, allowed_statuses) +
        process_swap_votes_v2(df, allowed_statuses) +
        process_block_periods_v2(df, allowed_statuses)
    )
    cascade_windows = find_cascade_windows_from_events(cascades)
    cascades = [assign_cascade_id(e, cascade_windows, debug=True) for e in cascades]
    walking_periods = extract_walking_periods_with_cascade_ids(df, cascade_windows)
    all_events = cascades + walking_periods
    return all_events

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