def process_feedback_collect_v5(df, allowed_statuses):
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

                synthetic = generate_synthetic_events(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to parse feedback values at row {i}: {e}")

    return events



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
                    "mid_eventType": "PinDrop",
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
                    "mid_eventType": "PinDrop_Animation",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity"
                    }

                synthetic = generate_synthetic_events(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process pin drop at row {i}: {e}")

            i = j
        else:
            i += 1

    return events


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

                synthetic = generate_synthetic_events(start_time, timestamp, offsets_events, common_info, event_meta)
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
                synthetic = generate_synthetic_events(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process chest coin collect at row {i}: {e}")

    return events