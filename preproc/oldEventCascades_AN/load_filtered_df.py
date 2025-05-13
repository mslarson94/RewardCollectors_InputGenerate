
import pandas as pd
from io import StringIO

def load_filtered_df(file_path):
    with open(file_path, "r") as f:
        lines = f.readlines()

    header = lines[0]
    start_index = next(
        (i for i, line in enumerate(lines) if "Mark should happen" in line), 1
    )

    # Include header + everything from the first valid data line onward
    filtered_lines = [header] + lines[start_index:]
    df = pd.read_csv(StringIO("".join(filtered_lines)))

    # Assign accurate original line indices (excluding header)
    df["original_index"] = list(range(start_index, start_index + len(df)))

    return df




def common_info_extractor(df, block_status, origRowGrabType):
    row_info = {}
    if origRowGrabType == 'j-1':
        row_info = {
                    **row_info
                    "original_row_start": df.at[idx, "original_index"],
                    "original_row_end": df.at[j - 1, "original_index"]
                }

    elif origRowGrabType == 'idx':
        row_info = {
                    **row_info
                    "original_row_start": df.at[idx, "original_index"],
                    "original_row_end": df.at[idx, "original_index"]
                    }

    elif origRowGrabType == 'i':
        row_info = {
                    **row_info
                    "original_row_start": df.at[i, "original_index"],
                    "original_row_end": df.at[i, "original_index"]
                }
    elif origRowGrabType == 'row':
        row_info = {
                    **row_info
                    "original_row_start": df.at[row.Index, "original_index"],
                    "original_row_end": df.at[row.Index, "original_index"]
                }

    elif origRowGrabType == 'row':
        row_info = {
                    **row_info
                    "original_row_start": df.at[idx - 1, "original_index"],
                    "original_row_end": df.at[idx - 1, "original_index"]
                }

    common_info = {
                "BlockNum": row.get("BlockNum", None),
                "RoundNum": row.get("RoundNum", None),
                "CoinSetID": row.get("CoinSetID", None),
                "BlockStatus": block_status,
                **row_info
            }

    return common_info



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
            common_info = common_info_extractor(df, block_status, "i")

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

