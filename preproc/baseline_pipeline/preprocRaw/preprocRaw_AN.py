import os
import pandas as pd
import numpy as np
import re

from preprocRawHelpers import (
    detect_and_tag_blocks,
    forward_fill_block_info,
    detect_block_completeness,
    fix_collecting_block_coinsetids,
    final_column_order_AN,
    safe_parse_timestamp,
    robust_parse_timestamp,
    enhance_timestamp_with_apptime,
    process_obsreward_file)



def correct_malformed_string(raw_string):
    """Fixes concatenated numeric values like -1.0000.000-6.000"""
    pattern = r"(-?\d+\.\d{3})(-?\d+\.\d{3})(-?\d+\.\d{3})"
    return re.sub(pattern, r"\1 \2 \3", raw_string)

# Updated assign_temporal_intervals with new phase definitions (7777, 8888, 9999)

def assign_temporal_intervals_AN(data):
    data["RoundNum"] = np.nan  # Reset RoundNum for clean assignment

    block_starts = data[data["Message"] == "Mark should happen if checked on terminal."].index
    idx_limit = len(data)

    for i, block_start in enumerate(block_starts):
        next_block_start = block_starts[i + 1] if i + 1 < len(block_starts) else idx_limit
        block_idxs = list(range(block_start, next_block_start))

        round_num = 0  # Initialize round numbering at start of block
        data.at[block_start, "RoundNum"] = round_num  # Mark block start

        last_finished_round = None
        last_reposition = None

        for idx in block_idxs:
            message = data.at[idx, "Message"]

            if isinstance(message, str):
                message_lower = message.lower().strip()

                # Detect "Started collecting" or "Started pindropping" — round start
                if re.match(r"started (collecting|pindropping)\. block:\d+", message_lower):
                    round_num += 1
                    data.at[idx, "RoundNum"] = round_num

                    # Tag 8888 for pre-round idle from last reposition
                    if last_reposition is not None:
                        for j in range(last_reposition, idx):
                            data.at[j, "RoundNum"] = 8888
                        last_reposition = None

                # Detect "Finished pindrop round" — round end
                elif message_lower.startswith("finished pindrop round:"):
                    last_finished_round = idx

                # Detect "Repositioned and ready to start block or round"
                elif message_lower.startswith("repositioned and ready to start block or round"):
                    # Tag 7777 between finished round and next reposition
                    if last_finished_round is not None:
                        for j in range(last_finished_round, idx):
                            data.at[j, "RoundNum"] = 7777
                        last_finished_round = None
                    last_reposition = idx

                # Detect "finished current task" — block end marker
                elif message_lower == "finished current task":
                    block_id = data.at[idx, "BlockNum"]
                    post_rows = data[(data.index >= idx) & (data["BlockNum"] == block_id)]

                    for j in post_rows.index:
                        if data.at[j, "RoundNum"] not in [7777, 8888]:
                            data.at[j, "RoundNum"] = 9999
                    break  # Exit block parsing

    return data

# Define a separate function to compute chestPin_num and totalRounds after detect_and_tag_blocks is run
def augment_with_chestpin_and_totalrounds_AN(data):
    data["chestPin_num"] = np.nan
    data["totalRounds"] = np.nan

    # Find the index of the first real block
    first_valid_block_idx = data["BlockNum"].first_valid_index()
    if first_valid_block_idx is None:
        return data  # No blocks found, return unmodified

    # Slice the data from the first valid block onward
    working_data = data.loc[first_valid_block_idx:].copy()

    # Fill missing RoundNum and BlockNum with forward fill for grouping
    working_data["RoundNum_filled"] = working_data["RoundNum"].ffill()
    working_data["BlockNum_filled"] = working_data["BlockNum"].ffill()

    # Compute totalRounds per block
    valid_rounds = working_data[~working_data["RoundNum_filled"].isin([0, 7777, 8888, 9999])]
    round_counts = valid_rounds.groupby("BlockNum_filled")["RoundNum_filled"].nunique()
    working_data["totalRounds"] = working_data["BlockNum_filled"].map(round_counts)

    # Reset chestPin_num on round start, accumulate within the round
    chest_pin_count = 0
    for idx, row in working_data.iterrows():
        message = row.get("Message", "")

        # Reset count on round start
        if isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
            chest_pin_count = 0

        # Count chest/pin events
        if isinstance(message, str) and (message.startswith("Chest opened: ") or message.startswith("Just dropped a pin.")):
            chest_pin_count += 1

        # Assign the current count
        working_data.at[idx, "chestPin_num"] = chest_pin_count

    # Copy augmented values back into the original DataFrame
    data.loc[working_data.index, ["chestPin_num", "totalRounds"]] = working_data[["chestPin_num", "totalRounds"]]

    return data

def clean_and_process_files(root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500):
    pattern = re.compile(r"^ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
    print('Started AN-specific processing!')
    baseDir = os.path.join(root_directory, "RawData")
    output_dir = os.path.join(root_directory, "ProcessedData")
    flatPath = os.path.join(root_directory, "ProcessedData_Flat")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(flatPath, exist_ok=True)
    for dirpath, _, filenames in os.walk(baseDir):
        for filename in filenames:
            #print(f"Checking file: {filename}")
            if pattern.match(filename):  
                print(f'Match found: {filename}')
                session_date_match = re.match(r"ObsReward_A_(\d{2}_\d{2}_\d{4})_\d{2}_\d{2}\.csv", filename)
                session_date = session_date_match.group(1) if session_date_match else None

                file_path = os.path.join(dirpath, filename)

                relative_path = os.path.relpath(dirpath, baseDir)
                nestedPath = os.path.join(output_dir, relative_path)
                os.makedirs(nestedPath, exist_ok=True)

                nestedFile = os.path.join(nestedPath, f"{filename.replace('.csv', '_processed.csv')}")
                flatFile = os.path.join(flatPath, f"{filename.replace('.csv', '_processed.csv')}")
                print(f"Processing file: {file_path}")
                data = pd.read_csv(file_path)
                # ✅ Track original row index before any manipulation
                data["origRow"] = data.index

                # ✅ Fix malformed 'Closest location was:' strings
                for idx, message in data['Message'].dropna().items():
                    if 'Closest location was:' in message:
                        try:
                            raw_string = re.search(r"Closest location was: (.+)", message).group(1)
                            corrected_string = correct_malformed_string(raw_string)
                            data.at[idx, 'Message'] = message.replace(raw_string, corrected_string)
                        except Exception as e:
                            print(f"Error correcting malformed string in message '{message}': {e}")

                # ✅ Continue processing
                process_obsreward_file(data, role = 'AN')
                assign_temporal_intervals_AN(data)
                data["RoundNum"] = data["RoundNum"].fillna(method="ffill")  # 🔧 Ensures all rows within block are tagged
                data = augment_with_chestpin_and_totalrounds_AN(data)

                rPT= data['Timestamp'].apply(lambda ts: robust_parse_timestamp(ts, session_date))
                data['mLTimestamp'] = rPT
                data = enhance_timestamp_with_apptime(data, "AN")
                data["mLTimestamp_raw"] = data.pop("Timestamp")

                # Reorder columns
                data = data[final_column_order_AN]
                data.to_csv(nestedFile, index=False)
                data.to_csv(flatFile, index=False)
                print(f"Processed and saved: {nestedFile}")



# ########### Execution Block #############

if __name__ == "__main__":
    print("🚀 Starting batch preprocRaw_AN.py ...")

    trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
    #procDir = 'SmallSelectedData/idealTestFile3'
    #procDir = 'SelectedData'
    procDir = 'FresherStart'
    root_directory = os.path.join(trueRootDir, procDir)

    metadata_file = trueRootDir + "/collatedData.xlsx"
    magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")
    clean_and_process_files(root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500)
