import os
import pandas as pd
import numpy as np
import re
from preprocRawHelpers import (
    detect_and_tag_blocks,
    forward_fill_block_info,
    detect_block_completeness,
    fix_collecting_block_coinsetids,
    robust_parse_timestamp,
    final_column_order_PO,
    enhance_timestamp_with_apptime,
    process_obsreward_file)


def correct_extraColumns(file_path, output_dir, save_large_files, max_memory_mb):
    """
    Cleans ObsReward_B files and optionally saves them if they're large.
    """
    cleaned_data = []
    
    with open(file_path, 'r') as file:
        for line in file:
            split_line = line.strip().split(',')

            # If more than 24 columns, merge extra columns into the 24th column
            if len(split_line) > 24:
                combined_value = ','.join(split_line[23:])
                split_line = split_line[:23] + [combined_value]

            cleaned_data.append(split_line)

    # Convert cleaned data to DataFrame
    data_cleaned = pd.DataFrame(cleaned_data)
    data_cleaned.columns = data_cleaned.iloc[0]  # Use first row as column names
    data_cleaned = data_cleaned[1:]  # Remove header row

    # Replace empty values with NaN and drop fully NaN rows
    data_cleaned.replace('', np.nan, inplace=True)
    data_cleaned.dropna(how='all', inplace=True)

    # Check if file size exceeds the threshold
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Convert bytes to MB
    if save_large_files and file_size_mb > max_memory_mb:
        cleaned_file_path = os.path.join(output_dir, f"{os.path.basename(file_path).replace('.csv', '_cleaned.csv')}")
        data_cleaned.to_csv(cleaned_file_path, index=False)
        print(f"Saved intermediate cleaned file: {cleaned_file_path}")
        return pd.read_csv(cleaned_file_path)  # Reload for processing

    return data_cleaned  # Return cleaned data for direct processing

def assign_temporal_intervals_PO(data):
    data["RoundNum"] = np.nan  # Reset RoundNum for clean assignment
    block_starts = data[data["Message"] == "Mark should happen if checked on terminal."].index

    for block_start in block_starts:
        round_num = 0
        data.at[block_start, "RoundNum"] = round_num  # Initialize block

        current_block = data.at[block_start, "BlockNum"]
        block_mask = (data.index > block_start) & (data["BlockNum"] == current_block)
        block_data = data[block_mask]

        phase = "waiting"  # start with waiting (looking for reposition)
        for idx in block_data.index:
            message = data.at[idx, "Message"]

            if isinstance(message, str):
                if phase == "waiting" and "Repositioned and ready to start block or round" in message:
                    data.at[idx, "RoundNum"] = 8888
                    phase = "ready"

                elif phase in ["waiting", "ready"] and message.startswith("Started watching other participant's"):
                    round_num = 1
                    data.at[idx, "RoundNum"] = round_num
                    phase = "rounds"

                elif phase == "rounds" and "Repositioned and ready to start block or round" in message:
                    round_num += 1
                    data.at[idx, "RoundNum"] = round_num

                elif "Finished watching other participant's" in message:
                    data.at[idx, "RoundNum"] = 9999
                    break  # end of this block

    return data

def augment_with_chestpin_and_totalrounds_PO(data):
    data["chestPin_num"] = np.nan
    data["totalRounds"] = np.nan
    first_valid_block_idx = data["BlockNum"].first_valid_index()
    if first_valid_block_idx is None:
        return data

    working_data = data.loc[first_valid_block_idx:].copy()
    working_data["RoundNum_filled"] = working_data["RoundNum"].ffill()
    working_data["BlockNum_filled"] = working_data["BlockNum"].ffill()
    valid_rounds = working_data[~working_data["RoundNum_filled"].isin([0, 7777, 8888, 9999])]
    round_counts = valid_rounds.groupby("BlockNum_filled")["RoundNum_filled"].nunique()
    working_data["totalRounds"] = working_data["BlockNum_filled"].map(round_counts)

    previous_block = None
    previous_round = None
    chest_pin_count = 0

    for idx, row in working_data.iterrows():
        message = row.get("Message", "")
        block = row["BlockNum"]
        round_ = row["RoundNum"]

        # Reset if new block or new round
        if block != previous_block or round_ != previous_round:
            chest_pin_count = 0

        if isinstance(message, str) and (
            message.startswith("Other participant just collected coin: ") or
            message.startswith("Other participant just dropped a new pin at ")
        ):
            chest_pin_count += 1

        working_data.at[idx, "chestPin_num"] = chest_pin_count
        previous_block = block
        previous_round = round_

    data.loc[working_data.index, ["chestPin_num", "totalRounds"]] = working_data[["chestPin_num", "totalRounds"]]
    return data


def clean_and_process_files(root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500):
    pattern = re.compile(r"^ObsReward_B_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
    print('Started PO-specific processing!')
    baseDir = os.path.join(root_directory, "RawData")
    output_dir = os.path.join(root_directory, "ProcessedData")
    flatPath = os.path.join(root_directory, "ProcessedData_Flat")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(flatPath, exist_ok=True)
    for dirpath, _, filenames in os.walk(baseDir):
        for filename in filenames:

            if pattern.match(filename):  
                print(f'Match found: {filename}')
                file_path = os.path.join(dirpath, filename)
                session_date_match = re.match(r"ObsReward_B_(\d{2}_\d{2}_\d{4})_\d{2}_\d{2}\.csv", filename)
                session_date = session_date_match.group(1) if session_date_match else None


                relative_path = os.path.relpath(dirpath, baseDir)
                nestedPath = os.path.join(output_dir, relative_path)
                os.makedirs(nestedPath, exist_ok=True)

                nestedFile = os.path.join(nestedPath, f"{filename.replace('.csv', '_processed.csv')}")
                flatFile = os.path.join(flatPath, f"{filename.replace('.csv', '_processed.csv')}")
                print(f"Processing file: {file_path}")

                data = correct_extraColumns(file_path, nestedPath, save_large_files, max_memory_mb)
                # ✅ Track original row index before any manipulation
                data["origRow"] = data.index

                # ✅ Continue processing
                process_obsreward_file(data, role = 'PO')
                assign_temporal_intervals_PO(data)
                data["RoundNum"] = data["RoundNum"].fillna(method="ffill")  # 🔧 Ensures all rows within block are tagged
                data = augment_with_chestpin_and_totalrounds_PO(data)
                
                data['mLTimestamp_orig'] = data['Timestamp'].apply(lambda ts: robust_parse_timestamp(ts, session_date))

                data = enhance_timestamp_with_apptime(data, "PO")
                data["mLTimestamp_raw"] = data.pop("Timestamp")

                # Reorder columns
                data = data[final_column_order_PO]
                data.to_csv(nestedFile, index=False)
                data.to_csv(flatFile, index=False)
                print(f"Processed and saved: {nestedFile}")

########### Execution Block #############
# Execution block

# trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
# #procDir = 'SmallSelectedData/idealTestFile3'
# procDir = 'FreshStart'
# root_directory = os.path.join(trueRootDir, procDir)


# metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
# magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")
# clean_and_process_files(root_directory,  magic_leap_data, save_large_files=True, max_memory_mb=500)

if __name__ == "__main__":
    trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
    #procDir = 'SmallSelectedData/idealTestFile3'
    procDir = 'FreshStart'
    root_directory = os.path.join(trueRootDir, procDir)


    metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
    magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")
    clean_and_process_files(root_directory,  magic_leap_data, save_large_files=True, max_memory_mb=500)
