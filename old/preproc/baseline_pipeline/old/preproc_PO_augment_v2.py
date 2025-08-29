import os
import pandas as pd
import numpy as np
import re
from preprocHelpers import (
    split_coordinates, parse_2D_coords, parse_3D_coords, parse_rotation, 
    drop_dead_cols, safe_parse_timestamp, add_elapsed_time_columns,
    cols_2D, cols_3D, cols_rotation, cols2Drop)


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


def detect_and_tag_blocksV1(data):
    block_start_idx = None
    block_num = None
    coinset_id = None
    block_type = None
    last_seen_coinset = None
    round_num = None

    data["RoundNum"] = np.nan
    data["BlockNum"] = np.nan
    data["CoinSetID"] = np.nan
    data["BlockType"] = np.nan

    for idx, row in data.iterrows():
        message = row.get("Message", "")

        if isinstance(message, str):
            # if message == "Mark should happen if checked on terminal.":
            #     block_start_idx = idx
            #     round_num = 0
            #     data.at[idx, "RoundNum"] = round_num
            if message == "Mark should happen if checked on terminal.":
                block_start_idx = idx
                round_num = 0  # round starts counting from this row forward
                continue  # don't set RoundNum here


            if message.startswith("coinsetID:"):
                match = re.search(r"coinsetID:(\d+)", message)
                if match:
                    last_seen_coinset = int(match.group(1))

            match = re.search(r"Started watching other participant's (collecting|pin dropping)\. Block:\s*(\d+)", message)
            # if match:
            #     block_type_raw = match.group(1)
            #     block_num = int(match.group(2))
            #     coinset_id = last_seen_coinset
            #     block_type = "pindropping" if block_type_raw == "pin dropping" else "collecting"
            #     start = block_start_idx if block_start_idx is not None else idx
            #     for j in range(start, idx + 1):
            #         data.at[j, "BlockNum"] = block_num
            #         data.at[j, "CoinSetID"] = coinset_id
            #         data.at[j, "BlockType"] = block_type
            #     block_start_idx = None
            if match:
                block_type_raw = match.group(1)
                block_num = int(match.group(2))
                coinset_id = last_seen_coinset
                block_type = "pindropping" if block_type_raw == "pin dropping" else "collecting"
                start = block_start_idx + 1 if block_start_idx is not None else idx
                for j in range(start, idx + 1):
                    data.at[j, "BlockNum"] = block_num
                    data.at[j, "CoinSetID"] = coinset_id
                    data.at[j, "BlockType"] = block_type
                block_start_idx = None

            if "Repositioned and ready to start block or round" in message:
                if round_num is not None:
                    round_num += 1
                    data.at[idx, "RoundNum"] = round_num
    return data

def detect_and_tag_blocks(data):
    block_start_idx = None
    block_num = None
    coinset_id = None
    block_type = None
    last_seen_coinset = None
    round_num = None
    block_instance = 0  # Initialize BlockInstance

    data["RoundNum"] = np.nan
    data["BlockNum"] = np.nan
    data["CoinSetID"] = np.nan
    data["BlockType"] = np.nan
    data["BlockInstance"] = np.nan  # Add BlockInstance column

    for idx, row in data.iterrows():
        message = row.get("Message", "")

        if isinstance(message, str):
            if message == "Mark should happen if checked on terminal.":
                block_start_idx = idx
                round_num = 0  # round starts counting from this row forward
                block_instance += 1  # Increment BlockInstance
                data.at[idx, "BlockInstance"] = block_instance  # Mark this row
                continue

            if message.startswith("coinsetID:"):
                match = re.search(r"coinsetID:(\d+)", message)
                if match:
                    last_seen_coinset = int(match.group(1))

            match = re.search(r"Started watching other participant's (collecting|pin dropping)\. Block:\s*(\d+)", message)
            if match:
                block_type_raw = match.group(1)
                block_num = int(match.group(2))
                coinset_id = last_seen_coinset
                block_type = "pindropping" if block_type_raw == "pin dropping" else "collecting"
                start = block_start_idx + 1 if block_start_idx is not None else idx
                for j in range(start, idx + 1):
                    data.at[j, "BlockNum"] = block_num
                    data.at[j, "CoinSetID"] = coinset_id
                    data.at[j, "BlockType"] = block_type
                    data.at[j, "BlockInstance"] = block_instance  # Propagate BlockInstance
                block_start_idx = None

            if "Repositioned and ready to start block or round" in message:
                if round_num is not None:
                    round_num += 1
                    data.at[idx, "RoundNum"] = round_num
    return data


def forward_fill_block_infoV1(data):
    current_block_num = None
    current_coinset_id = None
    current_block_type = None
    current_round_num = None

    for idx in range(len(data)):
        row = data.iloc[idx]
        round_num = row["RoundNum"]

        if not pd.isna(row["BlockNum"]):
            current_block_num = row["BlockNum"]
            current_coinset_id = row["CoinSetID"]
            current_block_type = row["BlockType"]

        if not pd.isna(round_num):
            current_round_num = round_num

        if current_block_num is not None:
            data.at[idx, "BlockNum"] = current_block_num
            data.at[idx, "CoinSetID"] = current_coinset_id
            data.at[idx, "BlockType"] = current_block_type

        if current_round_num is not None and pd.isna(round_num):
            data.at[idx, "RoundNum"] = current_round_num
    return data

def forward_fill_block_info(data):
    current_block_num = None
    current_coinset_id = None
    current_block_type = None
    current_round_num = None
    current_block_instance = None  # Add BlockInstance tracker

    for idx in range(len(data)):
        row = data.iloc[idx]
        round_num = row["RoundNum"]

        if not pd.isna(row["BlockNum"]):
            current_block_num = row["BlockNum"]
            current_coinset_id = row["CoinSetID"]
            current_block_type = row["BlockType"]

        if not pd.isna(row["BlockInstance"]):
            current_block_instance = row["BlockInstance"]

        if not pd.isna(round_num):
            current_round_num = round_num

        if current_block_num is not None:
            data.at[idx, "BlockNum"] = current_block_num
            data.at[idx, "CoinSetID"] = current_coinset_id
            data.at[idx, "BlockType"] = current_block_type
            data.at[idx, "BlockInstance"] = current_block_instance

        if current_round_num is not None and pd.isna(round_num):
            data.at[idx, "RoundNum"] = current_round_num

    return data


def fix_collecting_block_coinsetids(data):
    for block_id in data["BlockNum"].dropna().unique():
        block_rows = data[data["BlockNum"] == block_id]
        if block_rows["BlockType"].iloc[0] != "collecting":
            continue  # Only fix collecting blocks

        # Look within this block for the correct coinsetID
        correct_coinset = None
        for msg in block_rows["Message"].dropna():
            match = re.search(r"coinsetID:(\d+)", msg)
            if match:
                correct_coinset = int(match.group(1))
                break

        if correct_coinset is not None:
            data.loc[data["BlockNum"] == block_id, "CoinSetID"] = correct_coinset

    return data

def detect_block_completeness(data, block_num, block_rows):
    messages = block_rows["Message"].dropna().str.lower()
    has_start = any("mark should happen" in m for m in messages)
    has_end = any("finished current task" in m or "finished watching other participant's" in m for m in messages)

    if has_start and has_end:
        return "complete"
    elif has_start:
        return "truncated"
    return "incomplete"

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


def augment_with_chestpin_and_totalrounds(data):
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

def process_obsreward_file(data, nestedPath, flatPath):
    data["BlockNum"] = None
    data["RoundNum"] = None
    data["BlockType"] = None
    data["CoinSetID"] = None
    data["BlockStatus"] = None

    detect_and_tag_blocks(data)
    forward_fill_block_info(data)
    assign_temporal_intervals_PO(data)
    data["RoundNum"] = data["RoundNum"].fillna(method="ffill")  # 🔧 Ensures all rows within block are tagged

    fix_collecting_block_coinsetids(data)
    data["Messages_filled"] = data["Message"].fillna(method='ffill')

    for block_num in data["BlockNum"].dropna().unique():
        block_mask = data["BlockNum"] == block_num
        block_rows = data[block_mask]
        status = detect_block_completeness(data, block_num, block_rows)
        data.loc[block_mask, "BlockStatus"] = status
    data = augment_with_chestpin_and_totalrounds(data)
    data = drop_dead_cols(data, cols2Drop)
    data = parse_2D_coords(data, cols_2D)
    data = parse_3D_coords(data, cols_3D)
    data = parse_rotation(data, cols_rotation)
    data = add_elapsed_time_columns(data)

    # Coordinate Parsing

    data.to_csv(nestedPath, index=False)
    data.to_csv(flatPath, index=False)

    print(f"Processed and saved: {nestedPath}")



def clean_and_process_files(root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500):
    pattern = re.compile(r"^ObsReward_B_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
    print('Started PO-specific processing!')
    baseDir = os.path.join(root_directory, "RawData")
    output_dir = os.path.join(root_directory, "ProcessedData")
    output_dir_flat = os.path.join(root_directory, "ProcessedData_Flat")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(output_dir_flat, exist_ok=True)
    for dirpath, _, filenames in os.walk(baseDir):
        for filename in filenames:
            #print(f"Checking file: {filename}")
            if pattern.match(filename):  
                print(f'Match found: {filename}')
                file_path = os.path.join(dirpath, filename)

                relative_path = os.path.relpath(dirpath, baseDir)
                full_output_dir = os.path.join(output_dir, relative_path)
                os.makedirs(full_output_dir, exist_ok=True)

                outFile = os.path.join(full_output_dir, f"{filename.replace('.csv', '_processed.csv')}")
                outFileFlat = os.path.join(output_dir_flat, f"{filename.replace('.csv', '_processed.csv')}")
                print(f"Processing file: {file_path}")
                #data = pd.read_csv(file_path)
                data = correct_extraColumns(file_path, full_output_dir, save_large_files, max_memory_mb)
                # ✅ Track original row index before any manipulation
                data["original_index"] = data.index
                # ✅ Continue processing
                process_obsreward_file(data, outFile, outFileFlat)

########### Execution Block #############
# Execution block

trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
#procDir = 'SmallSelectedData/RNS'
procDir = 'SelectedData'
root_directory = os.path.join(trueRootDir, procDir)


metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")
clean_and_process_files(root_directory,  magic_leap_data, save_large_files=True, max_memory_mb=500)


