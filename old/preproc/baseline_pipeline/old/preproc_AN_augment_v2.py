import os
import pandas as pd
import numpy as np
import re

from preprocHelpers import (
    split_coordinates, parse_2D_coords, parse_3D_coords, parse_rotation, 
    drop_dead_cols, safe_parse_timestamp, add_elapsed_time_columns,
    cols_2D, cols_3D, cols_rotation, cols2Drop)



def correct_malformed_string(raw_string):
    """Fixes concatenated numeric values like -1.0000.000-6.000"""
    pattern = r"(-?\d+\.\d{3})(-?\d+\.\d{3})(-?\d+\.\d{3})"
    return re.sub(pattern, r"\1 \2 \3", raw_string)

def detect_and_tag_blocks(data):
    block_start_idx = None
    block_num = None
    coinset_id = None
    block_type = None
    last_seen_coinset = None
    round_num = None  # Will be initialized at block start

    data["RoundNum"] = np.nan
    data["BlockNum"] = np.nan
    data["CoinSetID"] = np.nan
    data["BlockType"] = np.nan

    for idx, row in data.iterrows():
        message = row.get("Message", "")

        if isinstance(message, str):
            # Block start detection
            if message == "Mark should happen if checked on terminal.":
                block_start_idx = idx
                round_num = 0
                data.at[idx, "RoundNum"] = round_num  # Tag block start with 0

            # CoinsetID tracking
            if message.startswith("coinsetID:"):
                coinset_match = re.search(r"coinsetID:(\d+)", message)
                if coinset_match:
                    last_seen_coinset = int(coinset_match.group(1))

            # Block type & metadata tagging
            block_match = re.search(r"Started (?:collecting|pindropping)\. Block:(\d+)", message)
            if block_match:
                block_num = int(block_match.group(1))
                coinset_id = last_seen_coinset
                block_type = "pindropping" if "pindropping" in message.lower() else "collecting"

                # Backfill to block start
                backfill_start = block_start_idx if block_start_idx is not None else idx
                for j in range(backfill_start, idx + 1):
                    data.at[j, "BlockNum"] = block_num
                    data.at[j, "CoinSetID"] = coinset_id
                    data.at[j, "BlockType"] = block_type
                block_start_idx = None  # reset

            # Increment on each reposition
            if "Repositioned and ready to start block or round" in message:
                if round_num is not None:
                    round_num += 1
                    data.at[idx, "RoundNum"] = round_num

    return data

def detect_block_completeness(data, block_num, block_rows):
    messages = block_rows["Message"].dropna().str.lower()

    has_start = any("mark should happen" in m for m in messages)
    has_reposition = any("repositioned and ready to start" in m for m in messages)
    has_end = any("finished current task" in m for m in messages)

    if has_start and has_end:
        return "complete"
    elif has_start and not has_end:
        return "truncated"
    else:
        return "incomplete"  # fallback if block structure is unclear

# Cleaned-up forward_fill_block_info function without future-state round checks
def forward_fill_block_info(data):
    current_block_num = None
    current_coinset_id = None
    current_block_type = None
    current_round_num = None

    for idx in range(len(data)):
        row = data.iloc[idx]
        round_num = row["RoundNum"]

        # Track latest known block metadata
        if not pd.isna(row["BlockNum"]):
            current_block_num = row["BlockNum"]
            current_coinset_id = row["CoinSetID"]
            current_block_type = row["BlockType"]

        if not pd.isna(round_num):
            current_round_num = round_num

        # Fill structural info
        if current_block_num is not None:
            data.at[idx, "BlockNum"] = current_block_num
            data.at[idx, "CoinSetID"] = current_coinset_id
            data.at[idx, "BlockType"] = current_block_type

        # Fill round number only if still missing
        if current_round_num is not None and pd.isna(round_num):
            data.at[idx, "RoundNum"] = current_round_num

    return data

# Updated assign_temporal_intervals with new phase definitions (7777, 8888, 9999)
def assign_temporal_intervals(data):
    all_indices = data.index.tolist()
    idx_limit = len(data)
    block_starts = data[data["Message"] == "Mark should happen if checked on terminal."].index

    for i, block_start in enumerate(block_starts):
        next_block_start = block_starts[i + 1] if i + 1 < len(block_starts) else idx_limit
        block_idxs = list(range(block_start, next_block_start))

        last_reposition = None
        last_finished_round = None

        for idx in block_idxs:
            message = data.at[idx, "Message"]

            # Track reposition markers     
            if isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
                last_reposition = idx

            # Assign 7777: after a round ends and before the next reposition
            if last_finished_round is not None and isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
                for j in range(last_finished_round, idx):
                    data.at[j, "RoundNum"] = 7777      
                last_finished_round = None

            # Track end of round
            if isinstance(message, str) and message.startswith("Finished pindrop round:"):
                last_finished_round = idx

            # Assign 8888: idle at blue cylinder before round start
            if last_reposition is not None and isinstance(message, str) and re.match(r"Started (?:collecting|pindropping)\. Block:\d+", message):
                for j in range(last_reposition, idx):
                    data.at[j, "RoundNum"] = 8888
                last_reposition = None

            # Assign 9999: inter-block idle time
            if isinstance(message, str) and message.lower().strip() == "finished current task":
                block_id = data.at[idx, "BlockNum"]
                post_rows = data[(data.index > idx-1) & (data["BlockNum"] == block_id)]

                for j in post_rows.index:
                    if data.at[j, "RoundNum"] not in [7777, 8888]:
                        data.at[j, "RoundNum"] = 9999
    return data

# Define a separate function to compute chestPin_num and totalRounds after detect_and_tag_blocks is run
def augment_with_chestpin_and_totalrounds_v1(data):
    data["chestPin_num"] = np.nan
    data["totalRounds"] = np.nan

    # Fill missing RoundNum and BlockNum with forward fill for grouping
    data["RoundNum_filled"] = data["RoundNum"].ffill()
    data["BlockNum_filled"] = data["BlockNum"].ffill()

    # Compute totalRounds per block
    valid_rounds = data[~data["RoundNum_filled"].isin([0, 7777, 8888, 9999])]
    round_counts = valid_rounds.groupby("BlockNum_filled")["RoundNum_filled"].nunique()
    data["totalRounds"] = data["BlockNum_filled"].map(round_counts)

    # Reset chestPin_num on round start, accumulate within the round
    chest_pin_count = 0
    for idx, row in data.iterrows():
        message = row.get("Message", "")

        # Reset count on round start
        if isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
            chest_pin_count = 0

        # Count chest/pin events
        if isinstance(message, str) and (message.startswith("Chest opened: ") or message.startswith("Just dropped a pin.")):
            chest_pin_count += 1

        # Assign the current count
        data.at[idx, "chestPin_num"] = chest_pin_count

    # Clean up temporary columns
    data.drop(columns=["RoundNum_filled", "BlockNum_filled"], inplace=True)

    return data


def augment_with_chestpin_and_totalrounds(data):
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

def process_obsreward_file(data, nestedPath, flatPath):
    data['BlockNum'] = None
    data['RoundNum'] = None
    data['BlockType'] = None
    data['CoinSetID'] = None

    detect_and_tag_blocks(data)
    forward_fill_block_info(data)
    fix_collecting_block_coinsetids(data)
    assign_temporal_intervals(data)
    data["Messages_filled"] = data["Message"].fillna(method='ffill')
    data['BlockStatus'] = None
    for block_num in data['BlockNum'].dropna().unique():
        block_mask = data['BlockNum'] == block_num
        status = detect_block_completeness(data, block_num, data[block_mask])
        data.loc[block_mask, 'BlockStatus'] = status
    data["AN_AlignTS"] = data["Timestamp"]  # add AN_AlignTS
    data = augment_with_chestpin_and_totalrounds(data)
    # Coordinate Parsing
    data = drop_dead_cols(data, cols2Drop)
    data = parse_2D_coords(data, cols_2D)
    data = parse_3D_coords(data, cols_3D)
    data = parse_rotation(data, cols_rotation)
    data = add_elapsed_time_columns(data)
    data.to_csv(nestedPath, index=False)
    data.to_csv(flatPath, index=False)

    print(f"Processed and saved: {nestedPath}")

def clean_and_process_files(root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500):
    pattern = re.compile(r"^ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
    print('Started AN-specific processing!')
    output_dir_flat = os.path.join(root_directory, "ProcessedData_Flat")
    baseDir = os.path.join(root_directory, "RawData")
    output_dir = os.path.join(root_directory, "ProcessedData")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(output_dir_flat, exist_ok=True)
    for dirpath, _, filenames in os.walk(baseDir):
        for filename in filenames:
            #print(f"Checking file: {filename}")
            if pattern.match(filename):  
                print(f'Match found: {filename}')
                file_path = os.path.join(dirpath, filename)

                relative_path = os.path.relpath(dirpath, root_directory)
                #relative_path = os.path.relpath(dirpath, baseDir)

                full_output_dir = os.path.join(output_dir, relative_path)
                os.makedirs(full_output_dir, exist_ok=True)

                outFile_nested = os.path.join(full_output_dir, f"{filename.replace('.csv', '_processed.csv')}")
                outFile_flat = os.path.join(output_dir_flat, f"{filename.replace('.csv', '_processed.csv')}")
                print(f"Processing file: {file_path}")
                data = pd.read_csv(file_path)
                # ✅ Track original row index before any manipulation
                data["original_index"] = data.index

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
                process_obsreward_file(data=data, nestedPath=outFile_nested, flatPath=outFile_flat)


########### Execution Block #############
# Execution block

trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
#procDir = 'SmallSelectedData/RNS'
procDir = 'SelectedData'
root_directory = os.path.join(trueRootDir, procDir)


metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")
clean_and_process_files(root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500)