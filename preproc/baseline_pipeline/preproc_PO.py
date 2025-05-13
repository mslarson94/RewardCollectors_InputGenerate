import os
import pandas as pd
import numpy as np
import re

###########################

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

###########################

def detect_and_tag_blocks(data):
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
            if message == "Mark should happen if checked on terminal.":
                block_start_idx = idx
                round_num = 0
                data.at[idx, "RoundNum"] = round_num

            if message.startswith("coinsetID:"):
                coinset_match = re.search(r"coinsetID:(\d+)", message)
                if coinset_match:
                    last_seen_coinset = int(coinset_match.group(1))

            block_match = re.search(r"Started watching other participant's (collecting|pin dropping)\. Block:\s*(\d+)", message)
            if block_match:
                block_type_raw = block_match.group(1)
                block_num = int(block_match.group(2))
                coinset_id = last_seen_coinset
                block_type = "pindropping" if block_type_raw == "pin dropping" else "collecting"

                backfill_start = block_start_idx if block_start_idx is not None else idx
                for j in range(backfill_start + 1, idx + 1):
                    data.at[j, "BlockNum"] = block_num
                    data.at[j, "CoinSetID"] = coinset_id
                    data.at[j, "BlockType"] = block_type
                block_start_idx = None

            if "Repositioned and ready to start block or round" in message:
                if round_num is not None:
                    round_num += 1
                    data.at[idx, "RoundNum"] = round_num

    return data

def detect_block_completeness(data, block_num, block_rows):
    messages = block_rows["Message"].dropna().str.lower()

    has_start = any("mark should happen" in m for m in messages)
    has_reposition = any("repositioned and ready to start" in m for m in messages)
    has_end = any(
        "finished current task" in m or
        "finished watching other participant's" in m
        for m in messages
    )

    if has_start and has_end:
        return "complete"
    elif has_start and not has_end:
        return "truncated"
    else:
        return "incomplete"  # fallback if block structure is unclear

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

###########################

def assign_7777(data, numOfCoins):
    """
    Assign RoundNum = 7777 starting from the 3rd 'A.N. collected coin' line
    within a round, continuing until the next round (or end-of-block transition).
    """
    block_nums = data["BlockNum"].dropna().unique()

    for block in block_nums:
        block_rows = data[data["BlockNum"] == block]
        current_round = None
        pin_count = 0
        third_coin_idx = None

        for idx in block_rows.index:
            msg = str(data.at[idx, "Message"]).strip().lower()
            round_num = data.at[idx, "RoundNum"]

            if round_num != current_round:
                current_round = round_num
                pin_count = 0
                third_coin_idx = None

            if msg.startswith("a.n. collected coin"):
                pin_count += 1
                if pin_count == numOfCoins:
                    third_coin_idx = idx

            if third_coin_idx and round_num == current_round:
                # Find transition point to next round
                lookahead_rows = data[(data.index > third_coin_idx) & (data["BlockNum"] == block)]
                for j in lookahead_rows.index:
                    r = data.at[j, "RoundNum"]
                    if r != current_round:
                        for k in range(third_coin_idx, j):
                            data.at[k, "RoundNum"] = 7777
                        third_coin_idx = None
                        break

    return data

def assign_8888(data):
    block_starts = data[data["Message"] == "Mark should happen if checked on terminal."].index
    for i in block_starts:
        block = data.at[i, "BlockNum"]
        block_rows = data[data["BlockNum"] == block]
        first_repos = block_rows[block_rows["Message"].str.contains("Repositioned", na=False)].index.min()
        first_watch = block_rows[block_rows["Message"].str.contains("Started watching", na=False)].index.min()
        if pd.notna(first_repos) and pd.notna(first_watch):
            for j in range(first_repos, first_watch):
                data.at[j, "RoundNum"] = 8888
    return data

def assign_9999(data):
    for idx, row in data.iterrows():
        msg = str(row["Message"]).strip().lower()
        if msg.startswith("finished watching "):
            block = row["BlockNum"]
            end = data[(data.index > idx) & (data["BlockNum"] == block)].index
            for j in end:
                data.at[j - 1, "RoundNum"] = 9999
    return data


###########################

def process_obsreward_file(data, file_path, numOfCoins):
    # Initialize all relevant columns
    data['BlockNum'] = None
    data['RoundNum'] = None
    data['BlockType'] = None
    data['CoinSetID'] = None
    data['BlockStatus'] = None

    # Step 1: Detect block boundaries and assign rounds
    data = detect_and_tag_blocks(data)

    # Step 2: Forward-fill structural info (block-level metadata and round)
    data = forward_fill_block_info(data)

    # Step 3: Add transitional phase codes
    data = assign_7777(data, numOfCoins)
    data = assign_8888(data)
    data = assign_9999(data)

    # Step 4: Fill forward any missing round numbers from prior known state
    data["RoundNum"] = data["RoundNum"].fillna(method="ffill")

    # Step 5: Fill any remaining null messages for tracking
    data["Messages_filled"] = data["Message"].fillna(method='ffill')

    # Step 6: Label completeness
    for block_num in data['BlockNum'].dropna().unique():
        block_mask = data['BlockNum'] == block_num
        block_rows = data[block_mask]
        status = detect_block_completeness(data, block_num, block_rows)
        data.loc[block_mask, 'BlockStatus'] = status

    # Save result
    data.to_csv(file_path, index=False)
    print(f"Processed and saved: {file_path}")

### version of clean_and_process_files that saves the files in a nested directory 'unaligned'
# def clean_and_process_files(root_directory, output_root_directory, magic_leap_data, numOfCoins, save_large_files=True, max_memory_mb=500):
#     pattern = re.compile(r"^ObsReward_B_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
#     print('Started PO-specific processing!')

#     for dirpath, _, filenames in os.walk(root_directory):
#         for filename in filenames:
#             #print(f"Checking file: {filename}")
#             if pattern.match(filename):  
#                 print(f'Match found: {filename}')
#                 file_path = os.path.join(dirpath, filename)

#                 relative_path = os.path.relpath(dirpath, root_directory)
#                 output_dir = os.path.join(output_root_directory, relative_path, 'unaligned')
#                 os.makedirs(output_dir, exist_ok=True)

#                 outFile = os.path.join(output_dir, f"{filename.replace('.csv', '_processed_unaligned.csv')}")
#                 print(f"Processing file: {file_path}")
#                 #data = pd.read_csv(file_path)
#                 data = correct_extraColumns(file_path, output_dir, save_large_files, max_memory_mb)
#                 # ✅ Continue processing
#                 process_obsreward_file(data, outFile, numOfCoins)
def clean_and_process_files(root_directory, output_root_directory, magic_leap_data, numOfCoins, save_large_files=True, max_memory_mb=500):
    pattern = re.compile(r"^ObsReward_B_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
    print('Started PO-specific processing!')

    for dirpath, _, filenames in os.walk(root_directory):
        for filename in filenames:
            #print(f"Checking file: {filename}")
            if pattern.match(filename):  
                print(f'Match found: {filename}')
                file_path = os.path.join(dirpath, filename)

                relative_path = os.path.relpath(dirpath, root_directory)
                output_dir = os.path.join(output_root_directory, relative_path)
                os.makedirs(output_dir, exist_ok=True)

                outFile = os.path.join(output_dir, f"{filename.replace('.csv', '_processed.csv')}")
                print(f"Processing file: {file_path}")
                #data = pd.read_csv(file_path)
                data = correct_extraColumns(file_path, output_dir, save_large_files, max_memory_mb)
                # ✅ Continue processing
                process_obsreward_file(data, outFile, numOfCoins)
###########################
# All the files
root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData"
output_root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/ProcessedData"

# # Single Test File 
# root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile/RawData"
# output_root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile/ProcessedData"

###########################
metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")

# Start processing files
clean_and_process_files(root_directory, output_root_directory, magic_leap_data, 3,  save_large_files=True, max_memory_mb=500)
