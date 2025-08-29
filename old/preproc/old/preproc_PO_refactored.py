import os
import pandas as pd
import numpy as np
import re
from datetime import datetime

def clean_and_process_files(root_directory, output_root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500):
    """
    Recursively processes all ObsReward_A and ObsReward_B files.
    - Saves processed files in a mirrored directory structure under `output_root_directory`.
    """
    pattern = re.compile(r"^ObsReward_B_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
    print('I have started!')
    for dirpath, _, filenames in os.walk(root_directory):
        #print('step 1')
        for filename in filenames:
            #print('step 2')
            #print(filename)
            if pattern.match(filename):  
                print('match found!')
                file_path = os.path.join(dirpath, filename)
                
                # Compute relative path and mirror structure in the new directory
                relative_path = os.path.relpath(dirpath, root_directory)
                output_dir = os.path.join(output_root_directory, relative_path)
                os.makedirs(output_dir, exist_ok=True)  # Ensure mirrored directory exists
                
                outFile = os.path.join(output_dir, f"{filename.replace('.csv', '_processed.csv')}")
                print(f"Cleaning and processing file: {file_path}")
                data = clean_obsreward_b_file(file_path, output_dir, save_large_files, max_memory_mb)

                process_obsreward_file(data, outFile)  # Process cleaned/regular data

def clean_obsreward_b_file(file_path, output_dir, save_large_files, max_memory_mb):
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

def track_rounds(data):
    """
    Tracks rounds within each block.
    Ensures RoundNum increments only when a new round starts, not on every row.
    """
    round_num = 0
    prev_block_num = None
    prev_message = None  # Track previous message to detect repeated "Started..." messages

    for idx, row in data.iterrows():
        current_block_num = row["BlockNum"]
        message = row.get("Message", "")

        # If BlockNum changes, reset round number (New Block)
        if pd.notna(current_block_num) and current_block_num != prev_block_num:
            round_num = 1  # First round of new block

        elif pd.notna(current_block_num) and isinstance(message, str) and re.search(r"Repositioned and ready to start block or round", message):
            round_num += 1  # Every "Repositioned..." message signals a new round


        # # Detect a new round by checking for repeated "Started..." messages within the same block
        # elif pd.notna(current_block_num) and isinstance(message, str) and re.search(r"^Started.*Block:", message):
        #     if prev_message and "started" in message.lower() and prev_message != message:
        #         round_num += 1  # Only increment if it's a different "Started..." message

        # Store updated round number
        data.at[idx, "RoundNum"] = round_num
        prev_block_num = current_block_num  # Update previous block tracker
        prev_message = message  # Track last message to detect new rounds

    return data


def process_obsreward_file(data, file_path):
    data['BlockNum'] = None
    data['RoundNum'] = None
    data['BlockType'] = None
    data['CoinSetID'] = None

    block_starts = []
    coinset_indices = []

    # First pass: find all relevant indices for block starts and coinsetID
    for idx, row in data.iterrows():
        message = row.get("Message", "")
        if isinstance(message, str):
            block_match = re.search(r"^Started.*Block:\s*(\d+)", message)
            coinset_match = re.search(r"coinsetID:(\d+)", message)

            if block_match:
                block_starts.append((idx, int(block_match.group(1))))
            if coinset_match:
                coinset_indices.append((idx, int(coinset_match.group(1))))

    # Second pass: assign block data from start to next block or end
    for i, (start_idx, block_num) in enumerate(block_starts):
        prev_coinset_idx = None
        coinset_id = None
        for idx, cid in reversed(coinset_indices):
            if idx <= start_idx:
                prev_coinset_idx = idx
                coinset_id = cid
                break

        end_idx = block_starts[i + 1][0] if i + 1 < len(block_starts) else len(data)

        # Infer BlockType once at start_idx
        blocktype = None
        for look_idx in range(start_idx, end_idx):
            msg = data.at[look_idx, "Message"]
            if isinstance(msg, str):
                if "pindropping" in msg.lower() or "pin dropping" in msg.lower():
                    blocktype = "pindropping"
                    break
                elif "watching other participant's collecting" in msg.lower() or "started collecting" in msg.lower():
                    blocktype = "collection"
                    break

        if prev_coinset_idx is not None:
            round_num = 0
            for j in range(prev_coinset_idx, end_idx):
                data.at[j, "BlockNum"] = block_num
                data.at[j, "CoinSetID"] = coinset_id
                data.at[j, "BlockType"] = blocktype

                if "Repositioned and ready to start block or round" in str(data.at[j, "Message"]):
                    round_num += 1
                data.at[j, "RoundNum"] = round_num

    # Preserve original messages
    data["Messages_filled"] = data["Message"].copy()

    data.to_csv(file_path, index=False)
    print(f"Processed and saved: {file_path}")




# Set root directory for input and output
root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData"
output_root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/ProcessedData"

metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")

# Start processing files
clean_and_process_files(root_directory, output_root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500)

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

# Updated assign_temporal_intervals with new phase definitions (6666, 7777, 8888, 9999)
# Simplified version that removes 6666 logic and keeps RoundNum = 0 for pre-first-round period

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