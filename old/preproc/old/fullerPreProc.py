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
    pattern = re.compile(r"^ObsReward_[AB]_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
    
    for dirpath, _, filenames in os.walk(root_directory):
        for filename in filenames:
            if pattern.match(filename):  
                file_path = os.path.join(dirpath, filename)
                
                # Compute relative path and mirror structure in the new directory
                relative_path = os.path.relpath(dirpath, root_directory)
                output_dir = os.path.join(output_root_directory, relative_path)
                os.makedirs(output_dir, exist_ok=True)  # Ensure mirrored directory exists
                
                outFile = os.path.join(output_dir, f"{filename.replace('.csv', '_processed.csv')}")

                if filename.startswith("ObsReward_B"):
                    print(f"Cleaning and processing file: {file_path}")
                    data = clean_obsreward_b_file(file_path, output_dir, save_large_files, max_memory_mb)
                else:
                    print(f"Processing file: {file_path}")
                    data = pd.read_csv(file_path)  # Directly read A files

                # Merge metadata
                # data = merge_metadata(data, filename, magic_leap_data)

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
    """
    Processes ObsReward_A or cleaned ObsReward_B files.
    Ensures BlockType persists until block_in_progress = False.
    Uses separate function `track_rounds()` to handle round tracking.
    """
    data['BlockNum'] = None
    data['RoundNum'] = None
    data['BlockType'] = None
    data['CoinSetID'] = None

    block_num = None
    block_in_progress = False
    current_block_type = None
    current_coinset_id = None
    prev_block_num = None  # Track previous block number

    for idx, row in data.iterrows():
        message = row.get('Message', None)

        if not isinstance(message, str):
            # Carry forward values for missing messages
            data.at[idx, 'BlockNum'] = block_num
            data.at[idx, 'BlockType'] = current_block_type  # Persist block type
            data.at[idx, 'CoinSetID'] = current_coinset_id
            continue

        # Extract CoinSetID
        coinset_match = re.search(r"coinsetID:(\d+)", message)
        if coinset_match:
            current_coinset_id = int(coinset_match.group(1))

        # Detect new block based on "Started ... Block:X"
        block_match = re.search(r"^Started.*Block:\s*(\d+)", message)
        if block_match:
            new_block_num = int(block_match.group(1))

            if new_block_num != prev_block_num:
                block_in_progress = True  # Start tracking a new block

            prev_block_num = new_block_num  # Update previous block tracker
            block_num = new_block_num  # Update block number

        # Detect BlockType **only if a new block starts**
        if block_in_progress:
            if "pindropping" in message.lower() or "pin dropping" in message.lower():
                current_block_type = "pindropping"
            elif "watching other participant's collecting" in message.lower() or "started collecting" in message.lower():
                current_block_type = "collection"

        # Ensure BlockType persists while block is in progress
        if block_in_progress and current_block_type is None:
            current_block_type = data.at[idx - 1, 'BlockType'] if idx > 0 else "unknown"

        # Store values
        data.at[idx, 'BlockNum'] = block_num
        data.at[idx, 'BlockType'] = current_block_type
        data.at[idx, 'CoinSetID'] = current_coinset_id

        # If block ends, stop persisting BlockType
        if "Finished pindrop round:" in message or "finished current task" in message:
            block_in_progress = False

    # Preserve the original Messages column
    data["Messages_filled"] = data["Message"].copy()

    # Call the separate function to track rounds correctly
    data = track_rounds(data)

    # Save processed file in the mirrored output directory
    data.to_csv(file_path, index=False)
    print(f"Processed and saved: {file_path}")



# Set root directory for input and output
root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData"
output_root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/ProcessedData"

metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")

# Start processing files
clean_and_process_files(root_directory, output_root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500)
