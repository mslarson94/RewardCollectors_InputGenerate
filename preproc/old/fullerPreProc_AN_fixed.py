
import os
import pandas as pd
import numpy as np
import re
from datetime import datetime

def clean_and_process_files(root_directory, output_root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500):
    pattern = re.compile(r"^ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
    print('Started AN-specific processing!')
    
    for dirpath, _, filenames in os.walk(root_directory):
        for filename in filenames:
            if pattern.match(filename):  
                print(f'Match found: {filename}')
                file_path = os.path.join(dirpath, filename)
                
                relative_path = os.path.relpath(dirpath, root_directory)
                output_dir = os.path.join(output_root_directory, relative_path)
                os.makedirs(output_dir, exist_ok=True)
                
                outFile = os.path.join(output_dir, f"{filename.replace('.csv', '_processed.csv')}")
                print(f"Processing file: {file_path}")
                data = pd.read_csv(file_path)

                process_obsreward_file(data, outFile)

def track_rounds(data):
    round_num = 0
    prev_block_num = None
    prev_message = None

    for idx, row in data.iterrows():
        current_block_num = row["BlockNum"]
        message = row.get("Message", "")

        if pd.notna(current_block_num) and current_block_num != prev_block_num:
            round_num = 1
        elif pd.notna(current_block_num) and isinstance(message, str) and "Repositioned and ready to start block or round" in message:
            round_num += 1

        data.at[idx, "RoundNum"] = round_num
        prev_block_num = current_block_num
        prev_message = message

    return data

def process_obsreward_file(data, file_path):
    data['BlockNum'] = None
    data['RoundNum'] = None
    data['BlockType'] = None
    data['CoinSetID'] = None

    block_starts = []
    coinset_indices = []

    for idx, row in data.iterrows():
        message = row.get("Message", "")
        if isinstance(message, str):
            block_match = re.search(r"^Started.*Block:\s*(\d+)", message)
            coinset_match = re.search(r"coinsetID:(\d+)", message)

            if block_match:
                block_starts.append((idx, int(block_match.group(1))))
            if coinset_match:
                coinset_indices.append((idx, int(coinset_match.group(1))))

    for i, (start_idx, block_num) in enumerate(block_starts):
        prev_coinset_idx = None
        coinset_id = None
        for idx, cid in reversed(coinset_indices):
            if idx <= start_idx:
                prev_coinset_idx = idx
                coinset_id = cid
                break

        end_idx = block_starts[i + 1][0] if i + 1 < len(block_starts) else len(data)

        if prev_coinset_idx is not None:
            # Determine block type first
            block_type = None
            for j in range(prev_coinset_idx, end_idx):
                message = data.at[j, "Message"]
                if isinstance(message, str):
                    if "pindropping" in message.lower() or "pin dropping" in message.lower():
                        block_type = "pindropping"
                    elif "watching other participant" in message.lower() or "started collecting" in message.lower():
                        block_type = "collection"

            round_num = 0
            for j in range(prev_coinset_idx, end_idx):
                data.at[j, "BlockNum"] = block_num
                data.at[j, "CoinSetID"] = coinset_id
                data.at[j, "BlockType"] = block_type

                message = data.at[j, "Message"]
                if isinstance(message, str):
                    if "Repositioned and ready to start block or round" in message:
                        round_num += 1

                data.at[j, "RoundNum"] = round_num

    data["Messages_filled"] = data["Message"].fillna(method='ffill')
    data.to_csv(file_path, index=False)
    print(f"Processed and saved: {file_path}")


# Set root directory for input and output
root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData"
output_root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/ProcessedData"

metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")

# Start processing files
clean_and_process_files(root_directory, output_root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500)

