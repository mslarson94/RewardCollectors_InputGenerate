
import os
import pandas as pd
import numpy as np
import re

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

def process_obsreward_file(data, file_path):
    data['BlockNum'] = None
    data['RoundNum'] = None
    data['BlockType'] = None
    data['CoinSetID'] = None

    def assign_block_to_rows(indices, blk_num, blk_type, coin_id, start_round):
        r = start_round
        for j in indices:
            msg = data.at[j, "Message"]
            data.at[j, "BlockNum"] = blk_num
            data.at[j, "BlockType"] = blk_type
            data.at[j, "CoinSetID"] = coin_id
            data.at[j, "RoundNum"] = r
            if isinstance(msg, str) and "Repositioned and ready to start block or round" in msg:
                r += 1

    block_start_idx = None
    temp_indices = []
    current_block_num = None
    current_block_type = None
    current_coinset_id = None
    round_num = 1

    for idx, row in data.iterrows():
        message = row.get("Message", "")

        if isinstance(message, str):
            # When we hit a new block marker, assign the previous block if ready
            if message == "Mark should happen if checked on terminal.":
                if temp_indices and current_block_num is not None and current_coinset_id is not None:
                    assign_block_to_rows(temp_indices, current_block_num, current_block_type, current_coinset_id, round_num)
                # Start buffering new block
                block_start_idx = idx
                temp_indices = [idx]
                current_block_num = None
                current_block_type = None
                current_coinset_id = None
                round_num = 1
                continue

            if block_start_idx is not None:
                temp_indices.append(idx)

                # Look for "Started...Block:X"
                block_match = re.search(r"Started.*Block:\s*(\d+)", message)
                if block_match:
                    current_block_num = int(block_match.group(1))
                    if "pindropping" in message.lower():
                        current_block_type = "pindropping"
                    elif "collecting" in message.lower():
                        current_block_type = "collection"

                # Look for "coinsetID:X"
                coinset_match = re.search(r"coinsetID:(\d+)", message)
                if coinset_match:
                    current_coinset_id = int(coinset_match.group(1))

    # Final cleanup: assign last buffered block if valid
    if temp_indices and current_block_num is not None and current_coinset_id is not None:
        assign_block_to_rows(temp_indices, current_block_num, current_block_type, current_coinset_id, round_num)

    data["Messages_filled"] = data["Message"].fillna(method='ffill')
    data.to_csv(file_path, index=False)
    print(f"Processed and saved: {file_path}")
