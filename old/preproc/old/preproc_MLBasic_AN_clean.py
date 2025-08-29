
import os
import pandas as pd
import numpy as np
import re

def correct_malformed_string(raw_string):
    """Fixes concatenated numeric values."""
    pattern = r"(\d+\.\d{3})(\d+\.\d{3})(\d+\.\d{3})"
    return re.sub(pattern, r"\1 \2 \3", raw_string)

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
                
def detect_and_tag_blocks(data):
    block_start_idx = None
    block_num = None
    block_type = None
    coinset_id = None
    buffer_indices = []

    for idx, row in data.iterrows():
        message = row.get("Message", "")

        if isinstance(message, str):
            if message == "Mark should happen if checked on terminal.":
                # New block start
                block_start_idx = idx
                buffer_indices = [idx]
                block_num = None
                block_type = None
                coinset_id = None

            elif block_start_idx is not None:
                buffer_indices.append(idx)

                block_match = re.search(r"Started.*Block:\s*(\d+)", message)
                if block_match:
                    block_num = int(block_match.group(1))
                    if "pindropping" in message.lower():
                        block_type = "pindropping"
                    elif "collecting" in message.lower():
                        block_type = "collection"

                coinset_match = re.search(r"coinsetID:(\d+)", message)
                if coinset_match:
                    coinset_id = int(coinset_match.group(1))

                if block_num is not None and coinset_id is not None:
                    for j in buffer_indices:
                        data.at[j, "BlockNum"] = block_num
                        data.at[j, "BlockType"] = block_type
                        data.at[j, "CoinSetID"] = coinset_id
                    block_start_idx = None
                    buffer_indices = []

def forward_fill_block_info(data):
    current_block_num = None
    current_coinset_id = None
    current_block_type = None
    round_num = 0

    for idx, row in data.iterrows():
        message = row.get("Message", "")

        if not pd.isna(row["BlockNum"]) and row["BlockNum"] != current_block_num:
            current_block_num = row["BlockNum"]
            current_coinset_id = row["CoinSetID"]
            current_block_type = row["BlockType"]
            round_num = 0  # New block starts

        if current_block_num is not None:
            data.at[idx, "BlockNum"] = current_block_num
            data.at[idx, "CoinSetID"] = current_coinset_id
            data.at[idx, "BlockType"] = current_block_type
            data.at[idx, "RoundNum"] = round_num

            if isinstance(message, str) and "Repositioned and ready to start block or round" in message:
                round_num += 1


def process_obsreward_file(data, file_path):
    data['BlockNum'] = None
    data['RoundNum'] = None
    data['BlockType'] = None
    data['CoinSetID'] = None

    detect_and_tag_blocks(data)
    forward_fill_block_info(data)
    data["Messages_filled"] = data["Message"].fillna(method='ffill')

    data.to_csv(file_path, index=False)
    print(f"Processed and saved: {file_path}")




# Execution block
root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData"
output_root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/ProcessedData"

metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")

clean_and_process_files(root_directory, output_root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500)