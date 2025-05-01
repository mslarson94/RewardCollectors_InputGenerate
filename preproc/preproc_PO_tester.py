import os
import pandas as pd
import numpy as np
import re

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

# Updated assign_temporal_intervals with new phase definitions (6666, 7777, 8888, 9999)
# Simplified version that removes 6666 logic and keeps RoundNum = 0 for pre-first-round period
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

            if isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
                last_reposition = idx

            if isinstance(message, str) and (
                message.startswith("A.N. finished a perfect dropround with:") or
                message.startswith("A.N. finished a dropround with:")
            ):
                last_finished_round = idx

            if last_reposition is not None and isinstance(message, str) and re.match(
                r"Started watching other participant's (collecting|pin dropping)\. Block:\s*\d+", message):
                for j in range(last_reposition, idx):
                    data.at[j, "RoundNum"] = 8888
                last_reposition = None

            if isinstance(message, str) and message.lower().strip() == "finished current task":
                block_id = data.at[idx, "BlockNum"]
                post_rows = data[(data.index > idx-1) & (data["BlockNum"] == block_id)]
                for j in post_rows.index:
                    if data.at[j, "RoundNum"] not in [7777, 8888]:
                        data.at[j, "RoundNum"] = 9999
    return data

# # mostly working
# def process_obsreward_file(data, file_path):
#     data['BlockNum'] = None
#     data['RoundNum'] = None
#     data['BlockType'] = None
#     data['CoinSetID'] = None

#     detect_and_tag_blocks(data)
#     forward_fill_block_info(data)
#     assign_temporal_intervals(data)
#     data["Messages_filled"] = data["Message"].fillna(method='ffill')
#     data['BlockStatus'] = None
#     for block_num in data['BlockNum'].dropna().unique():
#         block_mask = data['BlockNum'] == block_num
#         status = detect_block_completeness(data, block_num, data[block_mask])
#         data.loc[block_mask, 'BlockStatus'] = status
#     data.to_csv(file_path, index=False)
#     print(f"Processed and saved: {file_path}")

def process_obsreward_file(data, file_path):
    # Ensure necessary columns exist
    data['BlockNum'] = None
    data['RoundNum'] = None
    data['BlockType'] = None
    data['CoinSetID'] = None
    data['BlockStatus'] = None  # Ensure it's initialized

    # Sequential processing for structure and metadata
    data = detect_and_tag_blocks(data)
    data = forward_fill_block_info(data)
    data = assign_temporal_intervals(data)

    # Fill any remaining null messages for reference
    data["Messages_filled"] = data["Message"].fillna(method='ffill')

    # Detect completeness only after full annotation
    unique_blocks = data['BlockNum'].dropna().unique()
    for block_num in unique_blocks:
        block_mask = data['BlockNum'] == block_num
        block_rows = data[block_mask]
        status = detect_block_completeness(data, block_num, block_rows)
        data.loc[block_mask, 'BlockStatus'] = status

    # Save result
    data.to_csv(file_path, index=False)
    print(f"Processed and saved: {file_path}")


# working version 
def clean_and_process_files(root_directory, output_root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500):
    pattern = re.compile(r"^ObsReward_B_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
    print('Started PO-specific processing!')

    for dirpath, _, filenames in os.walk(root_directory):
        for filename in filenames:
            #print(f"Checking file: {filename}")
            if pattern.match(filename):  
                print(f'Match found: {filename}')
                file_path = os.path.join(dirpath, filename)

                relative_path = os.path.relpath(dirpath, root_directory)
                output_dir = os.path.join(output_root_directory, relative_path, 'unaligned')
                os.makedirs(output_dir, exist_ok=True)

                outFile = os.path.join(output_dir, f"{filename.replace('.csv', '_processed_unaligned.csv')}")
                print(f"Processing file: {file_path}")
                #data = pd.read_csv(file_path)
                data = correct_extraColumns(file_path, output_dir, save_large_files, max_memory_mb)
                # ✅ Continue processing
                process_obsreward_file(data, outFile)

def extract_testing_date(filename):
    """Extract date in MM_DD_YYYY format from filename."""
    match = re.search(r"\d{2}_\d{2}_\d{4}", filename)
    return match.group(0) if match else "unknown_date"

# def clean_and_process_files(root_directory, output_root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500):
#     pattern = re.compile(r"^ObsReward_[AB]_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
#     print("Started processing!")

#     for dirpath, _, filenames in os.walk(root_directory):
#         for filename in filenames:
#             if pattern.match(filename):
#                 file_path = os.path.join(dirpath, filename)

#                 # ✅ Extract testing date from filename
#                 test_date = extract_testing_date(filename)

#                 # ✅ Rebuild output path including testing date
#                 relative_path = os.path.relpath(dirpath, root_directory)
#                 output_dir = os.path.join(output_root_directory, test_date, relative_path)

#                 os.makedirs(output_dir, exist_ok=True)
#                 outFile = os.path.join(output_dir, filename.replace(".csv", "_processed.csv"))

#                 print(f"Processing file: {file_path}")
#                 data = correct_extraColumns(file_path, output_dir, save_large_files, max_memory_mb)
#                 # ✅ Continue processing
#                 process_obsreward_file(data, outFile)
                

# # Execution block
# root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData"
# output_root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/ProcessedData"

# # Single Test File 
# root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile/RawData"
# output_root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile/ProcessedData"

# two pairs of participants 
root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/twopairs/RawData"
output_root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/twopairs/ProcessedData"

metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")

# Start processing files
clean_and_process_files(root_directory, output_root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500)
