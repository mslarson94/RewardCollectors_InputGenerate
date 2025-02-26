import os
import pandas as pd
import numpy as np
import re

def clean_and_process_files(root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500):
    """
    Recursively processes all ObsReward_A and ObsReward_B files.
    - Cleans ObsReward_B files in memory (if small).
    - Saves intermediate cleaned files for large B files if save_large_files=True.
    - Merges metadata from `MagicLeapFiles`.
    """
    pattern = re.compile(r"^ObsReward_[AB]_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
    
    for dirpath, _, filenames in os.walk(root_directory):
        for filename in filenames:
            if pattern.match(filename):  
                file_path = os.path.join(dirpath, filename)
                outFolder = os.path.join(dirpath, "processed")
                os.makedirs(outFolder, exist_ok=True)  # Ensure "processed" folder exists
                outFile = os.path.join(outFolder, f"{os.path.basename(file_path).replace('.csv', '_processed.csv')}")

                if filename.startswith("ObsReward_B"):
                    print(f"Cleaning and processing file: {file_path}")
                    data = clean_obsreward_b_file(file_path, dirpath, save_large_files, max_memory_mb)
                else:
                    print(f"Processing file: {file_path}")
                    data = pd.read_csv(file_path)  # Directly read A files

                # Merge metadata
                #data = merge_metadata(data, filename, magic_leap_data)

                process_obsreward_file(data, outFile)  # Process cleaned/regular data

def clean_obsreward_b_file(file_path, dirpath, save_large_files, max_memory_mb):
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
        cleaned_file_path = os.path.join(dirpath, f"{os.path.basename(file_path).replace('.csv', '_cleaned.csv')}")
        data_cleaned.to_csv(cleaned_file_path, index=False)
        print(f"Saved intermediate cleaned file: {cleaned_file_path}")
        return pd.read_csv(cleaned_file_path)  # Reload for processing

    return data_cleaned  # Return cleaned data for direct processing

def merge_metadata(data, filename, metadata):
    """
    Merges metadata information (from the MagicLeapFiles sheet) into the data DataFrame.
    """
    if not isinstance(metadata, pd.DataFrame):
        raise ValueError("Metadata is not a valid DataFrame.")
    
    # Ensure the column 'MagicLeapFiles' exists
    if "MagicLeapFiles" not in metadata.columns:
        raise KeyError("'MagicLeapFiles' column not found in metadata.")
    
    # Match the file with its metadata
    matched_metadata = metadata[metadata["MagicLeapFiles"].str.strip() == filename.strip()]

    if matched_metadata.empty:
        print(f"⚠ Warning: No metadata found for file: {filename}")
        return data  # Return original data if no match is found

    # Extract metadata values
    metadata_values = matched_metadata.iloc[0]

    # Add metadata as new columns
    for column in ["participantID", "pairID", "AorB", "coinSet", "main_RR", "device"]:
        if column in metadata_values:
            data[column] = metadata_values[column]
        else:
            print(f"⚠ Warning: Column {column} not found in metadata!")

    return data


def process_obsreward_file(data, file_path):
    """
    Processes ObsReward_A or cleaned ObsReward_B files.
    Adds BlockNum, RoundNum, BlockType, CoinSetID, MarkSent, and SwapBlockVote.
    Overwrites the original file.
    """
    data['BlockNum'] = None
    data['RoundNum'] = None
    data['BlockType'] = None
    data['CoinSetID'] = None
    #data['MarkSent'] = None
    #data['SwapBlockVote'] = None

    block_num = None
    round_num = 0
    block_type = 'pindropping'
    block_in_progress = False
    current_block_num = None
    current_round_num = None
    current_block_type = None
    current_coinset_id = None

    for idx, row in data.iterrows():
        message = row.get('Message', None)

        if not isinstance(message, str):
            data.at[idx, 'BlockNum'] = current_block_num
            data.at[idx, 'RoundNum'] = current_round_num
            data.at[idx, 'BlockType'] = current_block_type
            data.at[idx, 'CoinSetID'] = current_coinset_id
            continue
        
        # Extract CoinSetID
        coinset_match = re.search(r"coinsetID:(\d+)", message)
        if coinset_match and not block_in_progress:
            current_coinset_id = int(coinset_match.group(1))

        # Fix malformed strings
        if "Closest location was:" in message:
            try:
                raw_string = re.search(r"Closest location was: (.+)", message).group(1)
                corrected_string = correct_malformed_string(raw_string)
                data.at[idx, 'Message'] = message.replace(raw_string, corrected_string)
            except Exception as e:
                print(f"Error correcting malformed string in message '{message}': {e}")

        # If a round or block is about to start, temporarily mark it as "interblock"
        if "Repositioned and ready to start block or round" in message:
            block_in_progress = True
            data.at[idx, 'BlockNum'] = "interblock"

        # If a 'Started ... Block:X' message appears, confirm block number and type
        elif block_in_progress and re.search(r"^Started.*Block:\s*(\d+)", message):
            block_num_match = re.search(r"Block:\s*(\d+)", message)
            if block_num_match:
                block_num = int(block_num_match.group(1))

                # Determine BlockType based on message content
                if "pindropping" in message or "watching other participant's pin dropping" in message:
                    current_block_type = "pindropping"
                elif "collecting" in message or "watching other participant's collecting" in message:
                    current_block_type = "collection"
                else:
                    current_block_type = "unknown"

                # Retroactively replace all "interblock" entries with the correct block number and type
                data.loc[data['BlockNum'] == "interblock", 'BlockNum'] = block_num
                data.loc[data['BlockNum'] == block_num, 'BlockType'] = current_block_type

                # Update the current row
                data.at[idx, 'BlockNum'] = block_num
                data.at[idx, 'BlockType'] = current_block_type

        # If the block number has been assigned, ensure it carries forward to subsequent rows
        elif block_num is not None:
            data.at[idx, 'BlockNum'] = block_num
            data.at[idx, 'BlockType'] = current_block_type

        # Explicitly mark block as finished when the task ends
        if "Finished pindrop round:" in message or "finished current task" in message:
            block_in_progress = False

        # # Assign MarkSent (1 = True, 0 = False)
        # data.at[idx, 'MarkSent'] = 1 if "Sending Headset mark" in message else 0

        # # Assign SwapBlockVote
        # if current_coinset_id is not None:
        #     if current_coinset_id != 1 and ("Observer says it was a NEW round." in message or "Active Navigator says it was a NEW round."):
        #         swap_vote = "Correct"
        #     elif current_coinset_id == 1 and ("Observer says it was an OLD round." in message or "Active Navigator says it was an OLD round."):
        #         swap_vote = "Correct"
        #     else:
        #         swap_vote = "Incorrect"
        # else:
        #     swap_vote = None

        # data.at[idx, 'SwapBlockVote'] = swap_vote

    # Preserve the original Messages column before modifying it
    data["Messages_filled"] = data["Message"].copy()

    # Apply forward fill only to Messages_filled to retain the original 'Message' column
    data.loc[:, "Messages_filled"] = data["Messages_filled"].fillna(method="ffill")

    # Fill missing values ONLY for non-metadata processing columns (BlockNum, RoundNum, etc.)
    data.loc[:, ["BlockNum", "RoundNum", "BlockType", "CoinSetID"]] = data[["BlockNum", "RoundNum", "BlockType", "CoinSetID"]].fillna(method='ffill')


    data.to_csv(file_path, index=False)  # Overwrite file
    print(f"Processed and saved: {file_path}")

def correct_malformed_string(raw_string):
    """Fixes concatenated numeric values."""
    pattern = r"(\d+\.\d{3})(\d+\.\d{3})(\d+\.\d{3})"
    return re.sub(pattern, r"\1 \2 \3", raw_string)
metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
# Set root directory and process all files
magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")
root_directory = "/Users/mairahmac/Desktop/ExtraSelectedData"
clean_and_process_files(root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500)
