import os
import pandas as pd
import numpy as np
import re

def clean_and_process_files(root_directory, save_large_files=True, max_memory_mb=500):
    """
    Recursively processes all ObsReward_A and ObsReward_B files.
    - Cleans ObsReward_B files in memory (if small).
    - Saves intermediate cleaned files for large B files if save_large_files=True.
    """
    pattern = re.compile(r"^ObsReward_[AB]_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
    
    for dirpath, dirnames, filenames in os.walk(root_directory):
        # **Skip the "processed" subdirectory** to prevent reprocessing
        if "processed" in dirpath.split(os.sep):
            continue

        for filename in filenames:
            if pattern.match(filename) and not filename.endswith("_processed.csv"):  # Prevent `_processed` reprocessing
                file_path = os.path.join(dirpath, filename)
                # Ensure the "processed" directory exists before saving output files
                outFolder = os.path.join(dirpath, "processed")
                os.makedirs(outFolder, exist_ok=True)  # Create if it doesn't exist
                outFile = os.path.join(outFolder, f"{os.path.basename(file_path).replace('.csv', '_processed.csv')}")
                #outDir = os.path.join(outFolder, filename)
                # If file is "ObsReward_B", clean it before processing
                #process_obsreward_file(data, outFile)

                if filename.startswith("ObsReward_B"):
                    print(f"Cleaning and processing file: {file_path}")
                    data = clean_obsreward_b_file(file_path, dirpath, save_large_files, max_memory_mb)
                else:
                    print(f"Processing file: {file_path}")
                    data = pd.read_csv(file_path)  # Directly read A files

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
    data_cleaned.columns = data_cleaned.iloc[0]
    data_cleaned = data_cleaned[1:]

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
    data['MarkSent'] = None
    data['SwapBlockVote'] = None

    block_num = 0
    round_num = 0
    block_type = 'pindropping'
    block_in_progress = False
    current_block_num = None
    current_round_num = None
    current_block_type = None
    current_coinset_id = None

    for idx, row in data.iterrows():
        message = row['Message']

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

        # Block/Round detection
        if "Repositioned and ready to start block or round" in message:
            if not block_in_progress:
                block_num += 1
                round_num = 1
                block_in_progress = True
            else:
                round_num += 1
            
            block_type = 'pindropping'
            current_block_num = block_num
            current_round_num = round_num
            current_block_type = block_type

        if re.search(r"Finished pindrop round:", message):
            block_in_progress = False

        if "finished current task" in message:
            block_in_progress = False

        if "Chest opened: 0" in message:
            block_type = 'collecting'
            current_block_type = block_type
            data.loc[data['BlockNum'] == block_num, 'BlockType'] = block_type

        # Assign MarkSent (1 = True, 0 = False)
        data.at[idx, 'MarkSent'] = 1 if "Sending Headset mark" in message else 0

        # Assign SwapBlockVote
        if current_coinset_id is not None:
            if current_coinset_id != 1 and ("Observer says it was a NEW round." in message or "Active Navigator says it was a NEW round." in message):
                swap_vote = "Correct"
            elif current_coinset_id == 1 and ("Observer says it was an OLD round." in message or "Active Navigator says it was an OLD round." in message):
                swap_vote = "Correct"
            elif current_coinset_id != 1 and ("Observer says it was a OLD round." in message or "Active Navigator says it was a OLD round." in message):
                swap_vote = "Incorrect"
            elif current_coinset_id == 1 and ("Observer says it was an NEW round." in message or "Active Navigator says it was an NEW round." in message):
                swap_vote = "Incorrect"
        else:
            swap_vote = None  # If CoinSetID is missing, leave it empty

        data.at[idx, 'SwapBlockVote'] = swap_vote

    data.fillna(method='ffill', inplace=True)  # Fill missing values
    data.to_csv(file_path, index=False)  # Overwrite file
    print(f"Processed and saved: {file_path}")

def correct_malformed_string(raw_string):
    """Fixes concatenated numeric values."""
    pattern = r"(\d+\.\d{3})(\d+\.\d{3})(\d+\.\d{3})"
    return re.sub(pattern, r"\1 \2 \3", raw_string)

# Set root directory and process all files
root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/pair_06/02082025/ML2C"
output_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/pair_06/02082025/ML2C/processed"
clean_and_process_files(root_directory, save_large_files=True, max_memory_mb=500)
