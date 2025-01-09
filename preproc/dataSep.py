
import pandas as pd
import os
import re

# Function to correct malformed strings
def correct_malformed_string(raw_string):
    pattern = r"(\d+\.\d{3})(\d+\.\d{3})(\d+\.\d{3})"  # Match concatenated numbers
    corrected = re.sub(pattern, r"\1 \2 \3", raw_string)  # Add spaces between numbers
    return corrected


# Define the pattern for matching files
pattern = re.compile(r"^ObsReward_[AB]_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")

def process_blocks_rounds_with_correct_rounds(data):
    # Initialize columns
    data['BlockNum'] = None
    data['RoundNum'] = None
    data['BlockType'] = None
    data['CoinSetID'] = None

    block_num = 0
    round_num = 0
    block_type = 'pindropping'
    round_in_progress = False
    block_in_progress = False

    # Track the current block, round, and coin set ID
    current_block_num = None
    current_round_num = None
    current_block_type = None
    current_coinset_id = None

    # Iterate through the data and populate BlockNum, RoundNum, BlockType, and CoinSetID
    for idx, row in data.iterrows():
        message = row['Message']
        
        # Ensure message is a string and not NaN
        if not isinstance(message, str):
            # Persist the current block and round values to the row even if no message
            data.at[idx, 'BlockNum'] = current_block_num
            data.at[idx, 'RoundNum'] = current_round_num
            data.at[idx, 'BlockType'] = current_block_type
            data.at[idx, 'CoinSetID'] = current_coinset_id
            continue
        
        # Extract CoinSetID if available
        coinset_match = re.search(r"coinsetID:(\d+)", message)
        if coinset_match and not block_in_progress:
            current_coinset_id = int(coinset_match.group(1))

        # Correct malformed strings if detected
        if "Closest location was:" in message:
            try:
                raw_string = re.search(r"Closest location was: (.+)", message).group(1)
                corrected_string = correct_malformed_string(raw_string)
                # Replace the malformed part in the original message
                data.at[idx, 'Message'] = message.replace(raw_string, corrected_string)
                print(f"Corrected malformed string in row {idx}")
            except Exception as e:
                print(f"Error correcting malformed string in message '{message}': {e}")

        # Check for block or round start
        if "Repositioned and ready to start block or round" in message:
            if not block_in_progress:
                # New block starts
                block_num += 1
                round_num = 1  # Reset round number when a new block starts
                block_in_progress = True
            else:
                # New round within the same block
                round_num += 1
            
            round_in_progress = True
            block_type = 'pindropping'  # Default block type unless 'Chest opened' is found

            # Update the current block and round values for persistence
            current_block_num = block_num
            current_round_num = round_num
            current_block_type = block_type

        # Check for round end
        if re.search(r"Finished pindrop round:", message):
            round_in_progress = False
        
        # Check for block end
        if "finished current task" in message:
            round_in_progress = False
            block_in_progress = False  # Mark the block as completed
        
        # Check for block type change to 'collecting'
        if "Chest opened: 0" in message:
            block_type = 'collecting'
            current_block_type = block_type
            data.loc[data['BlockNum'] == block_num, 'BlockType'] = block_type

        # Persist the current block, round, block type, and coinset ID values to the row
        data.at[idx, 'BlockNum'] = current_block_num
        data.at[idx, 'RoundNum'] = current_round_num
        data.at[idx, 'BlockType'] = current_block_type
        data.at[idx, 'CoinSetID'] = current_coinset_id

    # Fill any remaining None values with the last known block, round, and coinset values
    data['BlockNum'].fillna(method='ffill', inplace=True)
    data['RoundNum'].fillna(method='ffill', inplace=True)
    data['BlockType'].fillna(method='ffill', inplace=True)
    data['CoinSetID'].fillna(method='ffill', inplace=True)

    return data

# Function to process all matching files in a directory
def process_directory(directory):
    for filename in os.listdir(directory):
        if pattern.match(filename):
            file_path = os.path.join(directory, filename)
            print(f"Processing file: {file_path}")
            
            # Load the CSV file
            data = pd.read_csv(file_path)

            # Process the data to add BlockNum, RoundNum, and BlockType
            processed_data = process_blocks_rounds_with_correct_rounds(data)

            outDir = directory_path + '/Processed'
            os.makedirs(outDir, exist_ok=True)
            output_file = os.path.join(outDir, filename.replace('.csv', '_processed.csv'))

            processed_data.to_csv(output_file, index=False)

            print(f"Processed data saved to {output_file}")

# Set the directory path (replace with your actual directory path)
directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/12032024'

# Process all matching files in the directory
process_directory(directory_path)
