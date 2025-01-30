import pandas as pd
import re
import os

file_dir = '/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/SampleData/01202025/ML2D'
fileName = 'ObsReward_A_08_31_2024_12_09_cleaned'

lsit = ['ObsReward_A_08_31_2024_10_45', 'ObsReward_A_08_31_2024_11_08',
        'ObsReward_A_08_31_2024_11_34', 'ObsReward_A_08_31_2024_11_40', 
        'ObsReward_A_08_31_2024_11_49', 'ObsReward_A_08_31_2024_12_09']

inFile = file_dir + '/' + fileName + '.csv'
outFile = file_dir + '/' + fileName + '_eye.csv'
import pandas as pd
import re

# Define the pattern for matching files
pattern = re.compile(r"^ObsReward_[AB]_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
def extract_values(message):
    if not isinstance(message, str):
        return {
            'AmplitudeDeg': None,
            'DirectionRadial': None,
            'EyeLeft': None,
            'EyeRight': None,
            'VelocityDegps': None,
            'BlockType': None,
            'BlockNumber': None,
            'CleanedMessage': message
        }
    
    # Regex pattern to match the structure of your example line
    main_pattern = r"AmplitudeDeg:(\S+)\|DirectionRadial:(\S+)\|EyeLeft:\((\d+.\d+),\s(\d+.\d+)\)\|EyeRight:\((\d+.\d+),\s(\d+.\d+)\)\|VelocityDegps:(\S+)"
    match = re.search(main_pattern, message)
    
    # Initialize extraction dictionary
    extraction = {
        'AmplitudeDeg': None,
        'DirectionRadial': None,
        'EyeLeft': None,
        'EyeRight': None,
        'VelocityDegps': None,
        'BlockType': None,
        'BlockNumber': None,
        'CleanedMessage': message
    }
    
    if match:
        # Extract and remove the pattern from the message
        cleaned_message = re.sub(main_pattern, '', message).strip('| ')
        extraction.update({
            'AmplitudeDeg': match.group(1),
            'DirectionRadial': match.group(2),
            'EyeLeft': (float(match.group(3)), float(match.group(4))),
            'EyeRight': (float(match.group(5)), float(match.group(6))),
            'VelocityDegps': match.group(7),
            'CleanedMessage': cleaned_message
        })
    
    # Additional pattern to extract BlockType and BlockNumber
    block_pattern = r"Started (collecting|pindropping)\. Block:(\d+)"
    block_match = re.search(block_pattern, extraction['CleanedMessage'])
    
    if block_match:
        extraction.update({
            'BlockType': block_match.group(1),
            'BlockNumber': int(block_match.group(2)),
            'CleanedMessage': re.sub(block_pattern, '', extraction['CleanedMessage']).strip('| ')
        })
    
    return extraction

def process_csv(df):
    # Load the CSV file
    #df = pd.read_csv(input_file)
    
    # Apply the function to each row and expand the dictionary into separate columns
    df_extracted = df['Message'].apply(lambda x: pd.Series(extract_values(x)))
    
    # Track the current BlockType and BlockNumber
    current_block_type = 'noRunningBlock'
    current_block_number = 'noRunningBlock'
    interblock_interval = False
    
    # Update and fill in the BlockType and BlockNumber columns
    for i in range(len(df_extracted)):
        if df_extracted.at[i, 'BlockType'] is not None:
            # New block started, update the current block type and number
            current_block_type = df_extracted.at[i, 'BlockType']
            current_block_number = df_extracted.at[i, 'BlockNumber']
            interblock_interval = False
        elif "finished current task" in df_extracted.at[i, 'CleanedMessage']:
            # Start interblock interval
            interblock_interval = True
            df_extracted.at[i, 'BlockType'] = 'interblockinterval'
            df_extracted.at[i, 'BlockNumber'] = 'interblockinterval'
        elif interblock_interval:
            # Fill with 'interblockinterval' during the interval
            df_extracted.at[i, 'BlockType'] = 'interblockinterval'
            df_extracted.at[i, 'BlockNumber'] = 'interblockinterval'
        else:
            # Before the first block or continuing the current block type and number
            df_extracted.at[i, 'BlockType'] = current_block_type
            df_extracted.at[i, 'BlockNumber'] = current_block_number
    
    # Find the index of the "FixationPointAnchored" column
    eye_insert_at = df.columns.get_loc("FixationPointAnchored") + 1
    block_insert_at = df.columns.get_loc("AppTime") + 1
    
    # Insert the new columns after "FixationPointAnchored"
    for col in ['AmplitudeDeg', 'DirectionRadial', 'EyeLeft', 'EyeRight', 'VelocityDegps']:
        df.insert(eye_insert_at, col, df_extracted[col])
        eye_insert_at += 1
    
    for col in ['BlockType', 'BlockNumber']:
        df.insert(block_insert_at, col, df_extracted[col])
        block_insert_at += 1

    # Replace the original Message column with the cleaned message
    df['Message'] = df_extracted['CleanedMessage']
    return df
    # # Save the updated DataFrame to a new CSV file
    # df.to_csv(output_file, index=False)
    
    # # Save the updated DataFrame to a new CSV file
    # df.to_csv(output_file, index=False)
# if __name__ == "__main__":
#     input_file = 'path_to_your_input_file.csv'  # Replace with your input file path
#     output_file = 'path_to_your_output_file.csv'  # Replace with your desired output file path
    
#     process_csv(input_file, output_file)

# Function to process all matching files in a directory
def process_directory(directory):
    for filename in os.listdir(directory):
        if pattern.match(filename):
            file_path = os.path.join(directory, filename)
            print(f"Processing file: {file_path}")
            
            # Load the CSV file
            df = pd.read_csv(file_path)

            # Process the data to add BlockNum, RoundNum, and BlockType
            processed_data = process_csv(df)

            outDir = directory_path + '/Processed'
            os.makedirs(outDir, exist_ok=True)
            output_file = os.path.join(outDir, filename.replace('.csv', '_processed.csv'))

            processed_data.to_csv(output_file, index=False)

            print(f"Processed data saved to {output_file}")

process_directory(file_dir)