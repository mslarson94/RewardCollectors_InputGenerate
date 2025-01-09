
import os
import pandas as pd
import re
from collections import namedtuple

# Define the namedtuple for coin positions
CoinPosition = namedtuple('CoinPosition', ['abs_position', 'local_position'])

# Function to extract both absolute and local positions from 'coinpoint' messages
def extract_coin_positions(message):
    abs_pattern = r"x:\s*([-\d.]+)\s*y:\s*([-\d.]+)\s*z:\s*([-\d.]+)"
    delta_pattern = r"deltax:\s*([-\d.]+)\s*deltay:\s*([-\d.]+)\s*deltaz:\s*([-\d.]+)"
    
    abs_match = re.search(abs_pattern, message)
    delta_match = re.search(delta_pattern, message)

    if abs_match and delta_match:
        abs_x = round(float(abs_match.group(1)), 2)
        abs_z = round(float(abs_match.group(3)), 2)
        local_x = round(float(delta_match.group(1)), 2)
        local_z = round(float(delta_match.group(3)), 2)

        return {
            'abs_x': abs_x,  
            'abs_z': abs_z,  
            'local_x': local_x,  
            'local_z': local_z   
        }
    return None

# Function to correct malformed strings
def correct_malformed_string(raw_string):
    pattern = r"(\d+\.\d{3})(\d+\.\d{3})(\d+\.\d{3})"  # Match concatenated numbers
    corrected = re.sub(pattern, r"\1 \2 \3", raw_string)  # Add spaces between numbers
    return corrected

# Function to store coin data from a CSV file
def store_coin_data_as_dict(in_file):
    # Load the dataset
    data = pd.read_csv(in_file)
    coin_data = {}

    # Iterate through the dataset and store coin position data
    for i, row in data.iterrows():
        message = str(row['Message'])  # Safely convert 'Message' to string
        if "coinpoint" in message:
            coin_positions = extract_coin_positions(message)
            if coin_positions:
                abs_pos = (coin_positions['abs_x'], coin_positions['abs_z'])
                local_pos = (coin_positions['local_x'], coin_positions['local_z'])
                # Store the data in a namedtuple with absolute and local positions
                coin_data[abs_pos] = CoinPosition(abs_position=abs_pos, local_position=local_pos)

    return coin_data

# Revised process_waypoints function to use absolute position matching
def process_waypoints_by_abs_position(in_file, out_csv, coin_data):
    # Load the dataset
    data = pd.read_csv(in_file)
    filtered_data = data[data['BlockType'] == 'pindropping'].copy()

    # Drop rows where 'BlockNum' is NaN
    filtered_data = filtered_data.dropna(subset=['BlockNum'])
    filtered_data['BlockNumber'] = filtered_data['BlockNum'].astype(int)
    filtered_data = filtered_data.reset_index(drop=True)

    # Create an empty list to store the rows for waypoints
    waypoint_rows = []

    # Iterate through the dataset to find waypoints and collect relevant information
    for i, row in filtered_data.iterrows():
        if row['Message'] == 'Just dropped a pin.':
            # Extract head and hand positions
            headRow = filtered_data.at[i - 1, 'HeadPosAnchored']
            head_pos = (round(float(headRow.split()[0]), 2), round(float(headRow.split()[2]), 2))  # X and Z
            handRowRaw = filtered_data.at[i + 1, 'Message'].split("Dropped a new pin at ")[1]
            handRow_1 = handRowRaw.split('localpos: ')[1]
            handRow = handRow_1.split(' ')
            hand_pos = (round(float(handRow[0]), 2), round(float(handRow[2]), 2))  # X and Z

            # Extract coin location and judgement data
            coinRow = filtered_data.at[i + 2, 'Message'].split(" | ")
            coin_locRaw = coinRow[0].split('Closest location was: ')[1]
            
            # Updated robust parsing logic with correction for malformed strings
            raw_string = coin_locRaw.replace('(', '').replace(')', '').replace(',', '')
            corrected_string = correct_malformed_string(raw_string)
            numbers = re.findall(r'-?\d+\.\d+', corrected_string)  # Extract numbers
            if len(numbers) >= 3:
                coin_loc = (round(float(numbers[0]), 2), round(float(numbers[2]), 2))  # X and Z (absolute)
            else:
                print(f"Warning: Failed to correct malformed string: '{coin_locRaw}'")
                coin_loc = (0.0, 0.0)  # Default fallback
            
            actual_dist = round(float(coinRow[1].split('actual distance: ')[1]), 2)
            judgement = coinRow[2]

            # Use the absolute position to look up the corresponding local position in the coin_data dictionary
            if coin_loc in coin_data:
                local_coin_loc = coin_data[coin_loc].local_position
                print(f"Using local coordinates for absolute position {coin_loc}: {local_coin_loc}")
            else:
                local_coin_loc = coin_loc  # Fallback to absolute coordinates if local coordinates not found
                print(f"Falling back to absolute coordinates for row {i}: {coin_loc}")

            # Append the relevant data to waypoint_rows
            waypoint_rows.append({
                'Timestamp': row['Timestamp'],
                'AppTime': row['AppTime'],
                'BlockNum': row['BlockNum'],
                'BlockType': row['BlockType'],
                'RoundNum': row['RoundNum'],
                'CoinSetID': row['CoinSetID'],
                'headPos_anc': head_pos,
                'handPos_anc': hand_pos,
                'coinLoc': local_coin_loc,  # Use local coordinates if available
                'actualDist': actual_dist,
                'judgement': judgement,
                'LeftEyeOpen': row['LeftEyeOpen'],
                'RightEyeOpen': row['RightEyeOpen'],
                'EyeTarget': row['EyeTarget'],
                'EyeDirectionAnchored': row['EyeDirectionAnchored'],
                'FixationPointAnchored': row['FixationPointAnchored']
            })

    waypoint_df = pd.DataFrame(waypoint_rows)
    waypoint_df.to_csv(out_csv, index=False)
    print(f"Waypoints CSV saved to {out_csv}")

# Function to iterate over every CSV file in a directory and process each
def process_all_files_in_directory(directory_path):
    # Iterate over all files in the directory
    for file_name in os.listdir(directory_path):
        if file_name.endswith('.csv'):
            input_file = os.path.join(directory_path, file_name)
            outDir = directory_path + '/Waypoints'
            os.makedirs(outDir, exist_ok=True)
            output_file = os.path.join(outDir, file_name.replace('.csv', '_waypoints.csv'))
            
            # Store the coin data (absolute and local positions) for each file
            coin_data_dict = store_coin_data_as_dict(input_file)

            # Process waypoints and save the output CSV using absolute position matching
            process_waypoints_by_abs_position(input_file, output_file, coin_data_dict)

# Example usage
directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/11272024/Humza_206/ML2F/Processed'  # Replace with your directory path
process_all_files_in_directory(directory_path)
