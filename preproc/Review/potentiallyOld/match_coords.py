import os
import pandas as pd
import numpy as np

# Planned start positions (posData)
posData = {
    'pos1': (1.75, 4.25),
    'pos2': (4.25, 1.75),
    'pos3': (4.25, -1.75),
    'pos4': (1.75, -4.25),
    'pos5': (-1.75, -4.25),
    'pos6': (-4.25, -1.75),
    'pos7': (-4.25, 1.75),
    'pos8': (-1.75, 4.25),
    'tutorial': (0, -6)
}


# Function to calculate Euclidean distance using (x, z) values
def euclidean_distance(point1, point2):
    return np.sqrt((point1[0] - point2[0]) ** 2 + (point1[1] - point2[1]) ** 2)

# Function to safely extract (x, z) from the space-separated 'HeadPosAnchored' column
def extract_xz_from_headpos(head_pos):
    try:
        # Assuming head_pos is space-separated in 'x y z' format, split and extract (x, z)
        x, y, z = map(float, head_pos.split())
        return (x, z)
    except:
        return None

# Function to process the file and handle 'repositioned' messages by looking at the row before each 'repositioned' message
def process_file_with_persistence(file_path):
    df = pd.read_csv(file_path)

    # Initialize empty lists to hold the closest planned positions and coordinates
    closest_positions = [None] * len(df)
    closest_coords = [None] * len(df)

    # Identify rows with 'repositioned' in the 'Message' column
    repositioned_indices = df[df['Message'].str.contains('repositioned', case=False, na=False)].index

    # Variable to store the closest position to persist throughout the round
    current_closest_pos = None
    current_closest_coords = None

    # Loop over the entire DataFrame row by row
    for idx in range(len(df)):
        # If current row is a 'Repositioned' message, process the previous row
        if idx in repositioned_indices:
            # Ensure the previous row exists and the previous row's Type is not 'Event'
            if idx - 1 >= 0 and df.at[idx - 1, 'Type'] != 'Event':
                # Get the HeadPosAnchored value from the row before the 'repositioned' message
                prev_row_headpos = df.at[idx - 1, 'HeadPosAnchored']
                head_pos_xz = extract_xz_from_headpos(prev_row_headpos)

                if head_pos_xz is not None:
                    # Find the closest planned position based on (x, z)
                    min_distance = float('inf')
                    closest_pos = None
                    for pos_name, planned_pos in posData.items():
                        dist = euclidean_distance(head_pos_xz, planned_pos)
                        if dist < min_distance:
                            min_distance = dist
                            closest_pos = pos_name

                    # Set the closest position and persist it for the round
                    current_closest_pos = closest_pos
                    current_closest_coords = posData[closest_pos]

            # Assign the closest position to the current 'Repositioned' row
            closest_positions[idx] = current_closest_pos
            closest_coords[idx] = current_closest_coords

        # For the rest of the round, persist the closest position values
        elif current_closest_pos is not None:
            closest_positions[idx] = current_closest_pos
            closest_coords[idx] = current_closest_coords

    # Add closest planned position and its coordinates to the DataFrame
    df['closest_planned_pos'] = closest_positions
    df['closest_planned_pos_coords'] = closest_coords

    # Optionally save the updated file
    output_file_path = os.path.splitext(file_path)[0] + "_processed.csv"
    df.to_csv(output_file_path, index=False)

    return df[['Timestamp', 'Message', 'HeadPosAnchored', 'closest_planned_pos', 'closest_planned_pos_coords']]

# Example usage: Process the provided CSV file
file_path = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08112024/Cleaned/Processed/ObsReward_B_08_11_2024_14_24_cleaned_data_processed.csv'
processed_df = process_file_with_persistence(file_path)

processed_df.head()  # Display the first few rows of the processed data
