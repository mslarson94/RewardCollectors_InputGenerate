import pandas as pd
import numpy as np
from loggerTranslations import *

# Load the data
filePath = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08112024/ObsReward_A_08_09_2024_13_35_cleaned_data.csv'
df = pd.read_csv(filePath)

# Function to calculate the distance between two points (x1, y1) and (x2, y2)
def calculate_distance(x1, y1, x2, y2):
    return np.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)

# Initialize an empty list to store the rows of the new DataFrame
rows = []

# Loop over each unique GlobalBlock, but only for blocks 5 and onward
for global_block in df['GlobalBlock'].unique():
    if global_block >= 5:
        taskBlock = 3
        # Filter df to get the data for the current global block
        df_block = df[df['GlobalBlock'] == global_block].copy()

        # Determine if the block is a learning block (5-11) or other block (12+)
        if 5 <= global_block <= 11:
            block_type = 'learning_block'
        else:
            block_type = 'other_block'

        # Calculate the minimum AppTime for the current block
        block_start_time = df_block['AppTime'].min()

        # Adjust AppTimeCorr based on the block-specific start time
        df_block['AppTimeCorr'] = df_block['AppTime'] - block_start_time

        # Identify waypoints within the block
        waypoints_indices = df_block.index[df_block['Message'] == pinDrop].tolist()
        print(waypoints_indices)
        
        if len(waypoints_indices) > 0:  # Ensure there are waypoints found
            for i, waypoint_index in enumerate(waypoints_indices):
                # Extract information for each waypoint
                AppTime = df_block.loc[waypoint_index, 'AppTime']
                AppTimeCorr = df_block.loc[waypoint_index, 'AppTimeCorr']
                headPos_str = df_block.loc[waypoint_index - 1, 'HeadPosAnchored']
                headPosX, headPosY, headPosZ = map(float, headPos_str.split())

                pinDrop_str = df_block.loc[waypoint_index + 1, 'Message'].split(" localpos: ")[1]

                pinDropPosX, pinDropPosY, pinDropPosZ = map(float, pinDrop_str.split())

                coinPosition_str = df_block.loc[waypoint_index + 2, 'Message']
                coinPosition = coinPosition_str.split("Closest location was: (")[1].split(")")[0]
                coinPosX, coinPosY, coinPosZ = map(float, coinPosition.split(", "))

                actualDist = coinPosition_str.split("|")[1]
                print('*'*35)
                print('actualDist', actualDist)
                actualDist = float(actualDist.split(" ")[3])
                print('*'*35)
                print('actualDist', actualDist)

                verdict = coinPosition_str.split("|")[2]

                # Calculate the distance between (headPosX, headPosZ) and (pinDropPosX, pinDropPosZ)
                head_Hand_dist = calculate_distance(headPosX, headPosZ, pinDropPosX, pinDropPosZ)
                print(head_Hand_dist, type(head_Hand_dist))
                # Append the row to the list
                rows.append({
                    'taskBlock' : taskBlock,
                    'GlobalBlock': global_block,
                    'BlockType': block_type,
                    'WaypointNumber': i + 1,
                    'AppTime': AppTime,
                    'AppTimeCorr': round(AppTimeCorr, 7),
                    'headPosX': headPosX,
                    'headPosZ': headPosZ,
                    'pinDropPosX': pinDropPosX,
                    'pinDropPosZ': pinDropPosZ,
                    'coinPosX': coinPosX,
                    'coinPosZ': coinPosZ,
                    'coin_dist': actualDist,
                    'head_Hand_dist': round(head_Hand_dist, 7),
                    'verdict': verdict
                })
        taskBlock = taskBlock + 1

df_global_blocks = pd.DataFrame(rows)

df_block_file_path = 'df_Global_block.csv'
df_global_blocks.to_csv(df_block_file_path, index=False)
# Display the new DataFrame
print(df_global_blocks)
