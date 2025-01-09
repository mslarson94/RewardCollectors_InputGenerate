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
    if global_block >= 12:
        # Find the correct start of the block
        block_start_condition = (df['Message'] == markTime) & (df['GlobalBlock'] == global_block-1)
        block_start_indices = df.index[block_start_condition].tolist()
        
        if block_start_indices:
            blockStart_index = block_start_indices[0]
            
            # Filter df to get the data for the current block from the corrected start
            df_block = df.iloc[blockStart_index:].copy()
            df_block = df_block[df_block['GlobalBlock'] == global_block].copy()

            # Determine if the block is a learning block (5-11) or other block (12+)
            if 5 <= global_block <= 11:
                block_type = 'learning_block'
            else:
                block_type = 'other_block'

            # Extract required information from df_block
            AppTime = df_block.loc[df_block['Message'] == pinDrop, 'AppTime'].values[0]

            waypoint_indices = df_block.index[df_block['Message'] == pinDrop].tolist()
            pinDropLoc_indices = [x + 1 for x in waypoint_indices]
            headPosLoc_indices = [x - 1 for x in waypoint_indices]
            coinPos_indices = [x + 2 for x in waypoint_indices]

            headPos_str = df_block.iloc[df_block['Message'] == pinDrop, 'HeadPosAnchored'].values[0]
            print(headPos_str)
            #headPosX, headPosY, headPosZ = map(float, headPos_str.split())
            headPosX, headPosY, headPosZ = headPos_str.split()

            pinDrop_str = df_block[df_block['Message'].str.startswith(pinDrop_RoundEnd_start)]['Message'].values[0].split(" localpos: ")[1]
            #pinDropPosX, pinDropPosY, pinDropPosZ = map(float, pinDrop_str.split())
            print(pinDrop_str)
            pinDropPosX, pinDropPosY, pinDropPosZ = pinDrop_str.split()

            coinPosition_str = df_block.loc[df_block['Message'].str.contains("Closest location was:"), 'Message'].values[0]
            coinPosition = coinPosition_str.split("Closest location was: (")[1].split(")")[0]
            #coinPosX, coinPosY, coinPosZ = map(float, coinPosition.split(", "))
            coinPosX, coinPosY, coinPosZ = coinPosition.split(", ")

            actualDist = float(coinPosition_str.split("|")[1].split()[3])
            verdict = df_block.loc[df_block['Message'].str.contains("|"), 'Message'].values[0].split("|")[2]

            # Calculate the distance between (headPosX, headPosZ) and (pinDropPosX, pinDropPosZ)
            pinCoin_actualDistt = calculate_distance(headPosX, headPosZ, pinDropPosX, pinDropPosZ)

            # Append the row to the list
            rows.append({
                'GlobalBlock': global_block,
                'BlockType': block_type,
                'AppTime': AppTime,
                'headPosX': headPosX,
                'headPosZ': headPosZ,
                'pinDropPosX': pinDropPosX,
                'pinDropPosZ': pinDropPosZ,
                'coinPosX': coinPosX,
                'coinPosZ': coinPosZ,
                'pinCoin_actualDistt': pinCoin_actualDistt,
                'verdict': verdict
            })

# Convert the list of rows into a DataFrame
df_global_blocks = pd.DataFrame(rows)

df_global_blocksfile_path = 'df_Global_block.csv'
df_global_blocks.to_csv(df_block_file_path, index=False)
# Display the new DataFrame
print(df_global_blocks)
