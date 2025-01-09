import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from loggerTranslations import *

# Load the data
filePath = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08112024/ObsReward_A_08_09_2024_13_35_cleaned_data.csv'
df = pd.read_csv(filePath)

# Specify the GlobalBlock you want to plot
learning_blockStart = 5
learning_blockEnd = 11
global_block = 12

# Filter the data for the specific GlobalBlock
blockStart_indices = df.index[(df['Message'] == markTime) & (df['GlobalBlock'] == global_block-1)].tolist()
blockEnd_indices = df.index[(df['Message'] == blockEnd) & (df['GlobalBlock'] == global_block)].tolist()

df_round = None  # Initialize df_round to avoid NameError later

if blockStart_indices and blockEnd_indices:
    blockStart_index = blockStart_indices[0]
    blockEnd_index = blockEnd_indices[0]
    df_block = df.iloc[blockStart_index:blockEnd_index+1].copy()  # Use .copy() to avoid SettingWithCopyWarning
    df_block = df_block.reset_index(drop=True)
    #print(df_block)
    df_block_file_path = 'df_block_handHeadcsv'
    df_block.to_csv(df_block_file_path, index=False)

    # Handle NaN values in the 'Message' column using .loc[]
    df_block.loc[:, 'Message'] = df_block['Message'].fillna('')

    # Find round start and end indices
    roundStart_indices = df_block.index[df_block['Message'] == readyStart].tolist()
    roundEnd_indices = df_block.index[df_block['Message'].str.startswith(pinDrop_RoundEnd_start)].tolist()
    #print("Round Start Indices", roundStart_indices, "Round End Indices", roundEnd_indices)

    if roundStart_indices and roundEnd_indices:
        #print('yes')
        roundStart_index = roundStart_indices[0]
        roundEnd_index = roundEnd_indices[0]
        #print("Round Start Index", roundStart_index, "Round End Index", roundEnd_index)
        #print(df_block.iloc[roundStart_index:roundEnd_index+3])
        df_round = df_block.iloc[roundStart_index:roundEnd_index+3].copy()  # +3 to include that line & the next 2 lines
        df_round = df_round.reset_index(drop=True)
        #print(df_round)
    else:
        print("Round start or end marker not found in the block.")
else:
    print("Block start or end marker not found.")

# Additional processing if df_round was successfully created
if df_round is not None:
    waypoints = df_round[df_round['Message'] == pinDrop]
    waypoints_indices = df_round.index[df_round['Message'] == pinDrop].tolist()
    pinDropLoc_indices = [x + 1 for x in waypoints_indices]
    headPosLoc_indices = [x - 1 for x in waypoints_indices]
    coinPos_indices = [x + 2 for x in waypoints_indices]

    pinDrop_Locs = df_round.iloc[pinDropLoc_indices]
    headPos_Locs = df_round.iloc[headPosLoc_indices]
    coinPos_locs = df_round.iloc[coinPos_indices]

else:
    print("df_round was not created due to missing start or end markers.")


# Separate waypoints and RTdata
#waypoints = df_round[df_round['Message'] == pinDrop +3]
#waypoints = df_round.index[df_round['Message'] == pinDrop +3].tolist()

#print(waypoints)
position_data = df_round[df_round['Type'] == 'RTdata'].copy()

# Ensure that HeadPosAnchored is treated as a string and split it
position_data = position_data.dropna(subset=['HeadPosAnchored'])
#position_data = position_data.reset_index(drop=True)
position_data[['HeadPosX', 'HeadPosY', 'HeadPosZ']] = position_data['HeadPosAnchored'].astype(str).str.split(' ', expand=True).astype(float)

# Define colors for waypoints using provided hex codes
waypoint_colors = ["#f94144","#f3722c","#f8961e","#f9c74f","#90be6d","#43aa8b","#577590"]

# Given vertices for the octagon

# Annotate waypoints
for i, (index, waypoint) in enumerate(waypoints.iterrows()):

    head_position_str = str(df_round.loc[index-1, 'HeadPosAnchored'])
    headPosX, headPosY, headPosZ =  map(float, head_position_str.split(" "))

    pinDrop_position_str = str(df_round.loc[index+1, 'Message'])
    print('o'*35)
    print('pinDrop Position', pinDrop_position_str)
    #pinDrop_position_str = pinDrop_position_str.split(" localpos: ")[1]
    pinDrop_position_str = pinDrop_position_str.split(" ")[5]

    print(pinDrop_position_str)
    pinDropPosX, pinDropPosY, pinDropPosZ = map(float, pinDrop_position_str.split(" "))
    print(pinDropPosX)
    actualCoin_position_str = str(df_round.loc[index+2, 'Message'])
    #actualCoin_position_str = actualCoin_position_str.split("")[1]
    coinPosition, pinCoin_actualDist, verdict = actualCoin_position_str.split("|")
    coinPosition = coinPosition.split("Closest location was: (")[1]
    coinPosition = coinPosition.split(")")[0]
    #coinPosX, coinPosY, coinPosZ = coinPosition.split(", ")
    coinPosX, coinPosY, coinPosZ = map(float, coinPosition.split(", "))
    print('x', coinPosX, type(coinPosX), 'y', coinPosY, type(coinPosY), 'z', coinPosZ, type(coinPosZ))
    print('x', type(coinPosX), 'y', type(coinPosY), 'z', type(coinPosZ))
    print(actualDist)
    actualDist = float(actualDist.split(" ")[3])
    print('actualDist:', actualDist)

    coinValue = str(df_round.loc[index+3, 'Message'])
    print(coinValue)
    coinValue = coinValue.split("|")[3]
    print(coinValue)


    waypoint_time = str(df_round.loc[index, 'AppTime'])
    waypoint_position = waypoint_position_str.split()
    print(waypoint_position_str)
