import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from loggerTranslations import *

# Load the data
filePath = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08112024/ObsReward_A_08_09_2024_13_35_cleaned_data.csv'
df = pd.read_csv(filePath)

# Specify the GlobalBlock you want to plot
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
    df_block_file_path = 'df_block.csv'
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
    #waypoints = df_round.index[df_round['Message'] == pinDrop].tolist()
    waypoints = df_round[df_round['Message'] == pinDrop]
    waypoints_indices = df_round.index[df_round['Message'] == pinDrop].tolist()
    pinDropLoc_indices = [x + 1 for x in waypoints_indices]
    headPosLoc_indices = [x - 1 for x in waypoints_indices]
    coinPos_indices = [x + 2 for x in waypoints_indices]

    pinDrop_Locs = df_round.iloc[pinDropLoc_indices]
    headPos_Locs = df_round.iloc[headPosLoc_indices]
    coinPos_locs = df_round.iloc[coinPos_indices]

    #position_data[['HeadPosX', 'HeadPosY', 'HeadPosZ']] = position_data['HeadPosAnchored'].astype(str).str.split(' ', expand=True)
    print('*'*35)
    print('Pin Drop Location')
    print(pinDrop_Locs)
    print('*'*35)

    print('Head Pos Location')
    print(headPos_Locs)
    print('*'*35)

    print('Coin Pos Location')
    print(coinPos_locs)
    print('*'*35)
    # print('Waypoints')
    # print(waypoints)

else:
    print("df_round was not created due to missing start or end markers.")


# Separate waypoints and RTdata
#waypoints = df_round[df_round['Message'] == pinDrop +3]
#waypoints = df_round.index[df_round['Message'] == pinDrop +3].tolist()

print(waypoints)
position_data = df_round[df_round['Type'] == 'RTdata'].copy()

# Ensure that HeadPosAnchored is treated as a string and split it
position_data = position_data.dropna(subset=['HeadPosAnchored'])
#position_data = position_data.reset_index(drop=True)
position_data[['HeadPosX', 'HeadPosY', 'HeadPosZ']] = position_data['HeadPosAnchored'].astype(str).str.split(' ', expand=True).astype(float)

# Define colors for waypoints using provided hex codes
waypoint_colors = ["#f94144","#f3722c","#f8961e","#f9c74f","#90be6d","#43aa8b","#577590"]

# Plot the data
plt.figure()
# Given vertices for the octagon
vertices = [
    (5, 0),
    (3.5, 3.5),
    (0, 5),
    (-3.5, 3.5),
    (-5, 0),
    (-3.5, -3.5),
    (0, -5),
    (3.5, -3.5),
    (5, 0)  # Repeating the first point to close the octagon
]

# Unzip the vertices into x and y coordinates
oct_x, oct_y = zip(*vertices)

plt.plot(oct_x, oct_y, 'k-', alpha=0.3)  # 'b-' indicates a blue line; 'o' adds markers at the vertices
# Use the first waypoint as the starting point
previous_index = None

# Iterate through waypoints to plot the segments
for i, (index, waypoint) in enumerate(waypoints.iterrows()):
    if previous_index is not None:
        segment_data = position_data.loc[previous_index+3:index]
        if not segment_data.empty:  # Ensure there's data to plot
            # Get the color of the ending waypoint
            color = waypoint_colors[i % len(waypoint_colors)]  # Cycle through colors if more waypoints than colors
            num_points = len(segment_data)
            for j in range(num_points - 1):
                alpha = 0.1 + (j / (num_points - 1)) * 0.9  # Gradually increase opacity
                plt.plot(segment_data['HeadPosX'].iloc[j:j+2], 
                         segment_data['HeadPosZ'].iloc[j:j+2],
                         color=color, linewidth=1, alpha=alpha)
    previous_index = index

# Annotate waypoints
for i, (index, waypoint) in enumerate(waypoints.iterrows()):
    #waypoint_position_str = str(df_round.loc[index, 'HeadPosAnchored'])
    waypoint_position_str = str(df_round.loc[index-1, 'HeadPosAnchored'])
    pinDrop_position_str = str(df_round.loc[index+1, 'Message'])
    print(pinDrop_position_str)
    pinDrop_position_str = pinDrop_position_str.split(" localpos: ")[1]
    print(pinDrop_position_str)
    pinDropPosX, pinDropPosY, pinDropPosZ = pinDrop_position_str.split(" ")
    print(pinDropPosX)
    actualCoin_position_str = str(df_round.loc[index+2, 'Message'])
    print(actualCoin_position_str)
    #actualCoin_position_str = actualCoin_position_str.split("")[1]
    coinPosition, actualDist, verdict = actualCoin_position_str.split("|")
    coinPosition = coinPosition.split("Closest location was: (")[1]
    coinPosition = coinPosition.split(")")[0]
    coinPosX, coinPosY, coinPosZ = coinPosition.split(", ")
    print('x', coinPosX, 'y', coinPosY, 'z', coinPosZ)
    print(actualDist)
    actualDist = actualDist.split(" ")[3]
    print('actualDist:', actualDist)

    coinValue = str(df_round.loc[index+3, 'Message'])
    print(coinValue)
    coinValue = coinValue.split("|")[3]
    print(coinValue)


    waypoint_time = str(df_round.loc[index, 'AppTime'])
    waypoint_position = waypoint_position_str.split()
    print(waypoint_position_str)

    if len(waypoint_position) == 3:  # Ensure that we have exactly 3 components
        waypoint_position = [float(coord) for coord in waypoint_position]
        # plt.text(waypoint_position[0], waypoint_position[2] + 0.1, 
        #          f"Head {i+1} \nAppTime: {waypoint_time}", fontsize=10, ha='center')
        # plt.text(waypoint_position[0], waypoint_position[2] + 0.1, 
        #          f"Pin Drop # {i+1} \nAppTime: {waypoint_time}", fontsize=10, ha='center')
        plt.text(waypoint_position[0], waypoint_position[2] - 0.35, 
                 f"{i+1}", fontsize=10, ha='center')
        plt.scatter(waypoint_position[0], waypoint_position[2],edgecolor='black',
            facecolor=waypoint_colors[i % len(waypoint_colors)], s=100, marker='o')
        
        # plt.text(float(pinDropPosX), float(pinDropPosZ) + 0.1, 
        #          f"Hand {i+1}", fontsize=10, ha='center')
        plt.scatter(float(pinDropPosX), float(pinDropPosZ), edgecolor='black',
            facecolor=waypoint_colors[i % len(waypoint_colors)], s=100, marker='s')

        # plt.scatter(float(coinPosX), float(coinPosZ), 
        #             color='yellow', marker='*', s=100)
        # plt.text(float(coinPosX), float(coinPosZ) + 0.1, 
        #  f"{coinValue} points", fontsize=10, ha='center')
        
    else:
        print(f"Warning: Skipping waypoint at index {index} with invalid HeadPosAnchored value: {waypoint_position_str}")

plt.title(f'Plot of Head Positions for GlobalBlock {global_block}')
plt.xlabel('HeadPosX')
plt.ylabel('HeadPosZ')
plt.grid(True)
plt.xlim(-5, 5)
plt.ylim(-5, 5)
#plt.gca().set_aspect('equal')
plt.savefig("headPos_OverTime.png")
