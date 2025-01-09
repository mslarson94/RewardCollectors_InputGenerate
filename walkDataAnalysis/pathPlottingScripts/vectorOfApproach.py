import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import re

# Sample coinDict for testing

coinDict = {
        'HV_1' : (-2.5,1.9),
        'HV_2' : (1.8,-3.1),
        'LV_1' : (2.8,1.2),
        'LV_2' : (-2.1,-2.2),
        'NV_1' : (0.75, 3.0),
        'NV_2' : (0.2, -1.3)
}

# Load the data
fileDir = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08312024/magicLeap'
inFileName = 'ObsReward_A_08_31_2024'
outFileName = inFileName + '_waypoints'
plotFile = fileDir + '/' + outFileName + '_vectorOfApproach.png'
outCsv = fileDir + '/' + outFileName + '.csv'

# Load the dataset
inFile = fileDir + '/' + inFileName + '_allCleaned.csv' # Update this to your actual file path
tutorialBlocks = [0, 1]


data = pd.read_csv(inFile)
print(data['BlockType'].unique())

# Filter the data for relevant block types ('pindropping') and exclude BlockNum values 0 and 1
filtered_data = data[data['BlockType'] == 'pindropping'].copy()
print(f"Number of rows after filtering: {len(filtered_data)}")
print(f"Unique BlockType values: {filtered_data['BlockType'].unique()}")

# Drop rows where 'BlockNum' is NaN
filtered_data = filtered_data.dropna(subset=['BlockNumber'])
filtered_data['BlockNumber'] = filtered_data['BlockNumber'].astype(int)
# Then apply the filtering
filtered_data = filtered_data[~filtered_data['BlockNumber'].isin(tutorialBlocks)]
# Reset the index to make sure row indices are sequential
filtered_data = filtered_data.reset_index(drop=True)
print(f"Number of rows after filtering: {len(filtered_data)}")
filtered_data.to_csv((fileDir + '/' +'filtered_data.csv'))
# Create an empty list to store the rows for waypoints
waypoint_rows = []

# Iterate through the dataset to find waypoints and collect relevant information
for i, row in filtered_data.iterrows():

    if row['Message'] == 'Just dropped a pin.':
        #print(i, row['Message'])
        print('******* blah******** ')
        headRow = filtered_data.at[i - 1, 'HeadPosAnchored']
        head_pos = (float(headRow.split()[0]), float(headRow.split()[2]))
        print('head pos ', head_pos)
        handRowRaw = filtered_data.at[i + 1, 'Message'].split("Dropped a new pin at ")[1]
        handRow_1 = handRowRaw.split('localpos: ')[1]
        print('handRow_1', handRow_1)
        handRow = handRow_1.split(' ')
        print('handRow', handRow)
        hand_pos = (float(handRow[0]), float(handRow[2]))
        print('handpos', hand_pos)
        coinRow = filtered_data.at[i + 2, 'Message'].split(" | ")
        print('********************')
        print(coinRow)

        coin_locRaw = coinRow[0].split('Closest location was: ')[1]
        print('coin loc raw ', coin_locRaw)
        coin_locRaw_1 = coin_locRaw.split()
        print('coin loc raw ', coin_locRaw_1)
        print('*'*45)
        print('x: ', coin_locRaw_1[0], ', y: ', coin_locRaw_1[2])
        coin_loc = (float(coin_locRaw_1[0]), float(coin_locRaw_1[2]))
        print('please help', coin_loc)
        #position_data['HeadPosAnchored'].astype(str).str.split(' ', expand=True).astype(float)
        actual_dist = float(coinRow[1].split('actual distance: ')[1])
        judgement = coinRow[2]
        coin_value = float(coinRow[3].split('coinValue: ')[1])
        print('*'*35)
        print(coin_loc, actual_dist, judgement, coin_value)
        print('*'*35)
        if i > 0 and i + 2 < len(filtered_data):  # Ensure there are preceding and following rows
            waypoint_rows.append({
                'Timestamp': row['Timestamp'],
                'AppTime': row['AppTime'],
                'BlockNumber': row['BlockNumber'],
                'BlockType': row['BlockType'],
                'headPos_anc': head_pos,
                'handPos_anc': hand_pos,
                'coinLoc': coin_loc,
                'actualDist': actual_dist,
                'judgement': judgement,
                'coinValue': coin_value,
                'LeftEyeOpen': row['LeftEyeOpen'],
                'RightEyeOpen': row['RightEyeOpen'],
                'EyeTarget': row['EyeTarget'],
                'EyeDirectionAnchored': row['EyeDirectionAnchored'],
                'FixationPointAnchored': row['FixationPointAnchored'],
                'AmplitudeDeg': row['AmplitudeDeg'],
                'DirectionRadial': row['DirectionRadial'],
                'EyeLeft': row['EyeLeft'],
                'EyeRight': row['EyeRight'],
                'VelocityDegps': row['VelocityDegps']
            })
    else:
        continue

waypoint_df = pd.DataFrame(waypoint_rows)

# Exclude rows where 'coinLoc' is NaN
waypoint_df = waypoint_df.dropna(subset=['coinLoc'])
waypoint_df.to_csv(outCsv, index=False)


# Plotting the 2x6 grid of plots
fig, axs = plt.subplots(2, 6, figsize=(18, 6))
colors = plt.cm.viridis(np.linspace(0, 1, len(filtered_data['BlockNumber'].unique())))

# Set consistent x and y limits across all plots
x_limits = (-5, 5)
y_limits = (-5, 5)

# Go through each coin and plot in the appropriate row (top or bottom) based on judgement
for idx, (coin_name, coin_location) in enumerate(coinDict.items()):
    print(f"Processing coin: {coin_name}, location: {coin_location}")
    coin_data = waypoint_df[waypoint_df['coinLoc'] == coin_location]

    # Check for "good drop" and "bad drop"
    good_drop_data = coin_data[coin_data['judgement'].str.strip().str.lower() == 'good drop']
    bad_drop_data = coin_data[coin_data['judgement'].str.strip().str.lower() == 'bad drop']

    print(f"{coin_name} - Good drop count: {len(good_drop_data)}, Bad drop count: {len(bad_drop_data)}")

    # Plot for "good drop" (top row)
    if len(good_drop_data) > 0:
        ax = axs[0, idx]
        ax.set_title(f"{coin_name} (Good drop)")
        # Plot the coin circle
        coin_circle = plt.Circle(coin_location, 1.1 / 2, color='blue', fill=False, linewidth=2)
        ax.plot(coin_location[0], coin_location[1], marker='o', markersize=3, color='black')
        ax.add_patch(coin_circle)

        # Plot the vectors for "good drop"
        for i, row in good_drop_data.iterrows():
            head_pos = row['headPos_anc']
            hand_pos = row['handPos_anc']
            block_num = row['BlockNumber']
            if not (np.isnan(head_pos).any() or np.isnan(hand_pos).any()):
                ax.arrow(head_pos[0], head_pos[1], 
                         hand_pos[0] - head_pos[0], hand_pos[1] - head_pos[1], 
                         head_width=0.5, length_includes_head=True, 
                         color=colors[int(block_num) % len(colors)], alpha=0.3)


    # Plot for "bad drop" (bottom row)
    if len(bad_drop_data) > 0:
        ax = axs[1, idx]
        ax.set_title(f"{coin_name} (Bad drop)")
        # Plot the coin circle
        ax.plot(coin_location[0], coin_location[1], marker='o', markersize=3, color='black')
        coin_circle = plt.Circle(coin_location, 1.1 / 2, color='red', fill=False, linewidth=2)
        ax.add_patch(coin_circle)


        # Plot the vectors for "bad drop"
        for i, row in bad_drop_data.iterrows():
            head_pos = row['headPos_anc']
            hand_pos = row['handPos_anc']
            block_num = row['BlockNumber']
            if not (np.isnan(head_pos).any() or np.isnan(hand_pos).any()):
                ax.arrow(head_pos[0], head_pos[1], 
                         hand_pos[0] - head_pos[0], hand_pos[1] - head_pos[1], 
                         head_width=0.5, length_includes_head=True, 
                         color=colors[int(block_num) % len(colors)], alpha=0.3)


# Set the x and y limits and aspect ratio for ALL subplots after they are created
for ax in axs.flat:
    ax.set_xlim(x_limits)
    ax.set_ylim(y_limits)
    ax.set_aspect('equal')

plt.tight_layout()

plotFile = fileDir + '/' + outFileName + '_vectorOfApproach.png'
plt.savefig(plotFile)