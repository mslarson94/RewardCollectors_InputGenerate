import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import re
import seaborn as sns
from matplotlib.lines import Line2D
from dataConfigs_3Coins import *

# Define the coin dictionary
coinDict = {
    'HV': CoinSet['HV'][0],
    'LV': CoinSet['LV'][0],
    'NV': CoinSet['NV'][0]
}

# Function to correct malformed strings
def correct_malformed_string(raw_string):
    pattern = r"(\d+\.\d{3})(\d+\.\d{3})(\d+\.\d{3})"  # Match concatenated numbers
    corrected = re.sub(pattern, r"\1 \2 \3", raw_string)  # Add spaces between numbers
    return corrected

 # Function to process a single file
def process_file(file_path, output_dir):
    inFileName = os.path.splitext(os.path.basename(file_path))[0]
    outFileName = inFileName + '_waypoints'
    plotFile = os.path.join(output_dir, outFileName + '_vectorOfApproach.png')
    outCsv = os.path.join(output_dir, outFileName + '.csv')

    # Load the dataset
    data = pd.read_csv(file_path)
    filtered_data = data[data['BlockType'] == 'pindropping'].copy()
    filtered_data = filtered_data.dropna(subset=['BlockNum'])
    filtered_data['BlockNum'] = filtered_data['BlockNum'].astype(int)
    filtered_data = filtered_data[~filtered_data['BlockNum'].isin([0, 1])]
    filtered_data = filtered_data.reset_index(drop=True)

    waypoint_rows = []

    # Iterate through the dataset to find waypoints and collect relevant information
    for i, row in filtered_data.iterrows():
        if row['Message'] == 'Just dropped a pin.':
            try:
                headRow = filtered_data.at[i - 1, 'HeadPosAnchored']
                head_pos = (float(headRow.split()[0]), float(headRow.split()[2]))

                handRowRaw = filtered_data.at[i + 1, 'Message'].split("Dropped a new pin at ")[1]
                handRow_1 = handRowRaw.split('localpos: ')[1]
                handRow = handRow_1.split(' ')
                hand_pos = (float(handRow[0]), float(handRow[2]))

                coinRow = filtered_data.at[i + 2, 'Message'].split(" | ")
                coin_locRaw = coinRow[0].split('Closest location was: ')[1]

                # Correct and parse the coin location
                raw_string = coin_locRaw.replace('(', '').replace(')', '').replace(',', '')
                corrected_string = correct_malformed_string(raw_string)
                numbers = re.findall(r'-?\d+\.\d+', corrected_string)
                if len(numbers) >= 3:
                    coin_loc = (float(numbers[0]), float(numbers[2]))
                else:
                    print(f"Warning: Failed to correct malformed string: '{coin_locRaw}'")
                    coin_loc = None

                actual_dist = float(coinRow[1].split('actual distance: ')[1])
                judgement = coinRow[2]
                coin_value = float(coinRow[3].split('coinValue: ')[1])

                waypoint_rows.append({
                    'Timestamp': row['Timestamp'],
                    'AppTime': row['AppTime'],
                    'BlockNum': row['BlockNum'],
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
            except Exception as e:
                print(f"Error processing row {i}: {e}")

    # Check if waypoint_rows contains data
    if not waypoint_rows:
        print(f"No valid waypoints found in file: {file_path}")
        return  # Skip further processing

    waypoint_df = pd.DataFrame(waypoint_rows)

    # Ensure 'coinLoc' exists before filtering NaN values
    if 'coinLoc' in waypoint_df.columns:
        waypoint_df = waypoint_df.dropna(subset=['coinLoc'])
    else:
        print(f"Warning: 'coinLoc' column is missing in the data for file {file_path}")
        return

    waypoint_df.to_csv(outCsv, index=False)

    sns.set_theme(style="darkgrid")
    fig, axs = plt.subplots(2, 3, figsize=(9, 6))

    unique_blocks = filtered_data['BlockNum'].unique()
    palette = sns.color_palette("viridis", len(unique_blocks))
    block_colors = {block: palette[i] for i, block in enumerate(unique_blocks)}

    x_limits = (-5, 5)
    y_limits = (-5, 5)

    for idx, (coin_name, coin_location) in enumerate(coinDict.items()):
        coin_data = waypoint_df[waypoint_df['coinLoc'] == coin_location]

        good_drop_data = coin_data[coin_data['judgement'].str.strip().str.lower() == 'good drop']
        bad_drop_data = coin_data[coin_data['judgement'].str.strip().str.lower() == 'bad drop']

        if len(good_drop_data) > 0:
            ax = axs[0, idx]
            ax.set_title(f"{coin_name} (Good drop)")
            sns.scatterplot(x=[coin_location[0]], y=[coin_location[1]], ax=ax, color='black', s=50)
            coin_circle = plt.Circle(coin_location, 1 / 2, color='blue', fill=False, linewidth=2)
            ax.add_patch(coin_circle)

            for i, row in good_drop_data.iterrows():
                head_pos = row['headPos_anc']
                hand_pos = row['handPos_anc']
                block_num = row['BlockNum']
                if not (np.isnan(head_pos).any() or np.isnan(hand_pos).any()):
                    ax.arrow(head_pos[0], head_pos[1],
                             hand_pos[0] - head_pos[0], hand_pos[1] - head_pos[1],
                             head_width=0.5, length_includes_head=True,
                             color=palette[int(block_num) % len(palette)], alpha=0.3)

        if len(bad_drop_data) > 0:
            ax = axs[1, idx]
            ax.set_title(f"{coin_name} (Bad drop)")
            sns.scatterplot(x=[coin_location[0]], y=[coin_location[1]], ax=ax, color='black', s=50)
            coin_circle = plt.Circle(coin_location, 1.1 / 2, color='red', fill=False, linewidth=2)
            ax.add_patch(coin_circle)

            for i, row in bad_drop_data.iterrows():
                head_pos = row['headPos_anc']
                hand_pos = row['handPos_anc']
                block_num = row['BlockNum']
                if not (np.isnan(head_pos).any() or np.isnan(hand_pos).any()):
                    ax.arrow(head_pos[0], head_pos[1],
                             hand_pos[0] - head_pos[0], hand_pos[1] - head_pos[1],
                             head_width=0.5, length_includes_head=True,
                             color=palette[int(block_num) % len(palette)], alpha=0.3)

    legend_elements = [Line2D([0], [0], color=block_colors[block], lw=4, label=f'Block {block}') for block in unique_blocks]
    fig.legend(handles=legend_elements, loc='lower center', bbox_to_anchor=(0.5, -0.2), ncol=len(block_colors))
    plt.subplots_adjust(bottom=0.3)

    for ax in axs.flat:
        ax.set_xlim(x_limits)
        ax.set_ylim(y_limits)
        ax.set_aspect('equal')

    plt.tight_layout()
    plt.savefig(plotFile)


# Iterate over all files in a directory
def process_directory(directory_path):
    output_dir = os.path.join(directory_path, "Processed_Waypoints")
    os.makedirs(output_dir, exist_ok=True)
    for file_name in os.listdir(directory_path):
        if file_name.endswith('_processed.csv'):
            file_path = os.path.join(directory_path, file_name)
            process_file(file_path, output_dir)

# Example usage
directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/11272024/Humza_206/ML2F/Processed'
process_directory(directory_path)
