import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dataConfigs_3Coins import *


# Define the CoinSet based on whichCoinSet (local coordinates)
CoinSet = {
    'HV': CoinSet['HV'][0],
    'LV': CoinSet['LV'][0],
    'NV': CoinSet['NV'][0]
}

# Helper function to extract coin positions from the Message column
def extract_coin_positions(df):
    coin_positions = []
    for _, row in df.iterrows():
        if 'coinpoint' in str(row['Message']).lower():
            try:
                message_parts = row['Message'].split()
                x = [p.split(':')[1] for p in message_parts if 'deltax:' in p]
                y = [p.split(':')[1] for p in message_parts if 'deltaz:' in p]
                if x and y:  # Ensure both x and y values are found
                    coin_positions.append((float(x[0]), float(y[0])))
                else:
                    coin_positions.append(None)
            except (IndexError, ValueError):  # Handle cases with invalid formats
                coin_positions.append(None)
        else:
            coin_positions.append(None)
    return pd.DataFrame({'coinLoc': coin_positions})

# Function to plot waypoints from the CSV file
def plot_waypoints(csv_file, plot_file):
    waypoint_df = pd.read_csv(csv_file)
    print(f"Processing file: {csv_file}, Number of rows in waypoint_df: {len(waypoint_df)}")

    # Extract coin positions and merge with the waypoint dataframe
    coin_positions_df = extract_coin_positions(waypoint_df)
    waypoint_df = pd.concat([waypoint_df, coin_positions_df], axis=1)

    # Drop rows without valid coin positions
    waypoint_df = waypoint_df.dropna(subset=['coinLoc'])

    # Plotting the 1x3 grid of plots (since we are dealing with 3 coins)
    fig, axs = plt.subplots(1, 3, figsize=(18, 6))
    colors = plt.cm.viridis(np.linspace(0, 1, len(waypoint_df['BlockNum'].unique())))

    # Set consistent x and y limits across all plots
    x_limits = (-5, 5)
    y_limits = (-5, 5)

    # Go through each coin and plot
    for idx, (coin_name, coin_location) in enumerate(CoinSet.items()):
        print(f"Processing coin: {coin_name}, location: {coin_location}")
        coin_data = waypoint_df[
            waypoint_df['coinLoc'].apply(lambda loc: np.allclose(loc, coin_location, atol=0.1))
        ]
        print(f"Number of matching rows for {coin_name}: {len(coin_data)}")

        ax = axs[idx]
        ax.set_title(f"{coin_name} Waypoints")
        # Plot the coin circle
        coin_circle = plt.Circle(coin_location, 1.5 / 2, color='blue', fill=False, linewidth=2)
        ax.plot(coin_location[0], coin_location[1], marker='o', markersize=5, color='black')
        ax.add_patch(coin_circle)

        # Plot the vectors for waypoints
        for _, row in coin_data.iterrows():
            if 'headPos_anc' in row and 'handPos_anc' in row:
                head_pos = eval(row['headPos_anc'])
                hand_pos = eval(row['handPos_anc'])
                block_num = row['BlockNum']
                if not (np.isnan(head_pos).any() or np.isnan(hand_pos).any()):
                    ax.arrow(head_pos[0], head_pos[1],
                             hand_pos[0] - head_pos[0], hand_pos[1] - head_pos[1],
                             head_width=0.5, length_includes_head=True,
                             color=colors[int(block_num) % len(colors)], alpha=0.3)

        ax.set_xlim(x_limits)
        ax.set_ylim(y_limits)
        ax.set_aspect('equal')

    plt.tight_layout()

    # Save the plot to file
    plt.savefig(plot_file)
    print(f"Plot saved to {plot_file}")

# Iterate over all CSV files in the input directory and process them
def process_directory(input_dir):
    for file in os.listdir(input_dir):
        if file.endswith(".csv"):
            csv_file = os.path.join(input_dir, file)
            plot_file = os.path.join(input_dir, file.replace('.csv', '.png'))
            plot_waypoints(csv_file, plot_file)


# Input directory containing the CSV files
inDir = '/Users/mairahmac/Desktop/RC_TestingNotes/11272024/Humza_206/ML2F/Processed/Waypoints'

# Call the directory processing function
process_directory(inDir)
