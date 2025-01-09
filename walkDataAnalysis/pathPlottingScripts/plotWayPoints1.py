import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import dataConfigs
import os

# Sample coinDict for testing
whichCoinSet = dataConfigs.whichCoinSet

# Define the CoinSet based on whichCoinSet (local coordinates), excluding tutorial coins
CoinSet = {
    'HV_1': (dataConfigs.HV_1[0], dataConfigs.HV_1[1]), 'HV_2': (dataConfigs.HV_2[0], dataConfigs.HV_2[1]),
    'LV_1': (dataConfigs.LV_1[0], dataConfigs.LV_1[1]), 'LV_2': (dataConfigs.LV_2[0], dataConfigs.LV_2[1]),
    'NV_1': (dataConfigs.NV_1[0], dataConfigs.NV_1[1]), 'NV_2': (dataConfigs.NV_2[0], dataConfigs.NV_2[1])
}

# Function to plot waypoints from the CSV file
def plot_waypoints(csv_file, plot_file):
    waypoint_df = pd.read_csv(csv_file)
    print(f"Processing file: {csv_file}, Number of rows in waypoint_df: {len(waypoint_df)}")

    # Plotting the 2x6 grid of plots (since we are dealing with 6 non-tutorial coins)
    fig, axs = plt.subplots(2, 6, figsize=(18, 6))
    colors = plt.cm.viridis(np.linspace(0, 1, len(waypoint_df['BlockNum'].unique())))

    # Set consistent x and y limits across all plots
    x_limits = (-5, 5)
    y_limits = (-5, 5)

    # Go through each coin and plot in the appropriate row (top or bottom) based on judgement
    for idx, (coin_name, coin_location) in enumerate(CoinSet.items()):
        print(f"Processing coin: {coin_name}, location: {coin_location}")
        coin_data = waypoint_df[
            (np.abs(waypoint_df['coinLoc'].apply(lambda x: eval(x)[0]) - coin_location[0]) < 0.1) &  # Tolerance
            (np.abs(waypoint_df['coinLoc'].apply(lambda x: eval(x)[1]) - coin_location[1]) < 0.1)  # Tolerance
        ]
        print(f"Number of matching rows for {coin_name}: {len(coin_data)}")

        # Check for "good drop" and "bad drop"
        good_drop_data = coin_data[coin_data['judgement'].str.strip().str.lower() == 'good drop']
        bad_drop_data = coin_data[coin_data['judgement'].str.strip().str.lower() == 'bad drop']

        # Plot for "good drop" (top row)
        if len(good_drop_data) > 0:
            ax = axs[0, idx]
            ax.set_title(f"{coin_name} (Good drop)")
            # Plot the coin circle
            coin_circle = plt.Circle(coin_location, 1.5 / 2, color='blue', fill=False, linewidth=2)
            ax.plot(coin_location[0], coin_location[1], marker='o', markersize=3, color='black')
            ax.add_patch(coin_circle)

            # Plot the vectors for "good drop"
            for i, row in good_drop_data.iterrows():
                head_pos = eval(row['headPos_anc'])
                hand_pos = eval(row['handPos_anc'])
                block_num = row['BlockNum']
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
                head_pos = eval(row['headPos_anc'])
                hand_pos = eval(row['handPos_anc'])
                block_num = row['BlockNum']
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

    # Save the plot to file
    plt.savefig(plot_file)
    print(f"Plot saved to {plot_file}")

# Iterate over all CSV files in the input directory and process them
def process_directory(input_dir):
    # Iterate through all files in the directory
    for file in os.listdir(input_dir):
        if file.endswith(".csv"):
            csv_file = os.path.join(input_dir, file)
            plot_file = os.path.join(input_dir, file.replace('.csv', '.png'))
            # Call the plotting function for each file
            plot_waypoints(csv_file, plot_file)

# Input directory containing the CSV files
inDir = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08092024/Cleaned/Processed/Waypoints'

# Call the directory processing function
process_directory(inDir)
