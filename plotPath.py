import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Arena coordinates (corrected)
arena_x = [0, 3.5, 5, 3.5, 0, -3.5, -5, -3.5, 0]
arena_y = [5, 3.5, 0, -3.5, -5, -3.5, 0, 3.5, 5]  # Closing the loop

# Define coinDict and assign colors based on the coin type
coinDict = {
    'HV_1': (-2.5, 1.9),
    'HV_2': (1.8, -3.1),
    'LV_1': (2.8, 1.2),
    'LV_2': (-2.1, -2.2),
    'NV_1': (0.75, 3.0),
    'NV_2': (0.2, -1.3)
}

# Assign colors to the coins based on their type
coin_colors = {
    'HV_1': 'green',
    'HV_2': 'green',
    'LV_1': 'yellow',
    'LV_2': 'yellow',
    'NV_1': 'gray',
    'NV_2': 'gray'
}

# File paths
filePath = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08312024/magicLeap'
fileName = 'ObsReward_A_08_31_2024_allCleaned_RoundNum'
file = filePath + '/' + fileName + '.csv'
plotfile_base = filePath + '/' + fileName + '_Block'

# Load data
df = pd.read_csv(file)

# Convert the string format to a valid datetime format
df['TimeFormatted'] = df['Timestamp'].str[:-4] + '.' + df['Timestamp'].str[-3:]

# Convert to datetime
df['TimeParsed'] = pd.to_datetime(df['TimeFormatted'], format="%H:%M:%S.%f")

# Ensure no NaN blocks exist in the 'BlockNumber' column
df = df.dropna(subset=['BlockNumber'])

# Plot paths for each round within each block as subplots, only for 'pindropping' blocks
blocks = df['BlockNumber'].unique()

for block_num in blocks:
    block_data = df[(df['BlockNumber'] == block_num) & (df['BlockType'] == 'pindropping')]
    rounds = block_data['RoundNum'].dropna().unique()  # Ensure rounds are non-NaN and unique
    
    # Skip blocks with no valid rounds
    if len(rounds) == 0:
        print(f"No valid rounds for Block {block_num}. Skipping...")
        continue
    
    # Create subplots: number of rows equals the number of unique rounds
    num_rounds = len(rounds)
    
    # Create the figure and subplots
    if num_rounds == 1:
        fig, ax = plt.subplots(figsize=(6, 4))
        axes = [ax]  # Wrap the single subplot in a list to keep the handling consistent
    else:
        fig, axes = plt.subplots(num_rounds, 1, figsize=(6, num_rounds * 4))  # Multiple subplots

    fig.suptitle(f"Block {block_num}: Paths by Round (pindropping)", fontsize=16)

    # Loop through each round and plot it on its own subplot
    for i, round_num in enumerate(rounds):
        round_data = block_data[block_data['RoundNum'] == round_num]
        
        # Skip rounds with no valid data
        if round_data.empty:
            print(f"Round {round_num} in Block {block_num} has no data. Skipping...")
            continue
        
        ax = axes[i] if num_rounds > 1 else axes[0]  # Get the corresponding subplot

        # Get the start time of the round
        round_start_time = round_data['TimeParsed'].min()
        round_start_time_str = round_start_time.strftime("%H:%M:%S.%f")[:-3]  # Format with milliseconds

        # Convert 'TimeParsed' to seconds since the start of the round
        times = (round_data['TimeParsed'] - round_start_time).dt.total_seconds()

        norm = plt.Normalize(times.min(), times.max())  # Normalize time values to a color range
        cmap = plt.get_cmap('viridis')  # Choose a color map
        
        # Plot the arena boundary
        ax.plot(arena_x, arena_y, color='black', linestyle='-', linewidth=1, label='Arena Boundary')

        # # Plot the coins with their respective colors
        # for coin_name, coin_pos in coinDict.items():
        #     ax.scatter(coin_pos[0], coin_pos[1], color=coin_colors[coin_name], edgecolor='black', s=100, label=coin_name, zorder=5)

        # Plot the path between waypoints and handle Type == 'Event'
        for j in range(len(round_data) - 1):
            if round_data.iloc[j + 1]['Type'] == 'Event':
                # Handle the (0,0) for 'Type == Event': use previous valid X, Z and mark with a red star
                ax.plot(round_data['X'].values[j], round_data['Z'].values[j], marker='s', color='red', edgecolor='black', markersize=10, label='Before Event')
            else:
                # Plot the path segments normally
                ax.plot([round_data['X'].values[j], round_data['X'].values[j+1]],
                        [round_data['Z'].values[j], round_data['Z'].values[j+1]],
                        color=cmap(norm(times.iloc[j])), marker='o')
        # Plot the coins with their respective colors
        for coin_name, coin_pos in coinDict.items():
            ax.scatter(coin_pos[0], coin_pos[1], color=coin_colors[coin_name], edgecolor='black', s=100, label=coin_name, zorder=5)

        # Add the round start time to the subplot title
        ax.set_title(f'Round {int(round_num)} (Start Time: {round_start_time_str})')
        ax.set_xlabel("X")
        ax.set_ylabel("Z")
        ax.grid(True)

    # Adjust layout to prevent overlap
    plt.tight_layout(rect=[0, 0, 1, 0.96])  # Adjust for the title space

    # Save the plot to a file for each block
    plotfile = f"{plotfile_base}_Block_{int(block_num)}.png"
    plt.savefig(plotfile)
    plt.close()  # Close the plot to avoid displaying it inline
    print(f"Saved plot for Block {block_num} to {plotfile}")