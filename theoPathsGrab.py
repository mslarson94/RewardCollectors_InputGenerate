from collections import namedtuple, defaultdict
import pandas as pd
import csv
import os
import numpy as np
from fastdtw import fastdtw
from scipy.spatial.distance import euclidean
import matplotlib.pyplot as plt

# Load your data configurations
import dataConfigs  # Import the configuration file directly

# File paths
filePath = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08312024/magicLeap'
walkFileName = 'ObsReward_A_08_31_2024_waypoints'
theoFileName = 'TheoPaths_All'
walkFile = filePath + '/' + walkFileName + '.csv'
theoFile = filePath + '/' + theoFileName + '.csv'

# Use AN_positions and coin sets directly from dataConfigs
AN_positions = dict(zip(dataConfigs.pos_strList, dataConfigs.AN_positions))
whichCoinSet = dataConfigs.whichCoinSet

# Define the CoinSet based on whichCoinSet
CoinSet = {
    'HV_1': dataConfigs.HV_1, 'HV_2': dataConfigs.HV_2,
    'LV_1': dataConfigs.LV_1, 'LV_2': dataConfigs.LV_2,
    'NV_1': dataConfigs.NV_1, 'NV_2': dataConfigs.NV_2,
    'tutorial_1': dataConfigs.tutorial_1,
    'tutorial_2': dataConfigs.tutorial_2,
    'tutorial_3': dataConfigs.tutorial_3
}

# Define namedtuple for storing each path's coordinates and processed paths
PathData = namedtuple('PathData', ['theo_path_type', 'coin_set', 'start_position', 'path_coordinates', 'processed_coordinates'])

# Load the CSV data
theo_df = pd.read_csv(theoFile)

# Dictionary to store all paths keyed by (CoinSet + StartPosition) holding a list of path types
theoretical_paths_dict = defaultdict(list)

# Iterate through the DataFrame and fill the dictionary with path data
for index, row in theo_df.iterrows():
    unique_key = (row['CoinSet'], row['startPosition'])
    
    # Extract the coordinates from columns '1', '2', '3', '4', '5', '6'
    path_coordinates = [row['startPosition']] + [row[col] for col in ['1', '2', '3', '4', '5', '6']]
    
    # Process each coordinate (convert them to actual positions using CoinSet or AN_positions)
    processed_coordinates = []
    for coin in path_coordinates:
        if coin in CoinSet:
            processed_coordinates.append(CoinSet[coin][:2])  # Take only x, y
        elif coin in AN_positions:
            processed_coordinates.append(AN_positions[coin])
        else:
            print(f"Warning: Coin {coin} not found in CoinSet or AN_positions!")

    # Ensure all coordinates are numeric before storing them
    if all(isinstance(coord, (list, tuple)) and len(coord) == 2 for coord in processed_coordinates):
        path_data = PathData(
            theo_path_type=row['TheoPathType'],
            coin_set=row['CoinSet'],
            start_position=row['startPosition'],
            path_coordinates=path_coordinates,
            processed_coordinates=processed_coordinates
        )
        theoretical_paths_dict[unique_key].append(path_data)

# Print loaded paths for verification
print("\nLoaded Theoretical Paths and Path Types:")
for key, paths in theoretical_paths_dict.items():
    for path_data in paths:
        print(f"Key: {key}, Path Type: {path_data.theo_path_type}")

# Check the number of unique path types loaded
path_types = set([path_data.theo_path_type for paths in theoretical_paths_dict.values() for path_data in paths])
print(f"Unique path types loaded: {path_types}")

# Load walk data
walk_df = pd.read_csv(walkFile)

# Function to extract walk path data for each round in each block, including the starting position
def extract_walk_paths_with_start(df):
    walk_paths = {}
    grouped = df.groupby(['BlockNumber', 'RoundNum'])
    
    for (block_num, round_num), group in grouped:
        walk_path = group['handPos_anc'].apply(lambda pos: eval(pos)).tolist()
        start_position = eval(group['roundStart'].iloc[0])
        walk_paths[(block_num, round_num)] = [start_position] + walk_path
    return walk_paths

walk_paths_with_start_dict = extract_walk_paths_with_start(walk_df)

# Classification function using DTW
def classify_walk(walk_path, theoretical_paths):
    min_distance = float('inf')
    best_path_category = None
    
    # Iterate over all theoretical paths for the same (CoinSet, StartPosition)
    for path_data in theoretical_paths:
        theoretical_path = path_data.processed_coordinates  # Ensure numeric coordinates

        walk_path_np = np.array(walk_path)
        theoretical_path_np = np.array(theoretical_path)

        try:
            # Only attempt DTW if both paths have valid numeric coordinates
            if len(walk_path_np) > 0 and len(theoretical_path_np) > 0:
                distance, _ = fastdtw(walk_path_np, theoretical_path_np, dist=euclidean)
                if distance < min_distance:
                    min_distance = distance
                    best_path_category = path_data.theo_path_type
        except Exception as e:
            print(f"Error computing DTW distance: {e}")
            continue

    return best_path_category

# Classify each walk
classification_results = []
for (block_num, round_num), walk_path in walk_paths_with_start_dict.items():
    walk_path_np = np.array(walk_path)
    
    # Use the CoinSet and StartPosition to get the relevant theoretical paths
    start_position = walk_path[0]  # Assuming this is the starting position
    key = (whichCoinSet, start_position)

    if key in theoretical_paths_dict:
        theoretical_paths = theoretical_paths_dict[key]
        path_category = classify_walk(walk_path_np, theoretical_paths)
        print(f"Block {block_num}, Round {round_num}: Classified as Path {path_category}")
        classification_results.append([block_num, round_num, path_category])
    else:
        print(f"No theoretical paths found for Block {block_num}, Round {round_num}")
        classification_results.append([block_num, round_num, None])

# Plotting the theoretical paths (Fix the AttributeError)

# Save the classification results to a CSV file
output_csv_path = filePath + '/pathCat.csv'
with open(output_csv_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["BlockNumber", "RoundNum", "PathCategory"])
    writer.writerows(classification_results)

# Plotting functions omitted for brevity, but should work as before


# Function to plot a walk and overlay multiple theoretical paths
def plot_walk(walk_path, theoretical_paths=None, block_num=None, round_num=None):
    walk_x, walk_y = zip(*[(pos[0], pos[1]) for pos in walk_path if len(pos) >= 2])
    
    plt.figure(figsize=(8, 6))
    plt.plot(walk_x, walk_y, '-o', label='Actual Walk', color='blue')
    plt.arrow(walk_x[0], walk_y[0], walk_x[1] - walk_x[0], walk_y[1] - walk_y[0], 
              head_width=0.15, head_length=0.2, fc='green', ec='green', label='Start')
    plt.scatter(walk_x[-1], walk_y[-1], marker='x', color='red', s=100, label='End')
    
    # If theoretical paths are provided, overlay them
    if theoretical_paths is not None:
        for path_data in theoretical_paths:
            theo_x, theo_y = zip(*path_data.processed_coordinates)
            plt.plot(theo_x, theo_y, '--x', label=f'Theoretical Path ({path_data.theo_path_type})', color='orange')

    plt.title(f'Walk Path - Block {block_num}, Round {round_num}')
    plt.xlabel('X Coordinate')
    plt.ylabel('Y Coordinate')
    plt.legend()
    
    # Save plot to file
    plot_file = f'walk_path_plots/actualWalk_block{block_num}_round{round_num}.png'
    plt.savefig(plot_file)
    plt.close()
    print(f"Plot saved successfully to: {plot_file}")

# Now plot each walk path and the corresponding theoretical paths
for (block_num, round_num), walk_path in walk_paths_with_start_dict.items():
    walk_path_np = np.array(walk_path)
    
    # Get the relevant theoretical paths using CoinSet and StartPosition
    start_position = walk_path[0]  # Assuming the first point is the start position
    key = (whichCoinSet, start_position)

    if key in theoretical_paths_dict:
        theoretical_paths = theoretical_paths_dict[key]
        plot_walk(walk_path_np, theoretical_paths, block_num, round_num)
    else:
        print(f"No theoretical paths found for Block {block_num}, Round {round_num}")

# Create a directory to save the plots if it doesn't exist
output_dir = 'theoretical_path_plots'
os.makedirs(output_dir, exist_ok=True)

def plot_theoretical_path(theoretical_paths, block_num=None):
    for path_data in theoretical_paths:
        processed_coordinates = path_data.processed_coordinates

        if len(processed_coordinates) == 0:
            print(f"No valid numeric coordinates to plot for Block {block_num}, Path Type {path_data.theo_path_type}")
            continue
        
        theo_x, theo_y = zip(*processed_coordinates)
        
        plt.figure(figsize=(8, 6))
        plt.plot(theo_x, theo_y, '--o', label='Theoretical Path', color='orange')
        
        # Mark the start with an arrow
        plt.arrow(theo_x[0], theo_y[0], theo_x[1] - theo_x[0], theo_y[1] - theo_y[0], 
                  head_width=0.15, head_length=0.2, fc='green', ec='green', label='Start')

        # Mark the end with an X
        plt.scatter(theo_x[-1], theo_y[-1], marker='x', color='red', s=100, label='End')

        # Plot configuration
        plt.title(f'Theoretical Path - Block {block_num}, Type: {path_data.theo_path_type}')
        plt.xlabel('X Coordinate')
        plt.ylabel('Y Coordinate')
        plt.legend()

        # Save plot to a file
        plot_filename = f'{output_dir}/theoretical_path_block{block_num}_type_{path_data.theo_path_type}.png'
        plt.savefig(plot_filename)
        plt.close()
        print(f"Plot saved successfully to: {plot_filename}")

# Now plot all paths
for key, paths in theoretical_paths_dict.items():
    plot_theoretical_path(paths, block_num=key[1])