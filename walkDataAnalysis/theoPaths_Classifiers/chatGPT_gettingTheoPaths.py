import numpy as np
from fastdtw import fastdtw
from scipy.spatial.distance import euclidean
import dataConfigs  # Import the configuration file directly

# Use AN_positions and coin sets directly from dataConfigs
AN_positions = dict(zip(dataConfigs.pos_strList, dataConfigs.AN_positions))

# Select the correct coin set based on the `whichCoinSet` value from dataConfigs
if dataConfigs.whichCoinSet == 'A':
    CoinSet = {
        'HV_1': dataConfigs.HV_1, 'HV_2': dataConfigs.HV_2,
        'LV_1': dataConfigs.LV_1, 'LV_2': dataConfigs.LV_2,
        'NV_1': dataConfigs.NV_1, 'NV_2': dataConfigs.NV_2
    }
elif dataConfigs.whichCoinSet == 'B':
    CoinSet = {
        'HV_1': dataConfigs.HV_1, 'HV_2': dataConfigs.HV_2,
        'LV_1': dataConfigs.LV_1, 'LV_2': dataConfigs.LV_2,
        'NV_1': dataConfigs.NV_1, 'NV_2': dataConfigs.NV_2
    }
elif dataConfigs.whichCoinSet == 'C':
    CoinSet = {
        'HV_1': dataConfigs.HV_1, 'HV_2': dataConfigs.HV_2,
        'LV_1': dataConfigs.LV_1, 'LV_2': dataConfigs.LV_2,
        'NV_1': dataConfigs.NV_1, 'NV_2': dataConfigs.NV_2
    }

elif dataConfigs.whichCoinSet == 'D':
    CoinSet = {
        'HV_1': dataConfigs.HV_1, 'HV_2': dataConfigs.HV_2,
        'LV_1': dataConfigs.LV_1, 'LV_2': dataConfigs.LV_2,
        'NV_1': dataConfigs.NV_1, 'NV_2': dataConfigs.NV_2
    }

elif dataConfigs.whichCoinSet == 'E':
    CoinSet = {
        'HV_1': dataConfigs.HV_1, 'HV_2': dataConfigs.HV_2,
        'LV_1': dataConfigs.LV_1, 'LV_2': dataConfigs.LV_2,
        'NV_1': dataConfigs.NV_1, 'NV_2': dataConfigs.NV_2
    }
# Add other sets if needed
CoinSet['tutorial_1'] = dataConfigs.tutorial_1
CoinSet['tutorial_2'] = dataConfigs.tutorial_2
CoinSet['tutorial_3'] = dataConfigs.tutorial_3
# Function to classify the walk path
def classify_walk(walk_path, theoretical_paths):
    min_distance = float('inf')
    best_path_category = None

    # Translate paths from theoretical_paths to numerical coordinates
    for idx, row in theoretical_paths.iterrows():
        start_pos_key = row['startPosition']
        theoretical_path = [CoinSet.get(coin) for coin in row[3:].values if CoinSet.get(coin)]
        
        if start_pos_key in AN_positions:
            start_position = AN_positions[start_pos_key]
            theoretical_path.insert(0, start_position)  # Add start position to the path

        # Convert to numpy arrays for fastdtw processing
        walk_path_np = np.array(walk_path)
        theoretical_path_np = np.array(theoretical_path)

        # Calculate the distance using fastdtw
        distance, _ = fastdtw(walk_path_np, theoretical_path_np, dist=euclidean)

        # Find the best path
        if distance < min_distance:
            min_distance = distance
            best_path_category = row['TheoPathType']

    return best_path_category

# Example usage (assuming walk_path and theoretical_paths are defined earlier)
# walk_path = [[x1, y1], [x2, y2], ...]  # Your walk path data
# theoretical_paths = theo_paths_df       # Using the loaded theoretical paths

# path_category = classify_walk(walk_path, theo_paths_df)