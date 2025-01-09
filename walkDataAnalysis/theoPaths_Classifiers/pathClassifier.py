import numpy as np
from fastdtw import fastdtw
from scipy.spatial.distance import euclidean

# Sample walking path data (time-series of x, y coordinates)
walk_path = np.array([(1, 2), (2, 3), (3, 4), (4, 5)])  # Example path data

# Known theoretical paths (you would need to define these based on your data)
theoretical_paths = [
    np.array([(1, 1), (2, 2), (3, 3), (4, 4)]),  # Path 1
    np.array([(1, 2), (2, 3), (3, 4), (4, 5)]),  # Path 2
    # Add more paths as needed
]

def classify_walk(walk_path, theoretical_paths, threshold=5.0):
    min_distance = float('inf')
    best_match = None
    
    # Compare the walk path to each theoretical path using DTW
    for idx, theoretical_path in enumerate(theoretical_paths):
        distance, _ = fastdtw(walk_path, theoretical_path, dist=euclidean)
        
        if distance < min_distance:
            min_distance = distance
            best_match = idx + 1  # Path indices start from 1
    
    # If the distance exceeds a threshold, classify as the 7th "undetermined" path
    if min_distance > threshold:
        return 7  # Undetermined path
    else:
        return best_match

# Classify the walk
path_category = classify_walk(walk_path, theoretical_paths)
print(f"The walk is classified as Path {path_category}")