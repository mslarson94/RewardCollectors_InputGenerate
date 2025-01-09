import pandas as pd
import numpy as np
from dataConfigs import *

# Load the data
file_dir = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08312024/magicLeap'
fileName = 'ObsReward_A_08_31_2024_12_09'
lsit = ['ObsReward_A_08_31_2024_10_45', 'ObsReward_A_08_31_2024_11_08',
        'ObsReward_A_08_31_2024_11_34', 'ObsReward_A_08_31_2024_11_40', 
        'ObsReward_A_08_31_2024_11_49', 'ObsReward_A_08_31_2024_12_09']
file_path = file_dir + '/' + fileName + '.csv'
# Initialize an empty list to collect rows
cleaned_data = []

# Open the file and process it line by line
with open(file_path, 'r') as file:
    for line in file:

        # Split the line by commas
        split_line = line.strip().split(',')

        # If there are more than 19 columns, combine the extras into column 19
        if len(split_line) > 19:
            # Combine the extra columns starting from index 19
            combined_value = ','.join(split_line[18:])
            # Keep only the first 18 columns and append the combined value as the 19th column
            split_line = split_line[:18] + [combined_value]

        # Add the cleaned line to the list
        cleaned_data.append(split_line)

#|AmplitudeDeg:0.000|DirectionRadial:0.000|EyeLeft:(0.000 0.000)|EyeRight:(0.000  0.000)|VelocityDegps:0.000
#|AmplitudeDeg:0.000|DirectionRadial:0.000|EyeLeft:(0.000 0.000)|EyeRight:(0.000  0.000)|VelocityDegps:

#|AmplitudeDeg:
# Convert the cleaned data into a DataFrame
data_cleaned = pd.DataFrame(cleaned_data)
data_cleaned.columns = data_cleaned.iloc[0]

data_cleaned = data_cleaned[1:]

data_cleaned.replace('', np.nan, inplace=True)
data_cleaned.dropna(how='all', inplace=True)
print(data_cleaned)

new_file_dir = file_dir + '/Cleaned'
path_check(new_file_dir)
new_filePath = new_file_dir + '/' + fileName + '_cleaned.csv'

data_cleaned.to_csv(new_filePath, index=False)


print(f"Cleaned data saved to {new_filePath}")