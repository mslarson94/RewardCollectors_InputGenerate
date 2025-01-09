import pandas as pd
import numpy as np

# Load the data
file_path = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08112024/ObsReward_A_08_11_2024_11_29.csv'
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

# Convert the cleaned data into a DataFrame
data_cleaned = pd.DataFrame(cleaned_data)
data_cleaned.columns = data_cleaned.iloc[0]

data_cleaned = data_cleaned[1:]

data_cleaned.replace('', np.nan, inplace=True)
data_cleaned.dropna(how='all', inplace=True)
print(data_cleaned)


cleaned_file_path = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08112024/ObsReward_A_08_09_2024_13_35_cleaned_data.csv'
data_cleaned.to_csv(cleaned_file_path, index=False)

print(f"Cleaned data saved to {cleaned_file_path}")