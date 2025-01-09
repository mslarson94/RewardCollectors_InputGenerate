#basicCleaning_1.py
import pandas as pd
import numpy as np
import os
import re

# Define the directory containing the files
testing_path = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc'  # Replace with the actual directory path
testingDate = '07092024'
directory_path = testing_path + '/' + testingDate
output_directory = directory_path + '/Cleaned'  # Where cleaned files will be saved
# Create the output directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)

# Define the regex pattern for matching the file names
pattern = re.compile(r"^ObsReward_[AB]_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}\_.csv$")

# Iterate over all CSV files in the directory
for filename in os.listdir(directory_path):
    if pattern.match(filename):  # Only process files that match the pattern
        file_path = os.path.join(directory_path, filename)
        print(f"Processing file: {file_path}")
        
        # Initialize an empty list to collect rows
        cleaned_data = []

        # Open the file and process it line by line
        with open(file_path, 'r') as file:
            for line in file:
                # Split the line by commas
                split_line = line.strip().split(',')

                # If there are more than 19 columns, combine the extras into column 19
                if len(split_line) > 19:
                    combined_value = ','.join(split_line[18:])
                    split_line = split_line[:18] + [combined_value]

                # Add the cleaned line to the list
                cleaned_data.append(split_line)

        # Convert the cleaned data into a DataFrame
        data_cleaned = pd.DataFrame(cleaned_data)
        data_cleaned.columns = data_cleaned.iloc[0]
        data_cleaned = data_cleaned[1:]
        
        # Replace empty values with NaN and drop rows that are completely NaN
        data_cleaned.replace('', np.nan, inplace=True)
        data_cleaned.dropna(how='all', inplace=True)
        
        # Define the output file path
        cleaned_file_path = os.path.join(output_directory, f"{filename.split('.csv')[0]}_cleaned_data.csv")
        
        # Save the cleaned data to a new CSV file
        data_cleaned.to_csv(cleaned_file_path, index=False)
        print(f"Cleaned data saved to {cleaned_file_path}")