'''
basicCleaning_1.py
author: Myra Saraí Larson  
email: mairahsarai94@gmail.com

script to remedy the accidental data overflow in the 'Messages' column when text contains ','
'''
import pandas as pd
import numpy as np
import os
import re

#directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/pair_06/02082025/ML2C'
#directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/pair_06/02082025/ML2D'

#directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/pair_06/02092025/ML2C'
#directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/pair_06/02092025/ML2D'

#directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/02052025/ML2C'
#directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/02052025/ML2D'

#directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/02012025/ML2C'
#directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/02012025/ML2D'

#directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/01262025/ML2C'
#directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/01262025/ML2D'

directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/01252025/ML2C'
# Process all matching files in the directory
#directory_path = '/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/SampleData/01202025/ML2C' # Replace with the actual directory path
output_directory = directory_path + '/Cleaned'  # Where cleaned files will be saved
os.makedirs(output_directory, exist_ok=True) # Create the output directory if it doesn't exist

# Define the regex pattern for matching the file names
pattern = re.compile(r"^ObsReward_[AB]_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")

# Iterate over all CSV files in the directory
for filename in os.listdir(directory_path):
    print(filename)
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
                if len(split_line) > 24:
                    combined_value = ','.join(split_line[23:])
                    split_line = split_line[:23] + [combined_value]

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