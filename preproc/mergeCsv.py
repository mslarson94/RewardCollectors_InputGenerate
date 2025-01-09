import os
import pandas as pd
from dataConfigs import *


# Load the data
file_dir = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08312024/magicLeap/Cleaned/eye'
#fileName = 'ObsReward_A_08_31_2024_12_09'
lsit = ['ObsReward_A_08_31_2024_10_45', 'ObsReward_A_08_31_2024_11_08',
        'ObsReward_A_08_31_2024_11_34', 'ObsReward_A_08_31_2024_11_40', 
        'ObsReward_A_08_31_2024_11_49', 'ObsReward_A_08_31_2024_12_09']
file_list = [file + '_cleaned_eye' for file in lsit]

#file_path = file_dir + '/' + fileName + '.csv'

outpath = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08312024/magicLeap'
outfile = outpath + '/ObsReward_A_08_31_2024_allCleaned.csv'
path_check(outpath)
def append_csv_files(input_directory, output_file, file_list):
    # Initialize an empty DataFrame to store the concatenated data
    combined_df = pd.DataFrame()
    
    for file_name in file_list:
        file_path = os.path.join(input_directory, file_name + '.csv')
        
        # Check if the file exists
        if not os.path.exists(file_path):
            print(f"File {file_path} does not exist.")
            continue
        
        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        
        # If the DataFrame is not empty, remove the first 20 rows for subsequent files
        if not combined_df.empty:
            df = df.iloc[20:]
        
        # Append the DataFrame to the combined DataFrame
        combined_df = pd.concat([combined_df, df], ignore_index=True)
        print(f"Appended {file_name}.csv")
    
    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(output_file, index=False)
    print(f"Combined CSV saved to {output_file}")

if __name__ == "__main__":
    input_directory = file_dir  # Replace with your input directory path
    output_file = outfile  # Replace with your desired output file path
    file_list = file_list  # Replace with the list of CSV file names (without .csv extension)
    
    append_csv_files(input_directory, output_file, file_list)