import os
import pandas as pd
from dataConfigs import *

def remove_columns_from_csv(input_directory, output_directory, columns_to_remove, file_list):
    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)
    
    for file_name in file_list:
        input_file_path = os.path.join(input_directory, file_name + '.csv')
        output_file_path = os.path.join(output_directory, file_name + '_modified.csv')
        
        # Check if the file exists
        if not os.path.exists(input_file_path):
            print(f"File {input_file_path} does not exist.")
            continue
        
        # Load the CSV file
        df = pd.read_csv(input_file_path)
        
        # Remove the specified columns if they exist
        df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')
        
        # Save the modified DataFrame to a new CSV file
        df.to_csv(output_file_path, index=False)
        print(f"Processed {file_name}.csv and saved as {file_name}_modified.csv.")


infile_dir = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08312024/magicLeap/Cleaned/eye'
outfile_dir = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08312024/magicLeap/Cleaned/Humza'
path_check(outfile_dir)
# fileName = 'ObsReward_A_08_31_2024_12_09_cleaned'

lsit = ['ObsReward_A_08_31_2024_10_45_cleaned_eye', 'ObsReward_A_08_31_2024_11_08_cleaned_eye',
        'ObsReward_A_08_31_2024_11_34_cleaned_eye', 'ObsReward_A_08_31_2024_11_40_cleaned_eye', 
        'ObsReward_A_08_31_2024_11_49_cleaned_eye', 'ObsReward_A_08_31_2024_12_09_cleaned_eye']

columnList = ['BlockType', 'GlobalBlock', 'Type', 'HeadPos', 'HeadRot(pitch yaw roll)', 'EyeDirection', 'EyeTarget', 
              'HeadPosAnchored',  'FixationPointAnchored',   'AmplitudeDeg', 'DirectionRadial', 'VelocityDegps', 
              'optiRbodyposA',   'optiRbodyposB',   'optiRbodyrotA',   'optiRbodyrotB',   'Message']
# inFile = file_dir + '/' + fileName + '.csv'
# outFile = file_dir + '/' + fileName + '_eye.csv'


if __name__ == "__main__":
    input_directory = infile_dir  # Replace with your input directory path
    output_directory = outfile_dir  # Replace with your output directory path
    columns_to_remove = columnList  # Replace with the list of columns to remove
    file_list = lsit  # Replace with the list of CSV file names (without .csv extension)
    
    remove_columns_from_csv(input_directory, output_directory, columns_to_remove, file_list)