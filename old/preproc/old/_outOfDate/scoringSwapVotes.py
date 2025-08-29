import os
import pandas as pd
import numpy as np
import re

def mergeLogFiles(metaData, filePath):
    ''' 
    Merges log files into a new large log file for a specific participant within a pair with new columns added from the metaData file ['participantID', 'pairID', 'testingDate', 'AorB', 'primaryRole', 'coinSet', 'main_RR', 'device', 'MagicLeapFiles', 'time_MLReported'] 
    log files are located within subdirectories found within filePath, that follow a directory structure like this: 

    filePath
        - pair_06
            - 02082025
                - ML2D
                    - processed
                        - 'ObsReward_B_02_08_2025_13_33_processed.csv'
                        - 'ObsReward_B_02_08_2025_13_30_processed.csv'
                        - 'ObsReward_B_02_08_2025_14_14_processed.csv'
                - ML2A
                    - processed
                        - ObsReward_A_02_08_2025_14_14_processed.csv
                        - ObsReward_A_02_08_2025_13_30_processed.csv
                        - ObsReward_A_02_08_2025_13_33_processed.csv
                - ML2C
                    - processed
                        - 'ObsReward_B_02_08_2025_14_54_processed.csv'
                        - 'ObsReward_B_02_08_2025_15_08_processed.csv'
                        - 'ObsReward_B_02_08_2025_15_18_processed.csv'
                - ML2G
                    - processed
                        - 'ObsReward_A_02_08_2025_23_18_processed.csv'
                        - 'ObsReward_A_02_08_2025_22_54_processed.csv'
                        - 'ObsReward_A_02_08_2025_23_08_processed.csv'
            - 02092025
                - ML2A
                    - processed
                        - ObsReward_A_02_09_2025_10_41_processed.csv
                        - ObsReward_A_02_09_2025_11_00_processed.csv
                - ML2G
                    - processed
                        - ObsReward_A_02_09_2025_20_53_processed.csv
                        - ObsReward_A_02_09_2025_21_10_processed.csv
                        - ObsReward_A_02_09_2025_22_04_processed.csv
                        - ObsReward_A_02_09_2025_18_17_processed.csv
                        - ObsReward_A_02_09_2025_18_19_processed.csv
                        - ObsReward_A_02_09_2025_18_20_processed.csv
                        - ObsReward_A_02_09_2025_18_27_processed.csv
                        - ObsReward_A_02_09_2025_19_18_processed.csv
                        - ObsReward_A_02_09_2025_19_27_processed.csv
                        - ObsReward_A_02_09_2025_20_35_processed.csv
                        - ObsReward_A_02_09_2025_20_38_processed.csv
                - ML2C
                    - processed
                        - ObsReward_B_02_09_2025_13_10_processed.csv
                        - ObsReward_B_02_09_2025_12_38_processed.csv
                        - ObsReward_B_02_09_2025_12_53_processed.csv
                        - ObsReward_B_02_09_2025_14_04_processed.csv
                        - ObsReward_B_02_09_2025_10_17_processed.csv
                        - ObsReward_B_02_09_2025_10_19_processed.csv
                        - ObsReward_B_02_09_2025_10_20_processed.csv
                        - ObsReward_B_02_09_2025_10_27_processed.csv
                        - ObsReward_B_02_09_2025_11_18_processed.csv
                        - ObsReward_B_02_09_2025_11_27_processed.csv
                        - ObsReward_B_02_09_2025_12_35_processed.csv
    '''
    if not isinstance(metadata, pd.DataFrame):
        raise ValueError("Metadata is not a valid DataFrame.")
    
    # Ensure the column 'MagicLeapFiles' exists
    if "MagicLeapFiles" not in metadata.columns:
        raise KeyError("'MagicLeapFiles' column not found in metadata.")
    
    # Match the file with its metadata
    matched_metadata = metadata[metadata["MagicLeapFiles"].str.strip() == filename.strip()]

    if matched_metadata.empty:
        print(f"⚠ Warning: No metadata found for file: {filename}")
        return data  # Return original data if no match is found

    # Extract metadata values
    metadata_values = matched_metadata.iloc[0]

    # Add metadata as new columns
    for column in ["participantID", "pairID", "AorB", "coinSet", "main_RR", "device"]:
        if column in metadata_values:
            data[column] = metadata_values[column]
        else:
            print(f"⚠ Warning: Column {column} not found in metadata!")

    return data

def simplifyBlockData(data, file_path):
    """
    Processes merged ObsReward log files, scores SwapBlockVotes, and saves only those rows into a new dataframe 'scoredData' out to file_path as a csv file

    """
    for idx, row in data.iterrows():
        message = row.get('Message', None)

        rewardPattern = r"^(A\.N\. finished|Finished) a perfect dropround with:(\d+\.\d{2}) total reward: (\d+\.\d{2})$"
        match = re.search(rewardPattern, message)
        if match:
            #event_type = match.group(1)  # Captures "A.N. finished" or "Finished"
            dropround_value = match.group(2)  # Captures the first number (e.g., "0.00")
            total_reward = match.group(3)  # Captures the second number (e.g., "490.00")

            #print(f"Event Type: {event_type}")
            print(f"Dropround Value: {dropround_value}")
            print(f"Total Reward: {total_reward}")

        current_coinset_id = row.get('CoinSetID', None)

        # Assign SwapBlockVote
        if current_coinset_id is not None:
            if current_coinset_id != 1 and ("Observer says it was a NEW round." in message or "Active Navigator says it was a NEW round."):
                swap_vote = "Correct"
            elif current_coinset_id == 1 and ("Observer says it was an OLD round." in message or "Active Navigator says it was an OLD round."):
                swap_vote = "Correct"
            else:
                swap_vote = "Incorrect"
        else:
            swap_vote = None

        data.at[idx, 'SwapBlockVote'] = swap_vote
        data.at[idx, 'blockReward'] = dropround_value
        data.at[idx, 'total_reward'] = total_reward


