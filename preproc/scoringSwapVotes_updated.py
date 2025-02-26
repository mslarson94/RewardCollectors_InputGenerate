
import os
import pandas as pd
import numpy as np
import re

def find_log_files(directory):
    ''' Recursively find all ObsReward_*_processed.csv files in directory '''
    log_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if re.match(r"ObsReward_[A|B]_.*_processed\.csv", file):
                log_files.append(os.path.join(root, file))
    return log_files

def load_magic_leap_data(metadata_path):
    ''' Load the MagicLeapFiles sheet from collatedData.xlsx '''
    xls = pd.ExcelFile(metadata_path)
    magic_leap_data = pd.read_excel(xls, sheet_name="MagicLeapFiles")
    return magic_leap_data

def mergeLogFiles(metaDataPath, logsDirectory, outputDirectory):
    ''' 
    Merges log files with metadata from MagicLeapFiles and saves them per participant.
    '''
    magic_leap_data = load_magic_leap_data(metaDataPath)

    # Clean file names by removing '_processed'
    magic_leap_data["Cleaned_Filenames"] = magic_leap_data["MagicLeapFiles"].str.replace("_processed", "", regex=False)

    # Load all logs recursively
    log_files = find_log_files(logsDirectory)
    all_logs = []

    for log_file in log_files:
        log_data = pd.read_csv(log_file)

        # Extract the cleaned filename from the path
        file_name = os.path.basename(log_file).replace("_processed", "")

        # Find matching metadata row
        match = magic_leap_data[magic_leap_data["Cleaned_Filenames"] == file_name]

        if not match.empty:
            match_info = match.iloc[0]  # Take the first match

            # Add new metadata columns
            for col in ['participantID', 'pairID', 'testingDate', 'AorB', 'primaryRole', 'coinSet', 
                        'main_RR', 'device', 'MagicLeapFiles', 'time_MLReported']:
                log_data[col] = match_info[col]

            all_logs.append(log_data)

    if all_logs:
        merged_df = pd.concat(all_logs, ignore_index=True)

        # Convert `participantID` and `testingDate` to strings to ensure proper merging
        merged_df["participantID"] = merged_df["participantID"].astype(str)
        merged_df["testingDate"] = merged_df["testingDate"].astype(str)

        # Merge logs that share the same participantID & testingDate
        grouped = merged_df.groupby(["participantID", "testingDate"])

        for (participantID, testingDate), group in grouped:
            file_name = f"participant_{participantID}_{testingDate}.csv"
            save_path = os.path.join(outputDirectory, file_name)

            group.to_csv(save_path, index=False)
            print(f"Saved merged file: {save_path}")

        return merged_df
    else:
        print("No matching logs found to merge.")
        return pd.DataFrame()  # Return empty if no files were merged



def simplifyBlockData(data, output_path):
    '''
    Processes merged log files, scores SwapBlockVotes, and saves the result.
    '''

    # Debugging: Print unique values in "Type"
    print("Unique Type Values in Data:", data["Type"].unique())

    # Dictionary to store latest block reward & total reward per BlockNum
    block_rewards = {}

    for idx, row in data.iterrows():
        message = row.get('Message', None)
        if not isinstance(message, str) or message.strip() == "":
            continue  # Skip empty messages

        rewardPattern = r"^(A\.N\. finished|Finished) a perfect (dropround|round) with:(\d+\.\d{2}) total reward: (\d+\.\d{2})$"
        match = re.search(rewardPattern, message)
        #Finished a perfect round with:30.00 total reward: 0.00
        # Fix: Convert CoinSetID to integer (handle NaN)
        current_coinset_id = row.get('CoinSetID', None)
        #print(type(current_coinset_id), str(current_coinset_id))

        if not isinstance(message, str):
            message = ""

        # Store block reward & total reward in dictionary
        block_num = row.get("BlockNum")
        if pd.notna(block_num):  # Ensure BlockNum is valid
            block_num = int(float(block_num))  # Convert to int

            if match:
                block_rewards[block_num] = (match.group(3), match.group(4))
                print(f"Saved reward info for Block {block_num}: {match.group(3)}, {match.group(4)}")

        # Assign SwapBlockVote
        swap_vote = None
        # Fix: Be Selective in Identifying SwapBlockVotes
        if isinstance(message, str) and (
            "Observer says it was an OLD round." in message
            or "Observer says it was a NEW round." in message
            or "Active Navigator says it was an OLD round." in message
            or "Active Navigator says it was a NEW round." in message
        ):
            if current_coinset_id is not None:
                if current_coinset_id != 1.0 and ("NEW" in message):
                    swap_vote = "Correct"
                    print(str(current_coinset_id), message, swap_vote)
                elif current_coinset_id == 1.0 and ("OLD" in message):
                    swap_vote = "Correct"
                    print(str(current_coinset_id), message, swap_vote)
                elif current_coinset_id != 1.0 and ("OLD" in message):
                    swap_vote = "Incorrect"
                    print(str(current_coinset_id), message, swap_vote)
                elif current_coinset_id == 1.0 and ("NEW" in message):
                    swap_vote = "Incorrect"
                    print(str(current_coinset_id), message, swap_vote)

        # # Fix: Be Selective in Identifying SwapBlockVotes
        # if isinstance(message, str) and (
        #     "Observer says it was an OLD round." in message
        #     or "Observer says it was a NEW round." in message
        #     or "Active Navigator says it was an OLD round." in message
        #     or "Active Navigator says it was a NEW round." in message
        # ):
            if block_num in block_rewards:
                data.at[idx, 'ANblockReward'] = block_rewards[block_num][0]
                data.at[idx, 'ANtotal_reward'] = block_rewards[block_num][1]
            data.at[idx, 'SwapBlockVote'] = swap_vote

    # Debugging: Print number of detected SwapBlockVotes
    swap_block_data = data[data["SwapBlockVote"].notna()]
    print(f"Total SwapBlockVotes found: {len(swap_block_data)}")

    if not swap_block_data.empty:
        swap_block_data.to_csv(output_path, index=False)
        print(f"Processed data saved to {output_path}")
    else:
        print("No SwapBlockVotes data found. Nothing to save.")




metaDataPath = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
logsDirectory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/pair_06"
output_path = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/pair_06/mergedLogs_113.csv"
outputDirectory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/pair_06"

# merged_data = mergeLogFiles(metaDataPath, logsDirectory, outputDirectory)
# if not merged_data.empty:
#     simplifyBlockData(merged_data, output_path)
# else:
#     print("No matching logs found to merge.")

pt113_day1_df = pd.read_csv("/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/pair_06/participant_113_2092025.csv")
sampleOutPath = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/pair_06/pt_113_blockSummaryData.csv"
simplifyBlockData(pt113_day1_df, output_path)