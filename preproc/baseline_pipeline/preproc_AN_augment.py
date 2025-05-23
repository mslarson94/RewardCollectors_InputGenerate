import os
import pandas as pd
import numpy as np
import re

def correct_malformed_string(raw_string):
    """Fixes concatenated numeric values like -1.0000.000-6.000"""
    pattern = r"(-?\d+\.\d{3})(-?\d+\.\d{3})(-?\d+\.\d{3})"
    return re.sub(pattern, r"\1 \2 \3", raw_string)

def detect_and_tag_blocks(data):
    block_start_idx = None
    block_num = None
    coinset_id = None
    block_type = None
    last_seen_coinset = None
    round_num = None  # Will be initialized at block start

    data["RoundNum"] = np.nan
    data["BlockNum"] = np.nan
    data["CoinSetID"] = np.nan
    data["BlockType"] = np.nan

    for idx, row in data.iterrows():
        message = row.get("Message", "")

        if isinstance(message, str):
            # Block start detection
            if message == "Mark should happen if checked on terminal.":
                block_start_idx = idx
                round_num = 0
                data.at[idx, "RoundNum"] = round_num  # Tag block start with 0

            # CoinsetID tracking
            if message.startswith("coinsetID:"):
                coinset_match = re.search(r"coinsetID:(\d+)", message)
                if coinset_match:
                    last_seen_coinset = int(coinset_match.group(1))

            # Block type & metadata tagging
            block_match = re.search(r"Started (?:collecting|pindropping)\. Block:(\d+)", message)
            if block_match:
                block_num = int(block_match.group(1))
                coinset_id = last_seen_coinset
                block_type = "pindropping" if "pindropping" in message.lower() else "collecting"

                # Backfill to block start
                backfill_start = block_start_idx if block_start_idx is not None else idx
                for j in range(backfill_start, idx + 1):
                    data.at[j, "BlockNum"] = block_num
                    data.at[j, "CoinSetID"] = coinset_id
                    data.at[j, "BlockType"] = block_type
                block_start_idx = None  # reset

            # Increment on each reposition
            if "Repositioned and ready to start block or round" in message:
                if round_num is not None:
                    round_num += 1
                    data.at[idx, "RoundNum"] = round_num

    return data

def detect_block_completeness(data, block_num, block_rows):
    messages = block_rows["Message"].dropna().str.lower()

    has_start = any("mark should happen" in m for m in messages)
    has_reposition = any("repositioned and ready to start" in m for m in messages)
    has_end = any("finished current task" in m for m in messages)

    if has_start and has_end:
        return "complete"
    elif has_start and not has_end:
        return "truncated"
    else:
        return "incomplete"  # fallback if block structure is unclear

# Cleaned-up forward_fill_block_info function without future-state round checks
def forward_fill_block_info(data):
    current_block_num = None
    current_coinset_id = None
    current_block_type = None
    current_round_num = None

    for idx in range(len(data)):
        row = data.iloc[idx]
        round_num = row["RoundNum"]

        # Track latest known block metadata
        if not pd.isna(row["BlockNum"]):
            current_block_num = row["BlockNum"]
            current_coinset_id = row["CoinSetID"]
            current_block_type = row["BlockType"]

        if not pd.isna(round_num):
            current_round_num = round_num

        # Fill structural info
        if current_block_num is not None:
            data.at[idx, "BlockNum"] = current_block_num
            data.at[idx, "CoinSetID"] = current_coinset_id
            data.at[idx, "BlockType"] = current_block_type

        # Fill round number only if still missing
        if current_round_num is not None and pd.isna(round_num):
            data.at[idx, "RoundNum"] = current_round_num

    return data

# Updated assign_temporal_intervals with new phase definitions (7777, 8888, 9999)
def assign_temporal_intervals(data):
    all_indices = data.index.tolist()
    idx_limit = len(data)
    block_starts = data[data["Message"] == "Mark should happen if checked on terminal."].index

    for i, block_start in enumerate(block_starts):
        next_block_start = block_starts[i + 1] if i + 1 < len(block_starts) else idx_limit
        block_idxs = list(range(block_start, next_block_start))

        last_reposition = None
        last_finished_round = None

        for idx in block_idxs:
            message = data.at[idx, "Message"]

            # Track reposition markers     
            if isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
                last_reposition = idx

            # Assign 7777: after a round ends and before the next reposition
            if last_finished_round is not None and isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
                for j in range(last_finished_round, idx):
                    data.at[j, "RoundNum"] = 7777      
                last_finished_round = None

            # Track end of round
            if isinstance(message, str) and message.startswith("Finished pindrop round:"):
                last_finished_round = idx

            # Assign 8888: idle at blue cylinder before round start
            if last_reposition is not None and isinstance(message, str) and re.match(r"Started (?:collecting|pindropping)\. Block:\d+", message):
                for j in range(last_reposition, idx):
                    data.at[j, "RoundNum"] = 8888
                last_reposition = None

            # Assign 9999: inter-block idle time
            if isinstance(message, str) and message.lower().strip() == "finished current task":
                block_id = data.at[idx, "BlockNum"]
                post_rows = data[(data.index > idx-1) & (data["BlockNum"] == block_id)]

                for j in post_rows.index:
                    if data.at[j, "RoundNum"] not in [7777, 8888]:
                        data.at[j, "RoundNum"] = 9999
    return data

# Define a separate function to compute chestPin_num and totalRounds after detect_and_tag_blocks is run
def augment_with_chestpin_and_totalrounds(data):
    data["chestPin_num"] = np.nan
    data["totalRounds"] = np.nan

    # Fill missing RoundNum and BlockNum with forward fill for grouping
    data["RoundNum_filled"] = data["RoundNum"].ffill()
    data["BlockNum_filled"] = data["BlockNum"].ffill()

    # Compute totalRounds per block
    valid_rounds = data[~data["RoundNum_filled"].isin([0, 7777, 8888, 9999])]
    round_counts = valid_rounds.groupby("BlockNum_filled")["RoundNum_filled"].nunique()
    data["totalRounds"] = data["BlockNum_filled"].map(round_counts)

    # Reset chestPin_num on round start, accumulate within the round
    chest_pin_count = 0
    for idx, row in data.iterrows():
        message = row.get("Message", "")

        # Reset count on round start
        if isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
            chest_pin_count = 0

        # Count chest/pin events
        if isinstance(message, str) and (message.startswith("Chest opened: ") or message.startswith("Just dropped a pin.")):
            chest_pin_count += 1

        # Assign the current count
        data.at[idx, "chestPin_num"] = chest_pin_count

    # Clean up temporary columns
    data.drop(columns=["RoundNum_filled", "BlockNum_filled"], inplace=True)

    return data


def process_obsreward_file(data, file_path, file_path2):
    data['BlockNum'] = None
    data['RoundNum'] = None
    data['BlockType'] = None
    data['CoinSetID'] = None

    detect_and_tag_blocks(data)
    forward_fill_block_info(data)
    assign_temporal_intervals(data)
    data["Messages_filled"] = data["Message"].fillna(method='ffill')
    data['BlockStatus'] = None
    for block_num in data['BlockNum'].dropna().unique():
        block_mask = data['BlockNum'] == block_num
        status = detect_block_completeness(data, block_num, data[block_mask])
        data.loc[block_mask, 'BlockStatus'] = status
    data.to_csv(file_path, index=False)
    newData = augment_with_chestpin_and_totalrounds(data)
    newData.to_csv(file_path2, index=False)
    print(f"Processed and saved: {file_path}")

def clean_and_process_files(root_directory, output_root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500):
    pattern = re.compile(r"^ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
    print('Started AN-specific processing!')

    for dirpath, _, filenames in os.walk(root_directory):
        for filename in filenames:
            #print(f"Checking file: {filename}")
            if pattern.match(filename):  
                print(f'Match found: {filename}')
                file_path = os.path.join(dirpath, filename)

                relative_path = os.path.relpath(dirpath, root_directory)
                output_dir = os.path.join(output_root_directory, relative_path)
                os.makedirs(output_dir, exist_ok=True)

                outFile = os.path.join(output_dir, f"{filename.replace('.csv', '_processed.csv')}")
                outFile2 = os.path.join(output_dir, f"{filename.replace('.csv', '_processed_new.csv')}")
                print(f"Processing file: {file_path}")
                data = pd.read_csv(file_path)

                # ✅ Fix malformed 'Closest location was:' strings
                for idx, message in data['Message'].dropna().items():
                    if 'Closest location was:' in message:
                        try:
                            raw_string = re.search(r"Closest location was: (.+)", message).group(1)
                            corrected_string = correct_malformed_string(raw_string)
                            data.at[idx, 'Message'] = message.replace(raw_string, corrected_string)
                        except Exception as e:
                            print(f"Error correcting malformed string in message '{message}': {e}")

                # ✅ Continue processing
                process_obsreward_file(data, outFile, outFile2)

########### Execution Block #############
# # Execution block
# root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData"
# output_root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/ProcessedData"

# Single Test File 
root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile2/RawData"
output_root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile2/ProcessedData"

metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")
clean_and_process_files(root_directory, output_root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500)

##################################################################

########### New Chat GPT Code ###################
  
# Complete updated script with correct block tagging, temporal interval assignment, and augmentation
# Complete updated script with correct block tagging, temporal interval assignment, and augmentation
def detect_and_tag_blocks_deferred_coinset(data):
    data["RoundNum"] = np.nan
    data["BlockNum"] = np.nan
    data["CoinSetID"] = np.nan
    data["BlockType"] = np.nan

    current_block = {
        "start_idx": None,
        "block_num": None,
        "block_type": None,
        "coinset_id": None,
    }

    for idx in range(len(data)):
        message = str(data.at[idx, "Message"]).strip()

        if message == "Mark should happen if checked on terminal.":
            current_block = {
                "start_idx": idx,
                "block_num": None,
                "block_type": None,
                "coinset_id": None,
            }
            data.at[idx, "RoundNum"] = 0

        block_match = re.search(r"Started (collecting|pindropping)\. Block:(\d+)", message)
        if block_match:
            current_block["block_type"] = block_match.group(1)
            current_block["block_num"] = int(block_match.group(2))

        if message.startswith("coinsetID:"):
            match = re.search(r"coinsetID:(\d+)", message)
            if match:
                current_block["coinset_id"] = int(match.group(1))

        if message == "finished current task":
            if all(v is not None for v in current_block.values()):
                for j in range(current_block["start_idx"], idx + 1):
                    data.at[j, "BlockNum"] = current_block["block_num"]
                    data.at[j, "CoinSetID"] = current_block["coinset_id"]
                    data.at[j, "BlockType"] = current_block["block_type"]
            else:
                print(f"⚠️ Incomplete metadata for block ending at idx {idx}: {current_block}")
            current_block = {
                "start_idx": None,
                "block_num": None,
                "block_type": None,
                "coinset_id": None,
            }

        if "Repositioned and ready to start block or round" in message:
            if pd.isna(data.at[idx, "RoundNum"]):
                prev = data["RoundNum"].loc[:idx].dropna()
                data.at[idx, "RoundNum"] = (prev.iloc[-1] + 1) if not prev.empty else 1

    return data

def augment_with_chestpin_and_totalrounds(data):
    data["chestPin_num"] = np.nan
    data["totalRounds"] = np.nan

    data["RoundNum_filled"] = data["RoundNum"].ffill()
    data["BlockNum_filled"] = data["BlockNum"].ffill()

    valid_rounds = data[~data["RoundNum_filled"].isin([0, 7777, 8888, 9999])]
    round_counts = valid_rounds.groupby("BlockNum_filled")["RoundNum_filled"].nunique()
    data["totalRounds"] = data["BlockNum_filled"].map(round_counts)

    chest_pin_count = 0
    for idx, row in data.iterrows():
        message = row.get("Message", "")
        if isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
            chest_pin_count = 0
        if isinstance(message, str) and (message.startswith("Chest opened: ") or message.startswith("Just dropped a pin.")):
            chest_pin_count += 1
        data.at[idx, "chestPin_num"] = chest_pin_count

    data.drop(columns=["RoundNum_filled", "BlockNum_filled"], inplace=True)
    return data

# Load data
data = pd.read_csv("/mnt/data/ObsReward_A_02_17_2025_15_11.csv")

# Run full processing
data = detect_and_tag_blocks_deferred_coinset(data)
data = forward_fill_block_info(data)

# assign_temporal_intervals already defined in preproc_AN.py, we replicate here for consistency
def assign_temporal_intervals(data):
    all_indices = data.index.tolist()
    idx_limit = len(data)
    block_starts = data[data["Message"] == "Mark should happen if checked on terminal."].index

    for i, block_start in enumerate(block_starts):
        next_block_start = block_starts[i + 1] if i + 1 < len(block_starts) else idx_limit
        block_idxs = list(range(block_start, next_block_start))

        last_reposition = None
        last_finished_round = None

        for idx in block_idxs:
            message = data.at[idx, "Message"]

            if isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
                last_reposition = idx

            if last_finished_round is not None and isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
                for j in range(last_finished_round, idx):
                    data.at[j, "RoundNum"] = 7777
                last_finished_round = None

            if isinstance(message, str) and message.startswith("Finished pindrop round:"):
                last_finished_round = idx

            if last_reposition is not None and isinstance(message, str) and re.match(r"Started (?:collecting|pindropping)\. Block:\d+", message):
                for j in range(last_reposition, idx):
                    data.at[j, "RoundNum"] = 8888
                last_reposition = None

            if isinstance(message, str) and message.lower().strip() == "finished current task":
                block_id = data.at[idx, "BlockNum"]
                post_rows = data[(data.index > idx-1) & (data["BlockNum"] == block_id)]
                for j in post_rows.index:
                    if data.at[j, "RoundNum"] not in [7777, 8888]:
                        data.at[j, "RoundNum"] = 9999

    return data

# Apply temporal intervals and augmentation
data = assign_temporal_intervals(data)

# Assign block completeness
data['BlockStatus'] = None
for block_num in data['BlockNum'].dropna().unique():
    block_mask = data['BlockNum'] == block_num
    msgs = data[block_mask]["Message"].dropna().str.lower()
    has_start = any("mark should happen" in m for m in msgs)
    has_end = any("finished current task" in m for m in msgs)
    status = "complete" if has_start and has_end else "truncated" if has_start else "incomplete"
    data.loc[block_mask, 'BlockStatus'] = status

# Save mid-stage output
basic_output = "/mnt/data/ObsReward_A_02_17_2025_15_11_processed_with_specials.csv"
data.to_csv(basic_output, index=False)

# Apply chest/pin and total rounds augmentation
augmented_data = augment_with_chestpin_and_totalrounds(data)
final_output = "/mnt/data/ObsReward_A_02_17_2025_15_11_processed_FULL.csv"
augmented_data.to_csv(final_output, index=False)

basic_output, final_output
