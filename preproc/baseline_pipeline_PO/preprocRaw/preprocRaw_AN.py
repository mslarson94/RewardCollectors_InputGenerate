import os
import pandas as pd
import numpy as np
import re
import argparse
from pathlib import Path

## preprocRawHelpers functions
from RC_utilities.preprocHelpers.preprocRawHelpers import (
    detect_and_tag_blocks,
    forward_fill_block_info,
    detect_block_completeness,
    fix_collecting_block_coinsetids,
    final_column_order,
    safe_parse_timestamp,
    robust_parse_timestamp,
    enhance_timestamp_with_apptime,
    process_obsreward_file,
    check_monotonic_apptime,
    drop_malformed_trailing_rows,
    )
## warning_logger
from RC_utilities.segHelpers.warning_logger import WarningLogger


def correct_malformed_string(raw_string):
    """Fixes concatenated numeric values like -1.0000.000-6.000"""
    pattern = r"(-?\d+\.\d{3})(-?\d+\.\d{3})(-?\d+\.\d{3})"
    return re.sub(pattern, r"\1 \2 \3", raw_string)

# Updated assign_temporal_intervals with new phase definitions (7777, 8888, 9999)

def assign_temporal_intervals_AN_v1(data):
    data["RoundNum"] = np.nan  # Reset RoundNum for clean assignment

    block_starts = data[data["Message"] == "Mark should happen if checked on terminal."].index
    idx_limit = len(data)

    for i, block_start in enumerate(block_starts):
        next_block_start = block_starts[i + 1] if i + 1 < len(block_starts) else idx_limit
        block_idxs = list(range(block_start, next_block_start))

        round_num = 0  # Initialize round numbering at start of block
        data.at[block_start, "RoundNum"] = round_num  # Mark block start

        last_finished_round = None
        last_reposition = None

        for idx in block_idxs:
            message = data.at[idx, "Message"]

            if isinstance(message, str):
                message_lower = message.lower().strip()

                # Detect "Started collecting" or "Started pindropping" — round start
                if re.match(r"started (collecting|pindropping)\. block:\d+", message_lower):
                    round_num += 1
                    data.at[idx, "RoundNum"] = round_num

                    # Tag 8888 for pre-round idle from last reposition
                    if last_reposition is not None:
                        for j in range(last_reposition, idx):
                            data.at[j, "RoundNum"] = 8888
                        last_reposition = None

                # Detect "Finished pindrop round" — round end
                elif message_lower.startswith("finished pindrop round:"):
                    last_finished_round = idx

                # Detect "Repositioned and ready to start block or round"
                elif message_lower.startswith("repositioned and ready to start block or round"):
                    # Tag 7777 between finished round and next reposition
                    if last_finished_round is not None:
                        for j in range(last_finished_round, idx):
                            data.at[j, "RoundNum"] = 7777
                        last_finished_round = None
                    last_reposition = idx

                # Detect "finished current task" — block end marker
                elif message_lower == "finished current task":
                    block_id = data.at[idx, "BlockNum"]
                    post_rows = data[(data.index >= idx) & (data["BlockNum"] == block_id)]

                    for j in post_rows.index:
                        if data.at[j, "RoundNum"] not in [7777, 8888]:
                            data.at[j, "RoundNum"] = 9999
                    break  # Exit block parsing

    return data

MARK = "Mark should happen if checked on terminal."

def qc_roundnum_AN_origrow(df, subject_col="Subject", origrow_col="origRow", message_col="Message",
                          allow_nans=True):
    issues = []

    if subject_col in df.columns:
        groups = df.groupby(subject_col, sort=False)
    else:
        groups = [("ALL", df)]

    for subj, g in groups:
        # sort by true time
        g = g.sort_values(origrow_col, kind="mergesort").copy()

        mark_mask = (g[message_col] == MARK)
        mark_pos = np.flatnonzero(mark_mask.values)

        if len(mark_pos) == 0:
            issues.append((subj, None, None, None, "NO_BLOCK_STARTS", "No Mark rows found"))
            continue

        msg_lower = g[message_col].astype(str).str.lower().str.strip()

        for bi, start_p in enumerate(mark_pos):
            end_p = mark_pos[bi + 1] if bi + 1 < len(mark_pos) else len(g)
            block = g.iloc[start_p:end_p]

            start_idx_label = block.index[0]
            start_origrow = block[origrow_col].iloc[0]

            # Mark row should be RoundNum 0
            rn0 = block["RoundNum"].iloc[0]
            if rn0 != 0:
                issues.append((subj, bi, start_origrow, start_idx_label,
                               "BLOCK_START_NOT_ZERO", f"RoundNum at Mark is {rn0}"))

            # Optional: NaNs in block
            if not allow_nans:
                nan_mask = block["RoundNum"].isna().values
                if nan_mask.any():
                    first_nan_p = int(np.argmax(nan_mask))
                    issues.append((subj, bi,
                                   block[origrow_col].iloc[first_nan_p],
                                   block.index[first_nan_p],
                                   "NAN_IN_BLOCK", f"NaN count={nan_mask.sum()}"))

            # Identify finish marker inside this block
            block_msg_lower = msg_lower.iloc[start_p:end_p]
            finish_pos = np.flatnonzero(block_msg_lower.values == "finished current task")

            if len(finish_pos) > 0:
                f_p = int(finish_pos[0])
                post = block.iloc[f_p:]

                # After finish: no normal rounds should appear
                def is_normal(x):
                    return pd.notna(x) and (x not in [0, 7777, 8888, 9999]) and (x > 0)

                normal_after = post.index[post["RoundNum"].apply(is_normal)].to_list()
                if normal_after:
                    bad_idx = normal_after[0]
                    bad_row = block.loc[bad_idx]
                    issues.append((subj, bi, bad_row[origrow_col], bad_idx,
                                   "NORMAL_ROUND_AFTER_FINISH",
                                   f"Normal RoundNum after finish; first offending RoundNum={bad_row['RoundNum']}"))

                # 9999 should not appear before finish
                pre = block.iloc[:f_p]
                pre_9999 = pre.index[pre["RoundNum"] == 9999].to_list()
                if pre_9999:
                    bad_idx = pre_9999[0]
                    bad_row = block.loc[bad_idx]
                    issues.append((subj, bi, bad_row[origrow_col], bad_idx,
                                   "9999_BEFORE_FINISH",
                                   "9999 appears before 'finished current task'"))

            # First started should be 1 (if any started exists)
            started_mask = block_msg_lower.str.match(r"started (collecting|pindropping)\. block:\d+")
            if started_mask.any():
                first_started_p = int(np.argmax(started_mask.values))
                started_rn = block["RoundNum"].iloc[first_started_p]
                if started_rn != 1:
                    issues.append((subj, bi,
                                   block[origrow_col].iloc[first_started_p],
                                   block.index[first_started_p],
                                   "FIRST_STARTED_NOT_ONE",
                                   f"RoundNum at first started is {started_rn}"))

    issues_df = pd.DataFrame(
        issues,
        columns=["Subject", "BlockIndex", "origRow", "dfIndex", "Issue", "Context"]
    )
    summary_df = (issues_df.groupby("Issue").size()
                  .sort_values(ascending=False)
                  .reset_index(name="Count")) if not issues_df.empty else \
                 pd.DataFrame(columns=["Issue", "Count"])
    return issues_df, summary_df



def assign_temporal_intervals_AN(data):
    data["RoundNum"] = np.nan  # Reset RoundNum for clean assignment

    block_starts = data[data["Message"] == "Mark should happen if checked on terminal."].index
    idx_limit = len(data)

    for i, block_start in enumerate(block_starts):
        next_block_start = block_starts[i + 1] if i + 1 < len(block_starts) else idx_limit
        block_idxs = list(range(block_start, next_block_start))

        round_num = 0  # Initialize round numbering at start of block
        data.at[block_start, "RoundNum"] = round_num  # Mark block start

        last_finished_round = None
        last_reposition = None

        for idx in block_idxs:
            message = data.at[idx, "Message"]

            if isinstance(message, str):
                message_lower = message.lower().strip()

                # Detect "Started collecting" or "Started pindropping" — round start
                if re.match(r"started (collecting|pindropping)\. block:\d+", message_lower):
                    round_num += 1
                    data.at[idx, "RoundNum"] = round_num

                    # Tag 8888 for pre-round idle from last reposition
                    if last_reposition is not None:
                        for j in range(last_reposition, idx):
                            data.at[j, "RoundNum"] = 8888
                        last_reposition = None

                # Detect "Finished pindrop round" — round end
                elif message_lower.startswith("finished pindrop round:"):
                    last_finished_round = idx

                # Detect "Repositioned and ready to start block or round"
                elif message_lower.startswith("repositioned and ready to start block or round"):
                    # Tag 7777 between finished round and next reposition
                    if last_finished_round is not None:
                        for j in range(last_finished_round, idx):
                            data.at[j, "RoundNum"] = 7777
                        last_finished_round = None
                    last_reposition = idx

                # Detect "finished current task" — block end marker
                elif message_lower == "finished current task":
                    # IMPORTANT: only tag within the current block window
                    for j in range(idx, next_block_start):
                        if data.at[j, "RoundNum"] not in [7777, 8888]:
                            data.at[j, "RoundNum"] = 9999
                    break  # Exit block parsing

    return data


# Define a separate function to compute chestPin_num and totalRounds after detect_and_tag_blocks is run
def augment_with_chestpin_and_totalrounds_AN(data):
    data["chestPin_num"] = np.nan
    data["totalRounds"] = np.nan

    # Find the index of the first real block
    first_valid_block_idx = data["BlockNum"].first_valid_index()
    if first_valid_block_idx is None:
        return data  # No blocks found, return unmodified

    # Slice the data from the first valid block onward
    working_data = data.loc[first_valid_block_idx:].copy()

    # Fill missing RoundNum and BlockNum with forward fill for grouping
    working_data["RoundNum_filled"] = working_data["RoundNum"].ffill()
    working_data["BlockNum_filled"] = working_data["BlockNum"].ffill()

    # Compute totalRounds per block
    valid_rounds = working_data[~working_data["RoundNum_filled"].isin([0, 7777, 8888, 9999])]
    round_counts = valid_rounds.groupby("BlockNum_filled")["RoundNum_filled"].nunique()
    working_data["totalRounds"] = working_data["BlockNum_filled"].map(round_counts)

    # Reset chestPin_num on round start, accumulate within the round
    chest_pin_count = 0
    for idx, row in working_data.iterrows():
        message = row.get("Message", "")

        # Reset count on round start
        if isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
            chest_pin_count = 0

        # Count chest/pin events
        if isinstance(message, str) and (message.startswith("Chest opened: ") or message.startswith("Just dropped a pin.")):
            chest_pin_count += 1

        # Assign the current count
        working_data.at[idx, "chestPin_num"] = chest_pin_count

    # Copy augmented values back into the original DataFrame
    data.loc[working_data.index, ["chestPin_num", "totalRounds"]] = working_data[["chestPin_num", "totalRounds"]]

    return data

def clean_and_process_files(root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500):
    pattern = re.compile(r"^ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.csv$")
    print('Started AN-specific processing!')
    baseDir = os.path.join(root_directory, "RawData")
    output_dir = os.path.join(root_directory, "ProcessedData")
    flatPath = os.path.join(root_directory, "ProcessedData_Flat")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(flatPath, exist_ok=True)
    loggerDir = os.path.join(output_dir, "PreProcLogging_AN")
    os.makedirs(loggerDir, exist_ok=True)
    logger = WarningLogger(output_dir=loggerDir)
    for dirpath, _, filenames in os.walk(baseDir):
        for filename in filenames:
            #print(f"Checking file: {filename}")
            if pattern.match(filename):  
                print(f'Match found: {filename}')
                session_date_match = re.match(r"ObsReward_A_(\d{2}_\d{2}_\d{4})_\d{2}_\d{2}\.csv", filename)
                session_date = session_date_match.group(1) if session_date_match else None

                file_path = os.path.join(dirpath, filename)

                relative_path = os.path.relpath(dirpath, baseDir)
                nestedPath = os.path.join(output_dir, relative_path)
                os.makedirs(nestedPath, exist_ok=True)

                nestedFile = os.path.join(nestedPath, f"{filename.replace('.csv', '_processed.csv')}")
                flatFile = os.path.join(flatPath, f"{filename.replace('.csv', '_processed.csv')}")
                print(f"Processing file: {file_path}")
                logger.log(f"Processing file: {file_path}")
                data = pd.read_csv(file_path)
                # ✅ Track original row index before any manipulation
                data["origRow"] = data.index

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
                process_obsreward_file(data, role = 'AN')
                tagged = assign_temporal_intervals_AN(data)
                issues_df, summary_df = qc_roundnum_AN_origrow(tagged, subject_col="Subject")  # adjust subject col name if needed

                print(summary_df)
                print(issues_df.head(50))
                assign_temporal_intervals_AN(data)
                data["RoundNum"] = data["RoundNum"].fillna(method="ffill")  # 🔧 Ensures all rows within block are tagged
                data = augment_with_chestpin_and_totalrounds_AN(data)

                rPT= data['Timestamp'].apply(lambda ts: robust_parse_timestamp(ts, session_date))
                data['mLT_orig'] = rPT
                #print(data)
                data = enhance_timestamp_with_apptime(data)
                #print(data)
                data["mLT_raw"] = data.pop("Timestamp")

                # Reorder columns
                data = data[final_column_order]
                check_monotonic_apptime(data, col="AppTime", context="preprocRaw_AN.py", logger=logger)
                data = drop_malformed_trailing_rows(data)
                data.to_csv(nestedFile, index=False)
                data.to_csv(flatFile, index=False)
                print(f"Processed and saved: {nestedFile}")
                logger.log(f"Processed and saved: {nestedFile}")
                logger.save()



# ########### Execution Block #############

# if __name__ == "__main__":
#     print("🚀 Starting batch preprocRaw_AN.py ...")

#     trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
#     #procDir = 'SmallSelectedData/idealTestFile3'
#     #procDir = 'SelectedData'
#     procDir = 'FresherStart'
#     root_directory = os.path.join(trueRootDir, procDir)

#     metadata_file = trueRootDir + "/collatedData.xlsx"
#     magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")
#     clean_and_process_files(root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500)


def cli() -> None:
    parser = argparse.ArgumentParser(
        prog="preprocRaw_AN",
        description="Basic preprocessing of Magic Leap data for AN participants."
    )

    # Paths
    parser.add_argument(
        "--root-dir", required=True, type=Path,
        help="Base project directory (e.g., '/Users/you/RC_TestingNotes')."
    )
    parser.add_argument(
        "--proc-dir", required=True, type=Path,
        help="Dataset subdirectory under --root-dir (e.g., 'FresherStart')."
    )

    # Options (mirror defaults from original main)
    parser.add_argument(
        "--max-memory-mb", type=int, default=500,
        help="Max memory (MB) for processing (default: 500)."
    )
    save_group = parser.add_mutually_exclusive_group()
    save_group.add_argument(
        "--save-large-files", dest="save_large_files",
        action="store_true", default=True,
        help="Save intermediary large files (default)."
    )
    save_group.add_argument(
        "--no-save-large-files", dest="save_large_files",
        action="store_false",
        help="Do not save intermediary large files."
    )

    args = parser.parse_args()

    root = args.root_dir.expanduser()
    proc = args.proc_dir
    root_directory = proc if proc.is_absolute() else (root / proc)
    metadata_path = root / "collatedData.xlsx"

    # Optional sanity checks
    if not metadata_path.exists():
        parser.error(f"Metadata file not found: {metadata_path}")
    if not root_directory.exists():
        parser.error(f"Data root not found: {root_directory}")

    magic_leap_data = pd.read_excel(metadata_path, sheet_name="MagicLeapFiles")

    clean_and_process_files(
        root_directory=str(root_directory),
        magic_leap_data=magic_leap_data,
        save_large_files=args.save_large_files,
        max_memory_mb=args.max_memory_mb,
    )

if __name__ == "__main__":
    cli()

## Usage within sh script- if you wanted to up the max memory mb to 600 and not save large files: 
# CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline"
# TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
# DATA_DIR="FreshStart"
# python "${CODE_DIR}/preprocRaw/preprocRaw_AN.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --max-memory-mb 600 \
#   --no-save-large-files


