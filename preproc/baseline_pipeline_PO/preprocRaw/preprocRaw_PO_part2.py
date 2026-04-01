import os
import pandas as pd
import numpy as np
import re
import argparse
from pathlib import Path

# preprocRawHelpers functions
from RC_utilities.preprocHelpers.preprocRawHelpers import (
    detect_and_tag_blocks,
    forward_fill_block_info,
    detect_block_completeness,
    fix_collecting_block_coinsetids,
    robust_parse_timestamp,
    final_column_order,
    enhance_timestamp_with_apptime,
    process_obsreward_file_PO_pt2,
    check_monotonic_apptime,
    drop_malformed_trailing_rows,
)
## warning_logger
from RC_utilities.segHelpers.warning_logger import WarningLogger

COLLECT_REWARD_PATTERN = re.compile(
    r"^A\.N\. collected coin:.*round reward:\s*[-+]?\d*\.?\d+",
    re.IGNORECASE,
)

READY_MESSAGE = "Repositioned and ready to start block or round"

def assign_temporal_intervals_PO(data):
    data = data.copy()
    data["RoundNum"] = np.nan

    block_starts = data.index[
        data["Message"] == "Mark should happen if checked on terminal."
    ].tolist()

    for block_start in block_starts:
        round_num = 0
        data.at[block_start, "RoundNum"] = 0

        current_block = data.at[block_start, "BlockInstance"]
        block_mask = (data.index > block_start) & (data["BlockInstance"] == current_block)
        block_indices = data.index[block_mask].tolist()

        phase = "waiting_for_round1"
        awaiting_post_reposition_round_start = False

        for idx in block_indices:
            message = data.at[idx, "Message"]

            if not isinstance(message, str):
                continue

            if (
                message == "Finished watching other participant's pindropping."
                or message == "Finished watching other participant's collection."
            ):
                data.at[idx, "RoundNum"] = 9999
                break

            if "Repositioned and ready to start block or round" in message:
                data.at[idx, "RoundNum"] = 8888

                if phase == "rounds":
                    awaiting_post_reposition_round_start = True
                else:
                    phase = "ready_for_round1"

                continue

            if (
                phase in {"waiting_for_round1", "ready_for_round1"}
                and message.startswith("Started watching other participant")
            ):
                round_num += 1
                data.at[idx, "RoundNum"] = round_num
                phase = "rounds"
                awaiting_post_reposition_round_start = False
                continue

            if (
                phase == "rounds"
                and awaiting_post_reposition_round_start
                and message.startswith("Other participant just dropped a new pin at ")
            ):
                round_num += 1
                data.at[idx, "RoundNum"] = round_num
                awaiting_post_reposition_round_start = False
                continue

    return data


def augment_with_chestpin_and_totalrounds_PO(data):
    data["chestPin_num"] = np.nan
    data["totalRounds"] = np.nan
    first_valid_block_idx = data["BlockNum"].first_valid_index()
    if first_valid_block_idx is None:
        return data

    working_data = data.loc[first_valid_block_idx:].copy()
    working_data["RoundNum_filled"] = working_data["RoundNum"].ffill()
    working_data["BlockNum_filled"] = working_data["BlockNum"].ffill()
    valid_rounds = working_data[~working_data["RoundNum_filled"].isin([0, 7777, 8888, 9999])]
    round_counts = valid_rounds.groupby("BlockNum_filled")["RoundNum_filled"].nunique()
    working_data["totalRounds"] = working_data["BlockNum_filled"].map(round_counts)

    previous_block = None
    previous_round = None
    chest_pin_count = 0

    for idx, row in working_data.iterrows():
        message = row.get("Message", "")
        block = row["BlockNum"]
        round_ = row["RoundNum"]

        # Reset if new block or new round
        if block != previous_block or round_ != previous_round:
            chest_pin_count = 0

        if isinstance(message, str) and (
            message.startswith("Other participant just collected coin: ") or
            message.startswith("Other participant just dropped a new pin at ")
        ):
            chest_pin_count += 1

        working_data.at[idx, "chestPin_num"] = chest_pin_count
        previous_block = block
        previous_round = round_

    data.loc[working_data.index, ["chestPin_num", "totalRounds"]] = working_data[["chestPin_num", "totalRounds"]]
    return data


def forward_fill_rounds_within_block_PO(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()

    if "BlockInstance" not in data.columns:
        raise KeyError("Missing required column: BlockInstance")
    if "RoundNum" not in data.columns:
        raise KeyError("Missing required column: RoundNum")

    def _fill_group(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_index().copy()
        group["RoundNum"] = group["RoundNum"].ffill()
        return group

    data = (
        data.groupby("BlockInstance", group_keys=False, dropna=False)
        .apply(_fill_group)
        .reset_index(drop=True)
    )
    return data


def assign_7777_intervals_PO(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()

    required_cols = {"BlockInstance", "Message", "RoundNum"}
    missing = required_cols.difference(data.columns)
    if missing:
        raise KeyError(f"Missing required columns: {sorted(missing)}")

    for block_instance, block_df in data.groupby("BlockInstance", dropna=False, sort=False):
        block_df = block_df.sort_index()

        true_rounds = block_df.loc[
            block_df["RoundNum"].notna() & block_df["RoundNum"].between(1, 99),
            "RoundNum",
        ].nunique()

        if true_rounds <= 1:
            continue

        block_indices = block_df.index.tolist()

        for reposition_idx in block_indices:
            message = data.at[reposition_idx, "Message"]
            if not isinstance(message, str) or READY_MESSAGE not in message:
                continue

            prior_indices = [idx for idx in block_indices if idx < reposition_idx]
            if not prior_indices:
                continue

            collect_idx = None
            for idx in reversed(prior_indices):
                prior_message = data.at[idx, "Message"]
                if isinstance(prior_message, str) and COLLECT_REWARD_PATTERN.match(prior_message):
                    collect_idx = idx
                    break

            if collect_idx is None:
                continue

            interval_mask = (data.index > collect_idx) & (data.index < reposition_idx)
            same_block_mask = data["BlockInstance"] == block_instance
            protected_mask = data["RoundNum"].isin([0, 8888, 9999])

            data.loc[interval_mask & same_block_mask & ~protected_mask, "RoundNum"] = 7777

    return data
def clean_and_process_files(root_directory, magic_leap_data, save_large_files=True, max_memory_mb=500):
    pattern = re.compile(r"^ObsReward_B_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}_prelim_processed.csv$")
    print('Started PO-specific processing!')
    baseDir = os.path.join(root_directory, "Prelim_ProcessedData")
    output_dir = os.path.join(root_directory, "ProcessedData")
    flatPath = os.path.join(root_directory, "ProcessedData_Flat")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(flatPath, exist_ok=True)
    loggerDir = os.path.join(output_dir, "PreProcLogging_PO")
    os.makedirs(loggerDir, exist_ok=True)
    logger = WarningLogger(output_dir=loggerDir)
    for dirpath, _, filenames in os.walk(baseDir):
        for filename in filenames:

            if pattern.match(filename):  
                print(f'Match found: {filename}')
                file_path = os.path.join(dirpath, filename)
                session_date_match = re.match(r"ObsReward_B_(\d{2}_\d{2}_\d{4})_\d{2}_\d{2}\_prelim_processed.csv", filename)
                session_date = session_date_match.group(1) if session_date_match else None


                relative_path = os.path.relpath(dirpath, baseDir)
                nestedPath = os.path.join(output_dir, relative_path)
                os.makedirs(nestedPath, exist_ok=True)

                nestedFile = os.path.join(nestedPath, f"{filename.replace('_prelim_processed.csv', '_processed.csv')}")
                flatFile = os.path.join(flatPath, f"{filename.replace('_prelim_processed.csv', '_processed.csv')}")
                print(f"Processing file: {file_path}")
                logger.log(f"Processing file: {file_path}")

                data = pd.read_csv(file_path)
                # ✅ Continue processing
                data = process_obsreward_file_PO_pt2(data, role = 'PO')
                data = assign_temporal_intervals_PO(data)
                data = forward_fill_rounds_within_block_PO(data)
                data = assign_7777_intervals_PO(data)
                data = augment_with_chestpin_and_totalrounds_PO(data)

                # Reorder columns
                data = data[final_column_order]

                data.to_csv(nestedFile, index=False)
                data.to_csv(flatFile, index=False)
                print(f"Processed and saved: {nestedFile}")
                logger.log(f"Processed and saved: {nestedFile}")
                logger.save()

########### Execution Block #############
# Execution block

# trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
# #procDir = 'SmallSelectedData/idealTestFile3'
# procDir = 'FreshStart'
# root_directory = os.path.join(trueRootDir, procDir)


# metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
# magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")
# clean_and_process_files(root_directory,  magic_leap_data, save_large_files=True, max_memory_mb=500)

# if __name__ == "__main__":
#     trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
#     #procDir = 'SmallSelectedData/idealTestFile3'
#     procDir = 'FresherStart'
#     root_directory = os.path.join(trueRootDir, procDir)


#     metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
#     magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")
#     clean_and_process_files(root_directory,  magic_leap_data, save_large_files=True, max_memory_mb=500)


def cli() -> None:
    parser = argparse.ArgumentParser(
        prog="preprocRaw_PO",
        description="Basic preprocessing of Magic Leap data for PO participants."
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
