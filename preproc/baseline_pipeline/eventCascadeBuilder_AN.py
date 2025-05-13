
import os
import re
import pandas as pd
from io import StringIO
from pathlib import Path

from metadata_and_manifest_utils import attach_metadata_to_events, record_to_manifest, save_manifest
from warning_logger import WarningLogger
from eventParser_AN import (
    process_pin_drop,
    process_feedback_collect,
    process_chest_opened,
    process_IE_coin_collected,
    process_marks,
    process_swap_votes,
    process_block_periods,
    extract_walking_periods
)

# --- Utility: Loading Data & MetaData ---
def load_filtered_df(file_path):
    with open(file_path, "r") as f:
        lines = f.readlines()

    header = lines[0]
    start_index = next(
        (i for i, line in enumerate(lines) if "Mark should happen" in line), 1
    )

    filtered_lines = [header] + lines[start_index:]
    df = pd.read_csv(StringIO("".join(filtered_lines)))
    df["original_index"] = list(range(start_index + 1, start_index + 1 + len(df)))

    return df

def pullMetaData(metadataFile):
    full_metadata_df = pd.read_excel(metadataFile, sheet_name="MagicLeapFiles")
    full_metadata_df = full_metadata_df.dropna(subset=["cleanedFile"])
    full_metadata_df["cleanedFile"] = full_metadata_df["cleanedFile"].str.strip().str.lower()

    all_known_files = set(full_metadata_df["cleanedFile"])

    metadata_df = full_metadata_df[
        (full_metadata_df["participantID"] != "none") &
        (full_metadata_df["pairID"] != "none")
    ]
    valid_files = set(metadata_df["cleanedFile"])
    print(f"📊 Loaded metadata: {len(full_metadata_df)} total entries")
    print(f"✅ Valid entries after filtering: {len(metadata_df)}")
    print(f"🚮 Known trash entries: {len(full_metadata_df) - len(metadata_df)}")

    return full_metadata_df, metadata_df, all_known_files, valid_files

# --- Processing All Data with Entire Directory or Specified Directories ---
def process_all_obsreward_files(dataDir, metadata, subDirs=None, allowed_statuses=["complete", "truncated"]):
    print('Processing started...')
    pattern = re.compile(r"ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_processed\.csv$")
    manifest_records = []
    skipped_trash_files = []
    unrecognized_files = []
    uncorrectedDirs = []

    summary_dir = os.path.join(dataDir, "Summary")
    os.makedirs(summary_dir, exist_ok=True)
    logger = WarningLogger(output_dir=summary_dir)
    output_dataDir = os.path.join(dataDir, 'ExtractedEvents')
    input_dataDir = os.path.join(dataDir, 'ProcessedData')
    full_metadata_df, metadata_df, all_known_files, valid_files = pullMetaData(metadata)

    folders_to_process = []
    base_dirs = [os.path.join(input_dataDir, subdir) for subdir in subDirs] if subDirs else [
        os.path.join(input_dataDir, d) for d in os.listdir(input_dataDir)
        if os.path.isdir(os.path.join(input_dataDir, d))
    ]
    #print(base_dirs)
    for base_dir in base_dirs:
        for dirpath, _, filenames in os.walk(base_dir):
            # print(f"🔍 Checking: {dirpath}")
            # print(f"     → parts: {Path(dirpath).parts}")
            # print(f"     → filenames: {filenames}")
            # for f in filenames:
            #     if pattern.match(f):
            #         print(f"✅ Matched pattern: {f}")

            parts = Path(dirpath).parts
            if "Uncorrected" in parts or "UncorrectedTrueRaw" in parts:
                print(f"Skipping Uncorrected: {dirpath}")
                uncorrectedDirs.append(dirpath)
                continue  # 🚫 skip these directories entirely
            if 'MagicLeaps' in parts and any(pattern.match(f) for f in filenames):
                folders_to_process.append(dirpath)
    print(f"🗂 Will process {len(folders_to_process)} folders:")
    for path in folders_to_process:
        print(f"   → {path}")

    print('--------')

    for folder in folders_to_process:
        for fname in os.listdir(folder):
            if not pattern.match(fname):
                continue

            source_file = fname.strip().lower()
            file_path = os.path.join(folder, fname)
            relative_path = os.path.relpath(folder, input_dataDir)
            output_dir = os.path.join(output_dataDir, relative_path)
            os.makedirs(output_dir, exist_ok=True)

            # # Skip unrecognized or trash files early
            # if "obsreward_a_02_17_2025_15_11_processed.csv" in valid_files:
            #     print("✅ Test file is in valid_files set")
            # else:
            #     print("❌ Test file is NOT in valid_files set")
            if source_file not in all_known_files:
                logger.log(f"⚠️ Unrecognized file: {source_file} in {relative_path} — not listed in metadata")
                unrecognized_files.append((source_file, relative_path))
                continue

            if source_file not in valid_files:
                print(f"🗑️ Skipping known trash file: {source_file}")
                skipped_trash_files.append((source_file, relative_path))
                continue

            try:
                df = load_filtered_df(file_path)

                chest_events = process_chest_opened(df, allowed_statuses)
                cascades = (
                    process_pin_drop(df, allowed_statuses) +
                    process_feedback_collect(df, allowed_statuses) +
                    chest_events +
                    process_IE_coin_collected(df, chest_events, allowed_statuses) +
                    process_marks(df, allowed_statuses) +
                    process_swap_votes(df, allowed_statuses) +
                    process_block_periods(df, allowed_statuses)
                )

                walking_periods = extract_walking_periods(df, cascades, allowed_statuses)
                all_events = cascades + walking_periods

                out_source_file = fname.replace(".csv", "")
                events_csv_path = os.path.join(output_dir, f"{out_source_file}_events.csv")
                events_json_path = os.path.join(output_dir, f"{out_source_file}_events.json")

                matched_meta = metadata_df[metadata_df["cleanedFile"] == source_file]
                if not matched_meta.empty:
                    meta_row = matched_meta.iloc[0].to_dict()
                    enriched_events = attach_metadata_to_events(all_events, meta_row, fname, relative_path)
                else:
                    logger.log(f"❓ Unexpected metadata miss for {source_file} after validation.")
                    enriched_events = all_events

                enriched_events = pd.DataFrame(enriched_events).sort_values(by=["AppTime", "Timestamp"])
                enriched_events.to_csv(events_csv_path, index=False)
                enriched_events.to_json(events_json_path, orient='records', lines=True)

                manifest_records.append(
                    record_to_manifest(meta_row if not matched_meta.empty else {}, fname, relative_path,
                                       file_path, events_csv_path, events_json_path)
                )

                print(f"✓ Processed: {fname}")
            except Exception as e:
                logger.log(f"🚫 Failed to process {fname} — Error: {e}")
                print(f"🚫 Failed: {fname} with error {e}")

    if manifest_records:
        os.makedirs(output_dataDir, exist_ok=True)
        save_manifest(manifest_records, output_dataDir)
    else:
        print("⚠️ No valid files processed — skipping manifest save.")
    logger.save()

    # # Final reporting
    # print("\nProcessing Complete")

    if skipped_trash_files:
        trash_df = pd.DataFrame(skipped_trash_files, columns=["file", "relative_path"])
        trash_csv = os.path.join(summary_dir, "skipped_trash_files.csv")
        trash_df.to_csv(trash_csv, index=False)
        print(f"\n🚮 Skipped {len(skipped_trash_files)} known trash files. Saved list to {trash_csv}")
        for fname, rpath in skipped_trash_files:
            logger.log(f"Skipped trash file: {fname} in {rpath}")

    if uncorrectedDirs:
        uncorrectedDirs_df = pd.DataFrame(uncorrectedDirs, columns=["path"])
        uncorrectedDirs_csv = os.path.join(summary_dir, "uncorrectedDirs.csv")
        uncorrectedDirs_df.to_csv(uncorrectedDirs_csv, index=False)
        print(f"\n🚮 Skipped {len(uncorrectedDirs)} uncorrected data folders. Saved list to {uncorrectedDirs_csv}")
        for rpath in uncorrectedDirs:
            logger.log(f"Skipped {rpath}")

    if unrecognized_files:
        unknown_df = pd.DataFrame(unrecognized_files, columns=["file", "relative_path"])
        unknown_csv = os.path.join(summary_dir, "unrecognized_files.csv")
        unknown_df.to_csv(unknown_csv, index=False)
        print(f"\n⚠️ Found {len(unrecognized_files)} unrecognized files. Saved list to {unknown_csv}")
        for fname, rpath in unrecognized_files:
            logger.log(f"Unrecognized file: {fname} in {rpath}")

# --- Processing Specific Files ---
def process_file_list(file_list, metadata, dataDir, allowed_statuses=["complete", "truncated"]):
    summary_dir = os.path.join(dataDir, "Summary")
    output_dataDir = os.path.join(dataDir, "ExtractedEvents")
    os.makedirs(output_dataDir, exist_ok=True)
    os.makedirs(summary_dir, exist_ok=True)
    logger = WarningLogger(output_dir=summary_dir)

    full_metadata_df, metadata_df, all_known_files, valid_files = pullMetaData(metadata)

    manifest_records = []
    skipped_trash_files = []
    unrecognized_files = []

    processed_data_dir = Path(dataDir, "ProcessedData").resolve()

    for file_path in file_list:
        try:
            file_path = Path(file_path).resolve()
            fname = file_path.name
            source_file = fname.strip().lower()

            file_dir = file_path.parent
            relative_path = os.path.relpath(file_dir, start=processed_data_dir)
            print(f"📁 Relative path: {relative_path}")
            output_dir = Path(output_dataDir, relative_path)
            output_dir.mkdir(parents=True, exist_ok=True)

            if source_file not in all_known_files:
                logger.log(f"⚠️ Unrecognized file: {source_file} in {relative_path} — not listed in metadata")
                unrecognized_files.append((source_file, relative_path))
                continue

            if source_file not in valid_files:
                print(f"🗑️ Skipping known trash file: {source_file}")
                skipped_trash_files.append((source_file, relative_path))
                continue

            df = load_filtered_df(str(file_path))

            chest_events = process_chest_opened(df, allowed_statuses)
            cascades = (
                process_pin_drop(df, allowed_statuses) +
                process_feedback_collect(df, allowed_statuses) +
                chest_events +
                process_IE_coin_collected(df, chest_events, allowed_statuses) +
                process_marks(df, allowed_statuses) +
                process_swap_votes(df, allowed_statuses) +
                process_block_periods(df, allowed_statuses)
            )

            walking_periods = extract_walking_periods(df, cascades, allowed_statuses)
            all_events = cascades + walking_periods

            out_source_file = fname.replace(".csv", "")
            events_csv_path = output_dir / f"{out_source_file}_events.csv"
            events_json_path = output_dir / f"{out_source_file}_events.json"

            matched_meta = metadata_df[metadata_df["cleanedFile"] == source_file]
            if not matched_meta.empty:
                meta_row = matched_meta.iloc[0].to_dict()
                enriched_events = attach_metadata_to_events(all_events, meta_row, fname, relative_path)
            else:
                logger.log(f"❓ Unexpected metadata miss for {source_file} after validation.")
                enriched_events = all_events

            enriched_events = pd.DataFrame(enriched_events).sort_values(by=["AppTime", "Timestamp"])
            enriched_events.to_csv(events_csv_path, index=False)
            enriched_events.to_json(events_json_path, orient='records', lines=True)

            print(f"📝 Writing to: {events_csv_path}")
            manifest_records.append(
                record_to_manifest(meta_row if not matched_meta.empty else {}, fname, relative_path,
                                   str(file_path), str(events_csv_path), str(events_json_path))
            )

            print(f"✓ Processed: {fname}")

        except Exception as e:
            logger.log(f"🚫 Failed to process {fname} — Error: {e}")
            print(f"🚫 Failed: {fname} with error {e}")

    if manifest_records:
        os.makedirs(output_dataDir, exist_ok=True)
        save_manifest(manifest_records, output_dataDir)
    else:
        print("⚠️ No valid files processed — skipping manifest save.")
    logger.save()

    if skipped_trash_files:
        trash_df = pd.DataFrame(skipped_trash_files, columns=["file", "relative_path"])
        trash_df.to_csv(os.path.join(summary_dir, "skipped_trash_files.csv"), index=False)

    if unrecognized_files:
        unknown_df = pd.DataFrame(unrecognized_files, columns=["file", "relative_path"])
        unknown_df.to_csv(os.path.join(summary_dir, "unrecognized_files.csv"), index=False)

    # print("\nProcessing Complete")


# --- For Running This as a Script By Itself (and debugging) ---
def main():
    allowed_statuses = {"complete", "truncated"}
    trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
    procDir = 'SmallSelectedData/threepairs'
    dataDir = os.path.join(trueRootDir, procDir)
    metaDataFile = os.path.join(trueRootDir, 'collatedData.xlsx')
    file_list = [
    "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/threepairs/ProcessedData/pair_006/02_08_2025/Morning/MagicLeaps/ML2A/ObsReward_A_02_08_2025_13_30_processed.csv",
    "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/threepairs/ProcessedData/pair_008/02_19_2025/Morning/MagicLeaps/ML2G/ObsReward_A_02_19_2025_18_14_processed.csv",
    "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/threepairs/ProcessedData/pair_008/02_19_2025/Morning/MagicLeaps/ML2G/ObsReward_A_02_19_2025_18_10_processed.csv",
    "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/threepairs/ProcessedData/pair_200/03_17_2025/Afternoon/MagicLeaps/ML2G/ObsReward_A_03_17_2025_14_16_processed.csv"
    ]
    process_file_list(file_list, metaDataFile, dataDir, allowed_statuses=["complete", "truncated"])
    process_all_obsreward_files(dataDir, metaDataFile, subDirs=['pair_008', 'pair_006'], allowed_statuses=["complete"])

if __name__ == "__main__":
    main()
