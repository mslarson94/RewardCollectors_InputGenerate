
import os
import re
import pandas as pd
from io import StringIO
from pathlib import Path
import traceback
import json

from metadata_and_manifest_utils import attach_metadata_to_events, record_to_manifest, save_manifest, pullMetaData, load_filtered_df
#from coin_utils import classify_coin_type, classify_swap_vote, process_marks
from warning_logger import WarningLogger
#from eventParser_AN_patch_hiMeta_new import buildEvents_AN_v4
from eventParser_AN_patch_hiMeta_new_fixed import buildEvents_AN_v4
from eventParser_PO import buildEvents_PO
from extraMetaExtract import generate_meta_json

# # Inside the file processing loop, after df is loaded and before final save
# meta_json = generate_meta_json(df, full_metadata_df, source_file)
# meta_output_path = os.path.join(output_dir, f"{out_source_file}_meta.json")
# with open(meta_output_path, "w") as f:
#     json.dump(meta_json, f, indent=2)
# print(f"🧩 Meta JSON saved to: {meta_output_path}")

# --- Processing All Data with Entire Directory or Specified Directories ---
def process_all_obsreward_files_AN(dataDir, metadata, role, subDirs=None, allowed_statuses=["complete", "truncated"]):

    try:
    # Your existing event processing pipeline

        print('Processing started...')
        if role == 'AN':
            pattern = re.compile(r"ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_processed\.csv$")
        elif role == 'PO':
            pattern = re.compile(r"ObsReward_B_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_processed\.csv$")
        else: 
            raise Exception("The parameter 'role' can only take the values 'AN' or 'PO' - please check your inputs!")
        manifest_records = []
        skipped_trash_files = []
        unrecognized_files = []
        uncorrectedDirs = []

        summary_dir = os.path.join(dataDir, "Summary")
        os.makedirs(summary_dir, exist_ok=True)
        logger = WarningLogger(output_dir=summary_dir)
        output_dataDir = os.path.join(dataDir, 'ExtractedEvents')
        input_dataDir = os.path.join(dataDir, 'ProcessedData')
        flat_outputEvents_csv = os.path.join(dataDir, 'ExtractedEvents_csv_Flat')
        flat_outputEvents_json = os.path.join(dataDir, 'ExtractedEvents_json_Flat')
        flat_outputMetaData = os.path.join(dataDir, 'MetaData_Flat')
        os.makedirs(flat_outputEvents_csv, exist_ok=True)
        os.makedirs(flat_outputEvents_json, exist_ok=True)
        os.makedirs(flat_outputEvents_json, exist_ok=True)
        
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
                    if role == 'AN':
                        df = load_filtered_df(file_path)
                        os.makedirs(output_dir, exist_ok=True)
                        all_events = buildEvents_AN_v4(df, allowed_statuses)
                        meta_json = generate_meta_json(df, full_metadata_df, source_file)
                    elif role == 'PO':
                        df = load_filtered_df(file_path)
                        os.makedirs(output_dir, exist_ok=True)
                        all_events = buildEvents_PO(df, allowed_statuses)

                    out_source_file = fname.replace(".csv", "")
                    events_csv_path = os.path.join(output_dir, f"{out_source_file}_events.csv")
                    events_json_path = os.path.join(output_dir, f"{out_source_file}_events.json")
                    metaData_json_path = os.path.join(output_dir, f"{out_source_file}_meta.json")
                    eventsFlat_csv_path = os.path.join(flat_outputEvents_csv, f"{out_source_file}_events.csv")
                    eventsFlat_json_path = os.path.join(flat_outputEvents_json, f"{out_source_file}_events.json")
                    metaDataFlat_json_path = os.path.join(flat_outputMetaData, f"{out_source_file}_meta.json")

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
                    enriched_events.to_csv(eventsFlat_csv_path, index=False)
                    enriched_events.to_json(eventsFlat_json_path, orient='records', lines=True)

                    with open(metaData_json_path, "w") as f:
                        json.dump(meta_json, f, indent=2)
                    with open(metaDataFlat_json_path, "w") as f2:
                        json.dump(meta_json, f2, indent=2)
                    print(f"🧩 Meta JSON saved to: {metaData_json_path}")

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
    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise

# --- Processing Specific Files ---
def process_all_obsreward_files_AN(dataDir, metadata, role, subDirs=None, allowed_statuses=["complete", "truncated"], flat_output_dir=""):

    try:
        print('Processing started...')
        if role == 'AN':
            pattern = re.compile(r"ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_processed\.csv$")
        elif role == 'PO':
            pattern = re.compile(r"ObsReward_B_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_processed\.csv$")
        else: 
            raise Exception("The parameter 'role' must be 'AN' or 'PO'.")

        manifest_records = []
        skipped_trash_files = []
        unrecognized_files = []
        uncorrectedDirs = []

        summary_dir = os.path.join(dataDir, "Summary")
        os.makedirs(summary_dir, exist_ok=True)
        logger = WarningLogger(output_dir=summary_dir)
        output_dataDir = os.path.join(dataDir, 'ExtractedEvents')
        input_dataDir = os.path.join(dataDir, 'ProcessedData')

        if flat_output_dir:
            flat_outputEvents_csv = os.path.join(flat_output_dir, 'ExtractedEvents_csv_Flat')
            flat_outputEvents_json = os.path.join(flat_output_dir, 'ExtractedEvents_json_Flat')
            flat_outputMetaData = os.path.join(flat_output_dir, 'MetaData_Flat')
            os.makedirs(flat_outputEvents_csv, exist_ok=True)
            os.makedirs(flat_outputEvents_json, exist_ok=True)
            os.makedirs(flat_outputMetaData, exist_ok=True)

        full_metadata_df, metadata_df, all_known_files, valid_files = pullMetaData(metadata)

        base_dirs = [os.path.join(input_dataDir, subdir) for subdir in subDirs] if subDirs else [
            os.path.join(input_dataDir, d) for d in os.listdir(input_dataDir)
            if os.path.isdir(os.path.join(input_dataDir, d))
        ]
        folders_to_process = []
        for base_dir in base_dirs:
            for dirpath, _, filenames in os.walk(base_dir):
                parts = Path(dirpath).parts
                if "Uncorrected" in parts or "UncorrectedTrueRaw" in parts:
                    print(f"Skipping Uncorrected: {dirpath}")
                    uncorrectedDirs.append(dirpath)
                    continue
                if 'MagicLeaps' in parts and any(pattern.match(f) for f in filenames):
                    folders_to_process.append(dirpath)

        for folder in folders_to_process:
            for fname in os.listdir(folder):
                if not pattern.match(fname):
                    continue

                source_file = fname.strip().lower()
                file_path = os.path.join(folder, fname)
                relative_path = os.path.relpath(folder, input_dataDir)
                output_dir = os.path.join(output_dataDir, relative_path)

                if source_file not in all_known_files:
                    logger.log(f"⚠️ Unrecognized file: {source_file} in {relative_path}")
                    unrecognized_files.append((source_file, relative_path))
                    continue

                if source_file not in valid_files:
                    print(f"🗑️ Skipping known trash file: {source_file}")
                    skipped_trash_files.append((source_file, relative_path))
                    continue

                try:
                    df = load_filtered_df(file_path)
                    os.makedirs(output_dir, exist_ok=True)

                    if role == 'AN':
                        all_events = buildEvents_AN_v4(df, allowed_statuses)
                        meta_json = generate_meta_json(df, full_metadata_df, source_file)
                    else:
                        all_events = buildEvents_PO(df, allowed_statuses)
                        meta_json = {}

                    out_source_file = fname.replace(".csv", "")
                    events_csv_path = os.path.join(output_dir, f"{out_source_file}_events.csv")
                    events_json_path = os.path.join(output_dir, f"{out_source_file}_events.json")
                    metaData_json_path = os.path.join(output_dir, f"{out_source_file}_meta.json")

                    if flat_output_dir:
                        eventsFlat_csv_path = os.path.join(flat_outputEvents_csv, f"{out_source_file}_events.csv")
                        eventsFlat_json_path = os.path.join(flat_outputEvents_json, f"{out_source_file}_events.json")
                        metaDataFlat_json_path = os.path.join(flat_outputMetaData, f"{out_source_file}_meta.json")

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

                    with open(metaData_json_path, "w") as f:
                        json.dump(meta_json, f, indent=2)

                    if flat_output_dir:
                        enriched_events.to_csv(eventsFlat_csv_path, index=False)
                        enriched_events.to_json(eventsFlat_json_path, orient='records', lines=True)
                        with open(metaDataFlat_json_path, "w") as f2:
                            json.dump(meta_json, f2, indent=2)

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
        logger.save()

        if skipped_trash_files:
            trash_df = pd.DataFrame(skipped_trash_files, columns=["file", "relative_path"])
            trash_df.to_csv(os.path.join(summary_dir, "skipped_trash_files.csv"), index=False)
        if uncorrectedDirs:
            uncorrectedDirs_df = pd.DataFrame(uncorrectedDirs, columns=["path"])
            uncorrectedDirs_df.to_csv(os.path.join(summary_dir, "uncorrectedDirs.csv"), index=False)
        if unrecognized_files:
            unknown_df = pd.DataFrame(unrecognized_files, columns=["file", "relative_path"])
            unknown_df.to_csv(os.path.join(summary_dir, "unrecognized_files.csv"), index=False)

    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise



# --- For Running This as a Script By Itself (and debugging) ---
def main():
    allowed_statuses = {"complete", "truncated"}
    trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
    #procDir = 'SmallSelectedData/threepairs'
    procDir = 'SmallSelectedData/idealTestFile2'
    #procDir = 'SmallSelectedData/idealTestFile_singlePinDrop'
    #procDir = 'SmallSelectedData/idealTestFile_multiPinDrop'
    #procDir = 'SmallSelectedData/idealTestFile_coinCollect'
    dataDir = os.path.join(trueRootDir, procDir)
    metaDataFile = os.path.join(trueRootDir, 'collatedData.xlsx')
    file_list = [
    "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/threepairs/ProcessedData/pair_006/02_08_2025/Morning/MagicLeaps/ML2A/ObsReward_A_02_08_2025_13_30_processed.csv",
    "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/threepairs/ProcessedData/pair_008/02_19_2025/Morning/MagicLeaps/ML2G/ObsReward_A_02_19_2025_18_14_processed.csv",
    "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/threepairs/ProcessedData/pair_008/02_19_2025/Morning/MagicLeaps/ML2G/ObsReward_A_02_19_2025_18_10_processed.csv",
    "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/threepairs/ProcessedData/pair_200/03_17_2025/Afternoon/MagicLeaps/ML2G/ObsReward_A_03_17_2025_14_16_processed.csv"
    ]
    #process_file_list(file_list, metaDataFile, dataDir, allowed_statuses=["complete", "truncated"])
    #process_all_obsreward_files_AN(dataDir, metaDataFile, role='AN', subDirs=['pair_008', 'pair_006'], allowed_statuses=["complete"])
    process_all_obsreward_files_AN(dataDir, metaDataFile, role='AN', allowed_statuses=["complete"], flat_output_dir=dataDir)

if __name__ == "__main__":
    main()
