# preFrontalCortex_unifiedEventSeg.py

'''
preFrontalCortex_unifiedEventSeg.py
Author: Myra Sarai Larson   08/18/2025

   This high level script is designed to be the brains of the operation - orchestrating all the little 
   helper & parser scripts - (much like how the PreFrontal Cortex is commonly associated with Executive Functioning 
   i.e. planning/decision making/top-down control)

   This script should be limited to weaving everything together related to our event segmentation pipeline - completely 
   agnostic of participant roles. At the beginning of this script, we are feeding in _processed files in heavily nested directories and
   by the end of the script we should have our _events & _events_orig files saved as well as _meta.json files also saved in those 
   nested parent directories or in flattened directories.

'''
import os
import re
import sys
import pandas as pd
from io import StringIO
from pathlib import Path
import traceback
import json

from warning_logger import WarningLogger
from extraMetaExtract import generate_meta_json
from eventCascade_VariablePathHelpers import eventParserFolderCreatePart1, nestedEventParserDirs
from metadata_and_manifest_utils import (attach_metadata_to_events, record_to_manifest, 
                                         save_manifest, pullMetaData, load_filtered_df, 
                                         get_metadata_row_for_file)

from glia_eventsParserHelper_AN import buildGliaEvents_AN
from muscles_eventParser_AN import buildEvents_AN_v4

from glia_eventsParserHelper_PO import buildGliaEvents_PO_v2
from muscles_eventParser_PO import buildEvents_PO

from bareBonesEvents import buildBareBonesEvents


# --- Processing All Data with Entire Directory or Specified Directories ---
def process_all_obsreward_files(dataDir, metadata, role, segmentType, subDirs=None, allowed_statuses=["complete", "truncated"]):

    try:
        print('Processing started...')
        if role == 'AN':
            pattern = re.compile(r"ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_processed\.csv$")
        elif role == 'PO' and segmentType=='glia':
            pattern = re.compile(r"ObsReward_B_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_processed_orig\.csv$")
        elif role == 'PO' and segmentType!='glia':
            pattern = re.compile(r"ObsReward_B_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_processed\.csv$")
        else: 
            raise Exception("The parameter 'role' can only take the values 'AN' or 'PO' - please check your inputs!")

        segmentType = segmentType.lower()
        if segmentType not in ['barebones', 'full', 'glia']:
            raise Exception("The parameter 'segmentType' can only take the values 'barebones' or 'full' - please check your inputs!")

        # Initialize pipeline variables
        manifest_records = []
        skipped_trash_files = []
        unrecognized_files = []
        uncorrectedDirs = []
        folders_to_process = []


        if role == "PO" and segmentType=='glia':
            input_dataDir = os.path.join(dataDir, 'ProcessedData_PO_orig')
        else:
            input_dataDir = os.path.join(dataDir, 'ProcessedData')
        nonNestedOutDirs = eventParserFolderCreatePart1(dataDir, segmentType)
        print('if needed, non-nested out directories have been created')

        logger = WarningLogger(output_dir=nonNestedOutDirs["logging_dir"])

        full_metadata_df, metadata_df, all_known_files, valid_files = pullMetaData(metadata)

        # Adjust for PO + glia case
        if role == "PO" and segmentType == "glia":
            # Normalize metadata expectation to match on-disk files that lack "_orig"
            all_known_files = {fname.replace("_orig", "") for fname in all_known_files}
            valid_files = {fname.replace("_orig", "") for fname in valid_files}


        base_dirs = [os.path.join(input_dataDir, subdir) for subdir in subDirs] if subDirs else [
            os.path.join(input_dataDir, d) for d in os.listdir(input_dataDir)
            if os.path.isdir(os.path.join(input_dataDir, d))
        ]

        for base_dir in base_dirs:
            for dirpath, _, filenames in os.walk(base_dir):

                parts = Path(dirpath).parts
                if "Uncorrected" in parts or "UncorrectedTrueRaw" in parts:
                    print(f"Skipping Uncorrected: {dirpath}")
                    uncorrectedDirs.append(dirpath)
                    continue  # 🚫 skip these directories entirely
                if 'MagicLeaps' in parts and any(pattern.match(f) for f in filenames):
                    folders_to_process.append(dirpath)
        
        print(f"🗂 Will process {len(folders_to_process)} folders:")
        logger.log(f"🗂 Will process {len(folders_to_process)} folders:")
        
        for path in folders_to_process:
            print(f"   → {path}")
            logger.log(f"   → {path}")

        print('--------')
        logger.log('--------')

        for folder in folders_to_process:
            for fname in os.listdir(folder):
                if not pattern.match(fname):
                    continue

                source_file = fname.strip().lower()
                                # Normalize to match metadata naming
                if role == "PO" and segmentType == "glia":
                    source_file = source_file.replace("_orig", "")

                file_path = os.path.join(folder, fname)
                relative_path = os.path.relpath(folder, input_dataDir)
                output_dir = os.path.join(nonNestedOutDirs["output_dataDir"], relative_path)
                out_source_file = fname.replace(".csv", "")

                nestedOutDirs = nestedEventParserDirs(dataDir, output_dir, out_source_file, segmentType)

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

                    if role == 'PO' and segmentType == "glia":
                        timestamp_col = "mLTimestamp_orig"
                    else: 
                        timestamp_col = "mLTimestamp"
                    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")

                    os.makedirs(output_dir, exist_ok=True)

                    if segmentType == 'full':
                        if role == 'AN':
                            muscles = buildEvents_AN_v4(df, allowed_statuses)
                            glia =  buildGliaEvents_AN(df=df, allowed_statuses=allowed_statuses, role=role)
                            all_events = muscles + glia 
                            meta_json = generate_meta_json(df, full_metadata_df, source_file, role)
                        elif role == 'PO':
                            glia =  buildGliaEvents_PO_v2(df=df, allowed_statuses=allowed_statuses, role=role, segmentType=segmentType)
                            all_events = buildEvents_PO(df, allowed_statuses, segmentType=segmentType)
                            meta_json = generate_meta_json(df, full_metadata_df, source_file, role)
                    elif segmentType == 'glia':
                        if role == 'AN': 
                            all_events = buildGliaEvents_AN(df=df, allowed_statuses=allowed_statuses, role=role)
                            meta_json = generate_meta_json(df, full_metadata_df, source_file, role)
                        elif role == 'PO':
                            all_events = buildGliaEvents_PO_v2(df=df, allowed_statuses=allowed_statuses, role=role, segmentType=segmentType)
                            meta_json = generate_meta_json(df, full_metadata_df, source_file, role)
                    else: 
                        all_events = buildBareBonesEvents(df, role)
                        meta_json = None


                    matched_meta = metadata_df[metadata_df["cleanedFile"] == source_file]
                    if not matched_meta.empty:
                        meta_row = matched_meta.iloc[0].to_dict()
                        enriched_events = attach_metadata_to_events(all_events, meta_row, fname, relative_path)
                    else:
                        logger.log(f"❓ Unexpected metadata miss for {source_file} after validation.")
                        enriched_events = all_events

                    enriched_events = pd.DataFrame(enriched_events).sort_values(by=[timestamp_col])

                    enriched_events.to_csv(nestedOutDirs["events_csv_path"], index=False)
                    enriched_events.to_json(nestedOutDirs["events_json_path"], orient='records', lines=True)

                    enriched_events.to_csv(nestedOutDirs["eventsFlat_csv_path"], index=False)
                    enriched_events.to_json(nestedOutDirs["eventsFlat_json_path"], orient='records', lines=True)

                    if segmentType == 'full':
                        metaData_json_path = nestedOutDirs["metaData_json_path"]
                        with open(nestedOutDirs["metaData_json_path"], "w") as f:
                            json.dump(meta_json, f, indent=2)
                        with open(nestedOutDirs["metaData_json_path"], "w") as f2:
                            json.dump(meta_json, f2, indent=2)
                        print(f"🧩 Meta JSON saved to: {metaData_json_path}")

                    manifest_records.append(
                        record_to_manifest(meta_row if not matched_meta.empty else {}, fname, relative_path,
                                           file_path, nestedOutDirs["events_csv_path"], nestedOutDirs["events_json_path"])
                    )


                    print(f"✓ Processed: {fname}")
                except Exception as e:
                    logger.log(f"🚫 Failed to process {fname} — Error: {e}")
                    print(f"🚫 Failed: {fname} with error {e}")

        if manifest_records:
            #os.makedirs(nonNestedOutDirs.output_dataDir, exist_ok=True)
            save_manifest(manifest_records, nonNestedOutDirs["output_dataDir"])
        else:
            print("⚠️ No valid files processed — skipping manifest save.")
        logger.save()

        # # Final reporting
        # print("\nProcessing Complete")

        if skipped_trash_files:
            trash_df = pd.DataFrame(skipped_trash_files, columns=["file", "relative_path"])
            trash_csv = os.path.join(nonNestedOutDirs["logging_dir"], "skipped_trash_files.csv")
            trash_df.to_csv(trash_csv, index=False)
            print(f"\n🚮 Skipped {len(skipped_trash_files)} known trash files. Saved list to {trash_csv}")
            for fname, rpath in skipped_trash_files:
                logger.log(f"Skipped trash file: {fname} in {rpath}")

        if uncorrectedDirs:
            uncorrectedDirs_df = pd.DataFrame(uncorrectedDirs, columns=["path"])
            uncorrectedDirs_csv = os.path.join(nonNestedOutDirs["logging_dir"], "uncorrectedDirs.csv")
            uncorrectedDirs_df.to_csv(uncorrectedDirs_csv, index=False)
            print(f"\n🚮 Skipped {len(uncorrectedDirs)} uncorrected data folders. Saved list to {uncorrectedDirs_csv}")
            for rpath in uncorrectedDirs:
                logger.log(f"Skipped {rpath}")

        if unrecognized_files:
            unknown_df = pd.DataFrame(unrecognized_files, columns=["file", "relative_path"])
            unknown_csv = os.path.join(nonNestedOutDirs["logging_dir"], "unrecognized_files.csv")
            unknown_df.to_csv(unknown_csv, index=False)
            print(f"\n⚠️ Found {len(unrecognized_files)} unrecognized files. Saved list to {unknown_csv}")
            for fname, rpath in unrecognized_files:
                logger.log(f"Unrecognized file: {fname} in {rpath}")
    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise


# --- For Running This as a Script By Itself (and debugging) ---
def main():
    allowed_statuses = ["complete", "truncated"]
    trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
    metaDataFile = os.path.join(trueRootDir, 'collatedData.xlsx')

    procDir = 'FreshStart_mini'
    dataDir = os.path.join(trueRootDir, procDir)

    process_all_obsreward_files(dataDir, metaDataFile, role='AN', segmentType='glia')
    process_all_obsreward_files(dataDir, metaDataFile, role='PO', segmentType='glia')

    #process_all_obsreward_files(dataDir, metaDataFile, role='AN', segmentType='full')
    #process_all_obsreward_files(dataDir, metaDataFile, role='PO', segmentType='full')

if __name__ == "__main__":
    main()
