
import os
import pandas as pd
from io import StringIO
from metadata_and_manifest_utils import attach_metadata_to_events, record_to_manifest, save_manifest
from warning_logger import WarningLogger
from resolve_coin_ids_from_positions import resolve_coin_ids_from_positions
from dataConfigs_3Coins import CoinSet
from eventCascades_AN_cleaned_v2 import (
    process_pin_drop,
    process_feedback_collect,
    #process_ie_events,
    process_chest_opened,
    process_IE_coin_collected,
    process_marks,
    process_swap_votes,
    process_block_periods,
    extract_walking_periods
)

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

def process_all_obsreward_files_with_manifest(root_dir, metadata_df, allowed_statuses):
    import re
    pattern = re.compile(r"ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_processed\.csv$")
    # summary_rows = []
    manifest_records = []
    summary_dir = os.path.join(root_dir, "Summary")
    os.makedirs(summary_dir, exist_ok=True)
    logger = WarningLogger(output_dir=summary_dir)
    output_root_dir = os.path.join(root_dir, 'ExtractedEvents')
    input_root_dir = os.path.join(root_dir, 'ProcessedData')
    for dirpath, _, filenames in os.walk(input_root_dir):
        for fname in filenames:
            if pattern.match(fname):
                file_path = os.path.join(dirpath, fname)
                relative_path = os.path.relpath(dirpath, input_root_dir)
                output_dir = os.path.join(output_root_dir, relative_path)
                os.makedirs(output_dir, exist_ok=True)

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
                    all_events = resolve_coin_ids_from_positions(all_events, CoinSet)

                    source_file = fname
                    processed_path = file_path
                    events_csv_path = os.path.join(output_dir, f"{source_file}_events.csv")
                    events_json_path = os.path.join(output_dir, f"{source_file}_events.json")

                    matched_meta = metadata_df[metadata_df["source_file"] == source_file]
                    if not matched_meta.empty:
                        meta_row = matched_meta.iloc[0].to_dict()
                        enriched_events = attach_metadata_to_events(all_events, meta_row, source_file, relative_path)
                        # summary_rows.extend(enriched_events)
                    else:
                        logger.log(f"No metadata found for {source_file} in {relative_path}")
                        enriched_events = all_events
                        # summary_rows.extend(all_events)
                    enriched_events = pd.DataFrame(enriched_events).sort_values(by=["AppTime", "Timestamp"])
                    # Save events
                    pd.DataFrame(enriched_events).to_csv(events_csv_path, index=False)
                    pd.DataFrame(enriched_events).to_json(events_json_path, orient='records', lines=True)

                    # Record manifest entry
                    manifest_records.append(
                        record_to_manifest(meta_row if not matched_meta.empty else {}, source_file, relative_path,
                                           processed_path, events_csv_path, events_json_path)
                    )

                    print(f"✓ Processed: {fname}")

                except Exception as e:
                    logger.log(f"✗ Failed to process {fname} — Error: {e}")
                    print(f"✗ Failed: {fname} with error {e}")

    save_manifest(manifest_records, output_root_dir)
    logger.save()

# --- Constants ---

allowed_statuses = {"complete", "truncated"}

collated_path = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
MAGIC_LEAP_METADATA = pd.read_excel(collated_path, sheet_name="MagicLeapFiles")
MAGIC_LEAP_METADATA = MAGIC_LEAP_METADATA.dropna(subset=["cleanedFile"])
MAGIC_LEAP_METADATA = MAGIC_LEAP_METADATA[MAGIC_LEAP_METADATA["currentRole"] == "AN"]
MAGIC_LEAP_METADATA = MAGIC_LEAP_METADATA.rename(columns={"cleanedFile": "source_file"})

root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile"
process_all_obsreward_files_with_manifest(root_directory, MAGIC_LEAP_METADATA, allowed_statuses)
