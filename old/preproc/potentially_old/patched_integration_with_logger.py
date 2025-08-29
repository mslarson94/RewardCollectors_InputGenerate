
import os
import pandas as pd
from io import StringIO
from metadata_and_manifest_utils import attach_metadata_to_events, record_to_manifest, save_manifest
from warning_logger import WarningLogger
from eventCascades_AN_cleaned_v2 import (
    process_pin_drop,
    process_feedback_collect,
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
    start_index = next((i for i, line in enumerate(lines) if "Mark should happen" in line), 1)
    filtered_lines = [header] + lines[start_index:]
    df = pd.read_csv(StringIO("".join(filtered_lines)))
    df["original_index"] = list(range(start_index + 1, start_index + 1 + len(df)))
    return df

def collect_file_paths(input_root_dir, allowed_dirs=None):
    all_paths = []
    for dirpath, _, filenames in os.walk(input_root_dir):
        if allowed_dirs and not any(os.path.commonpath([dirpath, d]) == d for d in allowed_dirs):
            continue
        for fname in filenames:
            if fname.endswith("_processed.csv") and "ObsReward_A_" in fname:
                all_paths.append((dirpath, fname))
    return all_paths

def process_all_obsreward_files_with_manifest(root_dir, metadata_df, allowed_statuses, subset_dirs=None):
    manifest_records = []
    summary_dir = os.path.join(root_dir, "Summary")
    os.makedirs(summary_dir, exist_ok=True)
    logger = WarningLogger(output_dir=summary_dir)
    output_root_dir = os.path.join(root_dir, 'ExtractedEvents')
    input_root_dir = os.path.join(root_dir, 'ProcessedData')
    subset_dirs = [os.path.abspath(d) for d in subset_dirs] if subset_dirs else None

    file_paths = collect_file_paths(input_root_dir, subset_dirs)

    for dirpath, fname in file_paths:
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

            #source_file = fname
            source_file = Path(fname).with_suffix('')
            processed_path = file_path
            events_csv_path = os.path.join(output_dir, f"{source_file}_events.csv")
            events_json_path = os.path.join(output_dir, f"{source_file}_events.json")

            matched_meta = metadata_df[metadata_df["source_file"] == source_file]
            if not matched_meta.empty:
                meta_row = matched_meta.iloc[0].to_dict()
                enriched_events = attach_metadata_to_events(all_events, meta_row, source_file, relative_path)
            else:
                logger.log(f"No metadata found for {source_file} in {relative_path}")
                enriched_events = all_events

            enriched_events = pd.DataFrame(enriched_events).sort_values(by=["AppTime", "Timestamp"])
            enriched_events.to_csv(events_csv_path, index=False)
            enriched_events.to_json(events_json_path, orient='records', lines=True)

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
