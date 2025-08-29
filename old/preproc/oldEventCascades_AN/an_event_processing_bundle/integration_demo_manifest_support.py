
import os
import pandas as pd
from metadata_and_manifest_utils import attach_metadata_to_events, record_to_manifest, save_manifest

def process_all_obsreward_files_with_manifest(root_dir, output_root_dir, metadata_df):
    pattern = re.compile(r"ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_processed\.csv$")
    summary_rows = []
    manifest_records = []

    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if pattern.match(fname):
                file_path = os.path.join(dirpath, fname)
                relative_path = os.path.relpath(dirpath, root_dir)
                output_dir = os.path.join(output_root_dir, relative_path)
                os.makedirs(output_dir, exist_ok=True)

                try:
                    df = pd.read_csv(file_path, skiprows=range(1, 7))
                    cascades = (
                        process_pin_drop(df) +
                        process_feedback_collect(df) +
                        process_ie_events(df) +
                        process_marks(df) +
                        process_swap_votes(df) +
                        process_block_periods(df)
                    )
                    walking_periods = extract_walking_periods(df, cascades)
                    all_events = cascades + walking_periods

                    source_file = fname
                    processed_path = file_path
                    events_csv_path = os.path.join(output_dir, f"{source_file}_events.csv")
                    events_json_path = os.path.join(output_dir, f"{source_file}_events.json")

                    # Add metadata to each event
                    matched_meta = metadata_df[metadata_df["source_file"] == source_file]
                    if not matched_meta.empty:
                        meta_row = matched_meta.iloc[0].to_dict()
                        enriched_events = attach_metadata_to_events(all_events, meta_row, source_file, relative_path)
                        summary_rows.extend(enriched_events)
                    else:
                        enriched_events = all_events
                        summary_rows.extend(all_events)

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
                    print(f"✗ Failed to process {fname}: {e}")

    # Final saves
    summary_df = pd.DataFrame(summary_rows).sort_values(by=["AppTime", "Timestamp"])
    summary_df.to_csv(os.path.join(output_root_dir, "event_summary.csv"), index=False)
    save_manifest(manifest_records, output_root_dir)
