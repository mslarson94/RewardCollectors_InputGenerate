
import os
import pandas as pd

def attach_metadata_to_events(events, metadata_row, source_file, relative_path):
    metadata_fields = {
        "participant_id": metadata_row.get("participant_id", "unknown"),
        "session": metadata_row.get("session", "unknown"),
        "role": metadata_row.get("currentRole", "unknown"),
        "source_file": source_file,
        "relative_path": relative_path
    }
    return [event | metadata_fields for event in events]

def record_to_manifest(metadata_row, source_file, relative_path, processed_path, events_csv_path, events_json_path):
    return {
        "participant_id": metadata_row.get("participant_id", "unknown"),
        "session": metadata_row.get("session", "unknown"),
        "role": metadata_row.get("currentRole", "unknown"),
        "source_file": source_file,
        "relative_path": relative_path,
        "processed_path": processed_path,
        "events_csv": events_csv_path,
        "events_json": events_json_path
    }

def save_manifest(records, output_dir):
    manifest_path = os.path.join(output_dir, "run_manifest.csv")
    pd.DataFrame(records).to_csv(manifest_path, index=False)
    print(f"📄 Manifest saved to {manifest_path}")
