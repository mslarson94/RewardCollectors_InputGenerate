
import os
import pandas as pd
from datetime import datetime
from io import StringIO


def attach_metadata_to_events(events, metadata_row, source_file, relative_path):
    metadata_fields = {
        "participantID": metadata_row.get("participantID", "unknown"),
        "pairID": metadata_row.get("pairID", "unknown"),
        "testingDate": metadata_row.get("testingDate", "unknown"),
        "sessionType": metadata_row.get("testingDate", "unknown"),
        "AorB": metadata_row.get("AorB", "unknown"),
        "coinSet": metadata_row.get("coinSet", "unknown"),
        "main_RR": metadata_row.get("main_RR", "unknown"),
        "currentRole": metadata_row.get("currentRole", "unknown"),
        "source_file": source_file,
        "relative_path": relative_path
    }
    return [event | metadata_fields for event in events]

def record_to_manifest(metadata_row, source_file, relative_path, processed_path, events_csv_path, events_json_path):
    return {
        "participantID": metadata_row.get("participantID", "unknown"),
        "pairID": metadata_row.get("pairID", "unknown"),
        "testingDate": metadata_row.get("testingDate", "unknown"),
        "sessionType": metadata_row.get("testingDate", "unknown"),
        "AorB": metadata_row.get("AorB", "unknown"),
        "coinSet": metadata_row.get("coinSet", "unknown"),
        "main_RR": metadata_row.get("main_RR", "unknown"),
        "currentRole": metadata_row.get("currentRole", "unknown"),
        "source_file": source_file,
        "relative_path": relative_path,
        "processed_path": processed_path,
        "events_csv": events_csv_path,
        "events_json": events_json_path
    }

def save_manifest(records, output_dir):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    manifest_path = os.path.join(output_dir, f"run_manifest_{timestamp}.csv")
    pd.DataFrame(records).to_csv(manifest_path, index=False)
    print(f"📄 Manifest saved to {manifest_path}")



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