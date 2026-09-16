
import os
import pandas as pd
from datetime import datetime
from io import StringIO


def attach_metadata_to_events(events, metadata_row, source_file, relative_path):
    metadata_fields = {
        "participantID": metadata_row.get("participantID", "unknown"),
        "pairID": metadata_row.get("pairID", "unknown"),
        "testingDate": metadata_row.get("testingDate", "unknown"),
        "sessionType": metadata_row.get("sessionType", "unknown"),
        "ptIsAorB": metadata_row.get("AorB", "unknown"),
        "coinSet": metadata_row.get("coinSet", "unknown"),
        "device": metadata_row.get("device", "unknown"),
        "main_RR": metadata_row.get("main_RR", "unknown"),
        "currentRole": metadata_row.get("currentRole", "unknown"),
        "taskNaive": metadata_row.get("taskNaive", "unknown"),
        "source_file": source_file,
        "testingOrder": metadata_row.get("testingOrder", "unknown"),
        "relative_path": relative_path,
        "sessionID": metadata_row.get("sessionID", "unknown"),
    }
    return [event | metadata_fields for event in events]

def attach_testingOrder(events, metadata_row):
    metadata_fields = {
        "testingOrder": metadata_row.get("testingOrder", "unknown"),
    }
    return [event | metadata_fields for event in events]

def record_to_manifest(metadata_row, source_file, relative_path, processed_path, events_csv_path, events_json_path):
    return {
        "participantID": metadata_row.get("participantID", "unknown"),
        "pairID": metadata_row.get("pairID", "unknown"),
        "testingDate": metadata_row.get("testingDate", "unknown"),
        "sessionType": metadata_row.get("sessionType", "unknown"),
        "ptIsAorB": metadata_row.get("AorB", "unknown"),
        "coinSet": metadata_row.get("coinSet", "unknown"),
        "device": metadata_row.get("device", "unknown"),
        "main_RR": metadata_row.get("main_RR", "unknown"),
        "currentRole": metadata_row.get("currentRole", "unknown"),
        "taskNaive": metadata_row.get("taskNaive", "unknown"),
        "source_file": source_file,
        "testingOrder": metadata_row.get("testingOrder", "unknown"),
        "relative_path": relative_path,
        "sessionID": metadata_row.get("sessionID", "unknown"),
        "processed_path": processed_path,
        "events_csv": events_csv_path,
        "events_json": events_json_path
    }

def save_manifest(records, output_dir):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    manifest_path = os.path.join(output_dir, f"run_manifest_{timestamp}.csv")
    pd.DataFrame(records).to_csv(manifest_path, index=False)
    print(f"📄 Manifest saved to {manifest_path}")


def load_filtered_df(file_path):
    with open(file_path, "r") as f:
        lines = f.readlines()

    header = lines[0]
    start_index = next((i for i, line in enumerate(lines) if "Mark should happen" in line), 1)

    filtered_lines = [header] + lines[start_index:]
    df = pd.read_csv(StringIO("".join(filtered_lines)))

    # 🚫 Removed original_index assignment

    # Construct filtered filename
    base, ext = os.path.splitext(file_path)
    filtered_path = f"{base}_filtered{ext}"

    # Save the filtered DataFrame
    df.to_csv(filtered_path, index=False)

    print(f"🔄 Filtered DataFrame saved to: {filtered_path}")
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


def get_metadata_row_for_file(source_file, metadata_df):
    """
    Retrieve the metadata row corresponding to a given source file.
    """
    matched_rows = metadata_df.loc[metadata_df["source_file"] == os.path.basename(source_file)]
    if matched_rows.empty:
        raise ValueError(f"❌ No metadata found for {source_file} in metadata_df.")
    return matched_rows.iloc[0]


def generate_nestedDir(proc_dir, metadataFile, target_file):

    meta_df = pd.read_excel(metadataFile, sheet_name="MagicLeapFiles")
    meta_df = meta_df.dropna(subset=["cleanedFile"])
    
    meta_df["cleanedFile"] = meta_df["cleanedFile"].astype(str).str.strip().str.lower()
    matched_meta = meta_df[meta_df["cleanedFile"] == target_file.lower()] 
    nested_json = {
        "target_file": target_file,
        "proc_dir": proc_dir, 
        "pairID": meta_row.get("pairID_py", "unknown"),
        "testingDate": str(meta_row.get("testingDate", "unknown")),
        "sessionType": meta_row.get("sessionType", "Morning"),
        "MagicLeaps": "MagicLeaps",
        "device": meta_row.get("device", "unknown")
    }
    pairID= meta_row.get("pairID_py", "unknown")
    testingDate = str(meta_row.get("testingDate", "unknown"))
    sessionType = meta_row.get("sessionType", "Morning")
    device = meta_row.get("device", "unknown")
    
    almostNested = f"ProcessedData/{pairID}/{testingDate}/{sessionType}/MagicLeaps/{device}"
    nestedDirName = os.path.join(proc_dir, almostNested)

    return nestedDirName
