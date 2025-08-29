import os
import json
import pandas as pd
from glob import glob

def load_metadata(base_path, sheet="MagicLeapFiles"):
    path = os.path.join(base_path, "collatedData.xlsx")
    return pd.read_excel(path, sheet_name=sheet)

def group_metadata_v1(df, keys):
    grouped = df.dropna(subset=keys)
    grouped = grouped[grouped['cleanedFile'].notna()]
    grouped['group_key'] = grouped[keys].astype(str).agg('_'.join, axis=1)
    return grouped.groupby('group_key')

def get_file_paths(base_dir, file_stem, procDir):
    procDir = procDir.rstrip("/") + "/"  # ensure trailing slash
    return {
        "csv": os.path.join(base_dir, procDir+"ExtractedEvents_csv_Flat", f"{file_stem}_events.csv"),
        "json": os.path.join(base_dir, procDir+"ExtractedEvents_json_Flat", f"{file_stem}_events.json"),
        "meta": os.path.join(base_dir, procDir+"MetaData_Flat", f"{file_stem}_meta.json")
    }

def collapse_meta_files_v1(meta_paths):
    combined = {
        "CoinRegistry": {},
        "BlockStructureSummary": []
    }
    for path in meta_paths:
        with open(path, 'r') as f:
            meta = json.load(f)
        for k, v in meta.get("CoinRegistry", {}).items():
            if k not in combined["CoinRegistry"]:
                combined["CoinRegistry"][k] = v
            else:
                combined["CoinRegistry"][k].update(v)
        combined["BlockStructureSummary"].extend(meta.get("BlockStructureSummary", []))
    return combined

def collapse_meta_files(meta_paths, group_df):
    combined = {
        "CoinRegistry": {},
        "BlockStructureSummary": [],
        "SourceFiles": []
    }
    for path in meta_paths:
        with open(path, 'r') as f:
            meta = json.load(f)
        for k, v in meta.get("CoinRegistry", {}).items():
            if k not in combined["CoinRegistry"]:
                combined["CoinRegistry"][k] = v
            else:
                combined["CoinRegistry"][k].update(v)
        combined["BlockStructureSummary"].extend(meta.get("BlockStructureSummary", []))
    # Add SourceFiles key
    combined["SourceFiles"] = [
        f"{os.path.splitext(row['cleanedFile'])[0]}_events.csv"
        for _, row in group_df.iterrows()
    ]
    return combined



def merge_group_files(group_df, base_dir, procDir, group_key_fields):
    merged_csvs, merged_jsons, meta_paths = [], [], []

    for _, row in group_df.iterrows():
        file_stem = os.path.splitext(row['cleanedFile'])[0]
        paths = get_file_paths(base_dir, file_stem, procDir)

        if os.path.exists(paths["csv"]):
            merged_csvs.append(pd.read_csv(paths["csv"]))
        if os.path.exists(paths["json"]):
            merged_jsons.append(pd.read_json(paths["json"], lines=True))
        if os.path.exists(paths["meta"]):
            meta_paths.append(paths["meta"])

    final_csv = pd.concat(merged_csvs) if merged_csvs else None
    final_json = pd.concat(merged_jsons) if merged_jsons else None
    final_meta = collapse_meta_files(meta_paths, group_df) if meta_paths else None

    return {
        "csv": final_csv,
        "json": final_json,
        "meta": final_meta,
        "group_info": group_df.iloc[0][group_key_fields].to_dict()
    }

def merge_all_versions_v1(base_dir, procDir, group_key_fields):
    metadata = load_metadata(base_dir)
    grouped = group_metadata(metadata, group_key_fields)
    merged_data = []

    for _, group_df in grouped:
        merged = merge_group_files(group_df, base_dir, procDir, group_key_fields)
        merged_data.append(merged)

    return merged_data


def export_merged_data_v1(merged_data, base_dir, export_dir, group_key_fields):
    os.makedirs(export_dir, exist_ok=True)

    for item in merged_data:
        group_info = item["group_info"]
        group_key = '_'.join(str(group_info.get(k, 'NA')) for k in group_key_fields)
        group_folder = os.path.join(export_dir, group_key)
        os.makedirs(group_folder, exist_ok=True)

        if item["csv"] is not None:
            item["csv"].to_csv(os.path.join(group_folder, "merged_events.csv"), index=False)

        if item["json"] is not None:
            item["json"].to_json(os.path.join(group_folder, "merged_events.json"), orient="records", lines=True)

        if item["meta"] is not None:
            with open(os.path.join(group_folder, "merged_meta.json"), "w") as f:
                json.dump(item["meta"], f, indent=2)

def export_grouped_events(flat_events_df, output_dir, group_fields):
    """
    Save grouped event DataFrames to individual CSV files with flattened names.

    Args:
        flat_events_df (pd.DataFrame): The full events DataFrame.
        output_dir (str): Path to the directory where output CSVs will be saved.
        group_fields (list of str): Fields to group by for naming.
    """
    os.makedirs(output_dir, exist_ok=True)

    grouped = flat_events_df.groupby(group_fields)
    for keys, group_df in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        filename = "_".join(str(k) for k in keys) + ".csv"
        filepath = os.path.join(output_dir, filename)
        group_df.to_csv(filepath, index=False)

def group_metadata(df, keys, base_dir, procDir):
    procDir = procDir.rstrip("/") + "/"
    df = df.dropna(subset=keys)
    df = df[df['cleanedFile'].notna()]

    def file_exists(file_stem):
        paths = get_file_paths(base_dir, file_stem, procDir)
        return any(os.path.exists(p) for p in paths.values())

    df['file_stem'] = df['cleanedFile'].apply(lambda f: os.path.splitext(f)[0])
    df = df[df['file_stem'].apply(file_exists)]
    df['group_key'] = df[keys].astype(str).agg('_'.join, axis=1)
    return df.groupby('group_key')

def merge_all_versions(base_dir, procDir, group_key_fields):
    metadata = load_metadata(base_dir)
    grouped = group_metadata(df = metadata, keys = group_key_fields, base_dir = base_dir, procDir = procDir)
    merged_data = []

    for _, group_df in grouped:
        merged = merge_group_files(group_df=group_df, base_dir=base_dir, procDir=procDir, group_key_fields=group_key_fields)
        merged_data.append(merged)

    return merged_data


# Usage
trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
#procDir = 'SmallSelectedData/RNS/alignedPO'
procDir = 'SelectedData'
v1_outDir = os.path.join(trueRootDir, procDir, "MergedEvents_V1")
v2_outDir = os.path.join(trueRootDir, procDir, "MergedEvents_V2")
v3_outDir = os.path.join(trueRootDir, procDir, "MergedEvents_V3")
#procDir = 'SelectedData'

# Version 1: (ParticipantID, testingDate, sessionType, coinSet, device), Version 2: (PairID, testingDate, currentRole), Version 3: (ParticipantID, currentRole, coinSet, main_RR). 
v1_groupFields = ["participantID", "testingDate", "sessionType", "coinSet", "device"]
v2_groupFields = ["pairID", "testingDate", "currentRole"]
v3_groupFields = ["participantID", "currentRole", "coinSet", "main_RR"]

merged_version1_data = merge_all_versions(trueRootDir, procDir, v1_groupFields) 
merged_version2_data = merge_all_versions(trueRootDir, procDir, v2_groupFields)
merged_version3_data = merge_all_versions(trueRootDir, procDir, v3_groupFields)
export_merged_data_v1(merged_version1_data, trueRootDir, v1_outDir, v1_groupFields)
export_merged_data_v1(merged_version2_data, trueRootDir, v2_outDir, v2_groupFields)
export_merged_data_v1(merged_version3_data, trueRootDir, v3_outDir, v3_groupFields)
print('✨ done ✨')