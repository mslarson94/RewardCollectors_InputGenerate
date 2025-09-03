import os
import json
import pandas as pd
from glob import glob
import re

def load_metadata(base_path, sheet="MagicLeapFiles"):
    path = os.path.join(base_path, "collatedData.xlsx")
    return pd.read_excel(path, sheet_name=sheet)

def group_metadata_v1(df, keys):
    grouped = df.dropna(subset=keys)
    grouped = grouped[grouped['cleanedFile'].notna()]
    grouped['group_key'] = grouped[keys].astype(str).agg('_'.join, axis=1)
    return grouped.groupby('group_key')

def get_file_paths(base_dir, file_stem, procDir):
    #procDir = procDir.rstrip("/") + "/"  # ensure trailing slash
    procDir = procDir.rstrip("/")  # ensure trailing slash
    eventsStem = file_stem.replace("_processed", "")
    return {
        "csv": os.path.join(base_dir, procDir, "Events_AugFinal_withWalks", "augmented", f"{eventsStem}_events_final.csv"),
        "json": os.path.join(base_dir, procDir, "Events_Flat_json", f"{file_stem}_events.json"),
        "meta": os.path.join(base_dir, procDir, "MetaData_Flat", f"{file_stem}_meta.json")
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
        print(paths["csv"])
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
    print('starting!')
    metadata = load_metadata(base_dir)
    print(metadata)
    grouped = group_metadata(metadata, group_key_fields)
    print(grouped)
    merged_data = []

    for _, group_df in grouped:
        merged = merge_group_files(group_df, base_dir, procDir, group_key_fields)
        merged_data.append(merged)

    return merged_data


def export_merged_data_v1a(merged_data, base_dir, export_dir, group_key_fields):
    os.makedirs(export_dir, exist_ok=True)

    for item in merged_data:
        group_info = item["group_info"]
        group_key = '_'.join(str(group_info.get(k, 'NA')) for k in group_key_fields)
        group_folder = os.path.join(export_dir, group_key)
        os.makedirs(group_folder, exist_ok=True)
        #print(item)

        if item["csv"] is not None:
            #print("item['csv'] is not none")
            item["csv"].to_csv(os.path.join(group_folder, "merged_events.csv"), index=False)

        if item["json"] is not None:
            item["json"].to_json(os.path.join(group_folder, "merged_events.json"), orient="records", lines=True)

        if item["meta"] is not None:
            with open(os.path.join(group_folder, "merged_meta.json"), "w") as f:
                json.dump(item["meta"], f, indent=2)


# Replace your existing export_merged_data_v1 with this version
def export_merged_data_v1b(merged_data, base_dir, export_dir, group_key_fields, *, flat_dir=None, flat_csv_only=True):
    """
    Writes per-group nested outputs as before, and (optionally) a second flat copy.
      - Nested: <export_dir>/<group_key>/merged_events.(csv|json), merged_meta.json
      - Flat (if flat_dir is provided): <flat_dir>/<group_key>_events.csv
        If flat_csv_only=False, also writes JSON/meta to flat_dir.
    """
    os.makedirs(export_dir, exist_ok=True)
    if flat_dir:
        os.makedirs(flat_dir, exist_ok=True)

    for item in merged_data:
        group_info = item["group_info"]
        base = _sanitize_filename('_'.join(str(group_info.get(k, 'NA')) for k in group_key_fields))

        group_folder = os.path.join(export_dir, base)
        os.makedirs(group_folder, exist_ok=True)

        # CSV
        if item["csv"] is not None:
            nested_csv = os.path.join(group_folder, "merged_events.csv")
            item["csv"].to_csv(nested_csv, index=False)
            if flat_dir:
                item["csv"].to_csv(os.path.join(flat_dir, f"{base}_events.csv"), index=False)

        # JSON
        if item["json"] is not None:
            nested_json = os.path.join(group_folder, "merged_events.json")
            item["json"].to_json(nested_json, orient="records", lines=True)
            if flat_dir and not flat_csv_only:
                item["json"].to_json(os.path.join(flat_dir, f"{base}_events.json"), orient="records", lines=True)

        # META
        if item["meta"] is not None:
            nested_meta = os.path.join(group_folder, "merged_meta.json")
            with open(nested_meta, "w") as f:
                json.dump(item["meta"], f, indent=2)
            if flat_dir and not flat_csv_only:
                with open(os.path.join(flat_dir, f"{base}_meta.json"), "w") as f:
                    json.dump(item["meta"], f, indent=2)

def export_merged_data_v1(merged_data, base_dir, export_dir, group_key_fields):
    os.makedirs(export_dir, exist_ok=True)
    flat_dir_csv = export_dir + "_Flat_csv"
    #flat_dir_json = export_dir + "_Flat_json"
    flat_dir_meta = export_dir + "_Flat_metaJson"
    print(str(export_dir))
    print(flat_dir_csv)
    os.makedirs(flat_dir_csv, exist_ok=True)
    #os.makedirs(flat_dir_json, exist_ok=True)
    os.makedirs(flat_dir_meta, exist_ok=True)
    for item in merged_data:
        group_info = item["group_info"]
        #group_info_str = "_".join(str(group_info.get(k, "NA")) for k in order)
        group_key = '_'.join(str(group_info.get(k, 'NA')) for k in group_key_fields)
        print(group_key, type(group_key))
        group_folder = os.path.join(export_dir, group_key)
        os.makedirs(group_folder, exist_ok=True)
        #print(item)

        if item["csv"] is not None:
            #print("item['csv'] is not none")
            item["csv"].to_csv(os.path.join(group_folder, group_key+"_events.csv"), index=False)
            print("the flat directory", os.path.join(flat_dir_csv, group_key+"_events.csv"))
            item["csv"].to_csv(os.path.join(flat_dir_csv, group_key+"_events.csv"), index=False)

        # if item["json"] is not None:
        #     item["json"].to_json(os.path.join(group_folder, group_key+"_events.json"), orient="records", lines=True)
        #     item["json"].to_json(os.path.join(flat_dir_json, group_key+"_events.json"), orient="records", lines=True)
        if item["meta"] is not None:
            with open(os.path.join(group_folder, group_key+"_meta.json"), "w") as f:
                json.dump(item["meta"], f, indent=2)
            with open(os.path.join(flat_dir_meta, group_key+"_meta.json"), "w") as f:
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
    #print(df['file_stem'])
    df = df[df['file_stem'].apply(file_exists)]
    #print(df[keys].astype(str).agg('_'.join, axis=1))
    df['group_key'] = df[keys].astype(str).agg('_'.join, axis=1)
    return df.groupby('group_key')

def merge_all_versions(base_dir, procDir, group_key_fields):
    #print('starting!')
    metadata = load_metadata(base_dir)
    #print(metadata)
    grouped = group_metadata(df = metadata, keys = group_key_fields, base_dir = base_dir, procDir = procDir)
    #print(grouped)
    merged_data = []

    for _, group_df in grouped:
        merged = merge_group_files(group_df=group_df, base_dir=base_dir, procDir=procDir, group_key_fields=group_key_fields)
        merged_data.append(merged)

    return merged_data

# import re
# import os

# def _sanitize_filename(name: str) -> str:
#     name = re.sub(r"\s+", "_", str(name).strip())
#     return re.sub(r"[^A-Za-z0-9._-]", "_", name)

# def export_merged_data_flat(merged_data, export_dir, group_key_fields):
#     """
#     Save merged outputs as flat CSVs only (no JSON/meta) into `export_dir`.
#     Filenames look like: <group_key_joined>_events.csv
#     Returns the number of files written.
#     """
#     os.makedirs(export_dir, exist_ok=True)
#     wrote = 0

#     if not merged_data:
#         print("[export_flat] nothing to export (merged_data is empty)")
#         return 0

#     for idx, item in enumerate(merged_data, 1):
#         group_info = item.get("group_info") or {}
#         base_raw = "_".join(str(group_info.get(k, "NA")) for k in group_key_fields)
#         base = _sanitize_filename(base_raw) or f"group_{idx}"

#         df = item.get("csv")
#         if df is None:
#             print(f"[export_flat] SKIP #{idx}: no CSV for {base}")
#             continue
#         if getattr(df, "empty", False):
#             print(f"[export_flat] SKIP #{idx}: empty DataFrame for {base}")
#             continue

#         out_path = os.path.join(export_dir, f"{base}_events.csv")
#         df.to_csv(out_path, index=False)
#         print(f"[export_flat] WROTE #{idx}: {out_path} ({len(df)} rows)")
#         wrote += 1

#     print(f"[export_flat] done, wrote {wrote} file(s) to {export_dir}")
#     return wrote

# Usage
trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
procDir = 'FreshStart/full'
base_dir = os.path.join(trueRootDir, procDir)


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


v1_flatOutDir = os.path.join(trueRootDir, procDir, "MergedEvents_V1_flat")
v2_flatOutDir = os.path.join(trueRootDir, procDir, "MergedEvents_V2_flat")
v3_flatOutDir = os.path.join(trueRootDir, procDir, "MergedEvents_V3_flat")

# n1 = export_merged_data_flat(merged_version1_data, v1_flatOutDir, v1_groupFields)
# n2 = export_merged_data_flat(merged_version2_data, v2_flatOutDir, v2_groupFields)
# n3 = export_merged_data_flat(merged_version3_data, v3_flatOutDir, v3_groupFields)
# print(f"flat exports: v1={n1}, v2={n2}, v3={n3}")


# export_merged_data_v1(merged_version1_data, trueRootDir, v1_outDir, v1_groupFields, flat_dir=v1_flatOutDir)
# export_merged_data_v1(merged_version2_data, trueRootDir, v2_outDir, v2_groupFields, flat_dir=v2_flatOutDir)
# export_merged_data_v1(merged_version3_data, trueRootDir, v3_outDir, v3_groupFields, flat_dir=v3_flatOutDir)
print('✨ done ✨')