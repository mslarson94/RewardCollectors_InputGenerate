import os
import json
import pandas as pd
from glob import glob
import re

def load_metadata(base_path, sheet="MagicLeapFiles"):
    path = os.path.join(base_path, "collatedData.xlsx")
    return pd.read_excel(path, sheet_name=sheet)

def get_file_paths(base_dir, file_stem, procDir, eventDir="Events_AugFinal_withWalks", eventEnding="_events_final.csv"):
    procDir = procDir.rstrip("/")  # ensure trailing slash
    eventsStem = file_stem.replace("_processed", "")
    return {
        "csv": os.path.join(base_dir, procDir, eventDir, "augmented", f"{eventsStem}_{eventEnding}"),
        "meta": os.path.join(base_dir, procDir, "MetaData_Flat", f"{file_stem}_meta.json")
    }

def collapse_meta_files(meta_paths, group_df, eventEnding="_events.csv"):
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
        f"{os.path.splitext(row['cleanedFile'])[0]}{eventEnding}"
        for _, row in group_df.iterrows()
    ]
    return combined

def merge_group_files(group_df, base_dir, procDir, group_key_fields, eventDir="Events_AugFinal_withWalks", eventEnding="_events_final.csv"):
    merged_csvs, merged_jsons, meta_paths = [], [], []

    for _, row in group_df.iterrows():
        file_stem = os.path.splitext(row['cleanedFile'])[0]
        paths = get_file_paths(base_dir, file_stem, procDir, outDir, outEnding)
        print(paths["csv"])
        if os.path.exists(paths["csv"]):
            merged_csvs.append(pd.read_csv(paths["csv"]))
        if os.path.exists(paths["meta"]):
            meta_paths.append(paths["meta"])

    final_csv = pd.concat(merged_csvs) if merged_csvs else None
    final_json = pd.concat(merged_jsons) if merged_jsons else None
    final_meta = collapse_meta_files(meta_paths, group_df, eventEnding) if meta_paths else None

    return {
        "csv": final_csv,
        "meta": final_meta,
        "group_info": group_df.iloc[0][group_key_fields].to_dict()
    }

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

def merge_all_versions(base_dir, procDir, group_key_fields, eventDir="Events_AugFinal_withWalks", eventEnding="_events_final.csv"):
    #print('starting!')
    metadata = load_metadata(base_dir)
    #print(metadata)
    grouped = group_metadata(df = metadata, keys = group_key_fields, base_dir = base_dir, procDir = procDir)
    #print(grouped)
    merged_data = []

    for _, group_df in grouped:
        merged = merge_group_files(group_df=group_df, base_dir=base_dir, procDir=procDir, group_key_fields=group_key_fields, eventDir=eventDir, eventEnding=eventEnding)
        merged_data.append(merged)

    return merged_data


def export_merged_data_v1(merged_data, base_dir, export_dir, group_key_fields, eventDir="Events_AugFinal_withWalks", eventEnding="_events_final.csv", outEnding="_events.csv"):
    os.makedirs(export_dir, exist_ok=True)
    flat_dir_csv = export_dir + "_Flat_csv"
    flat_dir_meta = export_dir + "_Flat_metaJson"
    print(str(export_dir))
    print(flat_dir_csv)
    os.makedirs(flat_dir_csv, exist_ok=True)
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
            item["csv"].to_csv(os.path.join(group_folder, group_key+outEnding), index=False)
            print("the flat directory", os.path.join(flat_dir_csv, group_key+outEnding))
            item["csv"].to_csv(os.path.join(flat_dir_csv, group_key+outEnding), index=False)

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



# # Old Usage
# trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
# procDir = 'FreshStart/full'
# base_dir = os.path.join(trueRootDir, procDir)


# v1_outDir = os.path.join(trueRootDir, procDir, "MergedEvents_V1")

# # Version 1: (ParticipantID, testingDate, sessionType, coinSet, device), Version 2: (PairID, testingDate, currentRole), Version 3: (ParticipantID, currentRole, coinSet, main_RR). 
# v1_groupFields = ["participantID", "testingDate", "sessionType", "coinSet", "device"]

# merged_version1_data = merge_all_versions(trueRootDir, procDir, v1_groupFields) 
# export_merged_data_v1(merged_version1_data, trueRootDir, v1_outDir, v1_groupFields)



# v1_flatOutDir = os.path.join(trueRootDir, procDir, "MergedEvents_V1_flat")


# n1 = export_merged_data_flat(merged_version1_data, v1_flatOutDir, v1_groupFields)
# print(f"flat exports: v1={n1}, v2={n2}, v3={n3}")
# export_merged_data_v1(merged_version1_data, trueRootDir, v1_outDir, v1_groupFields, flat_dir=v1_flatOutDir)
print('✨ done ✨')


import argparse
from pathlib import Path

def cli() -> None:
    parser = argparse.ArgumentParser(
        prog="mergeEventsV2",
        description="Merge final events files by specific keys"
    )
    parser.add_argument(
        "--root-dir", required=True, type=Path,
        help="Base project directory (e.g., '/Users/you/RC_TestingNotes')."
    )
    parser.add_argument(
        "--proc-dir", required=True, type=Path,
        help="Dataset subdirectory under --root-dir (e.g., 'FreshStart'). "
             "If absolute, --root-dir is ignored."
    )
    parser.add_argument(
        "--event-dir", default="Events_AugPart1",
        help="Subdirectory under <root/proc/full> that we draw the source event .csv files"
    )
    parser.add_argument(
        "--eventEnding", default="_events_final.csv",
        help="file ending pattern for source event .csv files"
    )
    parser.add_argument(
        "--outEnding", default="_events.csv",
        help="file ending pattern for merged .csv files"
    )

    parser.add_argument(
        "--group-key-fields", dest="group_key_fields", action="append",
        choices=["participantID", "pairID", "testingDate", "currentRole", "ptIsAorB", ## Basic categorical information about participant
                    "coinSet", "sessionType", "device", "main_RR", ## Basic categorial information about testing session 
                    "BlockNum", "BlockType", "coinLabel", "actualClosestCoinLabel"], ## Advance information regarding elements of testing session
        default=["participantID", "testingDate", "currentRole", "coinSet"],
        help="Key fields in the events file that you wish to merge the event files by."
    )

    args = parser.parse_args()

    root = args.root_dir.expanduser()
    proc = args.proc_dir
    proc_dir = (proc if proc.is_absolute() else (root / proc)) / "full"


    events_dir = proc_dir / args.events_dir_name
    meta_dir = proc_dir / args.meta_dir_name
    output_dir = proc_dir / args.output_dir_name



    for p, label in ((proc_dir, "proc-dir"),
                     (events_dir, "events-dir"),
                     (meta_dir, "meta-dir")):
        if not p.exists():
            parser.error(f"{label} not found: {p}")

    output_dir.mkdir(parents=True, exist_ok=True)

    merged_data = merge_all_versions(
                            base_dir=root, 
                            procDir=proc_dir, 
                            group_key_fields=args.group_key_fields,
                            eventDir=args.event_dir,
                            eventEnding=args.eventEnding) 

    export_merged_data_v1(merged_data=merged_data, 
                            base_dir=root, 
                            export_dir=output_dir, 
                            group_key_fields=args.group_key_fields,
                            eventDir=args.event_dir,
                            eventEnding=args.eventEnding,
                            outEnding=args.outEnding)


if __name__ == "__main__":
    cli()
