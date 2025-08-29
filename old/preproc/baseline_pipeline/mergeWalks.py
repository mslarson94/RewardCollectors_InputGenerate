import pandas as pd
from pathlib import Path
import json
import re
import os

'''def fix_time_str(ts, session_date):
    if not isinstance(ts, str):
        return pd.NaT
    if ts.count(":") == 3:
        ts = ".".join(ts.rsplit(":", 1))
    try:
        return pd.to_datetime(f"{session_date.strftime('%Y-%m-%d')} {ts}", format="%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        return pd.NaT

def merge_events_and_walks_v1(events_path, walks_path, out_path):
    df_events = pd.read_csv(events_path, parse_dates=["start_Timestamp"], infer_datetime_format=True)
    df_walks = pd.read_csv(walks_path, parse_dates=["start_Timestamp"], infer_datetime_format=True)

    df_combined = pd.concat([df_events, df_walks], ignore_index=True)
    df_combined = df_combined.sort_values("start_Timestamp").reset_index(drop=True)
    df_combined.to_csv(out_path, index=False)
    print(f"✅ Final merged file written to {out_path}")

def merge_events_and_walks(events_path, walks_path, meta_path, out_path):
    # Load session date
    with open(meta_path, 'r') as f:
        meta = json.load(f)
    session_date = pd.to_datetime(meta.get("testingDate", "01_01_1970"), format="%m_%d_%Y")

    df_events = pd.read_csv(events_path)
    df_walks = pd.read_csv(walks_path)

    df_events['start_Timestamp'] = df_events['start_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))
    df_walks['start_Timestamp'] = df_walks['start_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))

    df_combined = pd.concat([df_events, df_walks], ignore_index=True)
    df_combined = df_combined.sort_values("start_Timestamp").reset_index(drop=True)
    df_combined.to_csv(out_path, index=False)
    print(f"✅ Final merged file written to {out_path}")


def batch_merge_events(input_dir, output_dir):
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    walk_files = {f.stem.replace("_walks", ""): f for f in input_dir.glob("*_walks.csv")}
    event_files = {f.stem.replace("_events", ""): f for f in input_dir.glob("*_events.csv")}

    matched_keys = set(walk_files) & set(event_files)
    print(f"🔍 Found {len(matched_keys)} matched file pairs to process.")

    for key in sorted(matched_keys):
        walk_path = walk_files[key]
        events_path = event_files[key]
        out_file = output_dir / f"{key}_events_with_walks.csv"
        print(f"➡️ Processing: {events_path.name} + {walk_path.name}")
        merge_events_and_walks(events_path, walk_path, out_file)
'''
import pandas as pd
import json
from pathlib import Path


def fix_time_str(ts, session_date):
    if not isinstance(ts, str):
        return pd.NaT
    if ts.count(":") == 3:
        ts = ".".join(ts.rsplit(":", 1))
    try:
        return pd.to_datetime(f"{session_date.strftime('%Y-%m-%d')} {ts}", format="%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        return pd.NaT

# def batch_merge_events_v1(input_dir, output_dir):
#     input_dir = Path(input_dir)
#     output_dir = Path(output_dir)
#     output_dir.mkdir(parents=True, exist_ok=True)

#     events_files = {f.stem.replace("_events_flat", ""): f for f in input_dir.glob("*_events_flat.csv")}
#     walks_files = {f.stem.replace("_walks", ""): f for f in input_dir.glob("*_walks.csv")}
#     meta_files = {f.stem.replace("_meta", ""): f for f in input_dir.glob("*_meta.json")}

#     matched_keys = set(events_files) & set(walks_files) & set(meta_files)
#     print(f"🔍 Found {len(matched_keys)} complete file sets.")

#     for key in sorted(matched_keys):
#         events_path = events_files[key]
#         walks_path = walks_files[key]
#         meta_path = meta_files[key]
#         out_path = output_dir / f"{key}_events_with_walks.csv"
#         print(f"➡️ Merging: {events_path.name} + {walks_path.name}")
#         merge_events_and_walks(events_path, walks_path, meta_path, out_path)

# def merge_events_and_walks_v2(events_path, walks_path, meta_path, out_path):
#     with open(meta_path, 'r') as f:
#         meta = json.load(f)
#     session_date = pd.to_datetime(meta.get("testingDate", "01_01_1970"), format="%m_%d_%Y")

#     df_events = pd.read_csv(events_path)
#     df_walks = pd.read_csv(walks_path)

#     df_events['start_Timestamp'] = df_events['start_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))
#     df_walks['start_Timestamp'] = df_walks['start_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))

#     df_combined = pd.concat([df_events, df_walks], ignore_index=True)
#     df_combined = df_combined.sort_values("start_Timestamp").reset_index(drop=True)
#     df_combined.to_csv(out_path, index=False)
#     print(f"✅ Final merged file written to {out_path}")

# def merge_events_and_walks_v3(events_path, walks_path, meta_path, out_path):
#     with open(meta_path, 'r') as f:
#         meta = json.load(f)
#     session_date = pd.to_datetime(meta.get("testingDate", "01_01_1970"), format="%m_%d_%Y")

#     df_events = pd.read_csv(events_path)

#     try:
#         df_walks = pd.read_csv(walks_path)
#     except pd.errors.EmptyDataError:
#         print(f"⚠️ Skipping empty walk file: {walks_path.name}")
#         df_walks = pd.DataFrame(columns=df_events.columns)  # create empty frame with same structure

#     df_events['start_Timestamp'] = df_events['start_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))
#     df_walks['start_Timestamp'] = df_walks['start_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))

#     df_combined = pd.concat([df_events, df_walks], ignore_index=True)
#     df_combined = df_combined.sort_values("start_Timestamp").reset_index(drop=True)
#     df_combined.to_csv(out_path, index=False)
#     print(f"✅ Final merged file written to {out_path}")

def merge_events_and_walks(events_path, walks_path, meta_path, out_path):
    with open(meta_path, 'r') as f:
        meta = json.load(f)
    session_date = pd.to_datetime(meta.get("testingDate", "01_01_1970"), format="%m_%d_%Y")

    # Load and fix timestamps
    df_events = pd.read_csv(events_path)
    df_walks = pd.read_csv(walks_path)

    # Apply timestamp parsing using shared logic
    def fix_time_str(ts):
        if not isinstance(ts, str):
            return pd.NaT
        if ts.count(":") == 3:
            ts = ".".join(ts.rsplit(":", 1))
        try:
            return pd.to_datetime(f"{session_date.strftime('%Y-%m-%d')} {ts}", format="%Y-%m-%d %H:%M:%S.%f")
        except Exception:
            return pd.NaT

    df_events['start_Timestamp'] = df_events['start_Timestamp'].apply(fix_time_str)
    df_events['end_Timestamp'] = df_events['end_Timestamp'].apply(fix_time_str)

    df_walks['start_Timestamp'] = df_walks['start_Timestamp'].apply(fix_time_str)
    df_walks['end_Timestamp'] = df_walks['end_Timestamp'].apply(fix_time_str)

    # No overwriting or manual manipulation — just concat and sort
    df_combined = pd.concat([df_events, df_walks], ignore_index=True)
    df_combined = df_combined.sort_values("start_Timestamp").reset_index(drop=True)
    df_combined.to_csv(out_path, index=False)
    print(f"✅ Final merged file written to {out_path}")

def strip_suffix(filename, suffix):
    return re.sub(f"{suffix}$", "", filename)


def batch_merge_events(events_dir, meta_dir, walks_dir, output_dir):
    events_dir = Path(events_dir)
    meta_dir =  Path(meta_dir)
    walks_dir =  Path(walks_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)


    # events_files = {strip_suffix(f.stem, "_events_flat"): f for f in events_dir.glob("*_events_flat.csv")}
    # walks_files = {strip_suffix(f.stem, "_walks"): f for f in walks_dir.glob("*_walks.csv")}
    # meta_files = {strip_suffix(f.stem, "_meta"): f for f in meta_dir.glob("*_meta.json")}

    # Find all meta and event files
    meta_files = {f.stem.replace("_processed_meta", ""): f for f in meta_dir.glob("*_meta.json")}
    events_files = {f.stem.replace("_events_flat", ""): f for f in events_dir.glob("*_events_flat.csv")}
    walks_files = {f.stem.replace("_walks", ""): f for f in walks_dir.glob("*_walks.csv")}
    #print('len meta files', meta_files)
    #print('len events files', events_files)
    #print('len walks files', walks_files)
    matched_keys = set(events_files) & set(walks_files) & set(meta_files)
    print(f"🔍 Found {len(matched_keys)} complete file sets.")

    for key in sorted(matched_keys):
        events_path = events_files[key]
        walks_path = walks_files[key]
        meta_path = meta_files[key]
        out_path = output_dir / f"{key}_events_with_walks.csv"
        print(f"➡️ Merging: {events_path.name} + {walks_path.name}")
        merge_events_and_walks(events_path, walks_path, meta_path, out_path)

# # Example usage
# morning = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day/pair_08/02_17_2025/Morning/MagicLeaps/ML2A'
# afternoon = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day/pair_08/02_17_2025/Afternoon/MagicLeaps/ML2G'
# output_dir = afternoon + "/flattened"

# batch_merge_events(output_dir, output_dir)

if __name__ == "__main__":
    base_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/ResurrectedData"
    #base_dir = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day'
    events_dir = os.path.join(base_dir, "Events_AugmentedPart3")
    meta_dir = os.path.join(base_dir, "MetaData_Flat")
    walks_dir = os.path.join(base_dir, "Events_ComputedWalks")
    output_dir = os.path.join(base_dir, "Events_AugmentedPart4")
    print("🚀 Starting batch flatten...")
    batch_merge_events(events_dir, meta_dir, walks_dir, output_dir)