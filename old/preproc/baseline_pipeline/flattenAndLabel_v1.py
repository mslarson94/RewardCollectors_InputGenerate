import pandas as pd
import json
import re
import ast
from pathlib import Path
import os


def parse_details(details):
    if isinstance(details, str) and details.startswith("{") and "'" in details:
        try:
            return ast.literal_eval(details)
        except Exception:
            return {}
    if isinstance(details, str):
        result = {}
        pairs = [seg.strip() for seg in details.split('|') if ':' in seg]
        for pair in pairs:
            key, val = pair.split(':', 1)
            key = key.strip()
            val = val.strip()
            if re.match(r"\(.*\)", val):
                nums = re.findall(r"-?\d+\.?\d*", val)
                if len(nums) == 3:
                    result[f"{key}_x"] = float(nums[0])
                    result[f"{key}_y"] = float(nums[1])
                    result[f"{key}_z"] = float(nums[2])
            else:
                try:
                    result[key] = float(val) if '.' in val else int(val)
                except ValueError:
                    result[key] = val
        return result
    return {}

def flatten_events(events_path, meta_path, out_path):
    with open(meta_path, 'r') as f:
        meta = json.load(f)

    df = pd.read_csv(events_path)
    details_expanded = df['details'].apply(parse_details).apply(pd.Series)
    df = pd.concat([df.drop(columns='details'), details_expanded], axis=1)

    # Annotate CoinRegistry if present
    registry = meta.get("CoinRegistry", {})
    if registry:
        coin_map = {}
        for k, val in registry.items():
            for coin in val:
                coords = tuple(round(float(c), 2) for c in coin)
                coin_map[coords] = int(k)

        def match_coin_type(row):
            if all(k in row for k in ['coinPos_x', 'coinPos_y', 'coinPos_z']):
                key = tuple(round(row[k], 2) for k in ['coinPos_x', 'coinPos_y', 'coinPos_z'])
                return coin_map.get(key)
            return None

        df['CoinRegistryType'] = df.apply(match_coin_type, axis=1)

    #Path(out_path).mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"✅ Flattened and annotated events written to {out_path}")

# Example usage:
# flatten_events("ObsReward_A_02_17_2025_15_11_processed_events.csv", "ObsReward_A_02_17_2025_15_11_processed_meta.json", "./outputs")



def batch_flatten_events(events_dir, meta_dir, output_dir):
    events_dir = Path(events_dir)
    meta_dir =  Path(meta_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all meta and event files
    meta_files = {f.stem.replace("_processed_meta", ""): f for f in meta_dir.glob("*_meta.json")}
    event_files = {f.stem.replace("_processed_events_augmented", ""): f for f in events_dir.glob("*_processed_events_augmented.csv")}

    # Match and process only pairs that exist
    matched_keys = set(meta_files) & set(event_files)
    print(f"🔍 Found {len(matched_keys)} matched file pairs to process.")

    for key in sorted(matched_keys):
        meta_path = meta_files[key]
        events_path = event_files[key]
        out_path = output_dir / f"{key}_events_flat.csv"
        print(f"➡️ Processing pair: {events_path.name} & {meta_path.name}")
        flatten_events(events_path, meta_path, out_path)


if __name__ == "__main__":
    base_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/ResurrectedData"
    #base_dir = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day'
    events_dir = os.path.join(base_dir, "Events_AugmentedPart1")
    meta_dir = os.path.join(base_dir, "MetaData_Flat")
    output_dir = os.path.join(base_dir, "Events_AugmentedPart3")
    print("🚀 Starting batch flatten...")
    batch_flatten_events(events_dir, meta_dir, output_dir)

