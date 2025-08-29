import pandas as pd
import numpy as np
import json
import re
import ast
from pathlib import Path
import os


# ---- details parsing ---------------------------------------------------------
def parse_details(details):
    """
    Parse the 'details' column which may be:
      - a python-literal string dict like "{'k': 1, 'coinPos': (x y z)}"
      - a 'key: val | key2: (x y z)' pipe-delimited string
    Produces flat columns; 3D tuples become *_x, *_y, *_z.
    """
    # Fast path: python-literal dict as string
    if isinstance(details, str) and details.startswith("{") and "'" in details:
        try:
            val = ast.literal_eval(details)
            # normalize any 3D tuple-ish items into _x/_y/_z keys
            out = {}
            for k, v in val.items():
                if isinstance(v, (tuple, list)) and len(v) == 3:
                    out[f"{k}_x"], out[f"{k}_y"], out[f"{k}_z"] = float(v[0]), float(v[1]), float(v[2])
                else:
                    out[k] = v
            return out
        except Exception:
            # fall through to pipe-delimited parsing
            pass

    if isinstance(details, str):
        result = {}
        pairs = [seg.strip() for seg in details.split('|') if ':' in seg]
        for pair in pairs:
            key, val = pair.split(':', 1)
            key = key.strip()
            val = val.strip()
            if re.match(r"\(.*\)", val):  # "(x y z)" form
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


# ---- coin registry labeling (robust NN) -------------------------------------
def _build_registry_arrays(registry_dict):
    """
    registry_dict shape (from meta):
      {
        "4": {"0": {"deltax":..,"deltay":..,"deltaz":..}, "1": {...}, "2": {...}},
        "1": {...}, ...
      }
    Returns:
      reg_arrays: { "4": [np.array([dx,dy,dz]), np.array(...), np.array(...)] , ... }
      order is by numeric index 0,1,2
    """
    out = {}
    for set_id, entries in registry_dict.items():
        arr = []
        for idx_str, vec in entries.items():
            try:
                idx = int(idx_str)
                arr.append(
                    (idx, np.array([float(vec['deltax']), float(vec['deltay']), float(vec['deltaz'])], dtype=float))
                )
            except Exception:
                continue
        arr.sort(key=lambda t: t[0])  # ensure 0,1,2 order
        out[str(set_id)] = [v for _, v in arr]
    return out


def _assign_coin_label_row(row, reg_arrays, distance_threshold=None):
    """
    Per-row labeler:
      - Requires CoinSetID and coinPos_x/y/z
      - Chooses nearest of the 3 registry positions in that CoinSetID
      - Returns coinIndex (0/1/2), coinLabel (LV/NV/HV), coinRegistryDist (float)
      - If distance_threshold set and best distance > threshold, leaves label empty (None)
    """
    try:
        set_key = str(int(row.get('CoinSetID')))
    except Exception:
        return pd.Series({'coinLabel': None, 'coinIndex': np.nan, 'coinRegistryDist': np.nan})

    if set_key not in reg_arrays:
        return pd.Series({'coinLabel': None, 'coinIndex': np.nan, 'coinRegistryDist': np.nan})

    needed = ('coinPos_x', 'coinPos_y', 'coinPos_z')
    if not all(k in row and pd.notna(row[k]) for k in needed):
        return pd.Series({'coinLabel': None, 'coinIndex': np.nan, 'coinRegistryDist': np.nan})

    try:
        pos = np.array([float(row['coinPos_x']), float(row['coinPos_y']), float(row['coinPos_z'])], dtype=float)
    except Exception:
        return pd.Series({'coinLabel': None, 'coinIndex': np.nan, 'coinRegistryDist': np.nan})

    best_i, best_d = None, float('inf')
    for i, ref in enumerate(reg_arrays[set_key]):
        d = float(np.linalg.norm(pos - ref))
        if d < best_d:
            best_d, best_i = d, i

    if distance_threshold is not None and best_d > float(distance_threshold):
        return pd.Series({'coinLabel': None, 'coinIndex': np.nan, 'coinRegistryDist': best_d})

    labels = {0: "LV", 1: "NV", 2: "HV"}
    return pd.Series({
        'coinLabel': labels.get(best_i),
        'coinIndex': best_i,
        'coinRegistryDist': best_d
    })


# ---- core flatten function ---------------------------------------------------
def flatten_events(events_path, meta_path, out_path, distance_threshold=None):
    """
    - Expands 'details' into columns.
    - Labels coins via nearest-neighbor in CoinRegistry per CoinSetID.
    - Writes a single flattened CSV.
    """
    with open(meta_path, 'r') as f:
        meta = json.load(f)

    df = pd.read_csv(events_path)

    # Expand details
    details_expanded = df['details'].apply(parse_details).apply(pd.Series)
    df = pd.concat([df.drop(columns='details'), details_expanded], axis=1)

    # Coin labeling (robust)
    registry = meta.get("CoinRegistry", {})
    if registry:
        reg_arrays = _build_registry_arrays(registry)
        labeled = df.apply(_assign_coin_label_row, axis=1, reg_arrays=reg_arrays, distance_threshold=distance_threshold)
        df = pd.concat([df, labeled], axis=1)

        # (Optional) keep a numeric type like your previous 'CoinRegistryType'
        df['CoinRegistryType'] = df['coinIndex']

        # Quick QC summary
        assigned = int(df['coinLabel'].notna().sum())
        print(f"🧭 coinLabel assigned on {assigned} rows"
              + (f" (threshold={distance_threshold})" if distance_threshold is not None else ""))

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"✅ Flattened and annotated events written to {out_path}")


# ---- batch driver ------------------------------------------------------------
def batch_flatten_events(events_dir, meta_dir, output_dir, distance_threshold=None):
    events_dir = Path(events_dir)
    meta_dir = Path(meta_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Match keys like "ObsReward_A_02_17_2025_15_11"
    meta_files = {f.stem.replace("_processed_meta", ""): f for f in meta_dir.glob("*_meta.json")}
    event_files = {f.stem.replace("_processed_events_augmented", ""): f for f in events_dir.glob("*_processed_events_augmented.csv")}

    matched_keys = set(meta_files) & set(event_files)
    print(f"🔍 Found {len(matched_keys)} matched file pairs to process.")

    for key in sorted(matched_keys):
        meta_path = meta_files[key]
        events_path = event_files[key]
        out_path = output_dir / f"{key}_events_flat.csv"
        print(f"➡️ Processing pair: {events_path.name} & {meta_path.name}")
        flatten_events(events_path, meta_path, out_path, distance_threshold=distance_threshold)


if __name__ == "__main__":
    base_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/ResurrectedData"
    events_dir = os.path.join(base_dir, "Events_AugmentedPart1")  # aligned, augmented events
    meta_dir = os.path.join(base_dir, "MetaData_Flat")
    output_dir = os.path.join(base_dir, "Events_AugmentedPart3")

    print("🚀 Starting batch flatten...")
    # Optionally set a tolerance (in meters) to drop dubious matches:
    # e.g., distance_threshold=0.75
    batch_flatten_events(events_dir, meta_dir, output_dir, distance_threshold=None)
