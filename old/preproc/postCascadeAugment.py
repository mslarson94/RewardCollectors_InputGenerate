import os
import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional

known_start_positions = {
    'pos1': [(0.0, 5.0), (5.0, 0.0)],
    'pos2': [(3.5, 3.5), (3.5, -3.5)],
    'pos3': [(5.0, 0.0), (0.0, -5.0)],
    'pos4': [(3.5, -3.5), (-3.5, -3.5)],
    'pos5': [(0.0, -5.0), (-5.0, 0.0)],
    'pos6': [(-3.5, -3.5), (-3.5, 3.5)],
    'pos7': [(-5.0, 0.0), (0.0, 5.0)],
    'pos8': [(-3.5, 3.5), (3.5, 3.5)],
    'tutorial_pos': [(0, -5), (2, -5)],
    'dummy': [(0, 0), (0, 0)]
}


def classify_coin_type(CoinSetID: int, idvCoinID: int) -> str:
    if CoinSetID == 2 and idvCoinID == 2:
        return "PPE"
    elif CoinSetID == 3 and idvCoinID == 0:
        return "NPE"
    elif CoinSetID == 1 or (CoinSetID in [2, 3] and idvCoinID in [0, 1]):
        return "Normal"
    elif CoinSetID == 4 or (CoinSetID == 5 and idvCoinID == 1):
        return "TutorialNorm"
    elif CoinSetID == 5 and idvCoinID in [0, 2]:
        return "TutorialRPE"
    return "Unknown"

def extract_coin_registry_from_meta(meta: Dict) -> Dict[int, List[Dict[str, float]]]:
    registry = {}
    for set_id, coins in meta.get("CoinRegistry", {}).items():
        registry[int(set_id)] = [
            {
                "deltax": float(c["deltax"]),
                "deltay": float(c["deltay"]),
                "deltaz": float(c["deltaz"])
            }
            for c in coins.values()
        ]
    return registry

def find_closest_coin(pindrop_event: Dict, coinpoints: List[Dict[str, float]]) -> Tuple[int, Dict[str, float]]:
    if "details" not in pindrop_event or "delta_position" not in pindrop_event["details"]:
        return -1, {}
    pdx, pdy, pdz = pindrop_event["details"]["delta_position"]
    distances = [
        np.sqrt((pdx - c["deltax"]) ** 2 + (pdy - c["deltay"]) ** 2 + (pdz - c["deltaz"]) ** 2)
        for c in coinpoints
    ]
    if not distances:
        return -1, {}
    min_idx = int(np.argmin(distances))
    return min_idx, coinpoints[min_idx]

def get_last_position_v1(original_row_start: int, df: pd.DataFrame) -> Optional[Dict[str, float]]:
    if pd.isna(original_row_start) or original_row_start <= 0:
        return None
    last_row_idx = int(original_row_start) - 1
    if last_row_idx < 0 or last_row_idx >= len(df):
        return None
    row = df.iloc[last_row_idx]
    position_data = {}
    for axis in ['x', 'y', 'z']:
        for prefix in ['head', 'pin']:
            colname = f'{prefix}_position_{axis}'
            if colname in row:
                position_data[f'{prefix}_{axis}'] = row[colname]
    return position_data

def get_last_position_v2(original_row_start: int, df: pd.DataFrame) -> Optional[Dict[str, float]]:
    if pd.isna(original_row_start) or original_row_start <= 0:
        return None
    last_row_idx = int(original_row_start) - 1
    if last_row_idx < 0 or last_row_idx >= len(df):
        return None
    row = df.iloc[last_row_idx]
    position_data = {}

    if 'HeadPosAnchored' in row:
        parts = str(row['HeadPosAnchored']).strip().split()
        if len(parts) == 3:
            position_data['head_x'] = float(parts[0])
            position_data['head_y'] = float(parts[1])
            position_data['head_z'] = float(parts[2])
    # If needed, also parse pin position if it's in another column:
    # if 'PinPosAnchored' in row:
    #     parts = str(row['PinPosAnchored']).strip().split()
    #     if len(parts) == 3:
    #         position_data['pin_x'] = float(parts[0])
    #         position_data['pin_y'] = float(parts[1])
    #         position_data['pin_z'] = float(parts[2])

    return position_data if position_data else None

def get_last_position_v3(original_row_start: int, df: pd.DataFrame) -> Optional[Dict[str, float]]:
    if pd.isna(original_row_start) or original_row_start <= 0:
        return None

    last_row_idx = int(original_row_start) - 1

    # Loop backwards to find a row with valid position data
    while last_row_idx >= 0:
        row = df.iloc[last_row_idx]

        # Check for HeadPosAnchored column
        if pd.notna(row.get('HeadPosAnchored')):
            parts = str(row['HeadPosAnchored']).strip().split()
            if len(parts) == 3:
                return {
                    'head_x': float(parts[0]),
                    'head_y': float(parts[1]),
                    'head_z': float(parts[2])
                }

        # Add PinPosAnchored logic here if needed
        # (skip for now unless needed)

        last_row_idx -= 1

    # If no valid position found
    return None

def get_last_position(original_row_start: int, df: pd.DataFrame) -> Optional[Dict[str, float]]:
    if df.empty:
        return None
    if pd.isna(original_row_start) or original_row_start <= 0:
        return None

    last_row_idx = int(original_row_start) - 1

    if last_row_idx < 0 or last_row_idx >= len(df):
        return None

    # Loop backwards to find a row with valid position data
    while last_row_idx >= 0:
        row = df.iloc[last_row_idx]

        # Check for HeadPosAnchored column
        if pd.notna(row.get('HeadPosAnchored')):
            parts = str(row['HeadPosAnchored']).strip().split()
            if len(parts) == 3:
                return {
                    'head_x': float(parts[0]),
                    'head_y': float(parts[1]),
                    'head_z': float(parts[2])
                }

        last_row_idx -= 1

    # If no valid position found
    return None

def augment_events_with_coin_proximity_v1(
    merged_events_path: Path,
    merged_meta_path: Path,
    source_data_dir: Path,
    output_json_path: Path,
    output_csv_path: Path,
    flat_csv_path: Path
    ):
    # Load merged events
    events = []
    with merged_events_path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))

    # Load metadata
    meta = json.loads(merged_meta_path.read_text())
    registry = extract_coin_registry_from_meta(meta)
    participant_start_positions = {}
    csv_cache: Dict[str, pd.DataFrame] = {}

    # Augment events
    for evt in events:
        source_file = evt.get("source_file")
        if not source_file:
            continue
        print(f"✅ Completed processing {subdir.name}")

        if source_file not in csv_cache:
            csv_path = source_data_dir / source_file
            if csv_path.exists():
                csv_cache[source_file] = pd.read_csv(csv_path)
            else:
                print(f"Warning: Source CSV not found: {csv_path}")
                csv_cache[source_file] = pd.DataFrame()

        df = csv_cache[source_file]

        # Last position extraction
        original_row_start = evt.get("original_row_start")
        last_position = get_last_position(original_row_start, df)
        evt["real_time_row"] = last_position is not None
        evt["last_position_before_event"] = last_position

        # Coin classification
        if evt.get("lo_eventType") == "PinDropped":
            coinset_id = int(evt.get("CoinSetID", -1))
            if coinset_id in registry:
                idx, coin = find_closest_coin(evt, registry[coinset_id])
                coinType = classify_coin_type(coinset_id, idx)
                if "details" not in evt:
                    evt["details"] = {}
                evt["details"]["idvCoinID"] = idx
                evt["details"]["coinType"] = coinType
                evt["details"]["drop_distance_manual"] = coin

        # For Dijkstra
        if evt.get("RoundNum") == 8888:
            participant_id = evt.get("participantID")
            if participant_id and participant_id not in participant_start_positions:
                if last_position is not None:
                    participant_start_positions[participant_id] = last_position

    # Save JSON output
    with open(output_json_path, "w") as f:
        json.dump(events, f, indent=2)

    # Convert events to DataFrame
    events_df = pd.json_normalize(events)

    # Expand last position columns into separate columns
    if 'last_position_before_event' in events_df.columns:
        last_pos_df = events_df['last_position_before_event'].apply(pd.Series)
        last_pos_df.columns = [f"last_{col}" for col in last_pos_df.columns]
        events_df = pd.concat([events_df.drop(columns=['last_position_before_event']), last_pos_df], axis=1)

    # Save CSV output
    events_df.to_csv(output_csv_path, index=False)
    events_df.to_csv(flat_csv_path, index=False)

    # Print Dijkstra start positions
    print("Start positions at RoundNum==8888:")
    for pid, pos in participant_start_positions.items():
        print(f"Participant {pid}: {pos}")

import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional

def augment_events_with_coin_proximity(
    merged_events_path: Path,
    merged_meta_path: Path,
    source_data_dir: Path,
    output_json_path: Path,
    output_csv_path: Path,
    flat_csv_path: Path
    ):
    print("🔹 Augmenting events with coin proximity...")
    outFileBaseName = os.path.basename(os.path.dirname(merged_events_path))
    print(outFileBaseName)
    # Load merged events
    events = []
    with merged_events_path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))

    # Load metadata
    meta = json.loads(merged_meta_path.read_text())
    registry = extract_coin_registry_from_meta(meta)
    participant_start_positions = {}
    csv_cache: Dict[str, pd.DataFrame] = {}

    # Augment events
    for evt in events:
        source_file = evt.get("source_file")
        if not source_file:
            print(f"⚠️ Skipping event {evt.get('event_id', 'unknown')} — no source_file found.")
            continue
        #print(f"✅ Completed processing {subdir.name}")

        if source_file not in csv_cache:
            csv_path = source_data_dir / source_file
            if csv_path.exists():
                print('reading in the source file')
                csv_cache[source_file] = pd.read_csv(csv_path)
            else:
                print(f"Warning: Source CSV not found: {csv_path}")
                csv_cache[source_file] = pd.DataFrame()

        df = csv_cache[source_file]
        #print(len(df))
        # Last position extraction
        original_row_start = evt.get("original_row_start")
        last_position = get_last_position(original_row_start, df)
        evt["real_time_row"] = last_position is not None

        # Embed last position into 'details'
        if not isinstance(evt.get("details"), dict):
            evt["details"] = {}
        evt["details"]["last_position_before_event"] = last_position

        # Coin classification if PinDropped
        if evt.get("lo_eventType") == "PinDropped":
            coinset_id = int(evt.get("CoinSetID", -1))
            if coinset_id in registry:
                idx, coin = find_closest_coin(evt, registry[coinset_id])
                coinType = classify_coin_type(coinset_id, idx)
                evt["details"]["idvCoinID"] = idx
                evt["details"]["coinType"] = coinType
                evt["details"]["drop_distance_manual"] = coin


        # For Dijkstra start positions
        if evt.get("RoundNum") == 8888:
            participant_id = evt.get("participantID")
            if participant_id and participant_id not in participant_start_positions:
                if last_position is not None:
                    participant_start_positions[participant_id] = last_position

            # Also classify start position label at RoundNum==8888
            if last_position:
                px = last_position.get("head_x")
                pz = last_position.get("head_z")
                if px is not None and pz is not None:
                    min_dist = float('inf')
                    assigned_pos_label = "unknown"
                    for label, positions in known_start_positions.items():
                        for x, z in positions:
                            dist = np.sqrt((px - x)**2 + (pz - z)**2)
                            if dist < min_dist:
                                min_dist = dist
                                assigned_pos_label = label
                    if not isinstance(evt.get("details"), dict):
                        evt["details"] = {}
                    evt["details"]["start_position_label"] = assigned_pos_label


    # Save augmented JSON
    with open(output_json_path, "w") as f:
        json.dump(events, f, indent=2)

    records = []
    for evt in events:
        # Copy event and pop 'details' for separate handling
        evt_copy = evt.copy()
        details = evt_copy.pop('details', {})
        if not isinstance(details, dict):
            details = {}
        evt_copy['details'] = json.dumps(details)
        records.append(evt_copy)

    events_df = pd.DataFrame(records)


    flat_csv_path = flatOutpath + '/' + outFileBaseName + '.csv'
    # Save augmented CSV
    events_df.to_csv(output_csv_path, index=False)
    events_df.to_csv(flat_csv_path, index=False)
    # Print Dijkstra start positions
    print("Start positions at RoundNum==8888:")
    for pid, pos in participant_start_positions.items():
        print(f"Participant {pid}: {pos}")
    csv_cache.clear()
    print("🔹 CSV cache cleared after participant.")

# 📝 Don't forget to adjust get_last_position() to handle your HeadPosAnchored column properly as before.


# Example usage
if __name__ == "__main__":
    #baseDirName = "SmallSelectedData/RNS/alignedPO"
    baseDirName = "SelectedData"
    eventsFileName = "R037_MergedEvents_Morning.csv"
    #eventsFileName = "R019_MergedEvents_Morning"
    trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
    ##############
    baseDir = os.path.join(trueRootDir, baseDirName)
    print(baseDir)
    #eventsDir = os.path.join(baseDir, "alignedMergedPO")
    procData = os.path.join(baseDir, 'ProcessedData_Flat')
    #subID = "R037"

    #trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
    #procDir = 'SmallSelectedData/RNS/alignedPO'
    procDir = Path(baseDir) / 'ProcessedData_Flat'
    #v1_outDir = os.path.join(trueRootDir, procDir, "MergedEvents_V1/R037_03_17_2025_Morning_A_ML2D")
    v2_outDir = os.path.join(trueRootDir, procDir, "MergedEvents_V2")
    v3_outDir = os.path.join(trueRootDir, procDir, "MergedEvents_V3")
    #eventsFile = os.path.join(eventsDir, eventsFileName)
    #print(eventsFile)
    #events_df = pd.read_csv(eventsFile)

    #outDir = os.path.join(baseDir, "alignedPO_MarksCylinders")

    #print(outDir)
    #os.makedirs(outDir, exist_ok=True)

    #v1_outDir = Path(baseDir) /  "MergedEvents_V1_augment"
    v1_inDir = Path(baseDir) / "MergedEvents_V1"
    print(v1_inDir)
    flatOutpath = os.path.join(baseDir, "MergedEvents_V1Flat_augment")
    #os.makedirs(v1_outDir, exist_ok=True)
    os.makedirs(flatOutpath, exist_ok=True)
    for subdir in v1_inDir.iterdir():
        if subdir.is_dir():
            merged_events_json = subdir / "merged_events.json"
            merged_meta_json = subdir / "merged_meta.json"
            output_json = subdir / "merged_events_augmented.json"
            output_csv = subdir / "merged_events_augmented.csv"

            if not merged_events_json.exists() or not merged_meta_json.exists():
                print(f"⚠️ Skipping {subdir.name}: missing JSON files.")
                continue

            try:
                print(f"Processing: {subdir.name}")
                augment_events_with_coin_proximity(
                    merged_events_path=merged_events_json,
                    merged_meta_path=merged_meta_json,
                    source_data_dir=Path(procDir),
                    output_json_path=output_json,
                    output_csv_path=output_csv,
                    flat_csv_path=Path(flatOutpath)
                )
                print(f"✅ Completed processing {subdir.name}")
            except Exception as e:
                print(f"❌ Error processing {subdir.name}: {e}")

    print(os.path.join(v1_inDir, "merged_events_augmented.json"))
# # Example usage
# if __name__ == "__main__":
#     print("Script is running!")
#     main()