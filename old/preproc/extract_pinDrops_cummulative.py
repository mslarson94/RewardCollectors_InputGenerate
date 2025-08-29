import pandas as pd
import json
from pathlib import Path
from collections import defaultdict

# ===== USER INPUTS =====
root_dir = "/Users/mairahmac/Desktop/RC_TestingNotes"
base_dir = Path(root_dir) / "ResurrectedData"
events_dir = base_dir / "Events_AugmentedPart4"
meta_dir = base_dir / "MetaData_Flat"
output_dir = base_dir / "pin_drops"
output_dir.mkdir(parents=True, exist_ok=True)

# ===== MATCH FILE PAIRS =====
meta_files = {f.stem.replace("_processed_meta", ""): f for f in meta_dir.glob("*_processed_meta.json")}
event_files = {f.stem.replace("_events_with_walks", ""): f for f in events_dir.glob("*_events_with_walks.csv")}
matched_keys = set(meta_files) & set(event_files)

print(f"🔍 Found {len(matched_keys)} matched file pairs to process.")

# Store per-participant pin drop data
participant_data = defaultdict(list)

# ===== PROCESS EACH MATCHED FILE PAIR =====
for key in sorted(matched_keys):
    events_file = event_files[key]
    meta_file = meta_files[key]

    try:
        events_df = pd.read_csv(events_file)
        with open(meta_file) as f:
            meta_data = json.load(f)
    except Exception as e:
        print(f"❌ Failed to load files for {key}: {e}")
        continue

    try:
        if "currentRole" in events_df.columns:
            current_role = events_df["currentRole"].dropna().iloc[0]
        else:
            current_role = "unknown"

        identifiers = {
            "participantID": meta_data.get("participantID"),
            "pairID": meta_data.get("pairID"),
            "testingDate": meta_data.get("testingDate"),
            "sessionType": meta_data.get("sessionType"),
            "main_RR": meta_data.get("main_RR"),
            "coinSet": meta_data.get("coinSet"),
            "device": meta_data.get("device"),
            "currentRole": current_role,
            "source_file": key
        }

        if "dropDist" not in events_df.columns:
            continue
        pin_drops = events_df[events_df["dropDist"].notna()].copy()
        if pin_drops.empty:
            continue

        pin_cols = [
            "ParsedTimestamp", "AN_ParsedTS", "BlockNum", "RoundNum",
            "BlockElapsedTime", "RoundElapsedTime", "SessionElapsedTime",
            "dropDist", "dropQual", "coinLabel","chestPin_num",
            "pinLocal_x", "pinLocal_y", "pinLocal_z",
            "coinPos_x", "coinPos_y", "coinPos_z"
        ]
        available_cols = [col for col in pin_cols if col in pin_drops.columns]

        for _, row in pin_drops.iterrows():
            pin_data = {col: row[col] for col in available_cols}
            pin_data.update(identifiers)
            participant_data[identifiers["participantID"]].append(pin_data)

    except Exception as e:
        print(f"⚠️ Error processing {key}: {e}")
        continue

# ===== COMPUTE TRUE SESSION TIME =====
for participant_id, rows in participant_data.items():
    df = pd.DataFrame(rows)

    # Ensure timestamp is datetime
    df["ParsedTimestamp"] = pd.to_datetime(df["ParsedTimestamp"], errors='coerce')

    # Sort by timestamp within each testing session
    df.sort_values(by=["testingDate", "coinSet", "ParsedTimestamp", "source_file"], inplace=True)

    df["TrueSessionElapsedTime"] = 0.0

    # Group by session per coin set and date
    group_cols = ["testingDate", "coinSet"]
    for _, group_idx in df.groupby(group_cols).groups.items():
        group_df = df.loc[group_idx]
        last_file = None
        cumulative_time = 0.0
        session_offset = 0.0

        for idx, row in group_df.iterrows():
            current_file = row["source_file"]
            current_elapsed = row["SessionElapsedTime"]

            if current_file != last_file:
                session_offset = cumulative_time - current_elapsed
                last_file = current_file

            true_elapsed = current_elapsed + session_offset
            df.at[idx, "TrueSessionElapsedTime"] = true_elapsed
            cumulative_time = max(cumulative_time, true_elapsed)

    # Save output
    out_path = output_dir / f"{participant_id}_pin_drops.csv"
    df.to_csv(out_path, index=False)
    print(f"✅ Saved {len(df)} pin drops for {participant_id} → {out_path}")

    # # Save output
    # out_path = output_dir / f"{participant_id}_pin_drops.csv"
    # df.to_csv(out_path, index=False)
    # print(f"✅ Saved {len(df)} pin drops for {participant_id} → {out_path}")
