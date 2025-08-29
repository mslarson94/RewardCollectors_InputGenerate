
import os
import pandas as pd
import ast
from collections import defaultdict

def find_all_event_files(root_dir):
    event_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if fname.endswith("_events.json") or fname.endswith("_events.csv"):
                event_files.append(os.path.join(dirpath, fname))
    return event_files

def group_files_by_coin_positions_auto(root_dir, metadata_path=None, output_summary="unique_coin_set_summary.txt"):
    print('group_files_by_coin_positions_auto starting!')
    position_sets = defaultdict(list)
    coin_group_map = defaultdict(lambda: defaultdict(list))  # CoinSet -> CoinGroup -> files

    valid_files = None
    coin_group_lookup = {}
    if metadata_path:
        print('metadata_path works')
        metadata_df = pd.read_excel(metadata_path, sheet_name="MagicLeapFiles")
        metadata_df = metadata_df.dropna(subset=["cleanedFile"])
        metadata_df = metadata_df[
            (metadata_df["participantID"] != "none") &
            (metadata_df["pairID"] != "none")
        ]
        metadata_df = metadata_df.rename(columns={"cleanedFile": "source_file"})
        valid_files = set(metadata_df["source_file"])
        coin_group_lookup = metadata_df.set_index("source_file")["CoinSetLabel"].to_dict()

    event_files = find_all_event_files(root_dir)
    for path in event_files:
        fname = os.path.basename(path)
        base_source = fname.replace("_events.csv", ".csv").replace("_events.json", ".csv")
        if valid_files and base_source not in valid_files:
            continue
        try:
            df = pd.read_json(path, lines=True) if path.endswith(".json") else pd.read_csv(path)
            positions = set()

            for _, row in df.iterrows():
                if row["event_type"] == "PinDrop":
                    details = row["details"]
                    if isinstance(details, str):
                        try:
                            details = ast.literal_eval(details)
                        except Exception:
                            details = {}
                    if isinstance(details, dict):
                        x = details.get("coin_pos_x")
                        z = details.get("coin_pos_z")
                        if x is not None and z is not None:
                            positions.add((round(x, 1), round(z, 1)))

            frozen = frozenset(sorted(positions))
            position_sets[frozen].append(path)

            # Group by CoinSetLabel from metadata
            group = coin_group_lookup.get(base_source, "Unknown")
            coin_group_map[frozen][group].append(path)

        except Exception as e:
            print(f"⚠️ Error processing {path}: {e}")

    summary_lines = []
    for i, (coords, groupings) in enumerate(coin_group_map.items(), 1):
        total_files = sum(len(files) for files in groupings.values())
        summary_lines.append(f"Coin Set {i}: {total_files} file(s)")
        for pos in sorted(coords):
            summary_lines.append(f"  {pos}")
        for group, files in groupings.items():
            summary_lines.append(f"  ↳ CoinGroup: {group}")
            summary_lines.extend([f"    - {os.path.relpath(f, root_dir)}" for f in files])
        summary_lines.append("")

    summary_text = "\n".join(summary_lines)
    print(summary_text)
    outpath = os.path.join(root_dir, output_summary)
    with open(outpath, "w") as f:
        f.write(summary_text)

if __name__ == "__main__":
    print('starting!')
    trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
    baseDir = 'SmallSelectedData/threePairs/ExtractedEvents/'
    eventsDir = os.path.join(trueRootDir, baseDir)
    metaDataFile = os.path.join(trueRootDir, 'collatedData.xlsx')

    ROOT_DIR = eventsDir
    METADATA_PATH = metaDataFile
    group_files_by_coin_positions_auto(ROOT_DIR, METADATA_PATH)
