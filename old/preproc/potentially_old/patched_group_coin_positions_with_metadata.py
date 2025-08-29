
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

    valid_files = None
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

    #print(valid_files)
    event_files = find_all_event_files(root_dir)
    #print(event_files)
    for path in event_files:
        #base_name = os.path.basename(path)
        #stripped_name = base_name.replace("_events.json", "").replace("_events.csv", "")
        fname = os.path.basename(path)
        base_source = fname.replace("_events.csv", ".csv").replace("_events.json", ".csv")
        if valid_files and base_source not in valid_files:
            print('valid_files and stripped_name not in valid_files')
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

        except Exception as e:
            print(f"⚠️ Error processing {path}: {e}")

    summary_lines = []
    for i, (coords, files) in enumerate(position_sets.items(), 1):
        summary_lines.append(f"Coin Set {i}: {len(files)} file(s)")
        for pos in sorted(coords):
            summary_lines.append(f"  {pos}")
        summary_lines.append("  ↳ Files:")
        summary_lines.extend([f"    - {os.path.relpath(f, root_dir)}" for f in files])
        summary_lines.append("")
        print(len(summary_lines))

    summary_text = "\n".join(summary_lines)
    print(summary_text)
    outpath = os.path.join(root_dir, output_summary)
    print(outpath)
    with open(os.path.join(root_dir, output_summary), "w") as f:
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
