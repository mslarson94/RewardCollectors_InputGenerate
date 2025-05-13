
import os
import pandas as pd
import ast
from collections import defaultdict

def group_files_by_coin_positions_auto(root_dir, output_summary="unique_coin_set_summary.txt"):
    position_sets = defaultdict(list)

    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if fname.endswith("_events.json") or fname.endswith("_events.csv"):
                path = os.path.join(dirpath, fname)
                try:
                    df = pd.read_json(path, lines=True) if fname.endswith(".json") else pd.read_csv(path)
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

    summary_text = "\n".join(summary_lines)
    print(summary_text)

    with open(os.path.join(root_dir, output_summary), "w") as f:
        f.write(summary_text)

# -- Meta Data extraction 
metadata_df = pd.read_excel(metadata_path, sheet_name="MagicLeapFiles")
total_loaded = len(metadata_df)

metadata_df = metadata_df.dropna(subset=["cleanedFile"])
metadata_df = metadata_df[metadata_df["currentRole"] == args.role]
metadata_df = metadata_df.rename(columns={"cleanedFile": "source_file"})

# Filter out rows where participantID or pairID == 'unknown'
metadata_df = metadata_df[
    (metadata_df["participantID"] != "unknown") &
    (metadata_df["pairID"] != "unknown")
]


if __name__ == "__main__":
    trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
    baseDir = 'SmallSelectedData/threePairs/ProcessedData/'
    rootDir = os.path.join(trueRootDir, baseDir)


    # -- Meta Data extraction 
    metadata_df = pd.read_excel(metadata_path, sheet_name="MagicLeapFiles")
    total_loaded = len(metadata_df)

    metadata_df = metadata_df.dropna(subset=["cleanedFile"])
    metadata_df = metadata_df[metadata_df["currentRole"] == args.role]
    metadata_df = metadata_df.rename(columns={"cleanedFile": "source_file"})

    # Filter out rows where participantID or pairID == 'unknown'
    metadata_df = metadata_df[
        (metadata_df["participantID"] != "unknown") &
        (metadata_df["pairID"] != "unknown")
    ]

    group_files_by_coin_positions_auto(rootDir)
