
import os
import pandas as pd
from collections import defaultdict

def find_all_event_files(root_dir):
    event_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if fname.endswith("_events.csv") or fname.endswith("_events.json"):
                event_files.append(os.path.join(dirpath, fname))
    return event_files

def group_files_by_coin_positions_auto(root_dir, output_summary="unique_coin_set_summary.txt"):
    position_sets = defaultdict(list)

    event_files = find_all_event_files(root_dir)
    for path in event_files:
        try:
            df = pd.read_json(path, lines=True) if path.endswith(".json") else pd.read_csv(path)
            positions = set()

            for _, row in df.iterrows():
                if row["event_type"] == "PinDrop":
                    if isinstance(row["details"], str):
                        try:
                            d = ast.literal_eval(row["details"])
                        except Exception:
                            d = {}
                    else:
                        d = row["details"]
                    x = d.get("coin_pos_x")
                    z = d.get("coin_pos_z")
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

if __name__ == "__main__":
    ROOT_DIR = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/twopairs/ExtractedEvents"
    #group_files_by_coin_positions_auto(ROOT_DIR)
    df = pd.read_csv("/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/twopairs/ExtractedEvents/pair_008/02_17_2025/Morning/MagicLeaps/ML2A/ObsReward_A_02_17_2025_15_11_processed.csv_events.csv")
    print(df[df["event_type"] == "PinDrop"]["details"])

