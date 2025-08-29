
# import os
# import pandas as pd

# def extract_unique_coin_positions(events_dir, output_path="unique_coin_positions.txt"):
#     positions = set()

#     for file in os.listdir(events_dir):
#         if file.endswith("_events.json") or file.endswith("_events.csv"):
#             full_path = os.path.join(events_dir, file)
#             try:
#                 df = pd.read_json(full_path, lines=True) if file.endswith(".json") else pd.read_csv(full_path)
#                 pin_drops = df[df["event_type"] == "PinDrop"]
#                 for _, row in pin_drops.iterrows():
#                     d = row.get("details", {})
#                     x = d.get("pin_local_x")
#                     z = d.get("pin_local_z")
#                     if x is not None and z is not None:
#                         positions.add((round(x, 1), round(z, 1)))
#             except Exception as e:
#                 print(f"Failed to process {file}: {e}")

#     output_file = os.path.join(events_dir, output_path)
#     with open(output_file, "w") as f:
#         for pos in sorted(positions):
#             f.write(f"{pos}\n")

#     print(f"✓ Extracted {len(positions)} unique coin positions to {output_file}")

# if __name__ == "__main__":
#     extract_unique_coin_positions("/path/to/events_dir")


import os
import pandas as pd
from collections import defaultdict

def group_files_by_coin_positions(events_dir, output_summary="unique_coin_set_summary.txt"):
    position_sets = defaultdict(list)

    for fname in os.listdir(events_dir):
        if fname.endswith("_events.json") or fname.endswith("_events.csv"):
            path = os.path.join(events_dir, fname)
            try:
                df = pd.read_json(path, lines=True) if fname.endswith(".json") else pd.read_csv(path)
                positions = set()

                for _, row in df.iterrows():
                    if row["event_type"] == "PinDrop":
                        d = row.get("details", {})
                        x = d.get("pin_local_x")
                        z = d.get("pin_local_z")
                        if x is not None and z is not None:
                            positions.add((round(x, 1), round(z, 1)))

                frozen = frozenset(sorted(positions))
                position_sets[frozen].append(fname)

            except Exception as e:
                print(f"⚠️ Error processing {fname}: {e}")

    summary_lines = []
    for i, (coords, files) in enumerate(position_sets.items(), 1):
        summary_lines.append(f"Coin Set {i}: {len(files)} file(s)")
        for pos in sorted(coords):
            summary_lines.append(f"  {pos}")
        summary_lines.append("  ↳ Files:")
        summary_lines.extend([f"    - {f}" for f in files])
        summary_lines.append("")

    summary_text = "\n".join(summary_lines)
    print(summary_text)

    with open(os.path.join(events_dir, output_summary), "w") as f:
        f.write(summary_text)

if __name__ == "__main__":
    INPUT_DIR = "/path/to/events"
    group_files_by_coin_positions(INPUT_DIR)
