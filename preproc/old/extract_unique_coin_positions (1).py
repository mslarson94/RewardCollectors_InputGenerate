
import os
import pandas as pd

def extract_unique_coin_positions(events_dir, output_path="unique_coin_positions.txt"):
    positions = set()

    for file in os.listdir(events_dir):
        if file.endswith("_events.json") or file.endswith("_events.csv"):
            full_path = os.path.join(events_dir, file)
            try:
                df = pd.read_json(full_path, lines=True) if file.endswith(".json") else pd.read_csv(full_path)
                pin_drops = df[df["event_type"] == "PinDrop"]
                for _, row in pin_drops.iterrows():
                    d = row.get("details", {})
                    x = d.get("pin_local_x")
                    z = d.get("pin_local_z")
                    if x is not None and z is not None:
                        positions.add((round(x, 1), round(z, 1)))
            except Exception as e:
                print(f"Failed to process {file}: {e}")

    output_file = os.path.join(events_dir, output_path)
    with open(output_file, "w") as f:
        for pos in sorted(positions):
            f.write(f"{pos}\n")

    print(f"✓ Extracted {len(positions)} unique coin positions to {output_file}")

if __name__ == "__main__":
    extract_unique_coin_positions("/path/to/events_dir")
