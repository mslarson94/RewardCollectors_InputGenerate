
import os
import pandas as pd
from collections import defaultdict

def summarize_event_file(processed_path, events_path):
    summary = {
        "source_file": os.path.basename(processed_path),
        "event_counts": defaultdict(int),
        "coin_positions": set(),
        "participant_id": None,
        "pair_id": None,
    }

    # Load raw processed CSV (just for metadata and positions)
    raw_df = pd.read_csv(processed_path)

    # Guess participant and pair ID from filename
    base = os.path.basename(processed_path)
    parts = base.split("_")
    if len(parts) >= 3:
        summary["participant_id"] = parts[1]
        summary["pair_id"] = parts[0]

    # Load events file
    if events_path.endswith(".json"):
        events_df = pd.read_json(events_path, lines=True)
    else:
        events_df = pd.read_csv(events_path)

    # Count events
    for evt in events_df["event_type"].unique():
        summary["event_counts"][evt] = events_df[events_df["event_type"] == evt].shape[0]

    # Extract (x, z) positions from pin drops
    for _, row in events_df.iterrows():
        if row["event_type"] == "PinDrop":
            d = row.get("details", {})
            x = d.get("pin_local_x")
            z = d.get("pin_local_z")
            if x is not None and z is not None:
                summary["coin_positions"].add((round(x, 1), round(z, 1)))

    return summary

if __name__ == "__main__":
    # Example usage
    processed_csv = "/path/to/file_processed.csv"
    events_file = "/path/to/file_events.json"
    summary = summarize_event_file(processed_csv, events_file)
    for k, v in summary.items():
        print(f"{k}: {v}")
