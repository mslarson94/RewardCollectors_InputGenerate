import os
import pandas as pd
import re
import json
from event_cascade_builder_AN import process_pin_drop

def flatten_event(event, source_file):
    """Flatten event dict for CSV summary."""
    flat = event.copy()
    flat["source_file"] = source_file
    # Flatten nested details dictionary (if any)
    if isinstance(flat.get("details"), dict):
        for k, v in flat["details"].items():
            flat[f"details_{k}"] = v
        flat.pop("details", None)
    return flat

def process_all_obsreward_files(root_dir, output_dir="EventCascades"):
    pattern = re.compile(r"ObsReward_[AB]_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*\.csv$")
    summary_rows = []

    os.makedirs(output_dir, exist_ok=True)

    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if pattern.match(fname):
                full_path = os.path.join(dirpath, fname)
                try:
                    df = pd.read_csv(full_path)
                    events = process_pin_drop(df)

                    # Save JSON file
                    output_fname = os.path.splitext(fname)[0] + ".json"
                    output_path = os.path.join(output_dir, output_fname)
                    with open(output_path, "w") as f:
                        json.dump(events, f, indent=2)

                    # Add to summary
                    for event in events:
                        summary_rows.append(flatten_event(event, fname))

                    print(f"✓ Processed: {fname} → {output_fname}")

                except Exception as e:
                    print(f"✗ Failed to process {fname}: {e}")

    # Save summary CSV
    if summary_rows:
        summary_df = pd.DataFrame(summary_rows)
        summary_df.to_csv(os.path.join(output_dir, "event_summary.csv"), index=False)
        print(f"\n📄 Summary saved to: {os.path.join(output_dir, 'event_summary.csv')}")
    else:
        print("⚠️ No events found to summarize.")

