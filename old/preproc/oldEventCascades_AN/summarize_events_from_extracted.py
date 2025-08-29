
import os
import pandas as pd
import json
import re

def summarize_event_logs(event_root_dir, output_summary_csv):
    pattern = re.compile(r"ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_events\.csv$")
    summaries = []

    for dirpath, _, filenames in os.walk(event_root_dir):
        for fname in filenames:
            if pattern.match(fname):
                fpath = os.path.join(dirpath, fname)
                try:
                    df = pd.read_csv(fpath)

                    if "event_type" not in df.columns:
                        print(f"⚠️ Skipping file with no event_type column: {fname}")
                        continue

                    event_counts = df["event_type"].value_counts().to_dict()

                    metadata = {
                        "source_file": fname,
                        "participant_id": df.get("participant_id", [None])[0],
                        "session": df.get("session", [None])[0],
                        "role": df.get("role", [None])[0],
                        "relative_path": df.get("relative_path", [None])[0]
                    }

                    summary_row = {**metadata, **event_counts}
                    summaries.append(summary_row)

                except Exception as e:
                    print(f"✗ Failed to summarize {fname}: {e}")

    summary_df = pd.DataFrame(summaries).fillna(0)
    summary_df.to_csv(output_summary_csv, index=False)
    print(f"📊 Summary saved to {output_summary_csv}")
