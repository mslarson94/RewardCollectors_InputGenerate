
import pandas as pd
import os
import json

def extract_event_segments_json(processed_path, events_path, output_path, pad_before=0.0, pad_after=0.0):
    # Load data
    processed_df = pd.read_csv(processed_path)
    events_df = pd.read_json(events_path, lines=True)

    # Ensure proper float AppTime
    processed_df["AppTime"] = pd.to_numeric(processed_df["AppTime"], errors="coerce")
    events_df["AppTime"] = pd.to_numeric(events_df["AppTime"], errors="coerce")

    # Filter for start/end events only
    start_events = events_df[events_df["event_type"].str.endswith("_start")]
    end_events = events_df[events_df["event_type"].str.endswith("_end")]

    segments = []
    for _, start_row in start_events.iterrows():
        prefix = start_row["event_type"].replace("_start", "")
        cascade_id = start_row.get("cascade_id", None)
        segment_group = prefix

        match = end_events[
            (end_events["event_type"] == f"{prefix}_end") &
            (end_events.get("cascade_id") == cascade_id)
        ]
        if match.empty:
            continue
        end_row = match.iloc[0]

        start_time = float(start_row["AppTime"]) - pad_before
        end_time = float(end_row["AppTime"]) + pad_after

        segment = processed_df[
            (processed_df["AppTime"] >= start_time) &
            (processed_df["AppTime"] <= end_time)
        ].copy()

        segment["cascade_id"] = cascade_id
        segment["segment_group"] = segment_group
        segment["start_AppTime"] = start_row["AppTime"]
        segment["end_AppTime"] = end_row["AppTime"]
        segment["segment_index"] = f"{cascade_id}_{segment_group}"

        segments.append(segment)

    if segments:
        combined = pd.concat(segments, ignore_index=True)
        combined.to_csv(output_path, index=False)
        print(f"✅ Extracted {len(segments)} segments to {output_path}")
    else:
        print("⚠ No matching segments found.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--processed", required=True, help="Path to _processed.csv")
    parser.add_argument("--events_json", required=True, help="Path to _events.json (line-delimited)")
    parser.add_argument("--output", required=True, help="Path to output .csv file")
    parser.add_argument("--pad_before", type=float, default=0.0, help="Seconds before segment start")
    parser.add_argument("--pad_after", type=float, default=0.0, help="Seconds after segment end")
    args = parser.parse_args()

    extract_event_segments_json(args.processed, args.events_json, args.output, args.pad_before, args.pad_after)
