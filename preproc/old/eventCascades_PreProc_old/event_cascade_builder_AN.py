
import pandas as pd
import re
import os
import json 

# --- Configuration ---
root_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/ProcessedData"
out_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/Summary"
os.makedirs(out_dir, exist_ok=True)
collated_path = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"

# Load metadata

MAGIC_LEAP_METADATA = pd.read_excel(collated_path, sheet_name='MagicLeapFiles')
MAGIC_LEAP_METADATA = MAGIC_LEAP_METADATA.dropna(subset=['cleanedFile'])
MAGIC_LEAP_METADATA = MAGIC_LEAP_METADATA[MAGIC_LEAP_METADATA['primaryRole'] == 'AN']
MAGIC_LEAP_METADATA = MAGIC_LEAP_METADATA.rename(columns={"cleanedFile": "source_file"})

# --- Utility Functions ---

def flatten_event(event, source_file):
    flat = event.copy()
    flat["source_file"] = source_file
    if isinstance(flat.get("details"), dict):
        for k, v in flat["details"].items():
            flat[f"details_{k}"] = v
        flat.pop("details", None)
    return flat

# def flatten_event(event, source_file):
#     """Flatten event dict for CSV summary."""
#     flat = event.copy()
#     flat["source_file"] = source_file
#     # Flatten nested details dictionary (if any)
#     if isinstance(flat.get("details"), dict):
#         for k, v in flat["details"].items():
#             flat[f"details_{k}"] = v
#         flat.pop("details", None)
#     return flat

def enrich_with_metadata(events, source_file, metadata_df):
    """Attach metadata from the collated sheet to each event."""
    matched = metadata_df[metadata_df["source_file"] == source_file]
    if matched.empty:
        return [flatten_event(event, source_file) for event in events]
    metadata = matched.iloc[0].to_dict()
    enriched = []
    for event in events:
        flat = flatten_event(event, source_file)
        flat.update(metadata)
        enriched.append(flat)
    return enriched

def save_event_summary(events, source_file, output_dir):
    """Save a per-file event summary CSV."""
    flat_events = [flatten_event(e, source_file) for e in events]
    df = pd.DataFrame(flat_events)
    output_path = os.path.join(output_dir, os.path.splitext(source_file)[0] + "_events.csv")
    df.to_csv(output_path, index=False)
    return output_path

# --- Event Extraction Functions ---

def process_pin_drop(df):
    events = []
    cascade_id = 0
    i = 0
    while i < len(df):
        row = df.iloc[i]
        if row["Type"] == "Event" and isinstance(row["Message"], str) and "Just dropped a pin" in row["Message"]:
            cascade_id += 1
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            common_info = {
                "BlockNum": row.get("BlockNum", None),
                "RoundNum": row.get("RoundNum", None),
                "CoinSetID": row.get("CoinSetID", None),
                "original_row": i
            }

            event = {
                "AppTime": start_time,
                "Timestamp": timestamp,
                "cascade_id": cascade_id,
                "event_type": "Just dropped a pin",
                "details": {},
                "source": "logged",
                **common_info
            }

            j = i + 1
            while j < len(df):
                next_row = df.iloc[j]
                if next_row["Type"] != "Event" or not isinstance(next_row["Message"], str):
                    break
                msg = next_row["Message"]
                if "Dropped a new pin at" in msg:
                    match = re.search(r'at ([\d\.\-]+) ([\d\.\-]+) ([\d\.\-]+) localpos: ([\d\.\-]+) ([\d\.\-]+) ([\d\.\-]+)', msg)
                    if match:
                        event["details"].update({
                            "world_x": float(match.group(1)),
                            "world_y": float(match.group(2)),
                            "world_z": float(match.group(3)),
                            "local_x": float(match.group(4)),
                            "local_y": float(match.group(5)),
                            "local_z": float(match.group(6)),
                        })
                elif "Closest location was" in msg:
                    match = re.search(r'distance: ([\d\.]+) \| (good|bad) drop \| coinValue: ([\d\.]+)', msg)
                    if match:
                        event["details"].update({
                            "drop_distance": float(match.group(1)),
                            "drop_quality": match.group(2),
                            "coin_value": float(match.group(3))
                        })
                elif "Dropped a good pin" in msg or "Dropped a bad pin" in msg:
                    parts = msg.split("|")
                    if len(parts) == 5:
                        event["details"].update({
                            "result_quality": parts[0].replace("Dropped a ", ""),
                            "result_zone": int(parts[1]),
                            "result_ring": int(parts[2]),
                            "result_score": float(parts[3]),
                            "result_bonus": float(parts[4]),
                        })
                j += 1

            events.append(event)

            for offset, evt in [
                (0.000, "Pin drop sound (start)"),
                (0.000, "Gray pin visible (start)"),
                (0.654, "Pin drop sound (end)"),
                (2.000, "Gray pin visible (end)"),
                (2.000, "Feedback sound (start)"),
                (2.000, "Feedback text and color visible (start)"),
                (3.000, "Feedback text and color visible (end)"),
                (3.000, "Coin visible (start)")
            ]:
                events.append({
                    "AppTime": start_time + offset,
                    "Timestamp": None,
                    "cascade_id": cascade_id,
                    "event_type": evt,
                    "details": {},
                    "source": "synthetic",
                    **{k: event[k] for k in ("BlockNum", "RoundNum", "CoinSetID")}
                })

            i = j
        else:
            i += 1
    return events


def process_feedback_collect(df):
    events = []
    for row in df.itertuples():
        if isinstance(row.Message, str) and "Feedback Coin Collect" in row.Message:
            parts = row.Message.split("|")
            if len(parts) == 3:
                details = {
                    "source": parts[0].strip(),
                    "score": float(parts[1]),
                    "bonus": float(parts[2])
                }
                events.append({
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "cascade_id": None,
                    "event_type": "Feedback Coin Collect",
                    "details": details,
                    "source": "logged",
                    "original_row": row.Index,
                    "BlockNum": getattr(row, "BlockNum", None),
                    "RoundNum": getattr(row, "RoundNum", None),
                    "CoinSetID": getattr(row, "CoinSetID", None)
                })
    return events

def process_ie_events(df):
    events = []
    for row in df.itertuples():
        if isinstance(row.Message, str) and "IE Chest Open" in row.Message:
            parts = row.Message.split("|")
            if len(parts) == 3:
                events.append({
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "cascade_id": None,
                    "event_type": "IE Chest Open",
                    "details": {"score": float(parts[1]), "bonus": float(parts[2])},
                    "source": "logged",
                    "original_row": row.Index,
                    "BlockNum": getattr(row, "BlockNum", None),
                    "RoundNum": getattr(row, "RoundNum", None),
                    "CoinSetID": getattr(row, "CoinSetID", None)
                })
        elif isinstance(row.Message, str) and "IE Coin Collect" in row.Message:
            parts = row.Message.split("|")
            if len(parts) == 3:
                events.append({
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "cascade_id": None,
                    "event_type": "IE Coin Collect",
                    "details": {"score": float(parts[1]), "bonus": float(parts[2])},
                    "source": "logged",
                    "original_row": row.Index,
                    "BlockNum": getattr(row, "BlockNum", None),
                    "RoundNum": getattr(row, "RoundNum", None),
                    "CoinSetID": getattr(row, "CoinSetID", None)
                })
    return events



def build_timeline_from_processed(file_path, output_path):
    df = pd.read_csv(file_path)
    all_events = (
        process_pin_drop(df) +
        process_feedback_collect(df) +
        process_ie_events(df)
    )
    timeline_df = pd.DataFrame(all_events).sort_values(by="AppTime")
    timeline_df.to_csv(output_path, index=False)

# --- Main Processor ---

def process_all_obsreward_files(root_dir, output_dir="EventCascades"):
    pattern = re.compile(r"ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_processed\.csv$")
    summary_rows = []
    os.makedirs(output_dir, exist_ok=True)

    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if pattern.match(fname):
                full_path = os.path.join(dirpath, fname)
                try:
                    df = pd.read_csv(full_path)
                    all_events = (
                        process_pin_drop(df) +
                        process_feedback_collect(df) +
                        process_ie_events(df)
                    )

                    # Save individual summary
                    save_event_summary(all_events, fname, dirpath)

                    # Enrich for final summary
                    enriched = enrich_with_metadata(all_events, fname, MAGIC_LEAP_METADATA)
                    summary_rows.extend(enriched)

                    print(f"✓ Processed: {fname}")

                except Exception as e:
                    print(f"✗ Failed to process {fname}: {e}")

    if summary_rows:
        summary_df = pd.DataFrame(summary_rows)
        summary_df.to_csv(os.path.join(output_dir, "event_summary.csv"), index=False)
        print(f"\n📄 Summary saved to: {os.path.join(output_dir, 'event_summary.csv')}")
    else:
        print("⚠️ No events found to summarize.")

process_all_obsreward_files(root_dir, out_dir)
