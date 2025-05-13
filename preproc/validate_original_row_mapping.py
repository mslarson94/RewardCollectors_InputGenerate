
import pandas as pd

# Path config — update if needed
raw_csv_path = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile/ProcessedData/pair_008/02_17_2025/Morning/MagicLeaps/ML2A/ObsReward_A_02_17_2025_15_11_processed.csv"
events_json_path = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile/ExtractedEvents/pair_008/02_17_2025/Morning/MagicLeaps/ML2A/ObsReward_A_02_17_2025_15_11_processed.csv_events.json"

# Load original log file lines
with open(raw_csv_path, "r") as f:
    raw_lines = f.readlines()

# Load processed events
events = pd.read_json(events_json_path, lines=True)

# Focus on logged events
logged = events[events["source"] == "logged"]

# Check each for a likely match in original line
for _, row in logged.iterrows():
    orig_idx = int(row["original_row_start"])
    event_type = row["event_type"]
    if orig_idx >= len(raw_lines):
        print(f"⚠️ Out-of-bounds index {orig_idx} for event {event_type}")
        continue

    raw_line = raw_lines[orig_idx].strip()

    # Basic fuzzy match on event type string
    if event_type.lower() not in raw_line.lower():
        print(f"❌ Potential mismatch at row {orig_idx} for event {event_type}:")
        print(f"    CSV line: {raw_line}")
