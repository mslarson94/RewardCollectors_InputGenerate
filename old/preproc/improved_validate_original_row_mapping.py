
import pandas as pd

# Load original and event files
raw_csv_path = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile/ProcessedData/pair_008/02_17_2025/Morning/MagicLeaps/ML2A/ObsReward_A_02_17_2025_15_11_processed.csv"
events_json_path = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile/ExtractedEvents/pair_008/02_17_2025/Morning/MagicLeaps/ML2A/ObsReward_A_02_17_2025_15_11_processed.csv_events.json"

with open(raw_csv_path, "r") as f:
    raw_lines = f.readlines()

events = pd.read_json(events_json_path, lines=True)
logged = events[events["source"] == "logged"]

# Define expected substrings per event_type
expected_phrases = {
    "PinDrop": "Just dropped a pin",
    "Feedback_CoinCollect": "Collected feedback coin",
    "Mark": "Sending Headset mark",
    "SwapVote": "Active Navigator says it was",
    "IE_CoinCollected": "coin collected",
    "IE_ChestOpen": "Chest opened",
    "WalkingPeriod": None,  # synthetic
    "ChestOpened": "Chest opened",
    "CoinCollect": "Collected feedback coin",  # or alternative
}

# Check for mismatches
for _, row in logged.iterrows():
    orig_idx = int(row["original_row_start"])
    event_type = row["event_type"]
    if orig_idx >= len(raw_lines):
        print(f"⚠️ Out-of-bounds index {orig_idx} for event {event_type}")
        continue

    raw_line = raw_lines[orig_idx].strip()
    expected = expected_phrases.get(event_type)

    if expected and expected not in raw_line:
        print(f"❌ Potential mismatch at row {orig_idx} for {event_type}:")
        print(f"    Expected pattern: '{expected}'")
        print(f"    CSV line: {raw_line}")
