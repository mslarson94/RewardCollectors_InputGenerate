
import pandas as pd
import os
import re
from datetime import datetime

# --- Load Unity Event Summary and Metadata ---
event_path = "event_summary.csv"
metadata_path = "collatedData.xlsx"
log_folder = "rpi_logs"

events = pd.read_csv(event_path)
metadata = pd.read_excel(metadata_path, sheet_name='MagicLeapFiles')
metadata = metadata.rename(columns={"cleanedFile": "source_file"})

# --- Filter for Headset Marks ---
mark_events = events[events["event_type"] == "Sent Headset Mark"].copy()
mark_events["AppTime"] = pd.to_numeric(mark_events["AppTime"], errors="coerce")

# --- Load RPi Logs ---
def parse_rpi_log(filepath):
    timestamps = []
    with open(filepath, "r") as f:
        for line in f:
            match = re.search(r"(\d{2}:\d{2}:\d{2}\.\d+)", line)
            if match:
                ts = match.group(1)
                try:
                    parsed = datetime.strptime(ts, "%H:%M:%S.%f")
                    timestamps.append((filepath, parsed))
                except ValueError:
                    continue
    return timestamps

log_data = {}
for fname in os.listdir(log_folder):
    ip_match = re.search(r"_(\d+\.\d+\.\d+\.\d+)_", fname)
    if ip_match:
        ip = ip_match.group(1)
        log_path = os.path.join(log_folder, fname)
        log_data[ip] = parse_rpi_log(log_path)

# --- Match IP from Metadata ---
ip_map = metadata.set_index("device")["ip"].to_dict()

# --- Attach EEG Times to Events ---
def find_closest(ts_list, target_time):
    return min(ts_list, key=lambda x: abs((x[1] - target_time).total_seconds()))

aligned = []

for _, row in mark_events.iterrows():
    device = row.get("device")
    ip = ip_map.get(device)
    app_time = row["AppTime"]
    if ip in log_data:
        eeg_times = log_data[ip]
        if eeg_times:
            closest_log = find_closest(eeg_times, datetime.strptime(row["Timestamp"], "%Y-%m-%d %H:%M:%S.%f"))
            aligned.append({
                **row,
                "aligned_eeg_time": closest_log[1].strftime("%H:%M:%S.%f"),
                "log_source": closest_log[0]
            })

aligned_df = pd.DataFrame(aligned)
aligned_df.to_csv("aligned_marks.csv", index=False)
print("✓ Aligned marks saved to aligned_marks.csv")
