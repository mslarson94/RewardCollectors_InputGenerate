
import pandas as pd
import os
from optimized_event_extraction import extract_all_events

# === User Config ===
input_dir = "./logs"  # replace with your log directory
metadata_file = "collatedData.xlsx"
output_dir = "./extracted_events"

# === Setup ===
os.makedirs(output_dir, exist_ok=True)
metadata_df = pd.read_excel(metadata_file)
metadata_df = metadata_df.rename(columns={"cleanedFile": "source_file"})

# === Process Each File ===
for fname in os.listdir(input_dir):
    if not fname.endswith(".csv"):
        continue

    log_path = os.path.join(input_dir, fname)
    try:
        df = pd.read_csv(log_path)
        source_file = os.path.basename(log_path)
        events = extract_all_events(df, metadata_df, source_file)
        output_path = os.path.join(output_dir, f"{source_file}_extracted.csv")
        pd.DataFrame(events).to_csv(output_path, index=False)
        print(f"✔ Processed: {fname} -> {output_path}")
    except Exception as e:
        print(f"⚠ Failed: {fname} with error {e}")
