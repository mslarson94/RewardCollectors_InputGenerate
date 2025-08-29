
import pandas as pd
import os
from optimized_event_extraction_loggable import extract_all_events

# === User Config ===

root_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile"
input_dir = os.path.join(root_dir, 'ProcessedData')
output_dir = os.path.join(root_dir, 'ExtractedEvents')
summary_dir = os.path.join(root_dir, 'Summary')
summary_file = os.path.join(summary_dir, "event_summary_counts.csv")

os.makedirs(output_dir, exist_ok=True)
os.makedirs(summary_dir, exist_ok=True)
# === Setup ===

metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
metadata_df = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")
metadata_df = metadata_df.rename(columns={"cleanedFile": "source_file"})
summary_rows = []

# === Process Each File Individually ===
for root, _, files in os.walk(input_dir):
    for fname in files:
        if not fname.endswith(".csv"):
            continue

        log_path = os.path.join(root, fname)
        source_file = os.path.basename(log_path)

        if not (metadata_df["source_file"] == source_file).any():
            print(f"⏭ Skipping {fname} — no match in metadata.")
            continue

        try:
            df = pd.read_csv(log_path)
            events = extract_all_events(df, metadata_df, source_file)
            output_path = os.path.join(output_dir, f"{source_file}_extracted.csv")
            pd.DataFrame(events).to_csv(output_path, index=False)

            # Count event types
            event_type_counts = pd.DataFrame(events)['event_type'].value_counts().to_dict()
            event_type_counts["LogFile"] = fname
            summary_rows.append(event_type_counts)

            print(f"✔ Processed: {fname} -> {output_path}")
        except Exception as e:
            print(f"⚠ Failed: {fname} with error {e}")

    # try:
    #     df = pd.read_csv(log_path)
    #     source_file = os.path.basename(log_path)
    #     events = extract_all_events(df, metadata_df, source_file)
    #     output_path = os.path.join(output_dir, f"{source_file}_extracted.csv")
    #     pd.DataFrame(events).to_csv(output_path, index=False)

    #     # Count event types
    #     event_type_counts = pd.DataFrame(events)['event_type'].value_counts().to_dict()
    #     event_type_counts["LogFile"] = fname
    #     summary_rows.append(event_type_counts)

    #     print(f"✔ Processed: {fname} -> {output_path}")
    # except Exception as e:
    #     print(f"⚠ Failed: {fname} with error {e}")

# === Write Summary ===
summary_df = pd.DataFrame(summary_rows).fillna(0)
summary_df.loc[:, summary_df.columns != "LogFile"] = summary_df.loc[:, summary_df.columns != "LogFile"].astype(int)

summary_df.to_csv(summary_file, index=False)
print(f"📊 Summary saved to {summary_file}")
