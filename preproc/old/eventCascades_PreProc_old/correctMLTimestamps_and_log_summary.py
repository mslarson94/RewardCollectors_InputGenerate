
import os
import shutil
import pandas as pd
from datetime import datetime, timedelta

root_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData"
metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")

# Filter only rows that need correction
correction_df = magic_leap_data[magic_leap_data["needCorrML2G"] == 1]

# Create lookup dictionary and reverse map for reporting
correction_lookup = {}
reverse_lookup = {}  # Map basename to row index
for idx, row in correction_df.iterrows():
    basename = os.path.splitext(str(row["MagicLeapFiles"]).strip())[0]
    date_clean = str(row['testingDate']).replace("_", "/")
    time_clean = str(row['time_MLReported']).strip()
    dt_str = f"{date_clean} {time_clean}"
    dt = pd.to_datetime(dt_str, format="%m/%d/%Y %H:%M")
    correction_lookup[basename] = dt
    reverse_lookup[basename] = idx

summary_records = []
missing_metadata = []

def adjust_timestamp(ts, offset=8):
    try:
        t = datetime.strptime(ts, "%H:%M:%S:%f")
        t -= timedelta(hours=offset)
        return t.strftime("%H:%M:%S:%f")[:-3]
    except ValueError:
        return ts

def individual_adjTimeStamp(inDir):
    corrected_dir = os.path.join(inDir, 'Corrected')
    uncorrected_dir = os.path.join(inDir, 'Uncorrected')
    os.makedirs(corrected_dir, exist_ok=True)
    os.makedirs(uncorrected_dir, exist_ok=True)

    for filename in os.listdir(inDir):
        if filename.startswith("ObsReward_A") and filename.endswith(".csv"):
            file_path = os.path.join(inDir, filename)
            basename = filename.replace(".csv", "").strip()
            #print(correction_lookup)
            if basename not in correction_lookup:
                missing_metadata.append(filename)
                continue

            # Move to Uncorrected
            shutil.move(file_path, os.path.join(uncorrected_dir, filename))

            df = pd.read_csv(os.path.join(uncorrected_dir, filename))
            df["Timestamp"] = df["Timestamp"].apply(lambda ts: adjust_timestamp(ts, offset=8))

            original_dt = correction_lookup[basename]
            newFilename = f"ObsReward_A_{original_dt:%m_%d_%Y_%H_%M}.csv"
            df.to_csv(os.path.join(inDir, newFilename), index=False)
            #df.to_csv(os.path.join(corrected_dir, newFilename), index=False)
            print(f"✅ Corrected: {filename} -> {newFilename}")
            summary_records.append({
                "original_filename": filename,
                "corrected_filename": newFilename,
                "directory": inDir,
                "matched_metadata_row": reverse_lookup[basename]
            })

def process_all_ml2g_dirs(root_dir):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if os.path.basename(dirpath) == "ML2G":
            print(f"📂 Processing ML2G directory: {dirpath}")
            individual_adjTimeStamp(dirpath)

process_all_ml2g_dirs(root_dir)
print("🎉 All corrections and relocations complete.")

# Save summary logs
summary_df = pd.DataFrame(summary_records)
missing_df = pd.DataFrame({"missing_metadata_files": missing_metadata})

summary_df.to_csv(os.path.join(root_dir, "correction_summary.csv"), index=False)
missing_df.to_csv(os.path.join(root_dir, "missing_metadata_files.csv"), index=False)
print("📝 Summary and missing metadata logs saved.")
