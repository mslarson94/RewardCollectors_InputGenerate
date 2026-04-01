
import os
import shutil
import pandas as pd
from datetime import datetime, timedelta

root_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData"
metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")

# Filter only rows that need correction
correction_df = magic_leap_data[magic_leap_data["needCorrML2G"] == 1]

# Create lookup dictionary
correction_lookup = {}
for _, row in correction_df.iterrows():
    basename = os.path.splitext(str(row["MagicLeapFiles"]).strip())[0]
    date_clean = str(row['testingDate']).replace("_", "/")
    time_clean = str(row['time_MLReported']).strip()
    dt_str = f"{date_clean} {time_clean}"
    dt = pd.to_datetime(dt_str, format="%m/%d/%Y %H:%M")
    correction_lookup[basename] = dt

def adjust_timestamp(ts, offset=8):
    try:
        t = datetime.strptime(ts, "%H:%M:%S:%f")
        t -= timedelta(hours=offset)
        return t.strftime("%H:%M:%S:%f")[:-3]
    except ValueError:
        return ts

def individual_adjTimeStamp(inDir):
    corrected_dir = os.path.join(inDir, 'CorrectedRaw')
    uncorrected_dir = os.path.join(inDir, 'UncorrectedTrueRaw')
    os.makedirs(corrected_dir, exist_ok=True)
    os.makedirs(uncorrected_dir, exist_ok=True)

    for filename in os.listdir(inDir):
        if filename.startswith("ObsReward_A"):
            file_path = os.path.join(inDir, filename)
            basename = filename.replace(".csv", "").strip()
            
            if basename not in correction_lookup:
                continue  # skip files that don't need correction

            # Move original file to Uncorrected/
            shutil.move(file_path, os.path.join(uncorrected_dir, filename))

            # Load and adjust timestamps
            df = pd.read_csv(os.path.join(uncorrected_dir, filename))
            df["Timestamp"] = df["Timestamp"].apply(lambda ts: adjust_timestamp(ts, offset=-8))

            # Save corrected file to Corrected/ with new name
            original_dt = correction_lookup[basename]
            #corrected_dt = original_dt - timedelta(hours=8)
            # old: corrected_dt = original_dt - timedelta(hours=8)
            newFilename = f"ObsReward_A_{original_dt:%m_%d_%Y_%H_%M}.csv"

            df.to_csv(os.path.join(inDir, newFilename), index=False)
            df.to_csv(os.path.join(corrected_dir, newFilename), index=False)

            print(f"✅ Moved: {filename} -> UncorrectedTrueRaw/")
            print(f"✅ Corrected: {filename} -> {newFilename}")

def process_all_ml2g_dirs(root_dir):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if os.path.basename(dirpath) == "ML2G":
            print(f"📂 Processing ML2G directory: {dirpath}")
            individual_adjTimeStamp(dirpath)

process_all_ml2g_dirs(root_dir)
print("🎉 All corrections and relocations complete.")
