
import os
import pandas as pd
from datetime import datetime, timedelta

root_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/ProcessedData"
metadata_file = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
magic_leap_data = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")

# Filter only rows that need correction
correction_df = magic_leap_data[magic_leap_data["needCorrML2G"] == 1]

# Convert to lookup dictionary by filename base
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
    outDir = os.path.join(inDir, 'Corrected')
    os.makedirs(outDir, exist_ok=True)

    for filename in os.listdir(inDir):
        if filename.startswith("ObsReward_A") and filename.endswith(".csv"):
            basename = filename.replace("_processed.csv", "").strip()
            if basename not in correction_lookup:
                continue

            file_path = os.path.join(inDir, filename)
            df = pd.read_csv(file_path)
            df["Timestamp"] = df["Timestamp"].apply(lambda ts: adjust_timestamp(ts, offset=8))

            original_dt = correction_lookup[basename]
            corrected_dt = original_dt - timedelta(hours=8)
            newFilename = f"ObsReward_A_{corrected_dt:%m_%d_%Y_%H_%M}_processed.csv"

            df.to_csv(os.path.join(outDir, newFilename), index=False)
            print(f"✅ Corrected: {filename} -> {newFilename}")

def process_all_ml2g_dirs(root_dir):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if os.path.basename(dirpath) == "ML2G":
            print(f"📂 Processing ML2G directory: {dirpath}")
            individual_adjTimeStamp(dirpath)

process_all_ml2g_dirs(root_dir)
print("🎉 All relevant files have been corrected.")
