import os
import pandas as pd
import numpy as np
import scipy.io as sio
import re

### === CONFIGURE PATHS === ###
MAGIC_LEAP_DIR = "/Users/mairahmac/Desktop/testPython2Matlab/ML2A/processed"
BIOPAC_DIR = "/Users/mairahmac/Desktop/testPython2Matlab/BioPac"
OUTPUT_DIR = "/Users/mairahmac/Desktop/testPython2Matlab/Matlab"
METADATA_FILE = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"

os.makedirs(OUTPUT_DIR, exist_ok=True)  # Ensure output folder exists

### === FUNCTION: LOAD MAGIC LEAP DATA === ###
def load_magic_leap_data(file_path):
    df = pd.read_csv(file_path)
    
    # Ensure required columns exist
    if "Message" not in df.columns or "Timestamp" not in df.columns:
        raise ValueError(f"Missing required columns in {file_path}")

    # Extract timestamps where 'Sending headset marks' was logged
    marks_df = df[df["Message"].str.contains("Sending headset marks", na=False)].copy()
    marks_df["Timestamp"] = pd.to_numeric(marks_df["Timestamp"], errors="coerce")  # Convert timestamps to float

    return marks_df

### === FUNCTION: LOAD BIOPAC `.txt` FILE === ###
def load_biopac_txt(biopac_file):
    with open(biopac_file, 'r') as f:
        lines = f.readlines()

    # Find where data starts
    data_start_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("milliSec"):
            data_start_idx = i + 2  # Skip header lines
            break
    
    if data_start_idx is None:
        raise ValueError(f"Could not find data section in {biopac_file}")

    # Extract column names from the header row
    column_names = lines[data_start_idx - 2].strip().split("\t")
    
    # Read the numerical data
    bio_df = pd.read_csv(biopac_file, delimiter="\t", skiprows=data_start_idx, names=column_names, engine="python")

    # Convert time column
    bio_df["milliSec"] = pd.to_numeric(bio_df["milliSec"], errors="coerce") / 1000  # Convert ms to seconds
    
    # Extract STP channel
    stp_column = "CH28"  # Adjust if needed
    if stp_column not in bio_df.columns:
        raise ValueError(f"STP channel '{stp_column}' not found in {biopac_file}")
    
    return bio_df["milliSec"].values, bio_df[stp_column].values

### === FUNCTION: MATCH MAGIC LEAP TIMESTAMPS TO BIOPAC STP MARKS === ###
def match_timestamps(magic_df, stp_times, stp_signal):
    aligned_events = []

    for idx, row in magic_df.iterrows():
        magic_time = row["Timestamp"]
        
        # Find closest STP time
        closest_idx = np.argmin(np.abs(stp_times - magic_time))
        matched_time = stp_times[closest_idx]
        matched_value = stp_signal[closest_idx]

        aligned_events.append({
            "Magic_Timestamp": magic_time,
            "Biopac_Timestamp": matched_time,
            "Biopac_STP": matched_value,
            **row.to_dict()  # Merge all metadata
        })

    return pd.DataFrame(aligned_events)

### === MAIN PROCESSING LOOP === ###
for magic_file in os.listdir(MAGIC_LEAP_DIR):
    if magic_file.startswith("ObsReward_") and magic_file.endswith("_processed.csv"):
        magic_path = os.path.join(MAGIC_LEAP_DIR, magic_file)
        print(f"📂 Processing Magic Leap file: {magic_path}")

        # Load Magic Leap timestamps
        magic_data = load_magic_leap_data(magic_path)

        # Find corresponding Biopac file (by date)
        date_match = re.search(r"_(\d{2}_\d{2}_\d{4})_", magic_file)
        if date_match:
            date_str = date_match.group(1)  # Extract MM_DD_YYYY
        else:
            print(f"⚠ Could not extract date from {magic_file}, skipping.")
            continue

        # Search for any Biopac file containing the same date
        matched_biopac = None
        for bio_file in os.listdir(BIOPAC_DIR):
            if date_str in bio_file and bio_file.endswith(".txt"):  # Match with .txt
                matched_biopac = os.path.join(BIOPAC_DIR, bio_file)
                break

        if matched_biopac is None:
            print(f"⚠ No BioPac file found for {magic_file}, skipping.")
            continue

        print(f"🔗 Matching with BioPac file: {matched_biopac}")

        # Load BioPac data
        try:
            stp_times, stp_signal = load_biopac_txt(matched_biopac)
        except ValueError as e:
            print(e)
            continue

        # Align timestamps
        aligned_df = match_timestamps(magic_data, stp_times, stp_signal)

        # Save to MATLAB `.mat` file
        mat_output_path = os.path.join(OUTPUT_DIR, magic_file.replace(".csv", ".mat"))
        sio.savemat(mat_output_path, {"aligned_data": aligned_df.to_dict("list")})

        print(f"✅ Saved aligned data to {mat_output_path}")

print("🎉 All files processed successfully!")
