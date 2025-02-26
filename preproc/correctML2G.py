import os
import pandas as pd
from datetime import datetime, timedelta

# Directory containing log files
log_dir = "/Users/mairahmac/Desktop/ExtraSelectedData/pair_07/MagicLeaps/ML2G/processed"  # Change this to your actual directory
outdir = "/Users/mairahmac/Desktop/ExtraSelectedData/pair_07/MagicLeaps/ML2G/processed/Corrected"
hoursOff = 8
time_offset = timedelta(hours=8)  # Adjust for the incorrect timezone

# Convert Timestamp column
def adjust_timestamp(ts):
    try:
        t = datetime.strptime(ts, "%H:%M:%S:%f")
        t -= time_offset
        return t.strftime("%H:%M:%S:%f")[:-3]  # Keep milliseconds
    except ValueError:
        return ts  # If conversion fails, keep original

# Process all files in the directory
for filename in os.listdir(log_dir):
    if filename.startswith("ObsReward_A") and filename.endswith(".csv"):
        file_path = os.path.join(log_dir, filename)
        os.makedirs(outdir, exist_ok=True)  # Ensure "processed" folder exists
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        df["Timestamp"] = df["Timestamp"].apply(adjust_timestamp)
        filename_strip = filename.strip("_processed.csv")
        filename_split = filename_strip.split("_")
        begString = filename_strip[:-5]
        newFilename = begString +  str((int(filename_split[-2]) - hoursOff)) + "_" + filename_split[-1] + "_processed.csv"
        print(newFilename)
        # Save the corrected file
        #new_filename = filename.replace(".csv", "_corrected.csv")
        df.to_csv(os.path.join(outdir, newFilename), index=False)
        
        print(f"Corrected: {filename} -> {newFilename}")

print("All relevant files have been corrected.")
