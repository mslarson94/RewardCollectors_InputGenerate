import os
import pandas as pd
from datetime import datetime, timedelta

# Root directory containing all "pair_xx" folders
root_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/ProcessedData"
hoursOff = 8
time_offset = timedelta(hours=hoursOff)  # Adjust for the incorrect timezone

# Convert Timestamp column
def adjust_timestamp(ts):
    try:
        t = datetime.strptime(ts, "%H:%M:%S:%f")
        t -= time_offset
        return t.strftime("%H:%M:%S:%f")[:-3]  # Keep milliseconds
    except ValueError:
        return ts  # If conversion fails, keep original

def individual_adjTimeStamp(inDir):
    """Processes all ObsReward_A files in the given ML2G directory."""
    outDir = os.path.join(inDir, 'Corrected')
    os.makedirs(outDir, exist_ok=True)  # Ensure the Corrected folder exists
    
    for filename in os.listdir(inDir):
        if filename.startswith("ObsReward_A") and filename.endswith(".csv"):
            file_path = os.path.join(inDir, filename)
            
            # Read the CSV file
            df = pd.read_csv(file_path)
            df["Timestamp"] = df["Timestamp"].apply(adjust_timestamp)
            
            # Modify filename
            filename_strip = filename.strip("_processed.csv")
            filename_split = filename_strip.split("_")
            begString = filename_strip[:-5]
            newFilename = begString + str((int(filename_split[-2]) - hoursOff)) + "_" + filename_split[-1] + "_processed.csv"
            
            # Save the corrected file
            df.to_csv(os.path.join(outDir, newFilename), index=False)
            
            print(f"✅ Corrected: {filename} -> {newFilename}")

def process_all_ml2g_dirs(root_dir):
    """Finds and processes all ML2G directories in SelectedData."""
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if os.path.basename(dirpath) == "ML2G":  # Only process ML2G directories
            print(f"📂 Processing ML2G directory: {dirpath}")
            individual_adjTimeStamp(dirpath)

# Run the script
process_all_ml2g_dirs(root_dir)
print("🎉 All relevant files have been corrected.")
