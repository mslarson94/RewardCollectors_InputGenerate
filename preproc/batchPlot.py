import os
import pandas as pd
import matplotlib.pyplot as plt

# 🔧 Configuration
directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/alignedPO'  # Replace this path with your own folder
outputDir = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/alignedPO'
# 🔎 Collect all .csv files
all_csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]

def parse_time_column(series):
    return pd.to_datetime(series, format='%H:%M:%S:%f', errors='coerce')

# 📊 Iterate over each file
for file_name in all_csv_files:
    file_path = os.path.join(directory_path, file_name)
    print(f"Processing: {file_name}")
    filename_without_extension = os.path.splitext(file_name)[0]
    try:
        df = pd.read_csv(file_path)
        
        # Check for necessary columns
        if 'Timestamp' not in df.columns or 'AdjustedTimestamp' not in df.columns:
            print(f"⚠️ Missing required columns in {file_name}. Skipping.")
            continue
        
        # Parse timestamps
        df['Timestamp'] = parse_time_column(df['Timestamp'])
        df['AdjustedTimestamp'] = parse_time_column(df['AdjustedTimestamp'])
        
        # Filter out missing timestamps
        valid_rows = df.dropna(subset=['Timestamp', 'AdjustedTimestamp'])
        
        # Plot
        plt.figure(figsize=(14, 6))
        plt.plot(valid_rows.index, valid_rows['Timestamp'], label='Original Timestamp', marker='o', linestyle='-', alpha=0.7)
        plt.plot(valid_rows.index, valid_rows['AdjustedTimestamp'], label='Adjusted Timestamp', marker='x', linestyle='-', alpha=0.7)
        plt.xlabel('Row Index')
        plt.ylabel('Time')
        plt.title(f'Timestamps in {file_name}')
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{outputDir}/{filename_without_extension}.png")
        plt.close()
        
    except Exception as e:
        print(f"⚠️ Could not process {file_name}. Reason: {str(e)}")
