#bioPac_sync_1st.py
from datetime import datetime, timedelta
import pandas as pd

# Example timestamps from your data
start_time_str = "2024-08-31 10:18:11.959"
start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S.%f")

# Assume task_data is already loaded with a Timestamp column
# Load biometric data (excluding metadata) and create synthetic timestamps
biometric_data_file = '08312014_altered1.csv'  # Replace with your file path
biometric_data_df = pd.read_csv(biometric_data_file)
biometric_data_df_clean = biometric_data_df.iloc[20:].copy()

# Create time series using sample rate (0.5 milliseconds per sample)
sample_rate = 0.0005
num_samples = len(biometric_data_df_clean)
time_deltas = [start_time + timedelta(seconds=i * sample_rate) for i in range(num_samples)]
biometric_data_df_clean['Timestamp'] = time_deltas

# Calculate time difference between task and biometric data start times
task_data_file = 'ObsReward_A_08_31_2024_10_45_cleaned_eye.csv'
task_data_df = pd.read_csv(task_data_file)
task_data_df['Timestamp'] = pd.to_datetime(task_data_df['Timestamp'], format='%H:%M:%S:%f')
task_start_time = task_data_df['Timestamp'].min()
time_difference = task_start_time - start_time

# Adjust biometric timestamps
biometric_data_df_clean['Adjusted_Timestamp'] = biometric_data_df_clean['Timestamp'] + time_difference

# Save adjusted biometric data
biometric_data_df_clean.to_csv('adjusted_biometric_data.csv', index=False)
