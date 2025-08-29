import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Paths to the two CSV files
file1 = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Chest/R019_AN_Only3Cylinders.csv"
file2 = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Chest/R037_AN_Only3Cylinders.csv"
file3 = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Chest/R019_AN_Chest_Morning.csv"
file4 = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Chest/R037_AN_Chest_Afternoon.csv"

# Load the CSVs
events_df1 = pd.read_csv(file1)
events_df2 = pd.read_csv(file2)

# Combine both DataFrames into one with an identifier column
#events_df1['source_file'] = 'R019'
#events_df2['source_file'] = 'R037'
#merged_events_df = pd.concat([events_df1, events_df2], ignore_index=True)

# Define the directory containing the log files (update this path as needed)
log_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/alignedPO/ProcessedDataAligned"  # Update to actual path

# Implement a log loader (update to your actual logic)
def log_loader(file_name, dir_path):
    file_path = os.path.join(dir_path, file_name)
    df = pd.read_csv(file_path)
    return df

# Timestamp parsing function
def safe_parse_timestamp(ts):
    try:
        if isinstance(ts, str) and ts.count(':') == 3:
            hh, mm, ss, ms = ts.split(':')
            ms = (ms + '000')[:6]
            return datetime.strptime(f"{hh}:{mm}:{ss}:{ms}", "%H:%M:%S:%f")
        return datetime.strptime(ts, "%H:%M:%S:%f")
    except Exception:
        return None

# Extract epochs with annotations
def extract_precise_epochs_from_events(
    events_df,
    log_df,
    pre_padding_seconds=3.0,
    post_padding_seconds=2.0,
    timestamp_col='Timestamp'
):
    log_df['parsed_Timestamp'] = log_df[timestamp_col].apply(safe_parse_timestamp)
    parsed = [(i, t) for i, t in enumerate(log_df['parsed_Timestamp']) if isinstance(t, datetime)]
    log_indices, parsed_times = zip(*parsed)
    log_indices = np.array(log_indices)
    parsed_times = np.array(parsed_times)

    all_epochs = []
    for idx, event in events_df.iterrows():
        start_idx = int(event['original_row_start'])
        end_idx = int(event['original_row_end'])

        start_time = safe_parse_timestamp(log_df.loc[start_idx, timestamp_col])
        end_time = safe_parse_timestamp(log_df.loc[end_idx, timestamp_col])

        padded_start_time = start_time - timedelta(seconds=pre_padding_seconds)
        padded_end_time = end_time + timedelta(seconds=post_padding_seconds)

        start_match_idx = log_indices[np.searchsorted(parsed_times, padded_start_time, side='left')]
        end_match_idx = log_indices[min(len(parsed_times) - 1, np.searchsorted(parsed_times, padded_end_time, side='right'))]

        padded_start_idx = max(0, start_match_idx - 1)
        padded_end_idx = min(len(log_df) - 1, end_match_idx + 1)

        epoch = log_df.iloc[padded_start_idx:padded_end_idx + 1].copy()
        epoch["source_event_index"] = idx
        epoch["event_original_row_start"] = start_idx
        epoch["event_original_row_end"] = end_idx
        epoch["source_file"] = event["source_file"]

        all_epochs.append(epoch)

    return pd.concat(all_epochs, ignore_index=True)
def extract_epochs_from_merged_events(
    merged_events_df,
    log_dir,
    log_loader,
    pre_padding_seconds=2.0,
    post_padding_seconds=2.0,
    timestamp_col='Timestamp'
):
    all_epochs = []
    
    # Group by actual log file references
    for log_file_name, events_subset in merged_events_df.groupby('source_file'):
        print(f"Processing log file: {log_file_name} with {len(events_subset)} events...")
        
        # Load the log file directly
        log_df = log_loader(log_file_name, log_dir)
        
        # Extract padded epochs
        epochs = extract_precise_epochs_from_events(
            events_df=events_subset,
            log_df=log_df,
            pre_padding_seconds=pre_padding_seconds,
            post_padding_seconds=post_padding_seconds,
            timestamp_col=timestamp_col
        )
        all_epochs.append(epochs)
    
    return pd.concat(all_epochs, ignore_index=True)

# Wrapper function
def extract_epochs_from_merged_eventsv1(
    merged_events_df,
    log_dir,
    log_loader,
    pre_padding_seconds=2.0,
    post_padding_seconds=2.0,
    timestamp_col='Timestamp'
):
    all_epochs = []
    for source_file, events_subset in merged_events_df.groupby('source_file'):
        print(f"Processing {source_file} with {len(events_subset)} events...")
        log_file_name = f"{source_file}_MergedEvents_Morning.csv"  # adjust if needed
        log_df = log_loader(log_file_name, log_dir)
        epochs = extract_precise_epochs_from_events(
            events_df=events_subset,
            log_df=log_df,
            pre_padding_seconds=pre_padding_seconds,
            post_padding_seconds=post_padding_seconds,
            timestamp_col=timestamp_col
        )
        all_epochs.append(epochs)
    return pd.concat(all_epochs, ignore_index=True)

# Run the extraction
extracted_epochs_R019 = extract_epochs_from_merged_events(
    merged_events_df=events_df1,
    log_dir=log_dir,
    log_loader=log_loader,
    pre_padding_seconds=3.0,
    post_padding_seconds=2.0,
    timestamp_col='Timestamp'
)

extracted_epochs_R037 = extract_epochs_from_merged_events(
    merged_events_df=events_df2,
    log_dir=log_dir,
    log_loader=log_loader,
    pre_padding_seconds=3.0,
    post_padding_seconds=2.0,
    timestamp_col='Timestamp'
)

# Output
output_file_R019 = "/Users/mairahmac/Desktop/R019_ExtractEpochs.csv"
extracted_epochs_R019.to_csv(output_file_R019, index=False)
print(f"Saved extracted epochs to: {output_file_R019}")


output_file_R037 = "/Users/mairahmac/Desktop/R037_ExtractEpochs.csv"
extracted_epochs_R037.to_csv(output_file_R037, index=False)
print(f"Saved extracted epochs to: {output_file_R019}")
