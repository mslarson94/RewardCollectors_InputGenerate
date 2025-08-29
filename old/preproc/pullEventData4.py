import ast
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

baseDirName = "SmallSelectedData/RNS/alignedPO"

eventsFileName = "R037_MergedEvents_Morning.csv"
#eventsFileName = "R019_MergedEvents_Morning"


##############
trueRootDir = "/Users/mairahmac/Desktop/RC_TestingNotes/"
baseDir = os.path.join(trueRootDir, baseDirName)

eventsDir = os.path.join(baseDir, "alignedMergedPO")

procDir = os.path.join(baseDir, "ProcessedData_Flat")

subID = "R037"


eventsFile = os.path.join(eventsDir, eventsFileName)
print(eventsFile)
events_df = pd.read_csv(eventsFile)

outDir = os.path.join(baseDir, "alignedPO_MarksCylinders")

print(outDir)
os.makedirs(outDir, exist_ok=True)

def log_loader(fileName, dir):
    filePath = os.path.join(dir, fileName)
    df = pd.read_csv(filePath)
    return df 

# Convert the 'details' column safely from strings to dictionaries where applicable
def parse_details_column_v1(df):
    def safe_parse(x):
        try:
            return ast.literal_eval(x) if isinstance(x, str) else x
        except Exception:
            return None
    df['parsed_details'] = df['details'].apply(safe_parse)
    return df

def parse_details_column(df):
    def safe_parse(x):
        if pd.isna(x) or x == '' or x == '{}':
            return None
        try:
            parsed = ast.literal_eval(x) if isinstance(x, str) else x
            return parsed if isinstance(parsed, dict) and len(parsed) > 0 else None
        except Exception:
            return None
    df['parsed_details'] = df['details'].apply(safe_parse)
    return df

# Function 1: Get all events where hiMeta_eventType == 'Infrastructure'
def get_infrastructure_events(df):
    return df[df['lo_eventType'].isin(["PinDrop", "PinDrop_Animation"])]

def get_ChestOpenevents(df):
    return df[df['hiMeta_eventType'] == 'Infrastructure']

def get_CylinderWalk(df):
    return df[df['hiMeta_eventType'] == 'Infrastructure']

def get_all_target_events(df):
    events = []
    events.append(get_infrastructure_events(df))
    events.append(get_chest_open_events(df))
    events.append(get_cylinder_walk_events(df))
    #events.append(get_bad_pin_drop_events(df))
    return pd.concat(events, ignore_index=True)


def get_events_by_lo_eventTypeAndBlockNum(df, event_types):
    return df[
        df['lo_eventType'].isin(event_types) &
        (df['BlockNum'] == 2)
    ]
def get_events_by_lo_eventType(df, event_types):
    return df[df['lo_eventType'].isin(event_types)]

# Function 2: Get all events where med_eventType is in the target list and details has 'dropQual': 'bad' for lo_eventType == "PinDrop_Moment"
def get_bad_pin_drop_events(df):
    df = parse_details_column(df)
    mask = (
        df['med_eventType'].isin(["PinDrop", "PinDrop_Animation"]) &
        (df['lo_eventType'] == "PinDrop_Moment") &
        df['parsed_details'].apply(lambda d: isinstance(d, dict) and d.get('dropQual') == 'bad')
    )
    return df[mask]

# Apply the functions and display results
infra_events = get_infrastructure_events(events_df)
bad_pin_drop_events = get_bad_pin_drop_events(events_df)
infra_events.to_csv(os.path.join(outDir, "infrastructure.csv"))
bad_pin_drop_events.to_csv(os.path.join(outDir, "badPinDrops.csv"))

event_types = ["ChestOpen_Moment"]
cylinder_types = ["PreBlock_CylinderWalk_segment"]

infra_types = ["Mark", "RoundStart", "BlockStart", "BlockEnd", "TrueBlockEnd", "TrueBlockStart"]
cylinders = get_events_by_lo_eventType(events_df, cylinder_types)

infras = get_events_by_lo_eventType(events_df, infra_types)
infras.to_csv(os.path.join(outDir, "infrasChests.csv"))

cylinders.to_csv(os.path.join(outDir, "cylinders.csv"))

trueBlocks_types = ["TrueBlockStart", "TrueBlockEnd"]
trueBlocks = get_events_by_lo_eventType(events_df, trueBlocks_types)
trueBlocks.to_csv(os.path.join(outDir, "trueBlocks.csv"))


onlyMarks_types = ["Mark"]
onlyMarks = get_events_by_lo_eventType(events_df, trueBlocks_types)
onlyMarks.to_csv(os.path.join(outDir, "onlyMarks.csv"))

combinedTrimmedCylinderMarks = pd.concat([onlyMarks, cylinders], ignore_index=True)
combinedTrimmedCylinderMarks.sort_values(by="AlignedTimestamp", inplace=True)
combinedTrimmedCylinderMarks.to_csv(os.path.join(outDir, "trimCylinderMarks.csv"), index=False)



def safe_parse_timestamp(ts):
    try:
        if isinstance(ts, str) and ts.count(':') == 3:
            hh, mm, ss, ms = ts.split(':')
            ms = (ms + '000')[:6]
            return datetime.strptime(f"{hh}:{mm}:{ss}:{ms}", "%H:%M:%S:%f")
        return datetime.strptime(ts, "%H:%M:%S:%f")
    except Exception:
        return None

# Packaging the logic into a reusable utility function
def extract_precise_epochs_from_events_v1(events_df, log_df, padding_seconds=2.0, timestamp_col='Timestamp'):
    """
    Extract epochs from log_df centered around events in events_df with precise 2s padding.
    
    Parameters:
    - events_df: DataFrame with at least ['original_row_start', 'original_row_end'] columns
    - log_df: DataFrame containing the raw log with a 'Timestamp' column
    - padding_seconds: Time to pad before and after each event (default 2.0s)
    - timestamp_col: Name of the column in log_df that contains time strings
    
    Returns:
    - DataFrame of all padded epochs with `event_index` and `original_log_index` columns
    """
    # Parse timestamps once

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

        padded_start_time = start_time - timedelta(seconds=padding_seconds)
        padded_end_time = end_time + timedelta(seconds=padding_seconds)

        start_match_idx = log_indices[np.searchsorted(parsed_times, padded_start_time, side='left')]
        end_match_idx = log_indices[min(len(parsed_times) - 1, np.searchsorted(parsed_times, padded_end_time, side='right'))]

        padded_start_idx = max(0, start_match_idx - 1)
        padded_end_idx = min(len(log_df) - 1, end_match_idx + 1)

        epoch = log_df.iloc[padded_start_idx:padded_end_idx + 1].copy()
        epoch["event_index"] = idx
        epoch["original_log_index"] = epoch.index
        all_epochs.append(epoch)

    return pd.concat(all_epochs, ignore_index=True)

def extract_precise_epochs_from_events(
    events_df,
    log_df,
    pre_padding_seconds=2.0,
    post_padding_seconds=2.0,
    timestamp_col='Timestamp'
):
    """
    Extract epochs from log_df centered around events in events_df with precise padding.

    Parameters:
    - events_df: DataFrame with at least ['original_row_start', 'original_row_end'] columns
    - log_df: DataFrame containing the raw log with a 'Timestamp' column
    - pre_padding_seconds: Time to pad before each event (default 2.0s)
    - post_padding_seconds: Time to pad after each event (default 2.0s)
    - timestamp_col: Name of the column in log_df that contains time strings

    Returns:
    - DataFrame of all padded epochs with `event_index` and `original_log_index` columns
    """
    # Parse timestamps once
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
        epoch["event_index"] = idx
        epoch["original_log_index"] = epoch.index
        all_epochs.append(epoch)

    return pd.concat(all_epochs, ignore_index=True)

from typing import Callable, Dict, Union

def extract_epochs_from_merged_events_v1(
    merged_events_df: pd.DataFrame,
    log_dir: str,
    log_loader: Callable[[str, str], pd.DataFrame],
    padding_seconds: float = 2.0,
    timestamp_col: str = 'AdjustedTimestamp'
    ) -> pd.DataFrame:
    all_epochs = []

    for source_file, events_subset in merged_events_df.groupby('source_file'):
        print(f"Processing {source_file} with {len(events_subset)} events...")
        log_df = log_loader(source_file, log_dir)

        epochs = extract_precise_epochs_from_events(
            events_df=events_subset,
            log_df=log_df.copy(),
            padding_seconds=padding_seconds,
            timestamp_col=timestamp_col
        )

        epochs['source_file'] = source_file
        all_epochs.append(epochs)

    return pd.concat(all_epochs, ignore_index=True)

def extract_epochs_from_merged_events(
    merged_events_df: pd.DataFrame,
    log_dir: str,
    log_loader: Callable[[str, str], pd.DataFrame],
    pre_padding_seconds: float = 5.0,
    post_padding_seconds: float = 2.0,
    timestamp_col: str = 'Timestamp'
    ) -> pd.DataFrame:
    all_epochs = []

    for source_file, events_subset in merged_events_df.groupby('source_file'):
        print(f"Processing {source_file} with {len(events_subset)} events...")
        log_df = log_loader(source_file, log_dir)

        epochs = extract_precise_epochs_from_events(
            events_df=events_subset,
            log_df=log_df.copy(),
            pre_padding_seconds=pre_padding_seconds,
            post_padding_seconds=post_padding_seconds,
            timestamp_col=timestamp_col
        )

        epochs['source_file'] = source_file
        all_epochs.append(epochs)

    return pd.concat(all_epochs, ignore_index=True)


extractedCylinders = extract_epochs_from_merged_events(
    merged_events_df=cylinders,
    log_dir=procDir,
    log_loader=log_loader,
    pre_padding_seconds=2,
    post_padding_seconds=2,
    timestamp_col='AdjustedTimestamp'
)

extractedCylinders.to_csv(os.path.join(outDir, "cylinder_extracts_afternoon.csv"))

extractedInfras = extract_epochs_from_merged_events(
    merged_events_df=infras,
    log_dir=procDir,
    log_loader=log_loader,
    pre_padding_seconds=0,
    post_padding_seconds=0,
    timestamp_col='AdjustedTimestamp'
)
extractedInfras.to_csv(os.path.join(outDir, "infras_extracts_afternoon.csv"))

extractedTrueBlocks = extract_epochs_from_merged_events(
    merged_events_df=trueBlocks,
    log_dir=procDir,
    log_loader=log_loader,
    pre_padding_seconds=0,
    post_padding_seconds=0,
    timestamp_col='AdjustedTimestamp'
)
extractedTrueBlocks.to_csv(os.path.join(outDir, "trueBlocks_extracts_afternoon.csv"))

extractedMarks = extract_epochs_from_merged_events(
    merged_events_df=onlyMarks,
    log_dir=procDir,
    log_loader=log_loader,
    pre_padding_seconds=0,
    post_padding_seconds=0,
    timestamp_col='AdjustedTimestamp'
)
extractedMarks.to_csv(os.path.join(outDir, "onlyMarks.csv"))
combined_Cylinderextracts = pd.concat([extractedCylinders, extractedMarks], ignore_index=True)

combined_Cylinderextracts.sort_values(by="AdjustedTimestamp", inplace=True)
combined_Cylinderextracts.to_csv(os.path.join(outDir, "combined_Cylinderextracts.csv"), index=False)

