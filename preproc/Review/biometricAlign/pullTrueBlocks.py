import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import ast

# Define paths
trueRootDir = "/Users/mairahmac/Desktop/RC_TestingNotes/"
baseDirName = "SmallSelectedData/RNS"
baseDir = os.path.join(trueRootDir, baseDirName)
eventsDir = os.path.join(baseDir, "ExtractedEvents_csv_Flat")
procDir = os.path.join(baseDir, "ProcessedData_Flat")
outDirBase = os.path.join(baseDir, "trueBlocks")
os.makedirs(outDirBase, exist_ok=True)

def log_loader(fileName, dir):
    filePath = os.path.join(dir, fileName)
    df = pd.read_csv(filePath)
    return df 

def safe_parse_timestamp(ts):
    try:
        if isinstance(ts, str) and ts.count(':') == 3:
            hh, mm, ss, ms = ts.split(':')
            ms = (ms + '000')[:6]
            return datetime.strptime(f"{hh}:{mm}:{ss}:{ms}", "%H:%M:%S:%f")
        return datetime.strptime(ts, "%H:%M:%S:%f")
    except Exception:
        return None

def get_events_by_lo_eventType(df, event_types):
    return df[df['lo_eventType'].isin(event_types)]

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

# Get all processed event CSVs
all_event_files = [f for f in os.listdir(eventsDir) if f.endswith('_processed_events.csv')]

for eventsFileName in all_event_files:
    print(f"Processing {eventsFileName}...")
    eventsFile = os.path.join(eventsDir, eventsFileName)
    events_df = pd.read_csv(eventsFile)

    # Extract trueBlocks events
    trueBlocks_types = ["TrueBlockStart", "TrueBlockEnd"]
    trueBlocks = get_events_by_lo_eventType(events_df, trueBlocks_types)
    
    outDir = os.path.join(outDirBase)
    os.makedirs(outDir, exist_ok=True)
    
    outFileName = f"trueBlocks_{eventsFileName.replace('_processed_events.csv', '')}.csv"
    trueBlocks.to_csv(os.path.join(outDir, outFileName), index=False)
