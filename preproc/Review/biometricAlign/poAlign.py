import pandas as pd
from datetime import datetime, timedelta
import os
import json
from glob import glob

# baseDirName = "SmallSelectedData/idealTestFile2"
baseDirName = "SmallSelectedData/RNS"

eventsFileName = "merged_events.csv"
# eventsFileName = "merged_events2.csv"
##############
trueRootDir = "/Users/mairahmac/Desktop/RC_TestingNotes/"
baseDir = os.path.join(trueRootDir, baseDirName)
procDir = os.path.join(baseDir, "ProcessedData_Flat")
eventsDir = os.path.join(baseDir, "merged_marks")

eventsFile = os.path.join(eventsDir, eventsFileName)
events_df = pd.read_csv(eventsFile)
outDir = os.path.join(baseDir, "alignedPO")
# outDir = os.path.join(baseDir, "fullyExtractedEvents_V3_AN")
print(outDir)
os.makedirs(outDir, exist_ok=True)

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

def parse_timestamp(ts_str):
    """Helper to parse timestamps like '14:01:12:018'."""
    return datetime.strptime(ts_str, "%H:%M:%S:%f")

def align_full_po_log(an_blocks_df, po_blocks_df, po_log_df, timestamp_col='Timestamp'):
    """
    Aligns the full PO log using offsets computed from AN block starts.

    - an_blocks_df: DataFrame with ['BlockNum', 'BlockInstance', 'Timestamp'] from AN log
    - po_blocks_df: DataFrame with ['BlockNum', 'BlockInstance', 'Timestamp'] from PO log
    - po_log_df: Full PO log DataFrame with ['BlockNum', 'Timestamp']
    """
    aligned_po_df = po_log_df.copy()
    aligned_po_df['AdjustedTimestamp'] = None

    for _, an_row in an_blocks_df.iterrows():
        block_num = an_row["BlockNum"]
        block_instance = an_row["BlockInstance"]
        an_time = safe_parse_timestamp(an_row["Timestamp"])
        
        po_match = po_blocks_df[
            (po_blocks_df["BlockNum"] == block_num) &
            (po_blocks_df["BlockInstance"] == block_instance)
        ]
        
        if po_match.empty:
            print(f"⚠️ BlockNum {block_num} instance {block_instance} missing in PO log.")
            continue

        po_time = safe_parse_timestamp(po_match.iloc[0]["Timestamp"])
        offset = an_time - po_time

        # Find all rows in PO log that belong to this block
        mask = (
            (po_log_df["BlockNum"] == block_num)
        )
        aligned_po_df.loc[mask, "AdjustedTimestamp"] = po_log_df.loc[mask, timestamp_col].apply(
            lambda ts: (safe_parse_timestamp(ts) + offset).strftime('%H:%M:%S:%f')
        )

    return aligned_po_df

# Usage example:
# aligned_po_log = align_full_po_log(an_blocks_df, po_blocks_df, po_log_df)
# aligned_po_log.to_csv('aligned_full_po_log.csv', index=False)
an_log_df = log_loader("ObsReward_A_03_17_2025_14_00_processed.csv", procDir)
po_log_df = log_loader("ObsReward_B_03_17_2025_14_00_processed.csv", procDir)

poBlocksDir = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/trueBlocks_PO'
anBlocksDir = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/trueBlocks_AN'
po_blocks_df = log_loader("trueBlocks_PO.csv", poBlocksDir)
an_blocks_df = log_loader("trueBlocks_AN.csv", anBlocksDir)
an_blocks_df["BlockInstance"] = an_blocks_df.groupby("BlockNum").cumcount()
po_blocks_df["BlockInstance"] = po_blocks_df.groupby("BlockNum").cumcount()
aligned_po_log = align_full_po_log(an_blocks_df, po_blocks_df, po_log_df)
po_out = os.path.join(outDir, 'Align_ObsReward_B_03_17_2025_14_00_processed.csv')
aligned_po_log.to_csv(po_out, index=False)