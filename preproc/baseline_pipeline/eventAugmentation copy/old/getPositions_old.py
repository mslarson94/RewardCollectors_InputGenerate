import pandas as pd
import numpy as np
from pathlib import Path
import os


trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
homeDir = 'FreshStart_mini'
eventsDir = os.path.join(trueRootDir, homeDir, 'full', 'Events_Flat_csv')
procDir = os.path.join(trueRootDir, homeDir, 'ProcessedData_Flat')
eventsFileName = os.path.join(eventsDir, "ObsReward_A_02_17_2025_15_11_processed_events.csv")
procFileName = os.path.join(procDir, "ObsReward_A_02_17_2025_15_11_processed.csv")
#PATH = os.path.join(root_directory, fileName)  # change as needed

events_path = Path(eventsFileName)
proc_path   = Path(procFileName)

ev   = pd.read_csv(events_path)
proc = pd.read_csv(proc_path)

# columns present in your file
headpos_anch_cols    = [c for c in proc.columns if c.startswith("HeadPosAnchored_")]
headforth_anch_cols  = [c for c in proc.columns if c.startswith("HeadForthAnchored_")]
anchor_cols = headpos_anch_cols + headforth_anch_cols

# sort by origRow and set index
proc_sorted = proc.sort_values("origRow").set_index("origRow", drop=True)

# build an index that includes all referenced event rows
rows_start = ev["origRow_start"].dropna().astype(int).tolist()
rows_end   = ev["origRow_end"].dropna().astype(int).tolist()
target_idx = sorted(set(proc_sorted.index.tolist()) | set(rows_start) | set(rows_end))

# reindex + forward-fill to get "last known at or before" each target row
pad = proc_sorted[anchor_cols].reindex(target_idx).ffill()

def extract_at(rows):
    # rows list may contain duplicates; list-loc preserves order and duplicates
    out = pad.loc[rows.fillna(-10).astype(int).tolist()].copy()
    # null-out the placeholder rows for NaNs in events
    out.loc[rows.isna().values] = np.nan
    return out

start_vals = extract_at(ev["origRow_start"]).add_suffix("_at_start").reset_index(drop=True)
end_vals   = extract_at(ev["origRow_end"]).add_suffix("_at_end").reset_index(drop=True)

ev_out = pd.concat([ev.reset_index(drop=True), start_vals, end_vals], axis=1)
outFileDir = os.path.join(trueRootDir, homeDir, 'full', 'eventsPositions')
outFileName = os.path.join(outFileDir, "ObsReward_A_02_17_2025_15_11_processed_events_pos.csv")
out_path = Path(outFileName)
ev_out.to_csv(out_path, index=False)

