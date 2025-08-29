import os
import json
import pandas as pd
import numpy as np
from pathlib import Path

# Define your merged directory
v1_outDir = Path("/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/MergedEvents_V1_flat")
output_dir = v1_outDir / "BehavioralAnalysis"
output_dir.mkdir(parents=True, exist_ok=True)

# Containers for all data
dropdist_all = []
blockdur_all = []
dropquality_all = []
swapvote_summary = []
pindropvote_po = []

for subdir in v1_outDir.iterdir():
    if subdir.is_dir():
        print(subdir)
        events_csv = subdir / "merged_events_augmented.csv"
        meta_json = subdir / "merged_meta.json"

        if not events_csv.exists() or not meta_json.exists():
            print(f"Skipping {subdir.name}: missing files.")
            continue

        participant_id = subdir.name

        # Load events
        df = pd.read_csv(events_csv)

        # Load meta
        with open(meta_json) as f:
            meta = json.load(f)

        # dropDist
        df_pindrop = df[df['lo_eventType'] == 'PinDrop_Moment'].copy()
        df_pindrop['dropDist'] = df_pindrop['details'].apply(lambda d: json.loads(d).get('drop_distance_manual', {}).get('deltax', np.nan))
        df_pindrop['ParticipantID'] = participant_id
        dropdist_all.append(df_pindrop[['ParticipantID', 'AppTime', 'dropDist']])

        # BlockDuration_sec
        block_df = pd.DataFrame(meta['BlockStructureSummary'])
        block_df['ParticipantID'] = participant_id
        blockdur_all.append(block_df[['ParticipantID', 'BlockNum', 'BlockDuration_sec']])

        # dropQuality % correct
        if 'details' in df.columns:
            df['dropQuality'] = df['details'].apply(lambda d: json.loads(d).get('dropQuality', np.nan))
            df_dq = df[df['dropQuality'].notna()]
            df_dq['correct'] = (df_dq['dropQuality'] == 1).astype(int)
            df_dq['cum_correct'] = df_dq['correct'].cumsum() / (np.arange(len(df_dq)) + 1)
            df_dq['ParticipantID'] = participant_id
            dropquality_all.append(df_dq[['ParticipantID', 'AppTime', 'cum_correct']])

        # SwapVoteScore % correct
        df['SwapVoteScore'] = df['details'].apply(lambda d: json.loads(d).get('SwapVoteScore', np.nan))
        total_swaps = df['SwapVoteScore'].notna().sum()
        correct_swaps = (df['SwapVoteScore'] == 1).sum()
        pct_correct_swaps = correct_swaps / total_swaps if total_swaps > 0 else np.nan
        swapvote_summary.append({
            'ParticipantID': participant_id,
            'TotalSwaps': total_swaps,
            'CorrectSwaps': correct_swaps,
            'PctCorrectSwaps': pct_correct_swaps
        })

        # PinDropVote PO only
        if 'PO' in participant_id:  # adjust this to your participant naming convention
            df['pinDropVote'] = df['details'].apply(lambda d: json.loads(d).get('pinDropVote', np.nan))
            df_pd = df[df['pinDropVote'].notna()]
            df_pd['correct'] = (df_pd['pinDropVote'] == 1).astype(int)
            df_pd['cum_correct'] = df_pd['correct'].cumsum() / (np.arange(len(df_pd)) + 1)
            df_pd['ParticipantID'] = participant_id
            pindropvote_po.append(df_pd[['ParticipantID', 'AppTime', 'cum_correct']])

# Combine and save
if dropdist_all:
    pd.concat(dropdist_all).to_csv(output_dir / "dropDist_over_time.csv", index=False)
if blockdur_all:
    pd.concat(blockdur_all).to_csv(output_dir / "BlockDuration_sec_over_time.csv", index=False)
if dropquality_all:
    pd.concat(dropquality_all).to_csv(output_dir / "dropQuality_correct_over_time.csv", index=False)
if swapvote_summary:
    pd.DataFrame(swapvote_summary).to_csv(output_dir / "SwapVoteScore_summary.csv", index=False)
if pindropvote_po:
    pd.concat(pindropvote_po).to_csv(output_dir / "pinDropVote_PO_over_time.csv", index=False)

print("Behavioral analysis complete! All files saved.")
