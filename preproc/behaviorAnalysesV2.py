import os
import json
import pandas as pd
import numpy as np
from pathlib import Path

# Define your flat directory of augmented merged CSVs
#v1_outDir = Path("/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/alignedPO/MergedEvents_V1_Flat")
v1_outDir = Path("/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/MergedEvents_V1_flat")
v1Meta = Path("/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/MergedEvents_V1_jsonFlat")
output_dir = v1_outDir / "BehavioralAnalysis"
output_dir.mkdir(parents=True, exist_ok=True)

# Containers for all data
dropdist_all = []
blockdur_all = []
dropquality_all = []
swapvote_summary = []
pindropvote_po = []

# Iterate over all CSV files in the directory
for events_csv in v1_outDir.glob("*.csv"):
    participant_id = events_csv.stem  # e.g., 'Participant001'
    meta_json = events_csv.with_name(f"{participant_id.replace('_augmented', '')}_meta.json")

    if not events_csv.exists() or not meta_json.exists():
        print(f"Skipping {participant_id}: missing files.")
        continue

    print(f"Processing {participant_id}...")

    # Load events
    df = pd.read_csv(events_csv)

    # Load meta
    with open(meta_json) as f:
        meta = json.load(f)

    # dropDist
    # df_pindrop['dropDist'] = df_pindrop['details'].apply(lambda d: json.loads(d).get('dropDist', np.nan))
    # df_pindrop['ParticipantID'] = participant_id
    # dropdist_all.append(df_pindrop[['ParticipantID', 'AppTime', 'dropDist']])

    df_pindrop = df[df['lo_eventType'] == 'PinDrop_Moment'].copy()
    if not df_pindrop.empty:
        df_pindrop['dropDist'] = df_pindrop['details'].apply(lambda d: json.loads(d).get('dropDist', np.nan))
        df_pindrop['ParticipantID'] = participant_id
        dropdist_all.append(df_pindrop[['ParticipantID', 'AppTime', 'dropDist']])


    

    # dropQuality % correct
    df['dropQuality'] = df['details'].apply(lambda d: json.loads(d).get('dropQual', np.nan))
    df_dq = df[df['dropQuality'].notna()]
    df_dq['correct'] = (df_dq['dropQuality'] == 'CORRECT').astype(int)
    df_dq['cum_correct'] = df_dq['correct'].cumsum() / (np.arange(len(df_dq)) + 1)
    df_dq['ParticipantID'] = participant_id
    dropquality_all.append(df_dq[['ParticipantID', 'AppTime', 'cum_correct']])

    # SwapVoteScore % correct
    df_swap = df[df['lo_eventType'] == 'SwapVoteMoment'].copy()
    df_swap['SwapVoteScore'] = df_swap['details'].apply(lambda d: json.loads(d).get('SwapVoteScore', np.nan))
    total_swaps = df_swap['SwapVoteScore'].notna().sum()
    correct_swaps = (df_swap['SwapVoteScore'] == 'Correct').sum()
    pct_correct_swaps = correct_swaps / total_swaps if total_swaps > 0 else np.nan
    swapvote_summary.append({
        'ParticipantID': participant_id,
        'TotalSwaps': total_swaps,
        'CorrectSwaps': correct_swaps,
        'PctCorrectSwaps': pct_correct_swaps
    })

    # PinDropVote PO only (compare pinDropVote to dropQual)
    if 'ML2C' in participant_id or 'ML2G' in participant_id:
        df['pinDropVote'] = df['details'].apply(lambda d: json.loads(d).get('pinDropVote', np.nan))
        df['dropQuality'] = df['details'].apply(lambda d: json.loads(d).get('dropQual', np.nan))
        df_pd = df[df['pinDropVote'].notna() & df['dropQuality'].notna()]
        df_pd['correct'] = (df_pd['pinDropVote'] == df_pd['dropQuality']).astype(int)
        df_pd['cum_correct'] = df_pd['correct'].cumsum() / (np.arange(len(df_pd)) + 1)
        df_pd['ParticipantID'] = participant_id
        pindropvote_po.append(df_pd[['ParticipantID', 'AppTime', 'cum_correct']])
    print(len(pindropvote_po))

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
