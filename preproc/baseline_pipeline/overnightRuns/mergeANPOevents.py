#!/usr/bin/env python3
# mergeANPOevents.py
# Usage:
#   python mergeANPOevents.py --a ObsReward_A_02_17_2025_15_11_events_final.csv \
#                             --b ObsReward_B_02_17_2025_15_11_events_final.csv \
#                             --outdir ./out \
#                             --match-window 60
#
# Outputs (CSV):
#   out/merged_pairs_nearest.csv        # matched non-infrastructure events with delay_ms = (B - A)
#   out/merged_timeline_union.csv       # union timeline, canonical ts=mLTimestamp, pair_id for matched
#   out/delay_summary_per_round.csv     # delay stats per (BlockNum, RoundNum)
#   out/delay_summary_per_block.csv     # delay stats per BlockNum

import argparse
import os
from typing import List, Dict
import numpy as np
import pandas as pd

INFRA_EVENTS = {
    'BlockStart','BlockEnd','RoundStart','RoundEnd',
    'PreBlock_CylinderWalk_start','PreBlock_CylinderWalk_segment','PreBlock_CylinderWalk_end',
    'InterRound_PostCylinderWalk_start','InterRound_PostCylinderWalk_end',
    'InterBlock_Idle_start','InterBlock_Idle_segment','InterBlock_Idle_end',
    'VotingWindow','VoteInstrText_Vis','SwapVoteText_end','SwapVoteMoment',
    'BlockScoreText_start','BlockScoreText_Vis','TrueContentStart','TrueContentEnd',
    'Mark','NextChestVis_start','CurrChestVis_end','CoinVis_start','CoinVis_end','ChestOpenAnimation','ChestOpenEmpty',
}

## Events we can't match 
noMatchSelfEvents = {'PreBlock_CylinderWalk_start','PreBlock_CylinderWalk_segment','PreBlock_CylinderWalk_end',
    'InterRound_PostCylinderWalk_start','InterRound_PostCylinderWalk_end',
    'InterBlock_Idle_start','InterBlock_Idle_segment','InterBlock_Idle_end', 'SwapVoteText_end','SwapVoteMoment',
    'BlockScoreText_start','BlockScoreText_Vis', 'Mark', 'AppTime'}

poSpecificEventsToMerge = {'VotingWindow','VoteInstrText_Vis'}
anSpecificEventsToMerge = {'NextChestVis_start','CurrChestVis_end','CoinVis_start','CoinVis_end','ChestOpenAnimation','ChestOpenEmpty'}

# True content start/stop might actually be inaccurate here depending on how I was segmenting things - Go through that tomorrow 
# AppTime might be inaccurately grouped in the noMatchSelfEvents crew, maybe we can align by taking those times as the anchor points & warping from there. 
commonEvents = {'TrueContentStart','TrueContentEnd','BlockStart','BlockEnd','RoundStart','RoundEnd',}


def _read(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df['ts'] = pd.to_datetime(df['mLTimestamp'], errors='coerce')
    df['BlockNum'] = pd.to_numeric(df.get('BlockNum'), errors='coerce')
    df['RoundNum'] = pd.to_numeric(df.get('RoundNum'), errors='coerce')
    if 'lo_eventType' not in df.columns:
        raise ValueError(f"{path}: required column 'lo_eventType' is missing.")
    df['eventType'] = df['lo_eventType'].astype(str)

    if 'AppTime' not in df.columns:
        df['AppTime'] = np.nan
    return df

def _apply_aliases_on_B(dfB: pd.DataFrame, alias_map: Dict[str,str]) -> pd.DataFrame:
    out = dfB.copy()
    out['eventType'] = out['eventType'].map(lambda s: alias_map.get(s, s))
    return out

def _match_nearest(A: pd.DataFrame, B: pd.DataFrame, shared_events: List[str], max_diff_s: float) -> pd.DataFrame:
    rows = []
    Afil = A[A['eventType'].isin(shared_events) & A['ts'].notna()]
    Bfil = B[B['eventType'].isin(shared_events) & B['ts'].notna()]

    for (blk, rnd, evt), ag in Afil.groupby(['BlockNum','RoundNum','eventType']):
        bg = Bfil[(Bfil['BlockNum']==blk) & (Bfil['RoundNum']==rnd) & (Bfil['eventType']==evt)].copy()
        if bg.empty:
            continue
        ag = ag.sort_values('ts').reset_index(drop=True)
        bg = bg.sort_values('ts').reset_index(drop=True)

        used = np.zeros(len(bg), dtype=bool)
        for i, ta in enumerate(ag['ts']):
            j_best, best = -1, None
            for j, tb in enumerate(bg['ts']):
                if used[j]:
                    continue
                d = abs((tb - ta).total_seconds())
                if best is None or d < best:
                    j_best, best = j, d
            if j_best >= 0 and best is not None and best <= max_diff_s:
                used[j_best] = True
                tb = bg.loc[j_best, 'ts']
                rows.append({
                    'BlockNum': blk,
                    'RoundNum': rnd,
                    'eventType': evt,
                    'ts_A': ta,
                    'ts_B': tb,
                    'AppTime_A': ag.loc[i, 'AppTime'],
                    'AppTime_B': bg.loc[j_best, 'AppTime'],
                    'delay_ms': (tb - ta).total_seconds() * 1000.0,
                })
    return pd.DataFrame(rows)

def _summaries(nearest_df: pd.DataFrame, A: pd.DataFrame, B: pd.DataFrame, shared_events: List[str]):
    a_counts = A[A['eventType'].isin(shared_events)].groupby(['BlockNum','RoundNum']).size().rename('n_A')
    b_counts = B[B['eventType'].isin(shared_events)].groupby(['BlockNum','RoundNum']).size().rename('n_B')

    per_round = nearest_df.groupby(['BlockNum','RoundNum']).agg(
        n_matched=('delay_ms','count'),
        mean_delay_ms=('delay_ms','mean'),
        median_delay_ms=('delay_ms','median'),
        p95_abs_delay_ms=('delay_ms', lambda s: float(np.percentile(np.abs(s), 95)) if len(s)>0 else np.nan),
        mean_abs_delay_ms=('delay_ms', lambda s: float(np.mean(np.abs(s))) if len(s)>0 else np.nan),
    ).reset_index().merge(a_counts, on=['BlockNum','RoundNum'], how='outer') \
     .merge(b_counts, on=['BlockNum','RoundNum'], how='outer')

    per_round['n_A'] = per_round['n_A'].fillna(0).astype(int)
    per_round['n_B'] = per_round['n_B'].fillna(0).astype(int)
    per_round['match_rate_vs_A'] = per_round['n_matched'] / per_round['n_A'].replace(0, np.nan)
    per_round['match_rate_vs_B'] = per_round['n_matched'] / per_round['n_B'].replace(0, np.nan)
    per_round = per_round.sort_values(['BlockNum','RoundNum'])

    a_counts_blk = A[A['eventType'].isin(shared_events)].groupby('BlockNum').size().rename('n_A')
    b_counts_blk = B[B['eventType'].isin(shared_events)].groupby('BlockNum').size().rename('n_B')
    per_block = nearest_df.groupby('BlockNum').agg(
        n_matched=('delay_ms','count'),
        mean_delay_ms=('delay_ms','mean'),
        median_delay_ms=('delay_ms','median'),
        p95_abs_delay_ms=('delay_ms', lambda s: float(np.percentile(np.abs(s), 95)) if len(s)>0 else np.nan),
        mean_abs_delay_ms=('delay_ms', lambda s: float(np.mean(np.abs(s))) if len(s)>0 else np.nan),
    ).reset_index().merge(a_counts_blk, on='BlockNum', how='outer') \
     .merge(b_counts_blk, on='BlockNum', how='outer')

    per_block['n_A'] = per_block['n_A'].fillna(0).astype(int)
    per_block['n_B'] = per_block['n_B'].fillna(0).astype(int)
    per_block['match_rate_vs_A'] = per_block['n_matched'] / per_block['n_A'].replace(0, np.nan)
    per_block['match_rate_vs_B'] = per_block['n_matched'] / per_block['n_B'].replace(0, np.nan)
    per_block = per_block.sort_values('BlockNum')
    return per_round, per_block

def _build_union_timeline(A: pd.DataFrame, B: pd.DataFrame, nearest_df: pd.DataFrame) -> pd.DataFrame:
    a_tl = A[['BlockNum','RoundNum','eventType','ts','AppTime']].copy()
    a_tl = a_tl.rename(columns={'ts':'ts_canon','AppTime':'AppTime_src'})
    a_tl['source'] = 'A'
    b_tl = B[['BlockNum','RoundNum','eventType','ts','AppTime']].copy()
    b_tl = b_tl.rename(columns={'ts':'ts_canon','AppTime':'AppTime_src'})
    b_tl['source'] = 'B'
    timeline = pd.concat([a_tl, b_tl], ignore_index=True).sort_values(['BlockNum','RoundNum','ts_canon','source'])

    pairs = nearest_df.copy().reset_index(drop=True)
    pairs['pair_id'] = pairs.index + 1

    tl = timeline.copy()
    tl['pair_id'] = np.nan
    tl['delay_ms'] = np.nan

    Akey = pairs[['BlockNum','RoundNum','eventType','ts_A','pair_id','delay_ms']]
    tl = tl.merge(Akey, left_on=['BlockNum','RoundNum','eventType','ts_canon'],
                  right_on=['BlockNum','RoundNum','eventType','ts_A'], how='left')
    tl['pair_id'] = tl['pair_id_y'].combine_first(tl['pair_id_x'])
    tl['delay_ms'] = tl['delay_ms_y'].combine_first(tl['delay_ms_x'])
    tl = tl.drop(columns=['pair_id_x','delay_ms_x','pair_id_y','delay_ms_y','ts_A'])

    Bkey = pairs[['BlockNum','RoundNum','eventType','ts_B','pair_id','delay_ms']]
    tl = tl.merge(Bkey, left_on=['BlockNum','RoundNum','eventType','ts_canon'],
                  right_on=['BlockNum','RoundNum','eventType','ts_B'], how='left', suffixes=('','_B'))
    tl['pair_id'] = tl['pair_id_B'].combine_first(tl['pair_id'])
    tl['delay_ms'] = tl['delay_ms_B'].combine_first(tl['delay_ms'])
    tl = tl.drop(columns=['pair_id_B','delay_ms_B','ts_B'])
    return tl

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--a', required=True, help='ObsReward A CSV (leader)')
    ap.add_argument('--b', required=True, help='ObsReward B CSV (follower)')
    ap.add_argument('--outdir', required=True)
    ap.add_argument('--match-window', type=float, default=60.0,
                    help='Nearest-neighbor match window in seconds (default 60)')
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    A = _read(args.a)
    B = _read(args.b)

    # Shared, non-infrastructure events
    shared = sorted(set(A['eventType']).intersection(set(B['eventType'])) - INFRA_EVENTS)

    nearest = _match_nearest(A, B, shared, max_diff_s=args.match_window)
    per_round, per_block = _summaries(nearest, A, B, shared)
    union_tl = _build_union_timeline(A, B, nearest)

    nearest.to_csv(os.path.join(args.outdir, 'merged_pairs_nearest.csv'), index=False)
    union_tl.to_csv(os.path.join(args.outdir, 'merged_timeline_union.csv'), index=False)
    per_round.to_csv(os.path.join(args.outdir, 'delay_summary_per_round.csv'), index=False)
    per_block.to_csv(os.path.join(args.outdir, 'delay_summary_per_block.csv'), index=False)

    print("OK")

if __name__ == '__main__':
    main()
