#!/usr/bin/env python3
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Dict, Tuple, List
import os

trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
procDir = 'FreshStart_mini'
root_directory = os.path.join(trueRootDir, procDir, 'full', 'Events_Flat_csv')

fileName = "ObsReward_A_02_17_2025_15_11_processed_events.csv"
PATH = os.path.join(root_directory, fileName)  # change as needed

df = pd.read_csv(PATH)
df["mLTimestamp"] = pd.to_datetime(df["mLTimestamp"], errors="coerce")
df = df.sort_values(["mLTimestamp"], kind="mergesort").reset_index(drop=True)

# keep rows that belong to a block instance
ev = df[df["BlockNum"].notna() & df["BlockInstance"].notna()].copy()
for col in ("BlockNum", "BlockInstance", "RoundNum"):
    if col in ev.columns:
        ev[col] = ev[col].astype("Int64")

ev = ev.sort_values(["BlockNum", "BlockInstance", "mLTimestamp"]).reset_index(drop=True)

for c in ("block_elapsed_s", "round_elapsed_s", "truecontent_elapsed_s"):
    ev[c] = np.nan

@dataclass
class Interval:
    start: pd.Timestamp
    end: pd.Timestamp
    duration_s: float
    blocknum: int
    blockinstance: int
    roundnum: int | None = None
    index_in_block: int | None = None

block_intervals: List[Interval] = []
round_intervals: List[Interval] = []
true_intervals: List[Interval] = []

open_start: Dict[Tuple[int,int,str], pd.Timestamp] = {}
round_idx: Dict[Tuple[int,int], int] = {}
true_idx: Dict[Tuple[int,int], int] = {}

def close_interval(kind: str, key: Tuple[int,int], end_ts: pd.Timestamp, roundnum):
    start_ts = open_start.pop((key[0], key[1], kind), None)
    if start_ts is None or pd.isna(start_ts):  # unmatched end
        return
    dur = (end_ts - start_ts).total_seconds()
    if kind == "Block":
        block_intervals.append(Interval(start_ts, end_ts, dur, key[0], key[1]))
    elif kind == "Round":
        round_intervals.append(Interval(start_ts, end_ts, dur, key[0], key[1], int(roundnum) if pd.notna(roundnum) else None, round_idx[key]))
    elif kind == "TrueContent":
        true_intervals.append(Interval(start_ts, end_ts, dur, key[0], key[1], int(roundnum) if pd.notna(roundnum) else None, true_idx[key]))

for (bn, bi), g in ev.groupby(["BlockNum", "BlockInstance"], sort=False):
    round_idx[(bn,bi)] = 0
    true_idx[(bn,bi)] = 0
    for i, row in g.iterrows():
        ts = row["mLTimestamp"]
        et = row["lo_eventType"]
        key = (bn, bi)

        # update running timers at this timestamp
        for kind, col in (("Block","block_elapsed_s"), ("Round","round_elapsed_s"), ("TrueContent","truecontent_elapsed_s")):
            st = open_start.get((bn,bi,kind))
            if st is not None and pd.notna(st):
                ev.at[i, col] = (ts - st).total_seconds()

        # state transitions
        if et == "BlockStart":
            open_start[(bn,bi,"Block")] = ts
            ev.at[i, "block_elapsed_s"] = 0.0
        elif et == "BlockEnd":
            if (bn,bi,"Block") in open_start:
                ev.at[i, "block_elapsed_s"] = (ts - open_start[(bn,bi,"Block")]).total_seconds()
            close_interval("Block", key, ts, roundnum=None)

        elif et == "RoundStart":
            open_start[(bn,bi,"Round")] = ts
            round_idx[(bn,bi)] += 1
            ev.at[i, "round_elapsed_s"] = 0.0
        elif et == "RoundEnd":
            if (bn,bi,"Round") in open_start:
                ev.at[i, "round_elapsed_s"] = (ts - open_start[(bn,bi,"Round")]).total_seconds()
            close_interval("Round", key, ts, roundnum=row.get("RoundNum", pd.NA))

        elif et == "TrueContentStart":
            open_start[(bn,bi,"TrueContent")] = ts
            true_idx[(bn,bi)] += 1
            ev.at[i, "truecontent_elapsed_s"] = 0.0
        elif et == "TrueContentEnd":
            if (bn,bi,"TrueContent") in open_start:
                ev.at[i, "truecontent_elapsed_s"] = (ts - open_start[(bn,bi,"TrueContent")]).total_seconds()
            close_interval("TrueContent", key, ts, roundnum=row.get("RoundNum", pd.NA))

# build summaries
blocks_df = pd.DataFrame([vars(x) for x in block_intervals]).sort_values(["blocknum","blockinstance","start"])
rounds_df = pd.DataFrame([vars(x) for x in round_intervals]).sort_values(["blocknum","blockinstance","start"])
true_df   = pd.DataFrame([vars(x) for x in true_intervals]).sort_values(["blocknum","blockinstance","start"])

# select compact event view
annot = ev[["BlockNum","BlockInstance","RoundNum","lo_eventType","mLTimestamp","mLTimestamp",
            "block_elapsed_s","round_elapsed_s","truecontent_elapsed_s"]]


out_directory = os.path.join(trueRootDir, procDir, 'full', 'elapsedTime')
# optional: write to disk
annot.to_csv(out_directory + "/annotated_events_with_timers.csv", index=False)
blocks_df.to_csv(out_directory + "/block_intervals.csv", index=False)
rounds_df.to_csv(out_directory + "/round_intervals.csv", index=False)
true_df.to_csv("truecontent_intervals.csv", index=False)
