# makeItCannonical.py
'''
sorts event dataframes to make them adhere to cannonical orders
Author: Myra Saraí Larson  10/02/2025
'''
#import re
#import os
from datetime import datetime, timedelta
#from io import StringIO
#import traceback
import pandas as pd 

EVENT_RANK = {
    "BlockStart": 0,
    "RoundStart": 1,
    "TrueContentStart": 2,
    "TrueContentEnd": 3,
    "BlockEnd": 4,
    "origRow_start": 5
}


def canonicalize_event_order_v1(df: pd.DataFrame, ts_col: str = "eMLT_orig") -> pd.DataFrame:
    for c in ("BlockNum","BlockInstance","RoundNum","origRow_start"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if ts_col in df.columns:
        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")

    # rank event types (start before end even if timestamps cross), origRow_start as tiebreaker
    df["__evt_rank"] = df["lo_eventType"].map(EVENT_RANK).fillna(3).astype(int)

    # stable sort -> block, instance, rank, time, origRow_start
    df = df.sort_values(
        by=["BlockNum","BlockInstance","__evt_rank", ts_col, "origRow_start"],
        kind="mergesort",
        na_position="last"
    ).drop(columns="__evt_rank")
    return df

def canonicalize_event_order(
    df: pd.DataFrame,
    *,
    start_col: str = "start_AppTime",
    end_col: str = "end_AppTime",
    group_first: bool = False,   # False => global chronological; True => grouped by block first
) -> pd.DataFrame:
    df = df.copy()

    for c in ("BlockNum","BlockInstance","RoundNum","origRow_start"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in (start_col, end_col):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["__evt_rank"] = df["lo_eventType"].map(EVENT_RANK).fillna(3).astype(int)

    # choose sort priority
    if group_first:
        by = ["BlockNum","BlockInstance","__evt_rank", start_col, end_col, "origRow_start"]
    else:
        by = [start_col, "__evt_rank", end_col, "BlockNum","BlockInstance","origRow_start"]

    by = [c for c in by if c in df.columns]

    df = df.sort_values(by=by, kind="mergesort", na_position="last").drop(columns="__evt_rank")
    return df

def blocks_table_from_events(events: pd.DataFrame) -> pd.DataFrame:
    """
    Build per-(BlockNum,BlockInstance) ranges using **ground truth** already in BlockStart rows.
    No recomputation if 'origRow_end' is present there; only fill if truly missing.
    """
    blocks = (events.loc[events["lo_eventType"].eq("BlockStart"),
                         ["BlockNum","BlockInstance","origRow_start","origRow_end","BlockStatus"]]
                   .copy())

    for c in ("BlockNum","BlockInstance","origRow_start","origRow_end"):
        blocks[c] = pd.to_numeric(blocks[c], errors="coerce")

    # Fill only missing ends (keep provided ground truth intact)
    need_end = blocks["origRow_end"].isna()
    if need_end.any():
        # fallback = next start - 1 within same (BlockNum, BlockInstance)
        blocks = blocks.sort_values(["BlockNum","BlockInstance","origRow_start"]).reset_index(drop=True)
        next_start_minus1 = blocks.groupby(["BlockNum","BlockInstance"])["origRow_start"].shift(-1) - 1
        blocks.loc[need_end, "origRow_end"] = next_start_minus1[need_end]

        # last block fallback: cap to max origRow seen for that (BlockNum, BlockInstance)
        last_rows = (events.groupby(["BlockNum","BlockInstance"])["origRow"]
                            .max(min_count=1).rename("last_row_in_block").reset_index())
        blocks = blocks.merge(last_rows, on=["BlockNum","BlockInstance"], how="left")
        blocks["origRow_end"] = blocks["origRow_end"].fillna(blocks["last_row_in_block"])
        blocks = blocks.drop(columns=["last_row_in_block"])

    blocks["origRow_start"] = blocks["origRow_start"].astype("Int64")
    blocks["origRow_end"]   = blocks["origRow_end"].astype("Int64")

    # Optional: keep complete only
    if "BlockStatus" in blocks.columns:
        ok = blocks["BlockStatus"].astype(str).str.lower().eq("complete")
        blocks = blocks.loc[ok].drop(columns=["BlockStatus"])

    return blocks


# I do not think we actually need this function at all but keeping it here just in case.
def enforce_single_row_event_invariants(df: pd.DataFrame, single_row_events: [str] = ("BlockStart","BlockEnd","TrueContentStart","TrueContentEnd")) -> pd.DataFrame:
    """Ensure single-row events have origRow_start == origRow_end and are integers. type: (pd.DataFrame) -> pd.DataFrame"""
    df = df.copy()

    # numeric normalization
    for c in ("origRow", "origRow_start", "origRow_end", "BlockNum", "BlockInstance", "RoundNum"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    m = df["lo_eventType"].isin(single_row_events)

    # if start/end missing, derive from origRow; else force end = start
    if "origRow" in df.columns:
        df.loc[m & df["origRow_start"].isna(), "origRow_start"] = df.loc[m, "origRow"]

    df.loc[m & df["origRow_end"].isna(),   "origRow_end"]   = df.loc[m, "origRow_start"]
    df.loc[m & (df["origRow_end"] != df["origRow_start"]), "origRow_end"] = df.loc[m, "origRow_start"]

    # integer nullable dtypes for row indices
    for c in ("origRow", "origRow_start", "origRow_end"):
        if c in df.columns:
            df[c] = df[c].astype("Int64")

    # final sanity
    bad = df.loc[m & (df["origRow_end"] != df["origRow_start"]), ["lo_eventType","origRow_start","origRow_end"]]
    if not bad.empty:
        raise ValueError(f"Single-row invariant violated for:\n{bad}")

    return df