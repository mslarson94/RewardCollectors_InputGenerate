#!/usr/bin/env python3
"""
Add session-level ordering, audit flags, and running totals for blocks/rounds.

For each input CSV (one participant/session):
1) Sort by testingOrder, start_AppTime
2) Create TotSesh_rowIndex (1..N)
3) Create isLastRoundOfSourceFile (last Round within each source_file)
4) Create isIncompleteRound (<3 chestPin events within a round group)
5) Create running totals:
   - TotSesh_runTot_RoundNum_all: all attempted rounds
   - TotSesh_runTot_RoundNum: excludes last Round of any BlockStatus=='truncated' block
   - TotSesh_runTot_BlockNum_all: all attempted blocks
   - TotSesh_runTot_BlockNum: completed blocks only (BlockStatus=='completed')

Usage:
  python add_session_running_totals.py --input_dir /path/to/csvs --output_dir /path/to/out
  python add_session_running_totals.py --input_file session.csv --output_file session_out.csv

Notes:
- Ordering uses ONLY: testingOrder ASC, start_AppTime ASC
- Round group for isIncompleteRound & isLastRoundOfSourceFile:
    (source_file, BlockNum, BlockInstance, RoundNum)
- Round attempt key for running totals:
    (RoundNum, BlockNum, BlockInstance, source_file, BlockStatus)
- Block attempt key for running totals:
    (BlockNum, BlockInstance, source_file, BlockStatus)
"""

from __future__ import annotations

import argparse
import glob
import os
from dataclasses import dataclass
from typing import Iterable, Optional

import numpy as np
import pandas as pd


REQUIRED_COLS = [
    "testingOrder",
    "start_AppTime",
    "source_file",
    "BlockNum",
    "BlockInstance",
    "RoundNum",
    "BlockStatus",
    "chestPin_num",
]


ROUND_GROUP_COLS = ["source_file", "BlockNum", "BlockInstance", "RoundNum"]
ROUND_ATTEMPT_KEY = ["RoundNum", "BlockNum", "BlockInstance", "source_file", "BlockStatus"]
BLOCK_ATTEMPT_KEY = ["BlockNum", "BlockInstance", "source_file", "BlockStatus"]


def _ensure_required_cols(df: pd.DataFrame, path: str) -> None:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"{path}: missing required columns: {missing}")


def _parse_start_time(df: pd.DataFrame) -> pd.Series:
    """
    Best-effort parse. Keeps original values for sorting if already numeric.
    Returns a Series suitable for sorting.
    """
    s = df["start_AppTime"]
    if np.issubdtype(s.dtype, np.number):
        return s.astype(float)
    # Try datetime parsing; fall back to original strings if parsing fails
    dt = pd.to_datetime(s, errors="coerce", utc=False)
    if dt.notna().any():
        # For NaT values, fall back to original value to preserve determinism
        # (NaTs will become very small/large if we force; so use string fallback ordering)
        # We'll sort by dt where available, then by original string.
        # Build a composite numeric sort key: datetime ns where present else NaN,
        # and later use secondary sort on original s.
        return dt
    # All failed -> keep as string
    return s.astype(str)


def _compute_first_index_map(df: pd.DataFrame, key_cols: list[str], idx_col: str) -> pd.DataFrame:
    """
    Return a dataframe of unique keys with their first occurrence index (min idx_col),
    sorted by first occurrence.
    """
    starts = (
        df.groupby(key_cols, dropna=False, sort=False)[idx_col]
        .min()
        .reset_index()
        .rename(columns={idx_col: "first_rowIndex"})
        .sort_values("first_rowIndex", kind="mergesort")
        .reset_index(drop=True)
    )
    return starts


def _assign_running_total_by_starts(
    df: pd.DataFrame,
    starts: pd.DataFrame,
    key_cols: list[str],
    out_col: str,
    exclude_mask_in_starts: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """
    Given 'starts' with one row per unique key and 'first_rowIndex' sorted,
    compute running totals and merge back to df, assigning to all rows sharing the key.
    If exclude_mask_in_starts is provided, excluded keys contribute 0 increment.
    """
    starts = starts.copy()

    if exclude_mask_in_starts is None:
        inc = np.ones(len(starts), dtype=int)
    else:
        inc = (~exclude_mask_in_starts.to_numpy(dtype=bool)).astype(int)

    starts[out_col] = np.cumsum(inc)

    df = df.merge(starts[key_cols + [out_col]], on=key_cols, how="left", validate="m:1")
    return df


def process_one_file(in_path: str, out_path: str) -> None:
    df = pd.read_csv(in_path)

    _ensure_required_cols(df, in_path)

    # Stable sort (mergesort) by ONLY testingOrder + start_AppTime
    start_time_sort = _parse_start_time(df)

    # Keep original row order as a deterministic last resort tie-breaker (not used for sorting keys),
    # so mergesort + existing order remains stable when equal.
    df["_origRow"] = np.arange(len(df), dtype=int)
    df["_startTimeSort"] = start_time_sort

    df = df.sort_values(
        by=["testingOrder", "_startTimeSort", "_origRow"],
        ascending=[True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    # TotSesh_rowIndex (1..N)
    df["TotSesh_rowIndex"] = np.arange(1, len(df) + 1, dtype=int)

    # ---- Audit/helper flags ----

    # isIncompleteRound: round groups with < 3 rows
    round_sizes = (
        df.groupby(ROUND_GROUP_COLS, dropna=False, sort=False)
        .size()
        .rename("round_rowCount")
        .reset_index()
    )
    round_sizes["isIncompleteRound"] = round_sizes["round_rowCount"] < 3
    df = df.merge(round_sizes[ROUND_GROUP_COLS + ["isIncompleteRound"]], on=ROUND_GROUP_COLS, how="left", validate="m:1")

    # isLastRoundOfSourceFile: for each source_file, identify the last round (by max TotSesh_rowIndex)
    # then mark all rows belonging to that last round.
    last_round_per_source = (
        df.groupby(["source_file"] + ROUND_GROUP_COLS[1:], dropna=False, sort=False)["TotSesh_rowIndex"]
        .max()
        .reset_index()
        .rename(columns={"TotSesh_rowIndex": "round_last_rowIndex"})
    )
    # pick the round with the maximum round_last_rowIndex per source_file
    last_round_per_source = (
        last_round_per_source.sort_values(["source_file", "round_last_rowIndex"], kind="mergesort")
        .groupby("source_file", dropna=False, sort=False)
        .tail(1)
        .reset_index(drop=True)
    )
    # merge marker
    last_round_per_source["isLastRoundOfSourceFile"] = True
    df = df.merge(
        last_round_per_source[ROUND_GROUP_COLS + ["isLastRoundOfSourceFile"]],
        on=ROUND_GROUP_COLS,
        how="left",
        validate="m:1",
    )
    df["isLastRoundOfSourceFile"] = df["isLastRoundOfSourceFile"].fillna(False)

    # ---- Running totals: Rounds ----

    # Round attempts start table
    round_starts = _compute_first_index_map(df, ROUND_ATTEMPT_KEY, "TotSesh_rowIndex")

    # TotSesh_runTot_RoundNum_all: all attempted rounds
    df = _assign_running_total_by_starts(
        df=df,
        starts=round_starts,
        key_cols=ROUND_ATTEMPT_KEY,
        out_col="TotSesh_runTot_RoundNum_all",
    )

    # Identify excluded rounds: last Round of each truncated block attempt
    # For each truncated block attempt, pick the RoundNum with max TotSesh_rowIndex (i.e., last round in that block)
    truncated_block_rows = df[df["BlockStatus"].astype(str).str.lower() == "truncated"].copy()
    if len(truncated_block_rows) > 0:
        # last round within each truncated block attempt
        last_round_in_trunc_block = (
            truncated_block_rows.groupby(BLOCK_ATTEMPT_KEY + ["RoundNum"], dropna=False, sort=False)["TotSesh_rowIndex"]
            .max()
            .reset_index()
            .rename(columns={"TotSesh_rowIndex": "round_last_rowIndex_in_block"})
        )
        last_round_in_trunc_block = (
            last_round_in_trunc_block.sort_values(BLOCK_ATTEMPT_KEY + ["round_last_rowIndex_in_block"], kind="mergesort")
            .groupby(BLOCK_ATTEMPT_KEY, dropna=False, sort=False)
            .tail(1)
            .reset_index(drop=True)
        )

        # build a set of excluded round-attempt keys (same key as ROUND_ATTEMPT_KEY)
        excluded_round_attempts = last_round_in_trunc_block[ROUND_ATTEMPT_KEY].drop_duplicates()
        excluded_round_attempts["_exclude"] = True

        # mark exclusions in round_starts
        round_starts2 = round_starts.merge(excluded_round_attempts, on=ROUND_ATTEMPT_KEY, how="left", validate="1:1")
        exclude_mask = round_starts2["_exclude"].fillna(False)

        df = _assign_running_total_by_starts(
            df=df.drop(columns=["TotSesh_runTot_RoundNum"], errors="ignore"),
            starts=round_starts2.drop(columns=["_exclude"], errors="ignore"),
            key_cols=ROUND_ATTEMPT_KEY,
            out_col="TotSesh_runTot_RoundNum",
            exclude_mask_in_starts=exclude_mask,
        )
    else:
        # No truncated blocks -> same as _all
        df["TotSesh_runTot_RoundNum"] = df["TotSesh_runTot_RoundNum_all"]

    # ---- Running totals: Blocks ----

    block_starts = _compute_first_index_map(df, BLOCK_ATTEMPT_KEY, "TotSesh_rowIndex")

    # All attempted blocks
    df = _assign_running_total_by_starts(
        df=df,
        starts=block_starts,
        key_cols=BLOCK_ATTEMPT_KEY,
        out_col="TotSesh_runTot_BlockNum_all",
    )

    # Completed blocks only: compute running count over completed block starts, then merge back as a per-row running total
    block_starts_completed = block_starts.copy()
    block_status_lower = block_starts_completed["BlockStatus"].astype(str).str.lower()
    block_starts_completed["isCompletedBlock"] = block_status_lower == "completed"

    # running total only increments at completed block starts
    block_starts_completed = block_starts_completed.sort_values("first_rowIndex", kind="mergesort").reset_index(drop=True)
    block_starts_completed["TotSesh_runTot_BlockNum_atStart"] = np.cumsum(block_starts_completed["isCompletedBlock"].astype(int))

    # Now assign to every row by session order (rowIndex): number of completed blocks started up to that rowIndex
    # This ensures it is a true running total throughout the session, not just block-labeled.
    start_indices = block_starts_completed.loc[block_starts_completed["isCompletedBlock"], "first_rowIndex"].to_numpy()
    # For each rowIndex, count how many completed block starts are <= rowIndex
    df = df.sort_values("TotSesh_rowIndex", kind="mergesort").reset_index(drop=True)
    df["TotSesh_runTot_BlockNum"] = np.searchsorted(start_indices, df["TotSesh_rowIndex"].to_numpy(), side="right").astype(int)

    # Cleanup temp columns
    df = df.drop(columns=["_origRow", "_startTimeSort"], errors="ignore")

    # Write
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    df.to_csv(out_path, index=False)


@dataclass
class Args:
    input_dir: Optional[str]
    output_dir: Optional[str]
    input_file: Optional[str]
    output_file: Optional[str]
    pattern: str


def parse_args() -> Args:
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--input_dir", type=str, default=None, help="Directory of CSV files to process")
    g.add_argument("--input_file", type=str, default=None, help="Single CSV file to process")

    p.add_argument("--output_dir", type=str, default=None, help="Output directory (required if using --input_dir)")
    p.add_argument("--output_file", type=str, default=None, help="Output file (required if using --input_file)")
    p.add_argument("--pattern", type=str, default="*.csv", help="Glob pattern within input_dir (default: *.csv)")
    a = p.parse_args()

    if a.input_dir and not a.output_dir:
        p.error("--output_dir is required when using --input_dir")
    if a.input_file and not a.output_file:
        p.error("--output_file is required when using --input_file")

    return Args(
        input_dir=a.input_dir,
        output_dir=a.output_dir,
        input_file=a.input_file,
        output_file=a.output_file,
        pattern=a.pattern,
    )


def main() -> None:
    args = parse_args()

    if args.input_file:
        process_one_file(args.input_file, args.output_file)  # type: ignore[arg-type]
        return

    # Directory mode
    in_dir = args.input_dir  # type: ignore[assignment]
    out_dir = args.output_dir  # type: ignore[assignment]

    paths = sorted(glob.glob(os.path.join(in_dir, args.pattern)))
    if not paths:
        raise FileNotFoundError(f"No files matched {args.pattern} in {in_dir}")

    for in_path in paths:
        base = os.path.basename(in_path)
        out_path = os.path.join(out_dir, base)
        process_one_file(in_path, out_path)


if __name__ == "__main__":
    main()