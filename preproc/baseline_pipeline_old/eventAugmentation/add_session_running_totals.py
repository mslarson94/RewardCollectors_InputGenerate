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
6) Create "actual test" round numbers (per round attempt), excluding:
   - BlockType == 'collecting'
   - coinSetID >= 4
   Produces:
     - TotSesh_actTest_RoundNum      (based on TotSesh_runTot_RoundNum)
     - TotSesh_actTest_RoundNum_all  (based on TotSesh_runTot_RoundNum_all)

Also writes a manifest text file per output CSV containing round-attempt inclusion/exclusion counts.

Usage:
  python add_session_running_totals.py --input_dir /path/to/csvs --output_dir /path/to/out --manifest_dir /path/to/manifests
  python add_session_running_totals.py --input_file session.csv --output_file session_out.csv --manifest_dir /path/to/manifests

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
from typing import Optional

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

ACTTEST_REQUIRED_COLS = ["BlockType", "CoinSetID"]

ROUND_GROUP_COLS = ["source_file", "BlockNum", "BlockInstance", "RoundNum"]
ROUND_ATTEMPT_KEY = ["RoundNum", "BlockNum", "BlockInstance", "source_file", "BlockStatus"]
BLOCK_ATTEMPT_KEY = ["BlockNum", "BlockInstance", "source_file", "BlockStatus"]


def _ensure_required_cols(df: pd.DataFrame, path: str) -> None:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"{path}: missing required columns: {missing}")


def _parse_start_time(df: pd.DataFrame) -> pd.Series:
    s = df["start_AppTime"]
    if np.issubdtype(s.dtype, np.number):
        return s.astype(float)
    dt = pd.to_datetime(s, errors="coerce", utc=False)
    if dt.notna().any():
        return dt
    return s.astype(str)


def _compute_first_index_map(df: pd.DataFrame, key_cols: list[str], idx_col: str) -> pd.DataFrame:
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
    starts = starts.copy()
    if exclude_mask_in_starts is None:
        inc = np.ones(len(starts), dtype=int)
    else:
        inc = (~exclude_mask_in_starts.to_numpy(dtype=bool)).astype(int)
    starts[out_col] = np.cumsum(inc)
    return df.merge(starts[key_cols + [out_col]], on=key_cols, how="left", validate="m:1")


def _compute_acttest_round_attempt_table(
    df: pd.DataFrame,
    *,
    blocktype_col: str = "BlockType",
    coinsetid_col: str = "CoinSetID",
    base_col: str = "TotSesh_runTot_RoundNum",
    base_col_all: str = "TotSesh_runTot_RoundNum_all",
) -> pd.DataFrame:
    missing = [c for c in ACTTEST_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns needed for act-test round nums: {missing}")

    required = ["TotSesh_rowIndex", base_col, base_col_all]
    missing2 = [c for c in required if c not in df.columns]
    if missing2:
        raise ValueError(f"Missing columns needed for act-test round nums: {missing2}")

    work = df.copy()
    work["_BlockType_norm"] = work[blocktype_col].astype(str).str.strip().str.lower()
    work["_CoinSetID_num"] = pd.to_numeric(work[coinsetid_col], errors="coerce")

    starts = (
        work.sort_values("TotSesh_rowIndex", kind="mergesort")
        .drop_duplicates(subset=ROUND_ATTEMPT_KEY, keep="first")[
            ROUND_ATTEMPT_KEY
            + ["TotSesh_rowIndex", base_col, base_col_all, "_BlockType_norm", "_CoinSetID_num"]
        ]
        .copy()
        .reset_index(drop=True)
    )
    return starts


def add_act_test_roundnums(
    df: pd.DataFrame,
    *,
    blocktype_col: str = "BlockType",
    coinsetid_col: str = "CoinSetID",
    out_col: str = "TotSesh_actTest_RoundNum",
    out_col_all: str = "TotSesh_actTest_RoundNum_all",
    base_col: str = "TotSesh_runTot_RoundNum",
    base_col_all: str = "TotSesh_runTot_RoundNum_all",
) -> pd.DataFrame:
    starts = _compute_acttest_round_attempt_table(
        df,
        blocktype_col=blocktype_col,
        coinsetid_col=coinsetid_col,
        base_col=base_col,
        base_col_all=base_col_all,
    )

    keep = (starts["_BlockType_norm"] != "collecting") & (starts["_CoinSetID_num"] < 4)

    def _make_mapping(starts_df: pd.DataFrame, base: str, out: str) -> pd.DataFrame:
        kept = starts_df.loc[keep, ROUND_ATTEMPT_KEY + [base]].copy()
        kept[base] = pd.to_numeric(kept[base], errors="coerce")
        kept = kept.dropna(subset=[base]).sort_values(base, kind="mergesort").reset_index(drop=True)
        kept[out] = np.arange(1, len(kept) + 1, dtype=int)
        return kept[ROUND_ATTEMPT_KEY + [out]]

    map_main = _make_mapping(starts, base_col, out_col)
    map_all = _make_mapping(starts, base_col_all, out_col_all)

    work = df.drop(columns=[out_col, out_col_all], errors="ignore").copy()
    work = work.merge(map_main, on=ROUND_ATTEMPT_KEY, how="left", validate="m:1")
    work = work.merge(map_all, on=ROUND_ATTEMPT_KEY, how="left", validate="m:1")
    return work


def _manifest_path(out_csv_path: str, manifest_dir: Optional[str]) -> str:
    base = os.path.splitext(os.path.basename(out_csv_path))[0] + "_manifest.txt"
    if manifest_dir:
        os.makedirs(manifest_dir, exist_ok=True)
        return os.path.join(manifest_dir, base)
    return os.path.splitext(out_csv_path)[0] + "_manifest.txt"


def write_manifest_for_file(
    df: pd.DataFrame,
    manifest_path: str,
    *,
    in_path: str,
    out_path: str,
    blocktype_col: str = "BlockType",
    coinsetid_col: str = "CoinSetID",
    base_col: str = "TotSesh_runTot_RoundNum",
    base_col_all: str = "TotSesh_runTot_RoundNum_all",
) -> None:
    starts = _compute_acttest_round_attempt_table(
        df,
        blocktype_col=blocktype_col,
        coinsetid_col=coinsetid_col,
        base_col=base_col,
        base_col_all=base_col_all,
    )

    is_collecting = starts["_BlockType_norm"] == "collecting"
    is_tutorial = ~(starts["_CoinSetID_num"] < 4)  # includes NaN as excluded
    keep = (~is_collecting) & (~is_tutorial)

    total = int(len(starts))
    kept = int(keep.sum())
    excluded = total - kept
    excl_collecting = int((is_collecting & ~is_tutorial).sum())
    excl_tutorial = int((is_tutorial & ~is_collecting).sum())
    excl_both = int((is_collecting & is_tutorial).sum())

    lines = [
        "add_session_running_totals manifest",
        f"input_file:  {in_path}",
        f"output_file: {out_path}",
        "",
        "round-attempt summary (unique ROUND_ATTEMPT_KEY):",
        f"  total_round_attempts: {total}",
        f"  kept_for_actTest:     {kept}",
        f"  excluded_total:       {excluded}",
        "",
        "exclusion breakdown:",
        f"  excluded_collecting_only: {excl_collecting}",
        f"  excluded_tutorial_only:   {excl_tutorial}",
        f"  excluded_both:            {excl_both}",
        "",
        "notes:",
        "  collecting is determined by BlockType (case/whitespace-insensitive).",
        "  tutorial is CoinSetID >= 4 OR CoinSetID missing/unparseable.",
    ]

    os.makedirs(os.path.dirname(manifest_path) or ".", exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def process_one_file(in_path: str, out_path: str, manifest_dir: Optional[str]) -> None:
    df = pd.read_csv(in_path)
    _ensure_required_cols(df, in_path)

    start_time_sort = _parse_start_time(df)
    df["_origRow"] = np.arange(len(df), dtype=int)
    df["_startTimeSort"] = start_time_sort

    df = df.sort_values(
        by=["testingOrder", "_startTimeSort", "_origRow"],
        ascending=[True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    df["TotSesh_rowIndex"] = np.arange(1, len(df) + 1, dtype=int)

    # isIncompleteRound
    round_sizes = (
        df.groupby(ROUND_GROUP_COLS, dropna=False, sort=False)
        .size()
        .rename("round_rowCount")
        .reset_index()
    )
    round_sizes["isIncompleteRound"] = round_sizes["round_rowCount"] < 3
    df = df.merge(round_sizes[ROUND_GROUP_COLS + ["isIncompleteRound"]], on=ROUND_GROUP_COLS, how="left", validate="m:1")

    # isLastRoundOfSourceFile
    last_round_per_source = (
        df.groupby(["source_file"] + ROUND_GROUP_COLS[1:], dropna=False, sort=False)["TotSesh_rowIndex"]
        .max()
        .reset_index()
        .rename(columns={"TotSesh_rowIndex": "round_last_rowIndex"})
    )
    last_round_per_source = (
        last_round_per_source.sort_values(["source_file", "round_last_rowIndex"], kind="mergesort")
        .groupby("source_file", dropna=False, sort=False)
        .tail(1)
        .reset_index(drop=True)
    )
    last_round_per_source["isLastRoundOfSourceFile"] = True
    df = df.merge(
        last_round_per_source[ROUND_GROUP_COLS + ["isLastRoundOfSourceFile"]],
        on=ROUND_GROUP_COLS,
        how="left",
        validate="m:1",
    )
    df["isLastRoundOfSourceFile"] = df["isLastRoundOfSourceFile"].fillna(False)

    # ---- Running totals: Rounds ----
    round_starts = _compute_first_index_map(df, ROUND_ATTEMPT_KEY, "TotSesh_rowIndex")

    df = _assign_running_total_by_starts(
        df=df,
        starts=round_starts,
        key_cols=ROUND_ATTEMPT_KEY,
        out_col="TotSesh_runTot_RoundNum_all",
    )

    truncated_block_rows = df[df["BlockStatus"].astype(str).str.lower() == "truncated"].copy()
    if len(truncated_block_rows) > 0:
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

        excluded_round_attempts = last_round_in_trunc_block[ROUND_ATTEMPT_KEY].drop_duplicates()
        excluded_round_attempts["_exclude"] = True

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
        df["TotSesh_runTot_RoundNum"] = df["TotSesh_runTot_RoundNum_all"]

    # ---- Running totals: Blocks ----
    block_starts = _compute_first_index_map(df, BLOCK_ATTEMPT_KEY, "TotSesh_rowIndex")

    df = _assign_running_total_by_starts(
        df=df,
        starts=block_starts,
        key_cols=BLOCK_ATTEMPT_KEY,
        out_col="TotSesh_runTot_BlockNum_all",
    )

    block_starts_completed = block_starts.copy()
    block_status_lower = block_starts_completed["BlockStatus"].astype(str).str.lower()
    block_starts_completed["isCompletedBlock"] = block_status_lower == "completed"
    block_starts_completed = block_starts_completed.sort_values("first_rowIndex", kind="mergesort").reset_index(drop=True)

    start_indices = block_starts_completed.loc[block_starts_completed["isCompletedBlock"], "first_rowIndex"].to_numpy()
    df = df.sort_values("TotSesh_rowIndex", kind="mergesort").reset_index(drop=True)
    df["TotSesh_runTot_BlockNum"] = np.searchsorted(start_indices, df["TotSesh_rowIndex"].to_numpy(), side="right").astype(int)

    # ---- Actual-test round numbering ----
    df = add_act_test_roundnums(df)

    # Cleanup temps
    df = df.drop(columns=["_origRow", "_startTimeSort"], errors="ignore")

    # Write CSV
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    df.to_csv(out_path, index=False)

    # Write manifest (in manifest_dir if provided)
    manifest_path = _manifest_path(out_path, manifest_dir)
    write_manifest_for_file(df, manifest_path, in_path=in_path, out_path=out_path)


@dataclass
class Args:
    input_dir: Optional[str]
    output_dir: Optional[str]
    input_file: Optional[str]
    output_file: Optional[str]
    manifest_dir: Optional[str]
    pattern: str


def parse_args() -> Args:
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--input_dir", type=str, default=None, help="Directory of CSV files to process")
    g.add_argument("--input_file", type=str, default=None, help="Single CSV file to process")

    p.add_argument("--output_dir", type=str, default=None, help="Output directory (required if using --input_dir)")
    p.add_argument("--output_file", type=str, default=None, help="Output file (required if using --input_file)")
    p.add_argument("--manifest_dir", type=str, default=None, help="Directory to write manifests (default: alongside output CSV)")
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
        manifest_dir=a.manifest_dir,
        pattern=a.pattern,
    )


def main() -> None:
    args = parse_args()

    if args.input_file:
        process_one_file(args.input_file, args.output_file, args.manifest_dir)  # type: ignore[arg-type]
        return

    in_dir = args.input_dir  # type: ignore[assignment]
    out_dir = args.output_dir  # type: ignore[assignment]

    paths = sorted(glob.glob(os.path.join(in_dir, args.pattern)))
    if not paths:
        raise FileNotFoundError(f"No files matched {args.pattern} in {in_dir}")

    for in_path in paths:
        base = os.path.basename(in_path)
        out_path = os.path.join(out_dir, base)
        process_one_file(in_path, out_path, args.manifest_dir)


if __name__ == "__main__":
    main()