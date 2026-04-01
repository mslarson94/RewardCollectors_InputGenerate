#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
merge_ml_with_rpi_marks.py — Align ML 'Mark' events to a precomputed RPi marks CSV
and merge alignment columns back into the ML CSV.
"""

from __future__ import annotations

import argparse
import numpy as np
import pandas as pd
from datetime import timedelta
from pathlib import Path
from typing import List, Optional, Sequence, Tuple
import re
import sys


def _select_mark_rows(df: pd.DataFrame, col: str, values: Sequence[str]) -> pd.DataFrame:
    vals = {str(v).strip().lower() for v in values}
    mask = df[col].astype(str).str.strip().str.lower().isin(vals)
    out = df.loc[mask].copy()
    return out


def _auto_offset_hours(ml_times: pd.Series, rpi_times: pd.Series) -> float:
    n = int(min(10, len(ml_times), len(rpi_times)))
    diffs = (ml_times.iloc[:n].reset_index(drop=True) - rpi_times.iloc[:n].reset_index(drop=True)).dt.total_seconds()
    return float(np.median(diffs) / 3600.0)


def _nearest_unique_alignment(ml_times: pd.Series, rpi_times: pd.Series, max_gap: Optional[float]) -> Tuple[np.ndarray, np.ndarray, List[str]]:
    ml = ml_times.reset_index(drop=True).to_numpy(dtype="datetime64[ns]")
    rpi = rpi_times.reset_index(drop=True).to_numpy(dtype="datetime64[ns]")
    n, m = len(ml), len(rpi)
    match_idx = np.full(n, -1, dtype=int)
    deltas = np.full(n, np.nan, dtype=float)
    reasons: List[str] = ["" for _ in range(n)]
    last_j = -1
    for i in range(n):
        if last_j + 1 >= m:
            for k in range(i, n):
                reasons[k] = "exhausted log"
            break
        diffs = np.abs((rpi[last_j+1:] - ml[i]).astype("timedelta64[ns]").astype("int64")) / 1e9
        j_rel = int(np.argmin(diffs))
        j = last_j + 1 + j_rel
        if (max_gap is not None) and (diffs[j_rel] > max_gap):
            reasons[i] = f"no log within ≤{max_gap:.3f}s"
            continue
        match_idx[i] = j
        deltas[i] = ((rpi[j] - ml[i]).astype("timedelta64[ns]").astype("int64")) / 1e9
        last_j = j
    return match_idx, deltas, reasons


def _normalize_ml_stem(stem: str, suffixes: Sequence[str]) -> str:
    base = stem
    changed = True
    while changed:
        changed = False
        for s in suffixes:
            if s and base.endswith(s):
                base = base[: -len(s)]
                changed = True
    return re.sub(r"[_-]+$", "", base)


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge ML Mark events with RPi marks CSV and write merged ML CSV")
    ap.add_argument("--ml_csv_file", required=True)
    ap.add_argument("--rpi_marks_csv", required=True)
    ap.add_argument("--csv_timestamp_column", default="mLTimestamp")
    ap.add_argument("--event_type_column", default="lo_eventType")
    ap.add_argument("--event_type_values", default="Mark", help="Comma-separated event type(s) to match (default: Mark)")
    ap.add_argument("--label", required=True, help="BioPac or RNS (used for column prefix)")
    ap.add_argument("--device", required=True)
    ap.add_argument("--timezone_offset_hours", default="auto", help="number or 'auto'")
    ap.add_argument("--max_match_gap_s", type=float, default=1.0)
    ap.add_argument("--out_dir", default="", help="Optional directory for output file (defaults to ML CSV directory)")
    ap.add_argument("--strip-ml-suffixes", default="_events_final,_processed", help="Comma-separated suffixes to strip from ML stem for output naming")
    args = ap.parse_args()

    label = args.label.strip()


    ml_df = pd.read_csv(args.ml_csv_file)
    ev_vals = [v.strip() for v in args.event_type_values.split(",") if v.strip()]

    marks_df = _select_mark_rows(ml_df, args.event_type_column, ev_vals)
    if marks_df.empty:
        print(f"\n🚫 [NO MARKS] In ML file '{Path(args.ml_csv_file).name}', column '{args.event_type_column}' contains no value in {ev_vals}.\n   This likely indicates a mismatched row in collatedData.xlsx. Skipping merge for this file.\n")
        sys.exit(0)

    marks_idx = marks_df.index
    ml_times = pd.to_datetime(marks_df[args.csv_timestamp_column], errors="coerce")
    if ml_times.isna().any():
        raise ValueError("some ML timestamps failed to parse")

    rpi_df = pd.read_csv(args.rpi_marks_csv)
    if "RPi_Timestamp" not in rpi_df.columns:
        raise KeyError("RPi marks CSV missing 'RPi_Timestamp'")
    rpi_times = pd.to_datetime(rpi_df["RPi_Timestamp"], errors="coerce")

    # if str(args.timezone_offset_hours).strip().lower() == "auto":
    #     est = _auto_offset_hours(ml_times, rpi_times)
    #     tz_offset = timedelta(hours=est)
    # else:
    #     tz_offset = timedelta(hours=float(args.timezone_offset_hours))

    # #rpi_aligned = rpi_times + pd.to_datetime(tz_offset)
    # rpi_aligned = rpi_times + tz_offset

    # offset
    is_auto = str(args.timezone_offset_hours).strip().lower() == "auto"
    if is_auto:
        est = _auto_offset_hours(ml_times, rpi_times)
        tz_offset = timedelta(hours=est)
    else:
        tz_offset = timedelta(hours=float(args.timezone_offset_hours))
    print(f"[merge] {Path(args.ml_csv_file).name} [{label}] tz_offset_hours={tz_offset.total_seconds()/3600:.3f} (mode={'auto' if is_auto else 'explicit'})")

    rpi_aligned = rpi_times + tz_offset


    match_idx, deltas, reasons = _nearest_unique_alignment(
        ml_times, rpi_aligned, args.max_match_gap_s if args.max_match_gap_s > 0 else None
    )
    matched_mask = match_idx >= 0
    chosen_rpi = pd.Series([pd.NaT] * len(ml_times), dtype="datetime64[ns]")
    chosen_rpi.loc[matched_mask] = rpi_aligned.iloc[match_idx[matched_mask]].values

    label = args.label.strip()
    col_ts = f"{label}_RPi_Timestamp"
    col_drift = f"{label}_RPi_Timestamp_drift"
    col_match = f"{label}_RPi_Matched"
    col_reason = f"{label}_RPi_MatchReason"

    ml_df[col_ts] = pd.NaT
    ml_df[col_drift] = np.nan
    ml_df[col_match] = 0
    ml_df[col_reason] = ""

    ml_df.loc[marks_idx, col_ts] = chosen_rpi.values
    ml_df.loc[marks_idx, col_drift] = deltas
    ml_df.loc[marks_idx, col_match] = matched_mask.astype(int)
    ml_df.loc[marks_idx, col_reason] = reasons

    suffixes = [s for s in (args.strip_ml_suffixes or "").split(",") if s]
    ml_root = _normalize_ml_stem(Path(args.ml_csv_file).stem, suffixes)
    out_dir = Path(args.out_dir) if args.out_dir else Path(args.ml_csv_file).parent
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"{ml_root}_{args.device}_{label}_events.csv"
    ml_df.to_csv(out_csv, index=False)
    print(out_csv)


if __name__ == "__main__":
    main()
