# ========================= merge_ml_with_rpi_marks.py =========================
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
merge_ml_with_rpi_marks.py — Align ML 'Mark' events to a precomputed RPi marks CSV
and merge alignment columns back into the ML CSV.

New columns (filled only on Mark rows):
  <Label>_RPi_Timestamp
  <Label>_RPi_Timestamp_drift   # seconds (RPi - ML)
  <Label>_RPi_Matched           # 1/0
  <Label>_RPi_MatchReason       # "" or explanation for no-match

Output filename:
  <ml_base>_<device>_<label>_events.csv

Example:
  python merge_ml_with_rpi_marks.py \
    --ml_csv_file \
      "/.../augmented/ObsReward_A_02_17_2025_15_11_events_final.csv" \
    --rpi_marks_csv \
      "/.../ObsReward_A_02_17_2025_15_11_events_final_ML2A_BioPac_RPiMarks.csv" \
    --csv_timestamp_column mLTimestamp \
    --event_type_column lo_eventType \
    --event_type_values Mark \
    --label BioPac \
    --device ML2A \
    --timezone_offset_hours auto \
    --max_match_gap_s 1.0
"""

from __future__ import annotations

import argparse
import os
import numpy as np
import pandas as pd
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from batchAlignHelpers import _select_mark_rows, _auto_offset_hours, _normalize_ml_stem, _nearest_unique_alignment




def main() -> None:
    ap = argparse.ArgumentParser(description="Merge ML Mark events with RPi marks CSV and write merged ML CSV")
    ap.add_argument("--ml_csv_file", required=True)
    ap.add_argument("--rpi_marks_csv", required=True)
    ap.add_argument("--csv_timestamp_column", default="mLTimestamp")
    ap.add_argument("--event_type_column", default="lo_eventType")
    ap.add_argument("--event_type_values", default="Mark")
    ap.add_argument("--label", required=True, help="BioPac or RNS (used for column prefix)")
    ap.add_argument("--device", required=True)
    ap.add_argument("--timezone_offset_hours", default="auto", help="number or 'auto'")
    ap.add_argument("--max_match_gap_s", type=float, default=1.0)
    ap.add_argument("--out_dir", default="", help="Optional directory for output file (defaults to ML CSV directory)")
    ap.add_argument("--strip-ml-suffixes", default="_events_final,_processed", help="Comma-separated suffixes to strip from ML stem for output naming")
    args = ap.parse_args()

    ml_df = pd.read_csv(args.ml_csv_file)
    ev_vals = [v.strip() for v in args.event_type_values.split(",") if v.strip()]

    marks_df = _select_mark_rows(ml_df, args.event_type_column, ev_vals)
    marks_idx = marks_df.index
    ml_times = pd.to_datetime(marks_df[args.csv_timestamp_column], errors="coerce")
    if ml_times.isna().any():
        raise ValueError("some ML timestamps failed to parse")

    rpi_df = pd.read_csv(args.rpi_marks_csv)
    if "RPi_Timestamp" not in rpi_df.columns:
        raise KeyError("RPi marks CSV missing 'RPi_Timestamp'")
    rpi_times = pd.to_datetime(rpi_df["RPi_Timestamp"], errors="coerce")

    # offset
    if args.timezone_offset_hours.strip().lower() == "auto":
        est = _auto_offset_hours(ml_times, rpi_times)
        tz_offset = timedelta(hours=est)
    else:
        tz_offset = timedelta(hours=float(args.timezone_offset_hours))

    rpi_aligned = rpi_times + pd.to_timedelta(tz_offset)

    match_idx, deltas, reasons = _nearest_unique_alignment(ml_times, rpi_aligned, args.max_match_gap_s if args.max_match_gap_s > 0 else None)
    matched_mask = match_idx >= 0
    chosen_rpi = pd.Series([pd.NaT] * len(ml_times), dtype="datetime64[ns]")
    chosen_rpi.loc[matched_mask] = rpi_aligned.iloc[match_idx[matched_mask]].values

    label = args.label.strip()
    col_ts = f"{label}_RPi_Timestamp"
    col_drift = f"{label}_RPi_Timestamp_drift"
    col_match = f"{label}_RPi_Matched"
    col_reason = f"{label}_RPi_MatchReason"

    # Create columns on full ML CSV
    ml_df[col_ts] = pd.NaT
    ml_df[col_drift] = np.nan
    ml_df[col_match] = 0
    ml_df[col_reason] = ""

    # Fill only at mark rows
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