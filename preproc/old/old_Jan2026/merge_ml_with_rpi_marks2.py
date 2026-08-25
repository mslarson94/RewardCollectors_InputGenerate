#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
merge_ml_with_rpi_marks2.py — Align ML 'Mark' events with RPi marks and
create synthetic ML rows for RPi-only marks using a template CSV (NewRowInfo.csv).

New functionality:
  • Detects RPi marks without nearby ML 'Mark' events
  • Creates synthetic rows for those marks using a provided template
"""

from __future__ import annotations

import argparse
import numpy as np
import pandas as pd
from datetime import timedelta
from pathlib import Path

from batchAlignHelpers import (
    _select_mark_rows,
    _auto_offset_hours,
    _normalize_ml_stem,
    _nearest_unique_alignment,
)


def _load_template_row(blankRowTemplate: Path) -> pd.Series:
    df = pd.read_csv(blankRowTemplate)
    if df.empty:
        raise ValueError(f"Template CSV {blankRowTemplate} is empty.")
    return df.iloc[0]


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge ML Mark events with RPi marks (and synthesize missing ML rows).")
    ap.add_argument("--ml_csv_file", required=True)
    ap.add_argument("--rpi_marks_csv", required=True)
    ap.add_argument("--blankRowTemplate", help="Template CSV for synthetic rows (e.g. NewRowInfo.csv)")
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
    # if ml_times.isna().any():
    #     raise ValueError("some ML timestamps failed to parse")

    rpi_df = pd.read_csv(args.rpi_marks_csv)
    if "RPi_Timestamp" not in rpi_df.columns:
        raise KeyError("RPi marks CSV missing 'RPi_Timestamp'")
    rpi_times = pd.to_datetime(rpi_df["RPi_Timestamp"], errors="coerce")

    # --- Time offset ---
    if args.timezone_offset_hours.strip().lower() == "auto":
        est = _auto_offset_hours(ml_times, rpi_times)
        tz_offset = timedelta(hours=est)
    else:
        tz_offset = timedelta(hours=float(args.timezone_offset_hours))
    rpi_aligned = rpi_times + pd.to_timedelta(tz_offset)

    # --- Align RPi to ML Marks ---
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

    # Create columns on full ML CSV
    ml_df[col_ts] = pd.NaT
    ml_df[col_drift] = np.nan
    ml_df[col_match] = 0
    ml_df[col_reason] = ""

    # Fill only Mark rows
    ml_df.loc[marks_idx, col_ts] = chosen_rpi.values
    ml_df.loc[marks_idx, col_drift] = deltas
    ml_df.loc[marks_idx, col_match] = matched_mask.astype(int)
    ml_df.loc[marks_idx, col_reason] = reasons

    # --- NEW: Handle unmatched RPi marks ---
    unmatched_mask = ~rpi_times.index.isin(match_idx[match_idx >= 0])
    unmatched_times = rpi_aligned[unmatched_mask]

    # Include RPi marks that happen after the last ML mark (possible trailing)
    if not rpi_aligned.empty and not ml_times.empty:
        last_ml = ml_times.max()
        trailing_marks = rpi_aligned[rpi_aligned > last_ml + timedelta(seconds=args.max_match_gap_s)]
        unmatched_times = pd.concat([unmatched_times, trailing_marks]).drop_duplicates()

    if args.blankRowTemplate and not unmatched_times.empty:
        template_row = _load_template_row(Path(args.blankRowTemplate))
        synthetic_rows = []
        for ts in unmatched_times:
            row = template_row.copy()
            for col in row.index:
                if pd.isna(row[col]):
                    row[col] = ""
            row[args.event_type_column] = "Mark"
            row[args.csv_timestamp_column] = ts
            row[col_ts] = ts
            row[col_drift] = np.nan
            row[col_match] = 0
            row[col_reason] = "No ML match"
            row["Source"] = "RPiOnly"
            synthetic_rows.append(row)

        synth_df = pd.DataFrame(synthetic_rows)
        print(f"[info] Added {len(synth_df)} RPi-only synthetic rows.")
        ml_df = pd.concat([ml_df, synth_df], ignore_index=True)

        # Force datetime parsing on timestamp column before sorting
        if args.csv_timestamp_column in ml_df.columns:
            ml_df[args.csv_timestamp_column] = pd.to_datetime(
                ml_df[args.csv_timestamp_column], errors="coerce"
            )
            ml_df = ml_df.sort_values(args.csv_timestamp_column, ignore_index=True)
    else:
        print("[info] No unmatched RPi marks found or no template provided.")

    # --- Save output ---
    suffixes = [s for s in (args.strip_ml_suffixes or "").split(",") if s]
    ml_root = _normalize_ml_stem(Path(args.ml_csv_file).stem, suffixes)
    out_dir = Path(args.out_dir) if args.out_dir else Path(args.ml_csv_file).parent
    out_dir = out_dir / f"{label}"
    out_dir.mkdir(parents=True, exist_ok=True)
    events_dir = out_dir / "Events"
    events_dir.mkdir(parents=True, exist_ok=True)

    out_csv = events_dir / f"{ml_root}_{args.device}_{label}_events.csv"
    
    ml_df.to_csv(out_csv, index=False)

    # --- Sanity summary ---
    sanityOutDir = out_dir / "SanitySummary"
    sanityOutDir.mkdir(parents=True, exist_ok=True)
    summary_path = out_csv.with_name(f"{ml_root}_{args.device}_{label}_sanity_summary.csv")
    summary_path = sanityOutDir / f"{ml_root}_{args.device}_{label}_sanity_summary.csv"

    # Basic counts
    total_ml_marks = int((ml_df[args.event_type_column].astype(str).str.lower() == "mark").sum())
    total_rpi_marks = len(rpi_df)
    matched_rpi_marks = int((match_idx >= 0).sum())
    unmatched_rpi_marks = total_rpi_marks - matched_rpi_marks
    synthetic_added = int("synth_df" in locals() and len(synth_df))

    # Time ranges
    ml_times_full = pd.to_datetime(ml_df[args.csv_timestamp_column], errors="coerce").dropna()
    rpi_times_full = pd.to_datetime(rpi_df["RPi_Timestamp"], errors="coerce").dropna()
    rpi_aligned_full = rpi_times_full + pd.to_timedelta(tz_offset)

    ml_min, ml_max = (ml_times_full.min(), ml_times_full.max()) if not ml_times_full.empty else (pd.NaT, pd.NaT)
    rpi_min, rpi_max = (rpi_aligned_full.min(), rpi_aligned_full.max()) if not rpi_aligned_full.empty else (pd.NaT, pd.NaT)

    summary = pd.DataFrame(
        [{
            "Device": args.device,
            "Label": label,
            "Total_ML_Marks": total_ml_marks,
            "Total_RPi_Marks": total_rpi_marks,
            "Matched_RPi_Marks": matched_rpi_marks,
            "Unmatched_RPi_Marks": unmatched_rpi_marks,
            "Synthetic_RPi_Rows_Added": synthetic_added,
            "Timezone_Offset_Hours": tz_offset.total_seconds() / 3600.0,
            "Max_Match_Gap_s": args.max_match_gap_s,
            "ML_First_Timestamp": ml_min,
            "ML_Last_Timestamp": ml_max,
            "RPi_First_Timestamp_Aligned": rpi_min,
            "RPi_Last_Timestamp_Aligned": rpi_max,
        }]
    )

    summary.to_csv(summary_path, index=False)
    print(f"[summary] wrote sanity check → {summary_path}")


    print(out_csv)





if __name__ == "__main__":
    main()
