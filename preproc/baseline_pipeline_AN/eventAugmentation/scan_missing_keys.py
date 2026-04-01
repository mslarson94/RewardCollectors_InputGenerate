#!/usr/bin/env python3
"""
Scan CSVs in a directory for rows missing any of:
  source_file, BlockNum, BlockStatus, RoundNum, chestPin_num

Outputs:
  1) A summary CSV listing each file and how many bad rows it contains
  2) A details CSV with the offending rows (plus row number and filename)

Usage:
  python scan_missing_keys.py --input_dir /path/to/csvs --output_dir /path/to/out
  python scan_missing_keys.py --input_dir /path/to/csvs --pattern "*.csv" --output_dir /path/to/out
"""

from __future__ import annotations

import argparse
import glob
import os
from typing import List

import numpy as np
import pandas as pd


KEY_COLS = ["source_file", "BlockNum", "BlockStatus", "RoundNum", "chestPin_num"]


def find_bad_rows(df: pd.DataFrame) -> pd.Series:
    """
    Returns boolean mask: True where any key column is missing/blank.
    Treats NaN as missing; for object columns also treats empty/whitespace as missing.
    """
    missing_any = pd.Series(False, index=df.index)

    for col in KEY_COLS:
        if col not in df.columns:
            # If a required column is missing entirely, all rows are "bad"
            return pd.Series(True, index=df.index)

        s = df[col]
        col_missing = s.isna()

        # For string-like columns, treat empty/whitespace as missing too
        if s.dtype == "object" or pd.api.types.is_string_dtype(s.dtype):
            col_missing = col_missing | s.astype(str).str.strip().eq("") | s.astype(str).str.lower().eq("nan")

        missing_any = missing_any | col_missing

    return missing_any


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir", required=True, help="Directory containing CSV files")
    ap.add_argument("--output_dir", required=True, help="Directory to write scan results")
    ap.add_argument("--pattern", default="*.csv", help='Glob pattern (default: "*.csv")')
    ap.add_argument("--max_details_per_file", type=int, default=5000,
                    help="Cap number of bad rows saved per file in details output (default: 5000)")
    args = ap.parse_args()

    in_paths = sorted(glob.glob(os.path.join(args.input_dir, args.pattern)))
    if not in_paths:
        raise SystemExit(f"No files matched pattern {args.pattern} in {args.input_dir}")

    os.makedirs(args.output_dir, exist_ok=True)

    summary_rows: List[dict] = []
    detail_frames: List[pd.DataFrame] = []

    for path in in_paths:
        try:
            df = pd.read_csv(path)
        except Exception as e:
            summary_rows.append(
                {"file": os.path.basename(path), "bad_row_count": np.nan, "note": f"FAILED_READ: {e}"}
            )
            continue

        bad_mask = find_bad_rows(df)
        bad_count = int(bad_mask.sum())

        note = ""
        # If any required column missing entirely
        missing_cols = [c for c in KEY_COLS if c not in df.columns]
        if missing_cols:
            note = f"MISSING_COLUMNS: {missing_cols}"

        summary_rows.append({"file": os.path.basename(path), "bad_row_count": bad_count, "note": note})

        if bad_count > 0:
            bad_df = df.loc[bad_mask].copy()
            bad_df.insert(0, "source_csv", os.path.basename(path))
            bad_df.insert(1, "row_number_1based", bad_df.index.to_series().add(1).to_numpy())

            # Keep details manageable
            if len(bad_df) > args.max_details_per_file:
                bad_df = bad_df.head(args.max_details_per_file)
                bad_df["details_truncated"] = True
            else:
                bad_df["details_truncated"] = False

            detail_frames.append(bad_df)

    summary = pd.DataFrame(summary_rows).sort_values(["bad_row_count", "file"], ascending=[False, True])
    summary_path = os.path.join(args.output_dir, "missing_key_fields_summary.csv")
    summary.to_csv(summary_path, index=False)

    details_path = os.path.join(args.output_dir, "missing_key_fields_details.csv")
    if detail_frames:
        details = pd.concat(detail_frames, ignore_index=True)
        details.to_csv(details_path, index=False)
    else:
        # write empty details with just these columns so downstream scripts don't break
        pd.DataFrame(columns=["source_csv", "row_number_1based"] + KEY_COLS).to_csv(details_path, index=False)

    print(f"Wrote:\n  {summary_path}\n  {details_path}")


if __name__ == "__main__":
    main()