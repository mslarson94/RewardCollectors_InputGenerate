# file: summarize_verb_marks.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Summarize *_verb_full.csv into one row per (ipAddress, markNumber):
- Keep the LAST mini-event's core times.
- Also keep the FIRST mini-event's times as backup (<col>_first).
- Add QC metrics:
    rows_in_mark              = count of rows in the mark
    rows_between_first_last   = number of rows between first and last (len-1)

Input (from translate_verb_log.py) must contain at least:
    ipAddress, markNumber, ML_Time, RPi_Time, Mono_Time, Mono_Time_Adj
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import pandas as pd


REQUIRED_TIME_COLS = [
    "ML_Time_verb",
    "RPi_Time_verb",
    "Mono_Time_Raw_verb",
    "Mono_Time_verb",
]

DEFAULT_GROUP_COLS = ["ipAddress", "markNumber"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Summarize *_verb_full.csv down to one row per mark (keep LAST row, stash FIRST row)."
    )
    p.add_argument("--in-csv", required=True, help="Path to *_verb_full.csv produced by translate_verb_log.py")
    p.add_argument("--out-csv", default="", help="Output CSV; default: <in-stem>_short.csv")
    p.add_argument(
        "--group-cols",
        default="ipAddress,markNumber",
        help="Comma-separated grouping columns; default: ipAddress,markNumber",
    )
    p.add_argument(
        "--keep-cols",
        default="",
        help="Comma-separated extra columns to copy from the LAST row into the summary (if present)",
    )
    p.add_argument("--debug", action="store_true", help="Print diagnostics")
    return p.parse_args()


def main() -> None:
    print('starting summarize verb marks')
    args = parse_args()
    in_path = Path(args.in_csv)
    if not in_path.exists():
        raise FileNotFoundError(in_path)

    # Default out name: replace trailing "_full" once; then add "_short.csv"
    default_out_name = in_path.stem.replace("_fullVerb", "", 1) + "_verb.csv"
    out_path = Path(args.out_csv) if args.out_csv else in_path.with_name(default_out_name)

    group_cols = [c.strip() for c in args.group_cols.split(",") if c.strip()] or DEFAULT_GROUP_COLS
    keep_cols = [c.strip() for c in args.keep_cols.split(",") if c.strip()]

    # Read as strings to avoid coercion; retain original file order
    df = pd.read_csv(in_path, dtype="string").reset_index(drop=False).rename(columns={"index": "order_idx"})

    # Validate required columns
    missing_group = [c for c in group_cols if c not in df.columns]
    if missing_group:
        raise KeyError(f"Missing group columns: {missing_group}")
    missing_time = [c for c in REQUIRED_TIME_COLS if c not in df.columns]
    if missing_time:
        raise KeyError(f"Missing required time columns: {missing_time}")

    keep_cols_present = [c for c in keep_cols if c in df.columns]

    # Ensure per-group order is original file order
    df = df.sort_values(["order_idx"])

    out_rows: List[dict] = []

    for keys, g in df.groupby(group_cols, sort=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        g = g.sort_values("order_idx")
        first = g.iloc[0]
        last = g.iloc[-1]

        row = {col: val for col, val in zip(group_cols, keys)}

        # LAST row core times
        row["ML_Time_verb"] = last["ML_Time_verb"]
        row["RPi_Time_verb"] = last["RPi_Time_verb"]
        row["Mono_Time_Raw_verb"] = last["Mono_Time_Raw_verb"]
        row["Mono_Time_verb"] = last["Mono_Time_verb"]

        # FIRST row backups
        row["ML_Time_first_verb"] = first["ML_Time_verb"]
        row["RPi_Time_first_verb"] = first["RPi_Time_verb"]
        row["Mono_Time_Raw_first_verb"] = first["Mono_Time_Raw_verb"]
        row["Mono_Time_first_verb"] = first["Mono_Time_verb"]

        # QC metrics
        n = len(g)
        row["rows_in_mark"] = str(n)
        # difference in file positions; equals n-1 when group rows are contiguous
        row["rows_between_first_last"] = str(int(last["order_idx"]) - int(first["order_idx"]))

        # optional extras from LAST row
        for c in keep_cols_present:
            row[c] = last[c]

        out_rows.append(row)

    out_df = pd.DataFrame(out_rows)

    # Column order
    core_last = ["ML_Time_verb", "RPi_Time_verb", "Mono_Time_Raw_verb", "Mono_Time_Adj_verb"]
    core_first = ["ML_Time_first_verb", "RPi_Time_first_verb", "Mono_Time_Raw_first_verb", "Mono_Time_first_verb"]
    ordered = group_cols + core_last + core_first + ["rows_in_mark", "rows_between_first_last"]
    ordered += [c for c in keep_cols_present if c not in ordered]
    ordered = [c for c in ordered if c in out_df.columns] + [c for c in out_df.columns if c not in ordered]
    out_df = out_df.loc[:, ordered]

    #out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.out_csv, index=False)

    if args.debug:
        print(f"[info] grouped by: {group_cols}")
        print(f"[info] kept extras: {keep_cols_present}")
        print(f"[info] wrote {len(out_df)} rows")

    print(f"[ok] wrote {len(out_df)} summarized marks -> {args.out_csv}")


if __name__ == "__main__":
    main()
