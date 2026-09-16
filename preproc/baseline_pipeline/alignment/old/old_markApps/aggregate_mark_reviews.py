# aggregate_mark_reviews.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


REVIEW_COLUMNS = [
    "events_file",
    "rpi_file",
    "label",
    "block",
    "stream",
    "mark_id",
    "ordinal",
    "mark_time",
    "exclude",
    "reason",
    "reviewed_at",
]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Aggregate per-pair mark review CSVs into one master CSV."
    )
    ap.add_argument("--review-dir", type=Path, required=True, help="Directory containing per-pair review CSVs")
    ap.add_argument("--out-csv", type=Path, required=True, help="Output aggregated CSV")
    ap.add_argument("--only-excluded", action="store_true", help="Keep only rows with exclude == True")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    review_dir = args.review_dir.expanduser()
    out_csv = args.out_csv.expanduser()

    if not review_dir.is_dir():
        raise NotADirectoryError(f"review-dir is not a directory: {review_dir}")

    csv_files = sorted(review_dir.glob("*_mark_review_decisions.csv"))
    if not csv_files:
        raise SystemExit(f"No per-pair review CSVs found in: {review_dir}")

    dfs: list[pd.DataFrame] = []
    for path in csv_files:
        try:
            df = pd.read_csv(path, dtype="string")
        except Exception as e:
            print(f"[warn] skipping unreadable file {path}: {e}")
            continue

        for col in REVIEW_COLUMNS:
            if col not in df.columns:
                df[col] = pd.Series(dtype="string")

        df = df[REVIEW_COLUMNS].copy()
        df["review_file"] = str(path)
        dfs.append(df)

    if not dfs:
        raise SystemExit("No readable review CSVs found.")

    out_df = pd.concat(dfs, ignore_index=True)
    out_df["exclude"] = out_df["exclude"].astype(str).str.lower().isin({"true", "1", "yes"})

    if args.only_excluded:
        out_df = out_df[out_df["exclude"]].copy()

    out_df = out_df.sort_values(
        by=["events_file", "label", "stream", "ordinal"],
        kind="stable",
        na_position="last",
    )

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_csv, index=False)

    print(f"[ok] wrote aggregated review CSV -> {out_csv}")
    print(f"[ok] source review files: {len(csv_files)}")
    print(f"[ok] output rows: {len(out_df)}")


if __name__ == "__main__":
    main()