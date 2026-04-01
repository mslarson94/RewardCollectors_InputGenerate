#!/usr/bin/env python3
"""
concat_csvs.py

Append all CSVs in a directory into one CSV (one header total) using pandas.

Features:
- --require-same-header: enforce identical header (same columns, same order) across files.
- --add-source-file: add a column with the source filename for each row.

Usage:
  python concat_csvs.py --indir /path/to/csvs --out merged.csv
  python concat_csvs.py --indir . --out merged.csv --pattern "*__withDemo.csv" --add-source-file
  python concat_csvs.py --indir . --out merged.csv --require-same-header
  python concat_csvs.py --indir . --out merged.csv --recursive --add-source-file --source-col src
"""

from __future__ import annotations

import argparse
from pathlib import Path
import math

import pandas as pd


def list_files(indir: Path, pattern: str, recursive: bool) -> list[Path]:
    globber = indir.rglob if recursive else indir.glob
    return sorted([p for p in globber(pattern) if p.is_file()])


def read_header(path: Path, *, encoding: str) -> list[str]:
    # nrows=0 reads only the header efficiently
    df0 = pd.read_csv(path, nrows=0, encoding=encoding)
    return list(df0.columns)

def normalize_pid(x: object) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and math.isnan(x):
        return ""
    if isinstance(x, float) and x.is_integer():
        return str(int(x))
    return str(x).strip()


def main() -> int:
    p = argparse.ArgumentParser(description="Concatenate many CSV files into one.")
    p.add_argument("--indir", type=Path, required=True, help="Directory containing CSV files")
    p.add_argument("--out", type=Path, required=True, help="Output CSV path")
    p.add_argument("--pattern", type=str, default="*.csv", help="Glob pattern (default: *.csv)")
    p.add_argument("--recursive", action="store_true", help="Search recursively")
    p.add_argument("--encoding", type=str, default="utf-8", help="CSV encoding (default: utf-8)")

    p.add_argument(
        "--require-same-header",
        action="store_true",
        help="Fail if any file's header differs from the first file (same columns + order).",
    )
    p.add_argument(
        "--add-source-file",
        action="store_true",
        help="Add a column with the source filename on each row.",
    )
    p.add_argument(
        "--source-col",
        type=str,
        default="intervalSourceFile",
        help="Column name to use with --add-source-file (default: intervalSourceFile)",
    )

    args = p.parse_args()

    files = list_files(args.indir, args.pattern, args.recursive)
    if not files:
        raise SystemExit(f"No CSVs matched {args.pattern!r} in {args.indir}")

    first_header = read_header(files[0], encoding=args.encoding)

    if args.require_same_header:
        mismatches: list[tuple[str, list[str]]] = []
        for f in files[1:]:
            hdr = read_header(f, encoding=args.encoding)
            if hdr != first_header:
                mismatches.append((f.name, hdr))

        if mismatches:
            msg = ["Header mismatch detected (expected first file's exact header)."]
            msg.append(f"First file: {files[0].name}")
            msg.append(f"Header: {first_header}")
            msg.append("")
            msg.append("Mismatching files:")
            for name, hdr in mismatches[:20]:
                msg.append(f"- {name}: {hdr}")
            if len(mismatches) > 20:
                msg.append(f"... and {len(mismatches) - 20} more")
            raise SystemExit("\n".join(msg))

    frames: list[pd.DataFrame] = []
    for f in files:
        df = pd.read_csv(f, encoding=args.encoding)

        if args.add_source_file:
            df[args.source_col] = f.name

        frames.append(df)

    out_df = pd.concat(frames, ignore_index=True, sort=False)
    pid = out_df["participantID"].map(normalize_pid)
    totSesh_actTest_RoundNum = out_df["TotSesh_actTest_RoundNum"].map(normalize_pid)

    out_df["roundID"] = (
    pid
    + "_"
    + out_df["sessionID"].astype(str)
    + "_"
    + totSesh_actTest_RoundNum)
    # optional integer encoding:
    out_df["roundID_int"] = out_df["roundID"].astype("category").cat.codes

    args.out.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.out, index=False, encoding=args.encoding)
    print(f"✅ Wrote {args.out} from {len(files)} file(s). Rows={len(out_df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())