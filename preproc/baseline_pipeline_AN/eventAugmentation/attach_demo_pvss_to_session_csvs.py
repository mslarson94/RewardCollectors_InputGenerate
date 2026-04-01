#!/usr/bin/env python3
"""
attach_demo_pvss_to_session_csvs.py

Attach static participant-level demo/PVSS values from an Excel master workbook
(sheet: 'Demo_PVSS-21') to one or many session CSV files.

Pulled columns:
  PVSS_TotalScore, PVSS_AvgScore, Age, Gender, SpatialMemRating

Participant matching:
  - Prefer CSV column 'participantID' if present (assumes single participant per file).
  - Fallback: parse from filename token '_participantID=XYZ__'

If participant isn't found in the workbook:
  - Output still contains the columns, filled with NA (no error).

Single-file:
  --input <csv> --output <csv> --workbook <xlsx>

Batch:
  --input-dir <dir> --outdir <dir> --workbook <xlsx>
  optional: --pattern "*.csv"
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
import math
import pandas as pd

SHEET_NAME = "Demo_PVSS-21"
ID_COL = "participantID"
VALUE_COLS = ["PVSS_TotalScore", "PVSS_AvgScore", "Age", "Gender", "SpatialMemRating"]

FILENAME_PID_RE = re.compile(r"_participantID=([^_]+)__")


def parse_participant_id_from_filename(path: Path) -> str | None:
    m = FILENAME_PID_RE.search(path.name)
    return m.group(1).strip() if m else None


def normalize_pid(x: object) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and math.isnan(x):
        return ""
    if isinstance(x, float) and x.is_integer():
        return str(int(x))
    return str(x).strip()

def load_demo_lookup(workbook: Path) -> pd.DataFrame:
    demo = pd.read_excel(workbook, sheet_name=SHEET_NAME)

    missing = [c for c in [ID_COL, *VALUE_COLS] if c not in demo.columns]
    if missing:
        raise ValueError(f"Workbook sheet '{SHEET_NAME}' missing columns: {missing}")

    out = demo[[ID_COL, *VALUE_COLS]].copy()
    out["__pid__"] = out[ID_COL].map(normalize_pid)

    # keep last occurrence if duplicates
    out = out.drop_duplicates(subset="__pid__", keep="last").set_index("__pid__")[VALUE_COLS]
    return out


def ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    return out


def get_pid_from_csv_or_name(df: pd.DataFrame, csv_path: Path) -> str | None:
    if ID_COL in df.columns and df[ID_COL].notna().any():
        # file should contain only one participant; take the first non-null
        return normalize_pid(df.loc[df[ID_COL].notna(), ID_COL].iloc[0])
    return parse_participant_id_from_filename(csv_path)


def attach_values(
    df: pd.DataFrame,
    pid: str | None,
    lookup: pd.DataFrame,
    *,
    quiet: bool,
) -> pd.DataFrame:
    out = ensure_cols(df, VALUE_COLS)

    if not pid:
        if not quiet:
            print("⚠️  Could not determine participantID; leaving demo columns as NA.")
        return out

    pid_norm = normalize_pid(pid)
    if pid_norm not in lookup.index:
        if not quiet:
            print(f"⚠️  participantID '{pid_norm}' not found in workbook; leaving demo columns as NA.")
        return out

    vals = lookup.loc[pid_norm].to_dict()
    for k, v in vals.items():
        out[k] = v
    return out


def process_one(
    csv_in: Path,
    csv_out: Path,
    lookup: pd.DataFrame,
    *,
    overwrite: bool,
    quiet: bool,
) -> None:
    if csv_out.exists() and not overwrite:
        raise FileExistsError(f"Output exists (use --overwrite): {csv_out}")

    df = pd.read_csv(csv_in)
    pid = get_pid_from_csv_or_name(df, csv_in)
    out = attach_values(df, pid, lookup, quiet=quiet)

    csv_out.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(csv_out, index=False)

    if not quiet:
        print(f"✅ {csv_in.name} -> {csv_out.name} (participantID={pid})")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Attach Demo_PVSS-21 columns from an Excel workbook to session CSV(s).")

    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--input", type=Path, help="Single CSV input")
    mode.add_argument("--input-dir", type=Path, help="Directory of CSVs to process")

    p.add_argument("--workbook", type=Path, required=True, help="Master participant Excel workbook (xlsx)")
    p.add_argument("--output", type=Path, help="Single CSV output (single-file mode)")
    p.add_argument("--outdir", type=Path, help="Output directory (batch mode)")
    p.add_argument("--pattern", type=str, default="*.csv", help="Batch glob pattern (default: *.csv)")
    p.add_argument("--suffix", type=str, default="__withDemo.csv", help="Batch output filename suffix")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--quiet", action="store_true")

    return p.parse_args()


def main() -> int:
    args = parse_args()

    lookup = load_demo_lookup(args.workbook)

    if args.input is not None:
        if args.output is None:
            print("Single-file mode requires --output.", file=sys.stderr)
            return 2
        process_one(args.input, args.output, lookup, overwrite=args.overwrite, quiet=args.quiet)
        return 0

    # batch mode
    assert args.input_dir is not None
    if args.outdir is None:
        print("Batch mode requires --outdir.", file=sys.stderr)
        return 2

    files = sorted([p for p in args.input_dir.glob(args.pattern) if p.is_file()])
    if not files:
        print(f"No files matched {args.pattern!r} in {args.input_dir}", file=sys.stderr)
        return 2

    n_ok = 0
    n_fail = 0
    for f in files:
        out_name = f"{f.stem}{args.suffix}"
        out_path = args.outdir / out_name
        try:
            process_one(f, out_path, lookup, overwrite=args.overwrite, quiet=args.quiet)
            n_ok += 1
        except Exception as e:
            n_fail += 1
            print(f"❌ {f.name}: {type(e).__name__}: {e}", file=sys.stderr)

    if not args.quiet:
        print(f"Done. Success={n_ok}, Fail={n_fail}")
    return 1 if n_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())