#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

TIME_COL = "AppTime"
GROUP_KEYS = ["BlockNum", "BlockInstance"]  # map specials within BlockInstance & BlockNum composite keys

SPECIAL_NEXT = {0, 7777, 8888}  # map to next true round
SPECIAL_PREV = {9999}           # map to previous true round


def replace_suffix(name: str, in_suffix: str, out_suffix: str) -> str:
    if not name.endswith(in_suffix):
        raise ValueError(f"Filename does not end with expected suffix {in_suffix!r}: {name!r}")
    return name[: -len(in_suffix)] + out_suffix


def make_out_path(in_path: Path, out_dir: Path, in_suffix: str, out_suffix: str) -> Path:
    return out_dir / replace_suffix(in_path.name, in_suffix, out_suffix)


def add_effective_roundnum(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # required columns
    for c in GROUP_KEYS + [TIME_COL, "RoundNum", "lo_eventType"]:
        if c not in df.columns:
            raise KeyError(f"Missing required column: {c}")

    # ensure numeric and sorted
    df[TIME_COL] = pd.to_numeric(df[TIME_COL], errors="coerce")
    df["RoundNum"] = pd.to_numeric(df["RoundNum"], errors="coerce")
    df = df.sort_values(GROUP_KEYS + [TIME_COL]).reset_index(drop=True)

    # true rounds: 0 < RoundNum < 100
    is_true = (df["RoundNum"] > 0) & (df["RoundNum"] < 100)

    # anchor series of "true round num at this timestamp"
    df["_trueRoundNum"] = np.where(is_true, df["RoundNum"], np.nan)

    # next/prev true round within group
    df["_nextTrueRound"] = df.groupby(GROUP_KEYS, sort=False)["_trueRoundNum"].bfill()
    df["_prevTrueRound"] = df.groupby(GROUP_KEYS, sort=False)["_trueRoundNum"].ffill()

    # start with true rounds
    df["effectiveRoundNum"] = np.where(is_true, df["RoundNum"], np.nan)

    # map specials
    mask_next = df["RoundNum"].isin(SPECIAL_NEXT)
    mask_prev = df["RoundNum"].isin(SPECIAL_PREV)

    df.loc[mask_next, "effectiveRoundNum"] = df.loc[mask_next, "_nextTrueRound"]
    df.loc[mask_prev, "effectiveRoundNum"] = df.loc[mask_prev, "_prevTrueRound"]

    # flag cases where mapping failed (special before first true round, etc.)
    df["effectiveRound_missingFlag"] = (
        df["RoundNum"].isin(list(SPECIAL_NEXT | SPECIAL_PREV)) & df["effectiveRoundNum"].isna()
    )

    # instance counter per lo_eventType within (BlockNum, BlockInstance)
    df["lo_eventType_instance"] = (
        df.groupby(GROUP_KEYS + ["lo_eventType"], sort=False)
          .cumcount()
          .add(1)
    )

    # cleanup
    df.drop(columns=["_trueRoundNum", "_nextTrueRound", "_prevTrueRound"], inplace=True)

    return df


def _iter_csvs(input_dir: Path, pattern: str, recursive: bool) -> list[Path]:
    if recursive:
        return sorted(input_dir.rglob(pattern))
    return sorted(input_dir.glob(pattern))


def process_one_file(
    in_path: Path,
    out_dir: Path,
    overwrite: bool,
    in_suffix: str,
    out_suffix: str,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = make_out_path(in_path, out_dir, in_suffix, out_suffix)

    if out_path.exists() and not overwrite:
        raise FileExistsError(f"Output exists: {out_path} (use --overwrite)")

    df = pd.read_csv(in_path)
    df2 = add_effective_roundnum(df)
    df2.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description="Add effectiveRoundNum (and instance flags) to CSV(s).")

    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--input", type=Path, help="Single input CSV")
    src.add_argument("--input-dir", type=Path, help="Directory of input CSVs")

    ap.add_argument("--output", type=Path, help="(single-file mode) Output directory (default: input parent)")
    ap.add_argument("--output-dir", type=Path, help="(dir mode) Output directory (required for --input-dir)")
    ap.add_argument("--pattern", type=str, default="*.csv", help="Glob pattern in dir mode (default: *.csv)")
    ap.add_argument("--recursive", action="store_true", help="Recurse into subdirectories in dir mode")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite outputs if they exist")

    ap.add_argument(
        "--in-suffix",
        type=str,
        default="_events_coinLabel.csv",
        help="Input filename suffix to replace (must match end of filename).",
    )
    ap.add_argument(
        "--out-suffix",
        type=str,
        default="_events_coinLabel_effectiveRound.csv",
        help="Output filename suffix.",
    )

    args = ap.parse_args()

    # Directory mode
    if args.input_dir is not None:
        in_dir: Path = args.input_dir
        out_dir: Path | None = args.output_dir
        if out_dir is None:
            ap.error("--output-dir is required when using --input-dir")

        csvs = _iter_csvs(in_dir, args.pattern, args.recursive)
        if not csvs:
            print(f"No files matched in {in_dir} with pattern={args.pattern!r}", file=sys.stderr)
            sys.exit(2)

        failures = 0
        for p in csvs:
            try:
                out_path = process_one_file(
                    p,
                    out_dir,
                    args.overwrite,
                    args.in_suffix,
                    args.out_suffix,
                )
                print(f"✅ {p.name} -> {out_path}")
            except Exception as e:
                failures += 1
                print(f"❌ Failed: {p} :: {e}", file=sys.stderr)

        sys.exit(1 if failures else 0)

    # Single-file mode
    in_path: Path = args.input
    out_dir = args.output or in_path.parent
    out_path = process_one_file(
        in_path,
        out_dir,
        args.overwrite,
        args.in_suffix,
        args.out_suffix,
    )
    print(f"Wrote: {out_path}")

if __name__ == "__main__":
    main()
