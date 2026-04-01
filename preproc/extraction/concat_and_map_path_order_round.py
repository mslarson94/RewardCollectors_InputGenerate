#!/usr/bin/env python3
"""
concat_and_map_path_order_round.py

Append multiple CSV files and create a numeric mapping column for `path_order_round`.

Usage:
  python concat_and_map_path_order_round.py \
    --inputs "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda1/*.csv" \
    --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_All/pathUtility_lambda1.csv"

Or pass multiple --inputs:
  python concat_and_map_path_order_round.py \
    --inputs "/path/to/a.csv" "/path/to/b.csv" "/path/to/c.csv" \
    --output "/path/to/combined.csv"
"""

from __future__ import annotations

import argparse
import glob
from pathlib import Path
from typing import Iterable, List

import pandas as pd


PATH_ORDER_MAP = {
    "HV->LV->NV": 1,
    "LV->HV->NV": 2,
    "NV->HV->LV": 3,
    "HV->NV->LV": 4,
    "NV->LV->HV": 5,
    "LV->NV->HV": 6,
}


def expand_inputs(inputs: Iterable[str]) -> List[Path]:
    paths: List[Path] = []
    for item in inputs:
        matches = glob.glob(item)
        if matches:
            paths.extend(Path(m).resolve() for m in matches)
        else:
            p = Path(item).resolve()
            if p.exists():
                paths.append(p)

    # Deduplicate while preserving order
    seen = set()
    uniq: List[Path] = []
    for p in paths:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inputs",
        nargs="+",
        required=True,
        help='File paths and/or globs (e.g. "/data/*.csv").',
    )
    parser.add_argument("--output", required=True, help="Output CSV path.")
    parser.add_argument(
        "--mapped-col",
        default="path_order_round_num",
        help='Name of numeric mapping column (default: "path_order_round_num").',
    )
    args = parser.parse_args()

    input_paths = expand_inputs(args.inputs)
    if not input_paths:
        raise SystemExit("No input files found. Check --inputs paths/globs.")

    frames = []
    for p in input_paths:
        df = pd.read_csv(p)
        if "path_order_round" not in df.columns:
            raise SystemExit(f'Missing column "path_order_round" in: {p}')
        df["_source_file"] = p.name
        frames.append(df)

    out = pd.concat(frames, ignore_index=True)

    out[args.mapped_col] = out["path_order_round"].map(PATH_ORDER_MAP)

    unmapped = out.loc[out[args.mapped_col].isna(), "path_order_round"].dropna().unique()
    if len(unmapped) > 0:
        raise SystemExit(
            "Unmapped path_order_round values found:\n"
            + "\n".join(f"  - {v}" for v in unmapped)
        )

    out_path = Path(args.output).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print(f"Combined rows: {len(out):,}")
    print(f"Wrote: {out_path}")
    print(f"Added mapping column: {args.mapped_col}")
    print('Added provenance column: "_source_file"')


if __name__ == "__main__":
    main()