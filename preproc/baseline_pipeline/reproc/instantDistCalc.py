#!/usr/bin/env python3
"""
Add instantaneous distances from each row's head position (XZ) to 11 labeled targets,
and write an updated CSV.

Uses columns:
  - HeadPosAnchored_x
  - HeadPosAnchored_z
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


# 8 labeled positions
POSITIONS = {
    "pos1": (0.0, 5.0),
    "pos2": (3.5, 3.5),
    "pos3": (5.0, 0.0),
    "pos4": (3.5, -3.5),
    "pos5": (0.0, -5.0),
    "pos6": (-3.5, -3.5),
    "pos7": (-5.0, 0.0),
    "pos8": (-3.5, 3.5),
}

# 3 reward positions
REWARDS = {
    "HV": (1.36, -3.04),
    "LV": (-3.76, -0.1),
    "NV": (-0.57, 2.4),
}

X_COL = "HeadPosAnchored_x"
Z_COL = "HeadPosAnchored_z"


def add_distance_columns(df: pd.DataFrame) -> pd.DataFrame:
    if X_COL not in df.columns or Z_COL not in df.columns:
        raise KeyError(f"Missing required columns: {X_COL!r} and/or {Z_COL!r}")

    # Ensure numeric (invalid parses become NaN)
    x = pd.to_numeric(df[X_COL], errors="coerce")
    z = pd.to_numeric(df[Z_COL], errors="coerce")

    targets = {}
    targets.update(POSITIONS)
    targets.update(REWARDS)

    for label, (tx, tz) in targets.items():
        # Euclidean distance in XZ plane
        df[f"dist_{label}"] = ((x - tx) ** 2 + (z - tz) ** 2) ** 0.5

    return df


def main() -> None:
    ap = argparse.ArgumentParser(description="Add distance-to-target columns to a CSV.")
    ap.add_argument("--input", type=Path, help="Path to input CSV")
    ap.add_argument(
        "--output",
        type=Path,
        help="Path to output CSV (default: <input>_with_dist.csv)",
    )
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists",
    )
    args = ap.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    in_path: Path = args.input
    #out_path: Path = args.output or in_path.with_name(in_path.stem + "_with_dist.csv")
    outName = in_path.stem + "_with_dist.csv"
    out_path = output / outName

    if out_path.exists() and not args.overwrite:
        raise FileExistsError(f"Output exists: {out_path}. Use --overwrite to replace it.")

    df = pd.read_csv(in_path)
    df = add_distance_columns(df)
    df.to_csv(out_path, index=False)

    print(f"Wrote: {out_path}")
    print(f"Added columns: {[c for c in df.columns if c.startswith('dist_')]}")

if __name__ == "__main__":
    main()
