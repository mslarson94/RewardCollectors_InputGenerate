"""
addKnotsForDecisionModel.py

Add piecewise-linear "early/late" knot features for session time.

For each knot K:
  t_early_K = min(TotSesh, K)
  t_late_K  = max(TotSesh - K, 0)

Optionally mean-center these columns (recommended when you plan to use interactions).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


TOTSESH_COL = "TotSesh_actTest_RoundNum"


def add_knot_features(
    df: pd.DataFrame,
    knots: Iterable[int],
    totsesh_col: str = TOTSESH_COL,
    center: bool = False,
) -> pd.DataFrame:
    if totsesh_col not in df.columns:
        raise KeyError(f"Missing required column: {totsesh_col}")

    t = pd.to_numeric(df[totsesh_col], errors="coerce")

    for k in knots:
        if k <= 0:
            raise ValueError(f"Knot must be positive; got {k}")

        early = np.minimum(t, k)
        late = np.maximum(t - k, 0)

        early_name = f"t_early_{k}"
        late_name = f"t_late_{k}"

        df[early_name] = early
        df[late_name] = late

        if center:
            df[f"{early_name}_c"] = df[early_name] - df[early_name].mean(skipna=True)
            df[f"{late_name}_c"] = df[late_name] - df[late_name].mean(skipna=True)

    return df


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Add piecewise knot features for TotSesh.")
    parser.add_argument("--in_csv", type=Path, required=True, help="Input CSV path")
    parser.add_argument("--out_csv", type=Path, required=True, help="Output CSV path")
    parser.add_argument("--out_pruned", type=Path, required=True, help="output csv path for the trimmed pindropping file")
    parser.add_argument(
        "--knots",
        type=int,
        nargs="+",
        default=[15, 20, 25],
        help="One or more knot positions (default: 15 20 25)",
    )
    parser.add_argument(
        "--center",
        action="store_true",
        help="If set, also write mean-centered versions *_c",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    df = pd.read_csv(args.in_csv)
    df = add_knot_features(df, knots=args.knots, center=args.center)

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out_csv, index=False)
    df_pruned = df[df["BlockType"].astype(str).str.strip().str.lower() != "collecting"]
    coinset = pd.to_numeric(df_pruned["CoinSetID"], errors="coerce")  # float dtype with NaN for bad values
    df_pruned = df_pruned[coinset.lt(4)] ## no coin sets that are greater than or equal to 4 (tutorial data)
    df_pruned.to_csv(args.out_pruned, index=False)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))