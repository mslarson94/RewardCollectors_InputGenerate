"""
prepDataForDecisionModel.py
Prepare round-level choice data for a decision model.

Steps:
1) Filter to pin-dropping rounds, non-tutorial coin sets (<4), and first step in round.
2) Expand each round into 6 alternatives (alt=1..6).
3) Merge alternative-specific attributes from pathUtility_lambda1.csv:
   - distance -> idealDistance
   - points
   - utility
4) Add chosen indicator based on path_order_round_num.

Example:
  python prepDataForDecisionModel.py \
    --interval_csv /path/to/allIntervalData_L1.csv \
    --utility_csv /path/to/pathUtility_lambda1.csv \
    --out_csv /path/to/choiceData.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def _require_cols(df: pd.DataFrame, cols: list[str], df_name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"{df_name} missing required columns: {missing}")


def load_and_filter_round_level(interval_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(interval_csv)

    _require_cols(
        df,
        ["BlockType", "CoinSetID", "path_step_in_round", "coinSet", "startPos", "path_order_round_num"],
        "allIntervalData_L1",
    )

    # 1) round-level: filtering
    df = df[df["BlockType"].astype(str).str.strip().str.lower() != "collecting"].copy()

    coinset_id = pd.to_numeric(df["CoinSetID"], errors="coerce")
    df = df[coinset_id.lt(4)].copy()  # exclude tutorial coin sets >= 4

    df = df[df["path_step_in_round"] == 1].copy()  # first row in round

    # Keep only valid choices 1..6 (drop NaN/other codes like 888/999)
    df["path_order_round_num"] = pd.to_numeric(df["path_order_round_num"], errors="coerce")
    df = df[df["path_order_round_num"].between(1, 6, inclusive="both")].copy()

    # Normalize merge keys (strings)
    df["coinSet"] = df["coinSet"].astype(str).str.strip()
    df["startPos"] = df["startPos"].astype(str).str.strip()

    return df


def load_utility_table(utility_csv: Path) -> pd.DataFrame:
    u = pd.read_csv(utility_csv)

    _require_cols(
        u,
        ["coinSet", "startPos", "path_order_round_num", "distance", "points", "utility"],
        "pathUtility_lambda1",
    )

    u = u.copy()
    u["coinSet"] = u["coinSet"].astype(str).str.strip()
    u["startPos"] = u["startPos"].astype(str).str.strip()
    u["path_order_round_num"] = pd.to_numeric(u["path_order_round_num"], errors="coerce").astype("Int64")

    u = u.rename(columns={"distance": "idealDistance", "path_order_round_num": "alt"})
    u = u[["coinSet", "startPos", "alt", "idealDistance", "points", "utility"]].copy()

    # De-dup in case of repeated rows
    u = u.drop_duplicates(subset=["coinSet", "startPos", "alt"], keep="first")

    return u


def expand_to_alternatives(round_df: pd.DataFrame) -> pd.DataFrame:
    alts = pd.DataFrame({"alt": np.arange(1, 7, dtype=int)})
    choice_df = round_df.merge(alts, how="cross")

    # chosen indicator based on numeric code
    choice_df["chosen"] = (choice_df["alt"] == choice_df["path_order_round_num"].astype(int)).astype(int)

    return choice_df


def merge_utility(choice_df: pd.DataFrame, utility_df: pd.DataFrame) -> pd.DataFrame:
    merged = choice_df.merge(
        utility_df,
        on=["coinSet", "startPos", "alt"],
        how="left",
        validate="many_to_one",
    )

    # Hard fail if merge is unexpectedly missing
    missing = merged["utility"].isna().mean()
    if missing > 0:
        # still allow a small fraction if your files are incomplete; otherwise raise
        raise ValueError(
            f"Utility merge produced missing values for {missing:.1%} of rows. "
            "Check that coinSet/startPos align between the two CSVs."
        )

    return merged


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval_csv", type=Path, required=True)
    parser.add_argument("--utility_csv", type=Path, required=True)
    parser.add_argument("--out_csv", type=Path, required=True)
    args = parser.parse_args(argv)

    round_df = load_and_filter_round_level(args.interval_csv)
    utility_df = load_utility_table(args.utility_csv)

    choice_df = expand_to_alternatives(round_df)
    choice_df = merge_utility(choice_df, utility_df)

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    choice_df = choice_df.drop(columns=["Unnamed: 0"], errors="ignore")
    choice_df.to_csv(args.out_csv, index=False)

    print(
        f"Wrote {len(choice_df):,} rows ({len(round_df):,} rounds x 6 alts) to {args.out_csv}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))