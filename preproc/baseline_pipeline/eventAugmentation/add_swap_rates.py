#!/usr/bin/env python3
"""
Compute session-history and recent-window swap rates from the output of the prior script.

Adds:
  - swapRate_t-1_all / _neg / _pos   (history rates over all prior eligible rows)
  - recentSwapRate_numOfTrials       (CLI-provided X, repeated per row)
  - recentSwapRate_all / _neg / _pos (rates over last X prior eligible rows)

Eligibility:
  - Exclude rows where CoinSetID == 4 from BOTH numerator and denominator.

Important:
  - All rates are t-1: they use ONLY rows strictly before the current row.
  - For "recent" rates: denominator SHRINKS to the number of eligible prior rows
    within the last X prior rows (and also naturally shrinks for the first X-1 rows).

Ordering:
  - Uses TotSesh_rowIndex to define session order (must exist from prior script).
  - If TotSesh_rowIndex not present, falls back to sorting by testingOrder,start_AppTime.

Usage:
  python add_swap_rates.py --input_dir /path/to/csvs --output_dir /path/to/out --recent_trials 30
  python add_swap_rates.py --input_file in.csv --output_file out.csv --recent_trials 30
"""

from __future__ import annotations

import argparse
import glob
import os
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


REQUIRED_COLS = ["CoinSetID", "coinLabel"]
PREFERRED_ORDER_COL = "TotSesh_rowIndex"
FALLBACK_ORDER_COLS = ["testingOrder", "start_AppTime"]


def _ensure_cols(df: pd.DataFrame, cols: list[str], path: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{path}: missing required columns: {missing}")


def _sort_session(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if PREFERRED_ORDER_COL in df.columns:
        # stable in case of ties
        df["_origRow"] = np.arange(len(df), dtype=int)
        df = df.sort_values([PREFERRED_ORDER_COL, "_origRow"], kind="mergesort").drop(columns=["_origRow"])
        return df.reset_index(drop=True)

    # fallback
    for c in FALLBACK_ORDER_COLS:
        if c not in df.columns:
            raise ValueError(f"Missing {PREFERRED_ORDER_COL} and fallback sort column '{c}'.")
    df["_origRow"] = np.arange(len(df), dtype=int)
    df = df.sort_values(FALLBACK_ORDER_COLS + ["_origRow"], kind="mergesort").drop(columns=["_origRow"])
    return df.reset_index(drop=True)


def _safe_div(num: np.ndarray, den: np.ndarray) -> np.ndarray:
    out = np.full_like(num, np.nan, dtype=float)
    mask = den > 0
    out[mask] = num[mask] / den[mask]
    return out


def process_one_file(in_path: str, out_path: str, recent_trials: int) -> None:
    df = pd.read_csv(in_path)
    _ensure_cols(df, REQUIRED_COLS, in_path)

    df = _sort_session(df)

    # Standardize CoinSetID to numeric if possible
    # Standardize CoinSetID to numeric if possible
    coin = pd.to_numeric(df["CoinSetID"], errors="coerce")

    eligible = (coin != 4) & coin.notna()

    # Normalize labels for robust matching (handles "nv", " NV ", etc.)
    # Normalize labels for robust matching
    label = df["coinLabel"].astype(str).str.strip().str.upper()

    # Swap definitions (eligible excludes CoinSetID==4)
    is_pos = eligible & (coin == 2) & (label == "NV")
    is_neg = eligible & (coin == 3) & (label == "HV")
    is_swap_any = is_pos | is_neg

    # ---------- New debug columns ----------
    # isSwap: 1 if swap event else 0 (including CoinSetID==4 as 0)
    df["isSwap"] = is_swap_any.astype(int)

    # isSwapType: "pos"/"neg" for swaps, else 0
    # (keeping your requested 0 for non-swap events; mix of int + str => object column)
    swap_type = np.where(is_pos.to_numpy(), "pos", np.where(is_neg.to_numpy(), "neg", 0))
    df["swapType"] = swap_type
    # ---------- 1) History swap rates (t-1) ----------
    # Denominator: number of eligible prior rows
    elig_cum_prior = eligible.astype(int).cumsum().shift(1, fill_value=0).to_numpy()

    swap_any_cum_prior = is_swap_any.astype(int).cumsum().shift(1, fill_value=0).to_numpy()
    neg_cum_prior = is_neg.astype(int).cumsum().shift(1, fill_value=0).to_numpy()
    pos_cum_prior = is_pos.astype(int).cumsum().shift(1, fill_value=0).to_numpy()

    df["swapRate_t-1_all"] = _safe_div(swap_any_cum_prior, elig_cum_prior)
    df["swapRate_t-1_neg"] = _safe_div(neg_cum_prior, elig_cum_prior)
    df["swapRate_t-1_pos"] = _safe_div(pos_cum_prior, elig_cum_prior)

    # ---------- 2) recentSwapRate_numOfTrials ----------
    df["recentSwapRate_numOfTrials"] = int(recent_trials)

    # ---------- 3) Recent-window swap rates (t-1), last X prior rows ----------
    X = int(recent_trials)
    if X <= 0:
        raise ValueError("--recent_trials must be a positive integer")

    # Work with int arrays for speed
    elig_i = eligible.astype(int)
    swap_any_i = is_swap_any.astype(int)
    neg_i = is_neg.astype(int)
    pos_i = is_pos.astype(int)

    # Rolling over last X rows INCLUDING current row; shift(1) to make it t-1 (prior rows only)
    # Denominators shrink automatically for first rows and for excluded (CoinSetID==4) rows.
    recent_den = elig_i.rolling(window=X, min_periods=1).sum().shift(1).fillna(0).to_numpy()

    recent_any = swap_any_i.rolling(window=X, min_periods=1).sum().shift(1).fillna(0).to_numpy()
    recent_neg = neg_i.rolling(window=X, min_periods=1).sum().shift(1).fillna(0).to_numpy()
    recent_pos = pos_i.rolling(window=X, min_periods=1).sum().shift(1).fillna(0).to_numpy()

    df["recentSwapRate_all"] = _safe_div(recent_any, recent_den)
    df["recentSwapRate_neg"] = _safe_div(recent_neg, recent_den)
    df["recentSwapRate_pos"] = _safe_div(recent_pos, recent_den)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    df.to_csv(out_path, index=False)


@dataclass
class Args:
    input_dir: Optional[str]
    output_dir: Optional[str]
    input_file: Optional[str]
    output_file: Optional[str]
    pattern: str
    recent_trials: int


def parse_args() -> Args:
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--input_dir", type=str, default=None, help="Directory of CSV files to process")
    g.add_argument("--input_file", type=str, default=None, help="Single CSV file to process")

    p.add_argument("--output_dir", type=str, default=None, help="Output directory (required if using --input_dir)")
    p.add_argument("--output_file", type=str, default=None, help="Output file (required if using --input_file)")
    p.add_argument("--pattern", type=str, default="*.csv", help="Glob pattern within input_dir (default: *.csv)")
    p.add_argument("--recent_trials", type=int, required=True, help="Window size X for recent swap rates")
    a = p.parse_args()

    if a.input_dir and not a.output_dir:
        p.error("--output_dir is required when using --input_dir")
    if a.input_file and not a.output_file:
        p.error("--output_file is required when using --input_file")

    return Args(
        input_dir=a.input_dir,
        output_dir=a.output_dir,
        input_file=a.input_file,
        output_file=a.output_file,
        pattern=a.pattern,
        recent_trials=a.recent_trials,
    )


def main() -> None:
    args = parse_args()

    if args.input_file:
        process_one_file(args.input_file, args.output_file, args.recent_trials)  # type: ignore[arg-type]
        return

    in_dir = args.input_dir  # type: ignore[assignment]
    out_dir = args.output_dir  # type: ignore[assignment]

    paths = sorted(glob.glob(os.path.join(in_dir, args.pattern)))
    if not paths:
        raise FileNotFoundError(f"No files matched {args.pattern} in {in_dir}")

    for in_path in paths:
        base = os.path.basename(in_path)
        out_path = os.path.join(out_dir, base)
        process_one_file(in_path, out_path, args.recent_trials)


if __name__ == "__main__":
    main()