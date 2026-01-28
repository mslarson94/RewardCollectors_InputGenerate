#!/usr/bin/env python3
"""
score_pinDropVotes_PO.py

Given a merged pin-drop file that includes:
- PinDrop_Moment rows
- a 'pinDropVote' column (e.g., 'CORRECT', 'INCORRECT', etc.)
- role / ID info

this script:

1. Filters to currentRole == 'PO'.
2. Restricts to PinDrop_Moment rows.
3. Groups by (participantID, coinSet, main_RR).
4. For each group, computes:
   - n_trials           : # PinDrop_Moment rows
   - n_votes            : # rows with non-null pinDropVote
   - n_correct          : # rows with pinDropVote == 'CORRECT'
   - prop_correct_all   : n_correct / n_trials
   - prop_correct_votes : n_correct / n_votes
   - (optional) p_chance_ge : binomial p-value vs given chance level.

5. Writes:
   - summary CSV with one row per (participantID, coinSet, main_RR)
   - per-group CSVs in a split directory.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import math
import numpy as np
import pandas as pd


def _norm_str(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.strip()
        .str.upper()
    )


def _choose_coinset_col(df: pd.DataFrame) -> str:
    if "coinSet" in df.columns:
        return "coinSet"
    if "coinSetUsed" in df.columns:
        return "coinSetUsed"
    raise ValueError("Neither 'coinSet' nor 'coinSetUsed' found in input file.")


def _binom_sf(k: int, n: int, p: float) -> float:
    """
    P(X >= k) for X ~ Binomial(n, p).
    Uses direct sum; fine for modest n (here n is per-participant).
    """
    if n <= 0:
        return float("nan")
    if k <= 0:
        return 1.0
    if k > n:
        return 0.0
    s = 0.0
    for i in range(k, n + 1):
        s += math.comb(n, i) * (p ** i) * ((1 - p) ** (n - i))
    return s


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Split PO pinDrop data and compute per-(participant, coinSet, main_RR) pinDropVote accuracy."
    )
    ap.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to merged CSV containing PinDrop_Moment rows with pinDropVote column.",
    )
    ap.add_argument(
        "--out-summary",
        required=True,
        type=Path,
        help="Output CSV path for summary table.",
    )
    ap.add_argument(
        "--out-split-dir",
        required=True,
        type=Path,
        help="Directory where per-(participant, coinSet, main_RR) CSVs will be written.",
    )
    ap.add_argument(
        "--chance-level",
        type=float,
        default=None,
        help="Optional chance level (e.g., 0.33). If set, compute binomial p(X>=k) per group.",
    )
    args = ap.parse_args()

    df = pd.read_csv(args.input)

    # Basic sanity checks
    for col in ["currentRole", "participantID", "main_RR"]:
        if col not in df.columns:
            raise ValueError(f"Input file is missing required column {col!r}.")

    if "pinDropVote" not in df.columns:
        raise ValueError("Input file must contain 'pinDropVote' column.")

    coinset_col = _choose_coinset_col(df)

    # Filter to PO only
    role_norm = _norm_str(df["currentRole"])
    df = df[role_norm == "PO"].copy()
    if df.empty:
        raise ValueError("No rows with currentRole == 'PO' after filtering.")

    # Restrict to PinDrop_Moment rows if lo_eventType present
    if "lo_eventType" in df.columns:
        ev_norm = _norm_str(df["lo_eventType"])
        mask_moment = ev_norm == "PINDROP_MOMENT"
        df = df[mask_moment].copy()
        if df.empty:
            raise ValueError("No PinDrop_Moment rows found for PO participants.")
    # If no event-type column, we just assume all rows are pin-drop trials.

    # Normalise pinDropVote and flag correctness
    vote_norm = _norm_str(df["pinDropVote"])
    df["pinDropVote_norm"] = vote_norm
    df["isCorrect"] = df["pinDropVote_norm"] == "CORRECT"

    group_cols = ["participantID", coinset_col, "main_RR"]

    # Group and compute counts
    grouped = df.groupby(group_cols, dropna=False)

    summary = grouped.agg(
        n_trials=("pinDropVote", "size"),                # total PinDrop_Moment rows
        n_votes=("pinDropVote", lambda s: s.notna().sum()),
        n_correct=("isCorrect", "sum"),
    ).reset_index()

    summary["prop_correct_all"] = (
        summary["n_correct"] / summary["n_trials"].replace(0, np.nan)
    )
    summary["prop_correct_votes"] = (
        summary["n_correct"] / summary["n_votes"].replace(0, np.nan)
    )

    # Optional: binomial test vs chance level
    if args.chance_level is not None:
        p0 = float(args.chance_level)

        def _p_row(row):
            n = int(row["n_trials"])
            k = int(row["n_correct"])
            return _binom_sf(k, n, p0)

        summary["p_chance_ge"] = summary.apply(_p_row, axis=1)

    # Write summary
    args.out_summary.parent.mkdir(parents=True, exist_ok=True)
    summary.sort_values(group_cols).to_csv(args.out_summary, index=False)
    print(f"[ok] wrote summary to {args.out_summary}")

    # Write per-group splits
    out_split_dir = args.out_split_dir
    out_split_dir.mkdir(parents=True, exist_ok=True)

    for keys, g in grouped:
        # keys is (participantID, coinSet/coinSetUsed, main_RR)
        pid, cs, rr = keys
        pid_str = str(pid).replace(" ", "")
        cs_str = str(cs).replace(" ", "")
        rr_str = str(rr).replace(" ", "")

        fname = f"pinDropVotes_PO_pid-{pid_str}_coinSet-{cs_str}_mainRR-{rr_str}.csv"
        path = out_split_dir / fname
        g.to_csv(path, index=False)

    print(f"[ok] wrote per-group CSVs to {out_split_dir}")


if __name__ == "__main__":
    main()
