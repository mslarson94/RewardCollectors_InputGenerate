# annotate_within_round_swap_behavior.py
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Annotate within-round swap-event timing and post-swap behavior flags. "
            "Creates round-level and pin-drop-level variables describing whether a row "
            "is in a swap-type round, where the swap event occurred, and whether the row "
            "occurs after the swap event within the same round."
        )
    )
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument("--output", required=True, help="Path to output CSV")
    parser.add_argument("--round-col", default="roundID", help="Round identifier column")
    parser.add_argument("--coinsetid-col", default="CoinSetID", help="CoinSetID column")
    parser.add_argument("--isswap-col", default="isSwap", help="Swap-event indicator column")
    parser.add_argument("--chestpin-col", default="chestPin_num", help="Pin-drop order column")
    return parser.parse_args()


def series_truthy(s: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(s):
        return s.fillna(False)
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce").fillna(0).astype(float) != 0.0
    text = s.astype("string").str.strip().str.lower()
    return text.isin({"1", "true", "t", "yes", "y"})


def first_valid_swap_pin(sub: pd.DataFrame, chestpin_col: str, isswap_col: str) -> float:
    mask = sub[isswap_col]
    if not mask.any():
        return np.nan
    vals = pd.to_numeric(sub.loc[mask, chestpin_col], errors="coerce").dropna()
    if vals.empty:
        return np.nan
    return float(vals.iloc[0])


def annotate_swap_behavior(
    df: pd.DataFrame,
    *,
    round_col: str,
    coinsetid_col: str,
    isswap_col: str,
    chestpin_col: str,
) -> pd.DataFrame:
    required = [round_col, coinsetid_col, isswap_col, chestpin_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    out = df.copy()

    out["_coinsetid_num"] = pd.to_numeric(out[coinsetid_col], errors="coerce")
    out["_chestpin_num"] = pd.to_numeric(out[chestpin_col], errors="coerce")
    out["_isSwap_bool"] = series_truthy(out[isswap_col])

    # Round-type indicator: swap-capable round vs. not
    out["isSwapRoundType"] = (out["_coinsetid_num"] > 1).fillna(False).astype(int)

    # Locate the swap-event pin position within each round
    swap_pin_by_round = (
        out.groupby(round_col, dropna=False)
        .apply(lambda sub: first_valid_swap_pin(sub, "_chestpin_num", "_isSwap_bool"))
        .rename("swapEventPin_num")
        .reset_index()
    )

    out = out.merge(swap_pin_by_round, on=round_col, how="left")

    # Did this round actually contain a detected swap event?
    out["hasSwapEventWithinRound"] = out["swapEventPin_num"].notna().astype(int)

    # Within-round phase annotation
    out["swapPhaseWithinRound"] = pd.Series("unknown", index=out.index, dtype="string")

    non_swap_round = out["isSwapRoundType"] == 0
    swap_round_no_event = (out["isSwapRoundType"] == 1) & (out["hasSwapEventWithinRound"] == 0)
    pre_swap = (
        (out["isSwapRoundType"] == 1)
        & (out["hasSwapEventWithinRound"] == 1)
        & out["_chestpin_num"].notna()
        & (out["_chestpin_num"] < out["swapEventPin_num"])
    )
    swap_event = (
        (out["isSwapRoundType"] == 1)
        & (out["hasSwapEventWithinRound"] == 1)
        & out["_chestpin_num"].notna()
        & (out["_chestpin_num"] == out["swapEventPin_num"])
    )
    post_swap = (
        (out["isSwapRoundType"] == 1)
        & (out["hasSwapEventWithinRound"] == 1)
        & out["_chestpin_num"].notna()
        & (out["_chestpin_num"] > out["swapEventPin_num"])
    )

    out.loc[non_swap_round, "swapPhaseWithinRound"] = "non_swap_round"
    out.loc[swap_round_no_event, "swapPhaseWithinRound"] = "swap_round_no_event"
    out.loc[pre_swap, "swapPhaseWithinRound"] = "pre_swap"
    out.loc[swap_event, "swapPhaseWithinRound"] = "swap_event"
    out.loc[post_swap, "swapPhaseWithinRound"] = "post_swap"

    # Key binary variable: post-swap-within-round behavior effect
    out["isPostSwapWithinRound"] = post_swap.astype(int)

    # User-facing behavior-modulation variable
    out["EV_behMod"] = out["isPostSwapWithinRound"].astype(int)

    # Optional compact numeric phase code for modeling if needed later
    phase_code_map = {
        "non_swap_round": 0,
        "swap_round_no_event": -1,
        "pre_swap": 1,
        "swap_event": 2,
        "post_swap": 3,
        "unknown": pd.NA,
    }
    out["swapPhaseWithinRound_num"] = out["swapPhaseWithinRound"].map(phase_code_map).astype("Int64")

    # Clean temp columns
    out = out.drop(columns=["_coinsetid_num", "_chestpin_num", "_isSwap_bool"])

    return out


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    df = pd.read_csv(input_path)

    out = annotate_swap_behavior(
        df,
        round_col=args.round_col,
        coinsetid_col=args.coinsetid_col,
        isswap_col=args.isswap_col,
        chestpin_col=args.chestpin_col,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)

    print(f"Wrote: {output_path}")
    print(f"Rows: {len(out)}")
    print(f"Unique rounds: {out[args.round_col].nunique(dropna=True)}")
    print("\nCounts: isSwapRoundType")
    print(out["isSwapRoundType"].value_counts(dropna=False).sort_index())
    print("\nCounts: hasSwapEventWithinRound")
    print(out["hasSwapEventWithinRound"].value_counts(dropna=False).sort_index())
    print("\nCounts: swapPhaseWithinRound")
    print(out["swapPhaseWithinRound"].value_counts(dropna=False))
    print("\nCounts: isPostSwapWithinRound")
    print(out["isPostSwapWithinRound"].value_counts(dropna=False).sort_index())
    print("\nCounts: EV_behMod")
    print(out["EV_behMod"].value_counts(dropna=False).sort_index())


if __name__ == "__main__":
    main()