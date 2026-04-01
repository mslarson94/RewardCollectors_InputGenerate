#!/usr/bin/env python3
"""
PinDrop_Moment path builder (timestamp-ordered).

- Filters rows where lo_eventType == 'PinDrop_Moment'
- Sorts within each (participantID, BlockNum, BlockInstance, RoundNum) by:
    1) mLTimestamp (parsed datetime)
    2) chestPin_num
    3) origRow_start
- Builds:
    category        = cumulative coinLabel path so far (good for verification)
    category_final  = full path for the round (same for all rows in the group)

Output columns include your static + dynamic fields (where present).
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


STATIC_COLS = [
    "participantID",
    "pairID",
    "testingDate",
    "sessionType",
    "ptIsAorB",
    "coinSet",
    "device",
    "main_RR",
    "currentRole",
    "source_file",
]

DYNAMIC_COLS = [
    "BlockNum",
    "RoundNum",
    "CoinSetID",
    "BlockStatus",
    "BlockInstance",
    "coinLabel",
    "dropDist",
    "chestPin_num",
    "origRow_start",
    "block_elapsed_s",
    "round_elapsed_s",
    "truecontent_elapsed_s",
    "HeadPosAnchored_x_at_start",
    "HeadPosAnchored_y_at_start",
    "HeadPosAnchored_z_at_start",
    "mLTimestamp",
    "mLTimestamp_raw",
]


def _ensure_cols(df: pd.DataFrame, cols: list[str], *, strict: bool) -> pd.DataFrame:
    missing = [c for c in cols if c not in df.columns]
    if missing and strict:
        raise ValueError("Missing required columns:\n  - " + "\n  - ".join(missing))
    out = df.copy()
    if missing and not strict:
        for c in missing:
            out[c] = pd.NA
    return out


def build_pindrop_category_df(
    df: pd.DataFrame,
    *,
    event_type_col: str = "lo_eventType",
    pindrop_value: str = "PinDrop_Moment",
    static_cols: list[str] = STATIC_COLS,
    dynamic_cols: list[str] = DYNAMIC_COLS,
    coin_label_col: str = "coinLabel",
    time_col: str = "mLTimestamp",
    chestpin_col: str = "chestPin_num",
    category_col: str = "category",
    category_final_col: str = "category_final",
    sep: str = "-",
    strict: bool = True,
) -> pd.DataFrame:
    """
    Returns a PinDrop_Moment-only dataframe where:
      - category is cumulative path (coinLabel appended in timestamp order)
      - category_final is the final full path for the round
    """

    required = [event_type_col, coin_label_col, time_col, chestpin_col, *static_cols, *dynamic_cols]
    out = _ensure_cols(df, required, strict=strict)

    # Filter to PinDrop_Moment
    out = out[out[event_type_col] == pindrop_value].copy()

    # Parse timestamp for sorting
    out["_time"] = pd.to_datetime(out[time_col], errors="coerce")

    # Grouping: within round within block instance within block (+ participant)
    group_cols = [c for c in ["participantID", "BlockNum", "BlockInstance", "RoundNum"] if c in out.columns]

    # Sort: group keys + time, then chestPin_num, then origRow_start
    sort_cols = [*group_cols]
    if "_time" in out.columns:
        sort_cols.append("_time")
    if chestpin_col in out.columns:
        sort_cols.append(chestpin_col)
    if "origRow_start" in out.columns:
        sort_cols.append("origRow_start")
    out = out.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    def _apply_paths(g: pd.DataFrame) -> pd.DataFrame:
        acc: list[str] = []
        cum: list[str] = []
        for v in g[coin_label_col].tolist():
            if pd.notna(v):
                acc.append(str(v))
            cum.append(sep.join(acc))
        final = sep.join(acc)

        g = g.copy()
        g[category_col] = cum
        g[category_final_col] = final
        return g

    out = out.groupby(group_cols, sort=False, group_keys=False).apply(_apply_paths)

    # Keep/order columns
    cols_order = [*static_cols, *dynamic_cols, category_col, category_final_col]
    cols_present = [c for c in cols_order if c in out.columns]
    out = out.loc[:, cols_present].reset_index(drop=True)

    return out


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("-i", "--input", required=True, help="Path to input CSV (events file).")
    p.add_argument(
        "-o", "--output", default=None,
        help="Path to output CSV. Default: <input_stem>_pindrop_category_cumulative.csv next to input.",
    )
    p.add_argument("--event-type-col", default="lo_eventType")
    p.add_argument("--pindrop-value", default="PinDrop_Moment")
    p.add_argument("--coin-label-col", default="coinLabel")
    p.add_argument("--time-col", default="mLTimestamp")
    p.add_argument("--chestpin-col", default="chestPin_num")
    p.add_argument("--category-col", default="pathCategory")
    p.add_argument("--category-final-col", default="pathCategory_final")
    p.add_argument("--sep", default="-")
    p.add_argument("--non-strict", action="store_true", help="Fill missing columns with NA instead of erroring.")
    args = p.parse_args(argv)

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"ERROR: input file not found: {in_path}", file=sys.stderr)
        return 2

    out_path = (
        Path(args.output)
        if args.output
        else in_path.with_name(f"{in_path.stem}_pindrop_category_cumulative.csv")
    )

    df = pd.read_csv(in_path, low_memory=False)

    result = build_pindrop_category_df(
        df,
        event_type_col=args.event_type_col,
        pindrop_value=args.pindrop_value,
        coin_label_col=args.coin_label_col,
        time_col=args.time_col,
        chestpin_col=args.chestpin_col,
        category_col=args.category_col,
        category_final_col=args.category_final_col,
        sep=args.sep,
        strict=not args.non_strict,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(out_path, index=False)
    print(f"Wrote {len(result):,} rows to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
