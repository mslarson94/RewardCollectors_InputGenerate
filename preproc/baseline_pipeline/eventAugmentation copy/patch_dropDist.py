# file: patch_dropqual_corrected.py
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

'''
path_order_round,   path_order_round_num,   orderedCollect,     pathValue
HV->LV->NV,         1,                      Ordered,            30 points
LV->HV->NV,         2,                      Unordered,          30 points
NV->HV->LV,         3,                      Unordered,          25 points
HV->NV->LV,         4,                      Ordered,            25 points
NV->LV->HV,         5,                      Unordered,          20 points
LV->NV->HV,         6,                      Ordered,            20 points
'''
ORDERED_COLLECT_BY_ROUND = {
    1: 1,
    2: 0,
    3: 0,
    4: 1,
    5: 0,
    6: 1,
}

PATH_VALUE_BY_ROUND = {
    1: 30,
    2: 30,
    3: 25,
    4: 25,
    5: 20,
    6: 20,
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Add log-transformed dropDist, orderedCollect, "
            "and pathValue columns."
        )
    )
    ap.add_argument("--input", required=True, help="Path to input CSV")
    ap.add_argument("--output", required=True, help="Path to output CSV")
    ap.add_argument(
        "--dropdist-col",
        default="dropDist",
        help="Column containing pin drop distance",
    )
    ap.add_argument(
        "--path-order-num-col",
        default="path_order_round_num",
        help="Column containing path order round number",
    )
    ap.add_argument(
        "--threshold",
        type=float,
        default=1.1,
        help="Good/bad cutoff for dropDist",
    )
    ap.add_argument(
        "--log-col",
        default="ln_dropDist",
        help="Name of output natural log-transformed column",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)

    df = pd.read_csv(in_path)

    required = [
        args.dropdist_col,
        args.path_order_num_col,
    ]
    missing = [column for column in required if column not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    drop_dist = pd.to_numeric(
        df[args.dropdist_col],
        errors="coerce",
    )

    path_order_num = pd.to_numeric(
        df[args.path_order_num_col],
        errors="coerce",
    )

    df[args.log_col] = np.where(
        drop_dist > 0,
        np.log(drop_dist),
        np.nan,
    )

    df["orderedCollect"] = path_order_num.map(
        ORDERED_COLLECT_BY_ROUND
    ).astype("Int64")

    df["pathValue"] = path_order_num.map(
        PATH_VALUE_BY_ROUND
    ).astype("Int64")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print(f"Wrote: {out_path}")
    print(
        "Added columns: "
        f"{args.log_col}, orderedCollect, pathValue"
    )


if __name__ == "__main__":
    main()