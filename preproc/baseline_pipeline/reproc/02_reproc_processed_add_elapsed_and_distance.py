#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import pandas as pd

## helpers_reproc
from RC_utilities.reProcHelpers.helpers_reproc import (
    augment_processed_with_intervals,
    compute_step_distance, 
    aggregate_total_distance,
    DEFAULT_MAX_TRUE_ROUNDNUM,
)

def main():
    ap = argparse.ArgumentParser(description="Augment processed with intervals + elapsed time + stepDist + totDistRound/Block.")
    ap.add_argument("--processed", required=True, help="Path to *_processed.csv")
    ap.add_argument("--blocks", required=True, help="Path to *_blockIntervals.csv")
    ap.add_argument("--rounds", required=True, help="Path to *_roundIntervals.csv")
    ap.add_argument("--out", required=True, help="Output *_prelim_reproc.csv")
    ap.add_argument("--max_round", type=int, default=DEFAULT_MAX_TRUE_ROUNDNUM)
    ap.add_argument("--pos_cols", nargs="+", default=["HeadPosAnchored_x","HeadPosAnchored_y","HeadPosAnchored_z"],
                    help="Position columns to use for distance (default HeadPosAnchored_x/y/z)")
    ap.add_argument("--group_for_diff", nargs="+", default=["BlockInstance","BlockNum"],
                    help="Grouping keys for dt/dpos diffs (default BlockInstance BlockNum)")
    args = ap.parse_args()

    proc = pd.read_csv(args.processed)
    blocks = pd.read_csv(args.blocks)
    rounds = pd.read_csv(args.rounds)

    # Add block/round starts/ends, elapsed, fractions, round assignment
    df = augment_processed_with_intervals(proc, blocks, rounds, max_round=args.max_round)

    # Compute dt + stepDist once
    df = compute_step_distance(
        df,
        pos_cols=args.pos_cols,
        time_col="AppTime",
        group_keys=args.group_for_diff,
        out_dt="dt",
        out_step="stepDist",
    )

    # totDistBlock: group by block keys
    df = aggregate_total_distance(
        df,
        group_keys=["BlockInstance","BlockNum"],
        step_col="stepDist",
        out_col="totDistBlock",
        how="sum",
    )

    # totDistRound: group by assigned true round (exclude >100 already handled by assignment)
    df = aggregate_total_distance(
        df,
        group_keys=["BlockInstance","BlockNum","RoundNum_interval_assigned"],
        step_col="stepDist",
        out_col="totDistRound",
        how="sum",
    )
    # Running (cumulative) distance within each block
    df["totDistBlock_current"] = (
        df.groupby(["BlockInstance", "BlockNum"], dropna=False)["stepDist"]
          .cumsum()
    )

    # Running (cumulative) distance within each round (true rounds only via assigned id)
    df["totDistRound_current"] = (
        df.groupby(["BlockInstance", "BlockNum", "RoundNum_interval_assigned"], dropna=False)["stepDist"]
          .cumsum()
    )
    df["inTrueRound"] = df["RoundNum_interval_assigned"].notna()

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)

if __name__ == "__main__":
    main()
