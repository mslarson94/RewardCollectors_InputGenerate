#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import pandas as pd

## helpers_reproc
from RC_utilities.reProcHelpers.helpers_reproc import (
    build_block_intervals, 
    build_round_intervals, 
    merge_block_and_round_intervals,
    DEFAULT_MAX_TRUE_ROUNDNUM, 
    cleanup_merge_suffixes,
)

def main():
    ap = argparse.ArgumentParser(description="Build block + round interval tables from events (AppTime).")
    ap.add_argument("--events", required=True, help="Path to last viable *_events*.csv")
    ap.add_argument("--processed", default=None, help="Optional *_processed.csv (used to fill missing BlockEnd)")
    ap.add_argument("--out_blocks", required=True, help="Output blocks interval CSV")
    ap.add_argument("--out_rounds", required=True, help="Output rounds interval CSV")
    ap.add_argument("--out_combined", required=True, help="Output combined prelim blk+round interval CSV")
    ap.add_argument("--round_mode", default="truecontent", choices=["truecontent", "roundstartend"],
                    help="How to define rounds (default truecontent)")
    ap.add_argument("--max_round", type=int, default=DEFAULT_MAX_TRUE_ROUNDNUM, help="Max 'true' RoundNum (default 100)")
    args = ap.parse_args()

    events = pd.read_csv(args.events)
    processed = pd.read_csv(args.processed) if args.processed else None

    blocks = build_block_intervals(events, processed_df=processed)
    rounds = build_round_intervals(events, blocks, mode=args.round_mode, max_round=args.max_round)
    combined = merge_block_and_round_intervals(blocks, rounds)
    combined = cleanup_merge_suffixes(combined, suffixes=("_r",), numeric_tol=0.0)
    blocks   = cleanup_merge_suffixes(blocks, suffixes=("_r",), numeric_tol=0.0)
    rounds   = cleanup_merge_suffixes(rounds, suffixes=("_r",), numeric_tol=0.0)
    Path(args.out_blocks).parent.mkdir(parents=True, exist_ok=True)
    blocks.to_csv(args.out_blocks, index=False)
    rounds.to_csv(args.out_rounds, index=False)
    combined.to_csv(args.out_combined, index=False)

if __name__ == "__main__":
    main()
