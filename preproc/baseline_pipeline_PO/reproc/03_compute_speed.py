#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import pandas as pd

## helpers_reproc
from RC_utilities.reProcHelpers.helpers_reproc import compute_speed

def main():
    ap = argparse.ArgumentParser(description="Compute currSpeed from stepDist/dt.")
    ap.add_argument("--in_csv", required=True, help="Path to *_prelim_reproc.csv")
    ap.add_argument("--out_csv", required=True, help="Output *_reprocessed.csv")
    ap.add_argument("--step_col", default="stepDist")
    ap.add_argument("--dt_col", default="dt")
    ap.add_argument("--out_col", default="currSpeed")
    args = ap.parse_args()

    df = pd.read_csv(args.in_csv)
    df = compute_speed(df, step_col=args.step_col, dt_col=args.dt_col, out_col=args.out_col)

    Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out_csv, index=False)

if __name__ == "__main__":
    main()
