# patch_dropqual_corrected.py
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Add corrected drop quality and log-transformed dropDist"
    )
    ap.add_argument("--input", required=True, help="Path to input CSV")
    ap.add_argument("--output", required=True, help="Path to output CSV")
    ap.add_argument(
        "--dropdist-col",
        default="dropDist",
        help="Column containing pin drop distance",
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

    required = [args.dropdist_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    x = pd.to_numeric(df[args.dropdist_col], errors="coerce")

    df["dropQual_corrected"] = pd.Series(pd.NA, index=df.index, dtype="string")
    df.loc[x.notna() & (x <= args.threshold), "dropQual_corrected"] = "good"
    df.loc[x.notna() & (x > args.threshold), "dropQual_corrected"] = "bad"

    df[args.log_col] = np.where(x > 0, np.log(x), np.nan)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print(f"Wrote: {out_path}")
    print("Added columns: "
          f"dropQual_corrected, {args.log_col}")


if __name__ == "__main__":
    main()