#!/usr/bin/env python3
"""
normalize_path_utility.py

Row-wise normalization within each start_pos:
    norm = (utility - floor_utility) / (ceiling_utility - floor_utility)

Where path type is identified by the 'order' column (e.g., HV->LV->NV).
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()

    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--root", type=Path, help="Directory containing input CSVs (used with --pattern).")
    src.add_argument("--inputs", type=Path, nargs="+", help="One or more input CSV files.")

    p.add_argument("--pattern", type=str, default="*.csv", help="Glob pattern used with --root (default: *.csv).")
    p.add_argument("--output", type=Path, required=True, help="Output directory for updated CSVs.")
    p.add_argument("--suffix", type=str, default="_normUtil", help="Suffix appended to output filename stem.")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs if present.")

    p.add_argument("--utility-col", type=str, default="utility", help="Utility column name (default: utility).")
    p.add_argument("--startpos-col", type=str, default="startPos", help="Start position column (default: start_pos).")
    p.add_argument("--order-col", type=str, default="path_order_round", help="Path type column (default: order).")

    p.add_argument(
        "--require-order-col",
        action="store_true",
        help="If set, error when order column is missing (recommended).",
    )

    p.add_argument(
        "--write-summary",
        action="store_true",
        help="If set, also write a per-(start_pos, order) summary CSV (mean/min/max/count).",
    )
    p.add_argument(
        "--summary-suffix",
        type=str,
        default="_normUtil_summary",
        help="Suffix for the summary CSV (default: _normUtil_summary).",
    )

    return p.parse_args()


def find_inputs(root: Path, pattern: str) -> list[Path]:
    if not root.exists():
        raise FileNotFoundError(f"--root does not exist: {root}")
    return sorted([p for p in root.glob(pattern) if p.is_file()])


def validate_columns(
    df: pd.DataFrame,
    src: Path,
    startpos_col: str,
    utility_col: str,
    order_col: str,
    require_order: bool,
) -> None:
    missing = [c for c in [startpos_col, utility_col] if c not in df.columns]
    if missing:
        raise ValueError(f"{src.name}: missing required column(s): {missing}. Found: {list(df.columns)}")
    if require_order and order_col not in df.columns:
        raise ValueError(f"{src.name}: missing required path-type column '{order_col}'.")


def normalize_df(df: pd.DataFrame, startpos_col: str, utility_col: str, coin_set_value: str) -> pd.DataFrame:
    out = df.copy()

    # Ensure numeric utility (bad parses -> NaN)
    out[utility_col] = pd.to_numeric(out[utility_col], errors="coerce")

    floor_u = out.groupby(startpos_col)[utility_col].transform("min")
    ceil_u = out.groupby(startpos_col)[utility_col].transform("max")
    denom = (ceil_u - floor_u)

    norm = np.where(
        denom.to_numpy() == 0,
        np.nan,  # choose np.nan (or 0.0) when ceiling==floor
        (out[utility_col].to_numpy() - floor_u.to_numpy()) / denom.to_numpy(),
    )

    out["floor_utility"] = floor_u
    out["ceiling_utility"] = ceil_u
    out["normalized_utility"] = norm
    return out


def write_outputs(
    src: Path,
    df_norm: pd.DataFrame,
    out_dir: Path,
    suffix: str,
    overwrite: bool,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{src.stem}{suffix}{src.suffix}"
    if out_path.exists() and not overwrite:
        raise FileExistsError(f"Output exists (use --overwrite): {out_path}")
    df_norm.to_csv(out_path, index=False)
    return out_path


def write_summary(
    src: Path,
    df_norm: pd.DataFrame,
    out_dir: Path,
    startpos_col: str,
    order_col: str,
    overwrite: bool,
    summary_suffix: str,
    coin_set_value: str,
) -> Path:
    if order_col not in df_norm.columns:
        raise ValueError(f"{src.name}: cannot write summary; missing '{order_col}' column.")

    summary = (
        df_norm.groupby([startpos_col, order_col], dropna=False)["normalized_utility"]
        .agg(["count", "mean", "min", "max"])
        .reset_index()
        .rename(columns={"count": "n"})
    )
    summary["coinSet"] = coin_set_value
    out_path = out_dir / f"{src.stem}{summary_suffix}{src.suffix}"
    if out_path.exists() and not overwrite:
        raise FileExistsError(f"Summary exists (use --overwrite): {out_path}")
    summary.to_csv(out_path, index=False)
    return out_path


def main() -> int:
    args = parse_args()

    if args.root is not None:
        inputs = find_inputs(args.root, args.pattern)
    else:
        inputs = [p for p in args.inputs if p.is_file()]

    if not inputs:
        print("No input files found.", file=sys.stderr)
        return 2

    wrote_any = False
    for src in inputs:
        try:
            df = pd.read_csv(src)
            
            src_path = Path(src)
            coin_set_value = src_path.stem.split("all_orders__layout_", 1)[1]
            coin_set_value = coin_set_value.split("_")[0]
            print('path:', src_path)
            print('coin_set_value:', coin_set_value)
            df["coinSet"] = coin_set_value
            validate_columns(
                df=df,
                src=src,
                startpos_col=args.startpos_col,
                utility_col=args.utility_col,
                order_col=args.order_col,
                require_order=args.require_order_col,
            )

            df_norm = normalize_df(df, args.startpos_col, args.utility_col, coin_set_value)
            out_path = write_outputs(src, df_norm, args.output, args.suffix, args.overwrite)
            print(f"✅ Wrote: {out_path}")
            wrote_any = True

            if args.write_summary:
                sum_path = write_summary(
                    src=src,
                    df_norm=df_norm,
                    out_dir=args.output,
                    startpos_col=args.startpos_col,
                    order_col=args.order_col,
                    overwrite=args.overoverwrite if False else args.overwrite,  # keeps mypy quiet
                    summary_suffix=args.summary_suffix,
                    coin_set_value=coin_set_value,
                )
                print(f"📌 Summary: {sum_path}")

        except Exception as e:
            print(f"❌ Failed on {src}: {e}", file=sys.stderr)

    return 0 if wrote_any else 1


if __name__ == "__main__":
    raise SystemExit(main())
