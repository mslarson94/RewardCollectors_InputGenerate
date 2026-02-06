#!/usr/bin/env python3
"""
assign_norm_util_and_efficiency.py (amended v2)

Eligibility:
  BlockType == 'pindropping' AND CoinSetID != 4

Merge keys:
  main.coinSet          <-> ref.coinSet
  main.startPos         <-> ref.start_pos
  main.path_order_round <-> ref.order

Main must include:
  BlockType, CoinSetID, coinSet, startPos, path_order_round,
  totalDistRound, adj_totalDistRound

Reference must include:
  coinSet, start_pos, order,
  normalized_utility, floor_utility, ceiling_utility,
  distance (ideal)
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import numpy as np
import pandas as pd


MAIN_REQUIRED = [
    "BlockType", "CoinSetID",
    "coinSet", "startPos", "path_order_round",
    "totalDistRound", "adj_totalDistRound",
]

REF_REQUIRED = [
    "coinSet", "start_pos", "order",
    "normalized_utility", "floor_utility", "ceiling_utility",
    "distance",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()

    p.add_argument("--main", type=Path, required=True, help="Main CSV to update.")
    p.add_argument("--ref-dir", type=Path, required=True, help="Directory containing normalized reference CSVs.")
    p.add_argument("--ref-pattern", type=str, default="*.csv", help="Glob for ref CSVs (default: *.csv).")

    p.add_argument("--output", type=Path, required=True, help="Output CSV path OR output directory (see --out-mode).")
    p.add_argument("--out-mode", choices=["file", "dir"], default="file",
                   help="If 'file', --output is full output CSV path. If 'dir', writes into dir with suffix.")
    p.add_argument("--suffix", type=str, default="_withNormUtilAndEff", help="Used when --out-mode=dir.")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing output file if present.")

    p.add_argument("--ref-dedup", action="store_true",
                   help="Drop duplicate ref rows on (coinSet, start_pos, order) keeping first.")

    return p.parse_args()


def require_cols(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{label}: missing required column(s): {missing}")


def safe_divide(num: pd.Series, den: pd.Series) -> np.ndarray:
    num_arr = pd.to_numeric(num, errors="coerce").to_numpy()
    den_arr = pd.to_numeric(den, errors="coerce").to_numpy()
    out = np.full_like(num_arr, np.nan, dtype=float)
    ok = (den_arr != 0) & ~np.isnan(den_arr) & ~np.isnan(num_arr)
    out[ok] = num_arr[ok] / den_arr[ok]
    return out


def load_refs(ref_dir: Path, pattern: str, dedup: bool) -> pd.DataFrame:
    if not ref_dir.exists():
        raise FileNotFoundError(f"--ref-dir does not exist: {ref_dir}")

    paths = sorted([p for p in ref_dir.glob(pattern) if p.is_file()])
    if not paths:
        raise FileNotFoundError(f"No reference files found in {ref_dir} with pattern '{pattern}'")

    frames = []
    for p in paths:
        r = pd.read_csv(p)
        require_cols(r, REF_REQUIRED, label=f"REF {p.name}")
        r = r.copy()
        r["util_file"] = p.name  # audit trail
        frames.append(r)

    ref = pd.concat(frames, ignore_index=True)

    if dedup:
        ref = ref.drop_duplicates(subset=["coinSet", "start_pos", "order"], keep="first")

    return ref


def resolve_output_path(main_path: Path, output: Path, out_mode: str, suffix: str) -> Path:
    if out_mode == "file":
        return output
    output.mkdir(parents=True, exist_ok=True)
    return output / f"{main_path.stem}{suffix}{main_path.suffix}"


def main() -> int:
    args = parse_args()

    if not args.main.exists():
        print(f"Main file not found: {args.main}", file=sys.stderr)
        return 2

    df = pd.read_csv(args.main)
    require_cols(df, MAIN_REQUIRED, label=f"MAIN {args.main.name}")

    ref = load_refs(args.ref_dir, args.ref_pattern, args.ref_dedup)

    # Eligibility mask
    eligible = (df["BlockType"] == "pindropping") & (pd.to_numeric(df["CoinSetID"], errors="coerce") != 4)

    # Prepare merge keys
    df_keys = df.copy()
    df_keys["_coinSet"] = df_keys["coinSet"]
    df_keys["_start_pos"] = df_keys["startPos"]
    df_keys["_order"] = df_keys["path_order_round"]

    ref_keys = ref.copy().rename(columns={
        "coinSet": "_coinSet",
        "start_pos": "_start_pos",
        "order": "_order",
        "distance": "ideal_distance",
    })

    ref_keep = [
        "_coinSet", "_start_pos", "_order",
        "normalized_utility", "floor_utility", "ceiling_utility",
        "ideal_distance", "util_file",
    ]
    ref_keys = ref_keys[ref_keep]

    merged = df_keys.merge(
        ref_keys,
        on=["_coinSet", "_start_pos", "_order"],
        how="left",
        validate="m:1",
    )

    # Match detection: if ideal_distance is present, we consider this a successful match
    has_match = merged["ideal_distance"].notna()

    # Create match status for all rows
    merged["util_matchStatus"] = "not_eligible"
    merged.loc[eligible & ~has_match, "util_matchStatus"] = "no_ref_match"
    merged.loc[eligible & has_match, "util_matchStatus"] = "ok"

    # Compute efficiencies using precomputed distances in main
    eff_raw = safe_divide(merged["ideal_distance"], merged["totalDistRound"])
    eff_adj = safe_divide(merged["ideal_distance"], merged["adj_totalDistRound"])

    # Initialize outputs as NaN, then fill only for eligible+matched rows
    out_cols = [
        "norm_path_utility_round",
        "ref_floor_utility",
        "ref_ceiling_utility",
        "ideal_distance",
        "util_file",
        "path_eff_raw",
        "path_eff_adj",
    ]
    for c in out_cols:
        merged[c] = np.nan

    ok = eligible & has_match
    merged.loc[ok, "norm_path_utility_round"] = merged.loc[ok, "normalized_utility"]
    merged.loc[ok, "ref_floor_utility"] = merged.loc[ok, "floor_utility"]
    merged.loc[ok, "ref_ceiling_utility"] = merged.loc[ok, "ceiling_utility"]
    merged.loc[ok, "ideal_distance"] = merged.loc[ok, "ideal_distance"]
    merged.loc[ok, "util_file"] = merged.loc[ok, "util_file"]
    merged.loc[ok, "path_eff_raw"] = eff_raw[ok.to_numpy()]
    merged.loc[ok, "path_eff_adj"] = eff_adj[ok.to_numpy()]

    # Drop internal keys + raw ref columns
    drop_cols = [
        "_coinSet", "_start_pos", "_order",
        "normalized_utility", "floor_utility", "ceiling_utility",
    ]
    merged = merged.drop(columns=[c for c in drop_cols if c in merged.columns])

    out_path = resolve_output_path(args.main, args.output, args.out_mode, args.suffix)
    if out_path.exists() and not args.overwrite:
        print(f"Output exists (use --overwrite): {out_path}", file=sys.stderr)
        return 3

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)
    print(f"✅ Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
