#!/usr/bin/env python3
"""
assign_norm_util_and_efficiency.py

Goal:
  For interval rows (one row per round), assign:
    - norm_path_utility_round
    - ref_floor_utility
    - ref_ceiling_utility
    - ideal_distance
    - util_file (which ref row / file contributed)
    - path_eff_raw = ideal_distance / totDistRound

Eligibility:
  BlockType == 'pindropping' AND CoinSetID != 4

Merge keys (normalized):
  main.coinSet          <-> ref.coinSet
  main.startPos         <-> ref.startPos
  main.path_order_round <-> ref.path_order_round

IMPORTANT:
  We normalize path_order_round to remove spacing differences, e.g.
    "HV -> NV -> LV" -> "HV->NV->LV"
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
    "totDistRound",
]

REF_REQUIRED = [
    "coinSet", "startPos", "path_order_round",
    "normalized_utility", "floor_utility", "ceiling_utility",
    "distance",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()

    main_grp = p.add_mutually_exclusive_group(required=True)
    main_grp.add_argument("--main", type=Path, help="Single main interval CSV to update.")
    main_grp.add_argument("--main-dir", type=Path, help="Directory of main interval CSVs to update.")

    p.add_argument(
        "--main-pattern",
        type=str,
        default="*.csv",
        help="Glob for main CSVs when using --main-dir (default: *.csv).",
    )

    p.add_argument("--ref-dir", type=Path, required=True, help="Directory containing normalized reference CSVs.")
    p.add_argument("--ref-pattern", type=str, default="*.csv", help="Glob for ref CSVs (default: *.csv).")

    p.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output CSV path OR output directory (see --out-mode).",
    )
    p.add_argument(
        "--out-mode",
        choices=["file", "dir"],
        default="file",
        help="If 'file', --output is a single output CSV path (only valid with --main). "
             "If 'dir', writes one output per main file into --output directory.",
    )
    p.add_argument("--suffix", type=str, default="_withNormUtilAndEff", help="Used when --out-mode=dir.")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing output file(s) if present.")

    p.add_argument(
        "--ref-dedup",
        action="store_true",
        help="Drop duplicate ref rows on (coinSet, startPos, path_order_round) keeping first.",
    )

    p.add_argument("--quiet", action="store_true", help="Reduce logging.")
    p.add_argument("--fail-fast", action="store_true", help="Stop immediately if any main file fails.")

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


def _norm_str(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def _norm_order(s: pd.Series) -> pd.Series:
    """
    Canonicalize order strings so spacing won't break merges.
      "HV -> NV -> LV" -> "HV->NV->LV"
      " HV-> NV->LV "  -> "HV->NV->LV"
    """
    s = s.astype("string")
    s = s.str.replace(r"\s*->\s*", "->", regex=True)
    s = s.str.replace(r"\s+", "", regex=True)
    return s.str.strip()


def load_refs(ref_dir: Path, pattern: str, dedup: bool, quiet: bool) -> pd.DataFrame:
    if not ref_dir.exists():
        raise FileNotFoundError(f"--ref-dir does not exist: {ref_dir}")

    paths = sorted([p for p in ref_dir.glob(pattern) if p.is_file()])
    if not paths:
        raise FileNotFoundError(f"No reference files found in {ref_dir} with pattern '{pattern}'")

    frames: list[pd.DataFrame] = []
    for p in paths:
        if not quiet:
            print(f"[ref] {p.name}")
        r = pd.read_csv(p)
        require_cols(r, REF_REQUIRED, label=f"REF {p.name}")
        r = r.copy()
        r["util_file"] = p.name  # audit trail
        frames.append(r)

    ref = pd.concat(frames, ignore_index=True)

    if dedup:
        ref = ref.drop_duplicates(subset=["coinSet", "startPos", "path_order_round"], keep="first")

    return ref


def resolve_output_path(main_path: Path, output: Path, out_mode: str, suffix: str) -> Path:
    if out_mode == "file":
        return output
    output.mkdir(parents=True, exist_ok=True)
    return output / f"{main_path.stem}{suffix}{main_path.suffix}"


def process_one_main(
    main_path: Path,
    *,
    ref: pd.DataFrame,
    output: Path,
    out_mode: str,
    suffix: str,
    overwrite: bool,
    quiet: bool,
) -> Path:
    if not main_path.exists():
        raise FileNotFoundError(f"Main file not found: {main_path}")

    df = pd.read_csv(main_path)
    require_cols(df, MAIN_REQUIRED, label=f"MAIN {main_path.name}")

    # Eligibility mask (interval-level)
    # eligible = (df["BlockType"].astype("string").str.strip().str.lower() == "pindropping") & (
    #     pd.to_numeric(df["CoinSetID"], errors="coerce") != 4
    # )
    eligible = (
        pd.to_numeric(df["CoinSetID"], errors="coerce") != 4
    )

    # --- Normalize merge keys on BOTH sides ---
    df_keys = df.copy()
    df_keys["_coinSet"] = _norm_str(df_keys["coinSet"])
    df_keys["_startPos"] = _norm_str(df_keys["startPos"])
    df_keys["_path_order_round"] = _norm_order(df_keys["path_order_round"])

    ref_keys = ref.copy()
    ref_keys["_coinSet"] = _norm_str(ref_keys["coinSet"])
    ref_keys["_startPos"] = _norm_str(ref_keys["startPos"])
    ref_keys["_path_order_round"] = _norm_order(ref_keys["path_order_round"])
    ref_keys["ideal_distance"] = pd.to_numeric(ref_keys["distance"], errors="coerce")

    ref_keep = [
        "_coinSet", "_startPos", "_path_order_round",
        "normalized_utility", "floor_utility", "ceiling_utility",
        "ideal_distance", "util_file",
    ]
    ref_keys = ref_keys[ref_keep]

    merged = df_keys.merge(
        ref_keys,
        on=["_coinSet", "_startPos", "_path_order_round"],
        how="left",
        validate="m:1",
    )

    has_match = merged["ideal_distance"].notna()

    merged["util_matchStatus"] = "not_eligible"
    merged.loc[eligible & ~has_match, "util_matchStatus"] = "no_ref_match"
    merged.loc[eligible & has_match, "util_matchStatus"] = "ok"

    # Efficiency: ideal_distance / observed totDistRound
    eff_raw = safe_divide(merged["ideal_distance"], merged["totDistRound"])

    out_cols = [
        "norm_path_utility_round",
        "ref_floor_utility",
        "ref_ceiling_utility",
        "ideal_distance",
        "util_file",
        "path_eff_raw",
    ]
    for c in out_cols:
        if c not in merged.columns:
            merged[c] = np.nan

    ok = eligible & has_match
    merged.loc[ok, "norm_path_utility_round"] = merged.loc[ok, "normalized_utility"]
    merged.loc[ok, "ref_floor_utility"] = merged.loc[ok, "floor_utility"]
    merged.loc[ok, "ref_ceiling_utility"] = merged.loc[ok, "ceiling_utility"]
    merged.loc[ok, "path_eff_raw"] = eff_raw[ok.to_numpy()]

    if not quiet:
        print(f"\n=== {main_path.name} DEBUG ===")
        print(f"rows: {len(merged):,}")
        print(f"eligible rows: {int(eligible.sum()):,}")
        print(f"eligible matched: {int((eligible & has_match).sum()):,}")
        print("matchStatus counts:")
        print(merged["util_matchStatus"].value_counts(dropna=False))
        print("=== end DEBUG ===\n")

    drop_cols = [
        "_coinSet", "_startPos", "_path_order_round",
        "normalized_utility", "floor_utility", "ceiling_utility",
    ]
    merged = merged.drop(columns=[c for c in drop_cols if c in merged.columns])

    out_path = resolve_output_path(main_path, output, out_mode, suffix)
    if out_path.exists() and not overwrite:
        raise FileExistsError(f"Output exists (use --overwrite): {out_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)
    return out_path


def iter_main_files(main: Path | None, main_dir: Path | None, pattern: str) -> list[Path]:
    if main is not None:
        return [main]

    assert main_dir is not None
    if not main_dir.exists():
        raise FileNotFoundError(f"--main-dir does not exist: {main_dir}")

    paths = sorted([p for p in main_dir.glob(pattern) if p.is_file()])
    if not paths:
        raise FileNotFoundError(f"No main files found in {main_dir} with pattern '{pattern}'")
    return paths


def main() -> int:
    args = parse_args()

    # Guard: out-mode=file only makes sense for a single input
    if args.out_mode == "file" and args.main_dir is not None:
        print("Error: --out-mode=file cannot be used with --main-dir. Use --out-mode=dir.", file=sys.stderr)
        return 2

    ref = load_refs(args.ref_dir, args.ref_pattern, args.ref_dedup, args.quiet)

    mains = iter_main_files(args.main, args.main_dir, args.main_pattern)

    failures: list[tuple[Path, str]] = []
    wrote: list[Path] = []

    for mp in mains:
        try:
            if not args.quiet:
                print(f"[main] {mp}")
            out_path = process_one_main(
                mp,
                ref=ref,
                output=args.output,
                out_mode=args.out_mode,
                suffix=args.suffix,
                overwrite=args.overwrite,
                quiet=args.quiet,
            )
            wrote.append(out_path)
            print(f"✅ Wrote: {out_path}")
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            failures.append((mp, msg))
            print(f"❌ Failed: {mp} :: {msg}", file=sys.stderr)
            if args.fail_fast:
                return 1

    if failures:
        print("\nSome files failed:", file=sys.stderr)
        for mp, msg in failures:
            print(f" - {mp.name}: {msg}", file=sys.stderr)
        return 1

    if not args.quiet:
        print(f"\nDone. Wrote {len(wrote)} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
