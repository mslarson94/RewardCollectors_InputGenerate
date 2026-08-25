# add_swapvote_registered_only.py

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


ROUND_KEYS = ["BlockNum", "RoundNum", "BlockInstance"]
SWAP_VOTE_COL = "SwapVote"

OUT_SWAP_VOTE_REGISTERED_COL = "swapVoteRegistered"
OUT_SWAP_VOTE_REGISTERED_N_COL = "swapVoteRegistered_n"
OUT_SWAP_VOTE_REGISTERED_D_COL = "swapVoteRegistered_d"


def require_cols(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{label}: missing required column(s): {missing}")


def norm_key_series(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip()
    num = pd.to_numeric(x, errors="coerce")
    is_num = num.notna()
    out = x.copy()
    out[is_num] = num[is_num].round(0).astype("Int64").astype("string")
    return out


def add_norm_keys(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    out = df.copy()
    for k in keys:
        out[f"_{k}"] = norm_key_series(out[k])
    return out


def normalize_text_series(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def is_actual_swap_vote_series(s: pd.Series) -> pd.Series:
    x = normalize_text_series(s)
    return (
        x.notna()
        & (x != "")
        & (x.str.casefold() != "correct")
        & (x.str.casefold() != "incorrect")
    )


def build_swap_vote_registered_table(interval_df: pd.DataFrame, *, nkeys: list[str]) -> pd.DataFrame:
    sub = interval_df[nkeys + [SWAP_VOTE_COL]].copy()
    sub["_is_actual_swap_vote"] = is_actual_swap_vote_series(sub[SWAP_VOTE_COL])

    by_round = (
        sub.groupby(nkeys, dropna=False)["_is_actual_swap_vote"]
        .any()
        .reset_index(name="_round_has_actual_swap_vote")
    )

    numerator = int(by_round["_round_has_actual_swap_vote"].sum())
    denominator = int(len(by_round))
    ratio = (numerator / denominator) if denominator else pd.NA

    return pd.DataFrame(
        {
            OUT_SWAP_VOTE_REGISTERED_N_COL: [numerator],
            OUT_SWAP_VOTE_REGISTERED_D_COL: [denominator],
            OUT_SWAP_VOTE_REGISTERED_COL: [ratio],
        }
    )


def process_one(interval_path: Path, out_path: Path, *, fill_only: bool, quiet: bool) -> None:
    interval_df = pd.read_csv(interval_path)

    require_cols(
        interval_df,
        ROUND_KEYS + [SWAP_VOTE_COL],
        label=f"INTERVAL {interval_path.name}",
    )

    interval_df = add_norm_keys(interval_df, ROUND_KEYS)
    nkeys = [f"_{k}" for k in ROUND_KEYS]

    swap_vote_tbl = build_swap_vote_registered_table(interval_df, nkeys=nkeys)

    for col in [
        OUT_SWAP_VOTE_REGISTERED_N_COL,
        OUT_SWAP_VOTE_REGISTERED_D_COL,
        OUT_SWAP_VOTE_REGISTERED_COL,
    ]:
        if col not in interval_df.columns:
            interval_df[col] = pd.NA

    for col in [
        OUT_SWAP_VOTE_REGISTERED_N_COL,
        OUT_SWAP_VOTE_REGISTERED_D_COL,
        OUT_SWAP_VOTE_REGISTERED_COL,
    ]:
        value = swap_vote_tbl.iloc[0][col]
        if fill_only:
            mask = interval_df[col].isna()
            interval_df.loc[mask, col] = value
        else:
            interval_df[col] = value

    interval_df = interval_df.drop(columns=nkeys, errors="ignore")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    interval_df.to_csv(out_path, index=False)

    if not quiet:
        print(f"swapVoteRegistered_n: {int(swap_vote_tbl.iloc[0][OUT_SWAP_VOTE_REGISTERED_N_COL])}")
        print(f"swapVoteRegistered_d: {int(swap_vote_tbl.iloc[0][OUT_SWAP_VOTE_REGISTERED_D_COL])}")
        print(f"swapVoteRegistered:   {swap_vote_tbl.iloc[0][OUT_SWAP_VOTE_REGISTERED_COL]}")
        print(f"✅ Wrote: {out_path}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Add only swapVoteRegistered metrics to interval CSV(s).")

    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--interval", type=Path, help="Single interval CSV.")
    mode.add_argument("--interval-dir", type=Path, help="Directory containing interval CSVs.")

    p.add_argument("--output", type=Path, help="Single output CSV path.")
    p.add_argument("--outdir", type=Path, help="Output directory for batch mode.")

    p.add_argument("--interval-pattern", type=str, default="*.csv")
    p.add_argument("--out-suffix", type=str, default="_swapVoteRegistered.csv")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--fill-only", action="store_true")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--fail-fast", action="store_true")

    return p.parse_args()


def main() -> int:
    args = parse_args()

    if args.interval is not None:
        if args.output is None:
            print("Single-file mode requires --output.", file=sys.stderr)
            return 2
        if not args.interval.exists():
            print(f"Interval not found: {args.interval}", file=sys.stderr)
            return 2
        if args.output.exists() and not args.overwrite:
            print(f"Output exists (use --overwrite): {args.output}", file=sys.stderr)
            return 3

        process_one(
            args.interval,
            args.output,
            fill_only=args.fill_only,
            quiet=args.quiet,
        )
        return 0

    assert args.interval_dir is not None
    if args.outdir is None:
        print("Dir mode requires --outdir.", file=sys.stderr)
        return 2
    if not args.interval_dir.exists():
        print(f"Interval dir not found: {args.interval_dir}", file=sys.stderr)
        return 2

    intervals = sorted([p for p in args.interval_dir.glob(args.interval_pattern) if p.is_file()])
    if not intervals:
        print(f"No interval files matched {args.interval_pattern!r} in {args.interval_dir}", file=sys.stderr)
        return 2

    failures = 0
    wrote = 0

    for interval_path in intervals:
        suffix_tail = args.interval_pattern.split("*", 1)[1] if "*" in args.interval_pattern else ""
        base = interval_path.name.removesuffix(suffix_tail) if suffix_tail else interval_path.stem
        out_path = args.outdir / f"{base}{args.out_suffix}"

        if out_path.exists() and not args.overwrite:
            failures += 1
            print(f"❌ Output exists (use --overwrite): {out_path}", file=sys.stderr)
            if args.fail_fast:
                return 1
            continue

        try:
            process_one(
                interval_path,
                out_path,
                fill_only=args.fill_only,
                quiet=args.quiet,
            )
            wrote += 1
        except Exception as e:
            failures += 1
            print(f"❌ Failed {interval_path.name}: {type(e).__name__}: {e}", file=sys.stderr)
            if args.fail_fast:
                return 1

    if not args.quiet:
        print(f"Done. Wrote {wrote} file(s). Failures: {failures}.")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())