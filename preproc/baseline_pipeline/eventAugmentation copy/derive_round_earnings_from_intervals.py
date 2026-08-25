#!/usr/bin/env python3
"""
derive_round_earnings_from_intervals.py (batchable)

From an interval file that already contains:
  - currGrandTotal
  - runningBlockTotal

derive and attach round-level columns, propagated to every row in the round:

  roundEarnings   = runningBlockTotal from chestPin_num == 3
  roundGrandTotal = currGrandTotal from chestPin_num == 1 + roundEarnings

Also derive and attach a file/session-level swap vote registration metric,
propagated to every row in the file:

  swapVoteRegistered
    = (# unique rounds with an actual swap vote)
      / (# unique rounds)

An "actual swap vote" is any SwapVote value that is not blank, not NA,
not "Correct", and not "Incorrect".

Join keys (normalized string join):
  (BlockNum, RoundNum, BlockInstance)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
import pandas as pd


ROUND_KEYS = ["BlockNum", "RoundNum", "BlockInstance"]
CHEST_COL = "chestPin_num"

CHEST1_SRC_COL = "currGrandTotal"
CHEST3_SRC_COL = "runningBlockTotal"
SWAP_VOTE_COL = "SwapVote"

OUT_ROUND_EARNINGS_COL = "roundEarnings"
OUT_ROUND_GRANDTOTAL_COL = "roundGrandTotal"
OUT_CHEST1_COL = "chest1_currGrandTotal"
OUT_CHEST3_COL = "chest3_runningBlockTotal"

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


def last_nonnull(s: pd.Series):
    s2 = s.dropna()
    return s2.iloc[-1] if not s2.empty else pd.NA


def is_actual_swap_vote_series(s: pd.Series) -> pd.Series:
    x = normalize_text_series(s)
    return (
        x.notna()
        & (x != "")
        & (x.str.casefold() != "correct")
        & (x.str.casefold() != "incorrect")
    )


def build_round_table(interval_df: pd.DataFrame, *, nkeys: list[str]) -> pd.DataFrame:
    needed = nkeys + [CHEST_COL, CHEST1_SRC_COL, CHEST3_SRC_COL]
    sub = interval_df[needed].copy()

    sub["_chestPin_num_norm"] = norm_key_series(sub[CHEST_COL])

    chest1 = sub[sub["_chestPin_num_norm"] == "1"].copy()
    chest1 = (
        chest1.groupby(nkeys, dropna=False)[CHEST1_SRC_COL]
        .agg(last_nonnull)
        .reset_index()
        .rename(columns={CHEST1_SRC_COL: OUT_CHEST1_COL})
    )

    chest3 = sub[sub["_chestPin_num_norm"] == "3"].copy()
    chest3 = (
        chest3.groupby(nkeys, dropna=False)[CHEST3_SRC_COL]
        .agg(last_nonnull)
        .reset_index()
        .rename(columns={CHEST3_SRC_COL: OUT_CHEST3_COL})
    )

    round_tbl = chest1.merge(chest3, on=nkeys, how="outer", validate="1:1")

    c1 = pd.to_numeric(round_tbl[OUT_CHEST1_COL], errors="coerce")
    c3 = pd.to_numeric(round_tbl[OUT_CHEST3_COL], errors="coerce")

    round_tbl[OUT_ROUND_EARNINGS_COL] = c3
    round_tbl[OUT_ROUND_GRANDTOTAL_COL] = c1 + c3

    return round_tbl


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


def parse_star_key(filename: str, pattern: str) -> str | None:
    if pattern.count("*") != 1:
        return None
    pre, post = pattern.split("*")
    if not filename.startswith(pre) or not filename.endswith(post):
        return None
    return filename[len(pre):(len(filename) - len(post) if len(post) else len(filename))]


def process_one(
    interval_path: Path,
    out_path: Path,
    *,
    fill_only: bool,
    quiet: bool,
    debug_keys: bool,
) -> None:
    interval_df = pd.read_csv(interval_path)

    require_cols(
        interval_df,
        ROUND_KEYS + [CHEST_COL, CHEST1_SRC_COL, CHEST3_SRC_COL, SWAP_VOTE_COL],
        label=f"INTERVAL {interval_path.name}",
    )

    interval_df = add_norm_keys(interval_df, ROUND_KEYS)
    nkeys = [f"_{k}" for k in ROUND_KEYS]

    round_tbl = build_round_table(interval_df, nkeys=nkeys)
    swap_vote_tbl = build_swap_vote_registered_table(interval_df, nkeys=nkeys)

    out_cols = [
        OUT_CHEST1_COL,
        OUT_CHEST3_COL,
        OUT_ROUND_EARNINGS_COL,
        OUT_ROUND_GRANDTOTAL_COL,
        OUT_SWAP_VOTE_REGISTERED_N_COL,
        OUT_SWAP_VOTE_REGISTERED_D_COL,
        OUT_SWAP_VOTE_REGISTERED_COL,
    ]

    for col in out_cols:
        if col not in interval_df.columns:
            interval_df[col] = pd.NA

    merged = interval_df.merge(
        round_tbl,
        on=nkeys,
        how="left",
        validate="m:1",
        suffixes=("", "__new"),
    )

    for col in [
        OUT_CHEST1_COL,
        OUT_CHEST3_COL,
        OUT_ROUND_EARNINGS_COL,
        OUT_ROUND_GRANDTOTAL_COL,
    ]:
        incoming = merged[f"{col}__new"]
        if fill_only:
            mask = merged[col].isna() & incoming.notna()
        else:
            mask = incoming.notna()

        merged.loc[mask, col] = incoming[mask].values
        merged = merged.drop(columns=[f"{col}__new"])

    for col in [
        OUT_SWAP_VOTE_REGISTERED_N_COL,
        OUT_SWAP_VOTE_REGISTERED_D_COL,
        OUT_SWAP_VOTE_REGISTERED_COL,
    ]:
        value = swap_vote_tbl.iloc[0][col]
        if fill_only:
            mask = merged[col].isna()
            merged.loc[mask, col] = value
        else:
            merged[col] = value

    merged = merged.drop(columns=nkeys, errors="ignore")

    if debug_keys and not quiet:
        tmp = interval_df.copy()
        tmp["_chestPin_num_norm"] = norm_key_series(tmp[CHEST_COL])

        print("\n=== DEBUG KEYS ===")
        print("interval unique round keys:", interval_df[[f'_{k}' for k in ROUND_KEYS]].drop_duplicates().shape[0])
        print("rows chestPin_num == 1:    ", (tmp["_chestPin_num_norm"] == "1").sum())
        print("rows chestPin_num == 3:    ", (tmp["_chestPin_num_norm"] == "3").sum())
        print("derived round rows:        ", len(round_tbl))
        print("rows with roundEarnings:   ", merged[OUT_ROUND_EARNINGS_COL].notna().sum())
        print("rows with roundGrandTotal: ", merged[OUT_ROUND_GRANDTOTAL_COL].notna().sum())
        print("swapVoteRegistered_n:      ", int(swap_vote_tbl.iloc[0][OUT_SWAP_VOTE_REGISTERED_N_COL]))
        print("swapVoteRegistered_d:      ", int(swap_vote_tbl.iloc[0][OUT_SWAP_VOTE_REGISTERED_D_COL]))
        print("swapVoteRegistered:        ", swap_vote_tbl.iloc[0][OUT_SWAP_VOTE_REGISTERED_COL])
        print("=== END DEBUG KEYS ===\n")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Derive round earnings columns from interval rows.")

    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--interval", type=Path, help="Single interval CSV (target).")
    mode.add_argument("--interval-dir", type=Path, help="Directory containing interval CSVs (targets).")

    p.add_argument("--output", type=Path, help="Single output CSV path (single-file mode).")
    p.add_argument("--outdir", type=Path, help="Output directory (dir mode).")

    p.add_argument("--interval-pattern", type=str, default="*_intervalsPinDropRawValues.csv", help="(dir mode) glob for intervals.")
    p.add_argument("--out-suffix", type=str, default="_intervalsRoundEarnings.csv", help="(dir mode) suffix added to interval stem.")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing output file(s).")

    p.add_argument("--fill-only", action="store_true")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--debug-keys", action="store_true")
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

        if not args.quiet:
            print("=== RUNNING: derive_round_earnings_from_intervals.py ===")
            print("interval:", args.interval)
            print("output:  ", args.output)

        process_one(
            args.interval,
            args.output,
            fill_only=args.fill_only,
            quiet=args.quiet,
            debug_keys=args.debug_keys,
        )

        if not args.quiet:
            print(f"✅ Wrote: {args.output}")
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

    if not args.quiet:
        print("=== RUNNING (BATCH): derive_round_earnings_from_intervals.py ===")
        print("interval-dir:", args.interval_dir, "pattern:", args.interval_pattern)
        print("outdir:      ", args.outdir)

    for interval_path in intervals:
        pattern_str = str(args.interval_pattern).split("*")[1]
        base = interval_path.name.removesuffix(pattern_str)
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
                debug_keys=args.debug_keys,
            )
            wrote += 1
            if not args.quiet:
                print(f"✅ {interval_path.name} -> {out_path.name}")
        except Exception as e:
            failures += 1
            print(f"❌ Failed {interval_path.name}: {type(e).__name__}: {e}", file=sys.stderr)
            if args.fail_fast:
                return 1

    if not args.quiet:
        print(f"\nDone. Wrote {wrote} file(s). Failures: {failures}.")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())