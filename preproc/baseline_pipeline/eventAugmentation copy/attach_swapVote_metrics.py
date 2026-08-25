#!/usr/bin/env python3
"""
attach_swapvote_metrics.py

Attaches SwapVote_Moment fields from an events file (*_filled_intervalProps.csv)
onto an interval file (e.g., *_nearlyFilledInterval.csv) using the composite key:

  BlockNum + BlockInstance + RoundNum

It pulls from events where:
  lo_eventType == "SwapVote_Moment"

And attaches (if present in events; otherwise filled with NA):
  - SwapVote
  - SwapVoteScore
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


BASE_KEYS = ["BlockNum", "BlockInstance", "RoundNum"]

EVENTTYPE_COL = "lo_eventType"
SWAPVOTE_TYPE = "SwapVote_Moment"

SWAP_FIELDS = ["SwapVote", "SwapVoteScore"]


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


def parse_star_key(filename: str, pattern: str) -> str | None:
    if pattern.count("*") != 1:
        return None
    pre, post = pattern.split("*")
    if not filename.startswith(pre) or not filename.endswith(post):
        return None
    return filename[len(pre) : (len(filename) - len(post) if len(post) else len(filename))]


def index_by_pattern_key(paths: list[Path], pattern: str) -> dict[str, Path]:
    idx: dict[str, Path] = {}
    for p in paths:
        k = parse_star_key(p.name, pattern)
        if k is None:
            k = p.stem
        idx[k] = p
    return idx


def ensure_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    return out


def build_swapvote_table(
    events_df: pd.DataFrame,
    *,
    nkeys: list[str],
    keep: str,
) -> pd.DataFrame:
    """
    Returns a de-duplicated table keyed by nkeys with SwapVote fields.
    If events lacks SwapVote fields, they are created as NA and no error is raised.
    If there are no SwapVote_Moment rows, returns empty table with correct columns.
    """
    events_df = ensure_columns(events_df, SWAP_FIELDS)

    mask = events_df[EVENTTYPE_COL].astype("string").str.strip().eq(SWAPVOTE_TYPE)
    sub = events_df.loc[mask].copy()

    cols = nkeys + SWAP_FIELDS
    if sub.empty:
        return pd.DataFrame(columns=cols)

    sub = sub.dropna(subset=nkeys)
    sub = sub[cols].drop_duplicates(subset=nkeys, keep=keep)
    return sub


def process_one(
    interval_path: Path,
    events_path: Path,
    out_path: Path,
    *,
    keep: str,
    fill_only: bool,
    quiet: bool,
    debug: bool,
) -> None:
    interval_df = pd.read_csv(interval_path)
    events_df = pd.read_csv(events_path)

    require_cols(interval_df, BASE_KEYS, label=f"INTERVAL {interval_path.name}")
    require_cols(events_df, BASE_KEYS + [EVENTTYPE_COL], label=f"EVENTS {events_path.name}")

    # Always guarantee output has these columns even if no swaps exist anywhere
    interval_df = ensure_columns(interval_df, SWAP_FIELDS)

    interval_df = add_norm_keys(interval_df, BASE_KEYS)
    events_df = add_norm_keys(events_df, BASE_KEYS)

    nkeys = [f"_{k}" for k in BASE_KEYS]
    swap_tbl = build_swapvote_table(events_df, nkeys=nkeys, keep=keep)

    if swap_tbl.empty:
        out = interval_df.drop(columns=[c for c in interval_df.columns if c.startswith("_")], errors="ignore")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(out_path, index=False)
        if debug and not quiet:
            print(f"[debug] No SwapVote_Moment rows found; wrote NA columns to {out_path.name}")
        return

    merged = interval_df.merge(swap_tbl, on=nkeys, how="left", validate="m:1", suffixes=("", "__incoming"))

    for col in SWAP_FIELDS:
        inc = f"{col}__incoming"
        if inc not in merged.columns:
            continue
        incoming = merged[inc]
        if fill_only:
            mask = merged[col].isna() & incoming.notna()
        else:
            mask = incoming.notna()
        merged.loc[mask, col] = incoming[mask].values

    merged.drop(columns=[c for c in merged.columns if c.endswith("__incoming")], inplace=True, errors="ignore")
    merged.drop(columns=[c for c in merged.columns if c.startswith("_")], inplace=True, errors="ignore")

    if debug and not quiet:
        n_total = len(merged)
        n_have = merged["SwapVote"].notna().sum()
        print(f"[debug] {interval_path.name}: rows={n_total}, SwapVote filled={n_have}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Attach SwapVote fields (SwapVote_Moment) onto interval rows by BlockNum/BlockInstance/RoundNum.")

    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--interval", type=Path)
    mode.add_argument("--interval-dir", type=Path)

    p.add_argument("--events", type=Path)
    p.add_argument("--events-dir", type=Path)

    p.add_argument("--output", type=Path)
    p.add_argument("--outdir", type=Path)

    p.add_argument("--interval-pattern", type=str, default="*_nearlyFilledInterval.csv")
    p.add_argument("--events-pattern", type=str, default="*_filled_intervalProps.csv")
    p.add_argument("--out-suffix", type=str, default="_filledIntervals.csv")
    p.add_argument("--overwrite", action="store_true")

    p.add_argument("--keep", choices=["first", "last"], default="last")
    p.add_argument("--fill-only", action="store_true")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--fail-fast", action="store_true")

    return p.parse_args()


def main() -> int:
    args = parse_args()

    if args.interval is not None:
        if args.events is None or args.output is None:
            print("Single-file mode requires --events and --output.", file=sys.stderr)
            return 2
        if args.output.exists() and not args.overwrite:
            print(f"Output exists (use --overwrite): {args.output}", file=sys.stderr)
            return 3

        process_one(
            args.interval,
            args.events,
            args.output,
            keep=args.keep,
            fill_only=args.fill_only,
            quiet=args.quiet,
            debug=args.debug,
        )
        return 0

    # dir mode
    assert args.interval_dir is not None
    if args.events_dir is None or args.outdir is None:
        print("Dir mode requires --events-dir and --outdir.", file=sys.stderr)
        return 2

    intervals = sorted([p for p in args.interval_dir.glob(args.interval_pattern) if p.is_file()])
    events = sorted([p for p in args.events_dir.glob(args.events_pattern) if p.is_file()])

    if not intervals:
        print(f"No interval files matched {args.interval_pattern!r} in {args.interval_dir}", file=sys.stderr)
        return 2
    if not events:
        print(f"No event files matched {args.events_pattern!r} in {args.events_dir}", file=sys.stderr)
        return 2

    event_index = index_by_pattern_key(events, args.events_pattern)

    failures = 0
    wrote = 0
    pattern_tail = args.interval_pattern.split("*", 1)[1] if "*" in args.interval_pattern else ""

    for interval_path in intervals:
        key = parse_star_key(interval_path.name, args.interval_pattern) or interval_path.stem
        events_path = event_index.get(key)

        if events_path is None:
            failures += 1
            print(f"❌ No matching events file for interval={interval_path.name} (key={key!r})", file=sys.stderr)
            if args.fail_fast:
                return 1
            continue

        base = interval_path.name
        if pattern_tail and base.endswith(pattern_tail):
            base = base[: -len(pattern_tail)]
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
                events_path,
                out_path,
                keep=args.keep,
                fill_only=args.fill_only,
                quiet=args.quiet,
                debug=args.debug,
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