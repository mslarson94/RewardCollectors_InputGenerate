#!/usr/bin/env python3
"""
attach_walk_pindrop_metrics.py (batchable)

Attach Walk_PinDrop AND Adjusted_1st_Walk_PinDrop metrics from an EVENTS file onto an INTERVAL file.

Keys (normalized string join):
  (BlockNum, RoundNum, BlockInstance, chestPin_num)

Directory mode:
  Provide --interval-dir, --events-dir, --outdir plus patterns like:
    --interval-pattern "*_filledIntervals.csv"
    --events-pattern   "*_filled_intervalProps.csv"
  Files are paired by the '*' portion of the pattern.

Single-file mode:
  Provide --interval, --events, --output
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
import pandas as pd


KEYS = ["BlockNum", "RoundNum", "BlockInstance", "chestPin_num"]
EVENTTYPE_COL = "lo_eventType"

ATTACH_SPECS: dict[str, dict[str, str]] = {
    "Walk_PinDrop": {
        "totEventDur": "Walk_Dur",
        "totEventDist": "WalkDist",
        "avgEventSpeed": "WalkAvgSpeed",
        "origRow_start": "origRow_start_walk",
        "origRow_end": "origRow_end_walk",
        "testingOrder": "testingOrder",
        "source_file": "source_file",
        "participantID": "participantID",
        "pairID": "pairID",
        "testingDate": "testingDate",
        "sessionType": "sessionType",
        "ptIsAorB" : "ptIsAorB",
        "device" : "device",
        "main_RR" : "main_RR",
        "currentRole" : "currentRole",
        "taskNaive": "taskNaive",
        "BlockStatus": "BlockStatus",
        "CoinSetID": "CoinSetID",
        "roundElapsed_s_end": "roundElapsed_s",
        "blockElapsed_s_end": "blockElapsed_s",
        "roundFrac_end": "roundFrac",
        "blockFrac_end": "blockFrac",
        "sessionID": "sessionID",
        "totalRounds": "totalRounds",
        "round_dur_s": "round_dur_s",
        "totDistRound": "totDistRound",
        "avgRoundSpeed": "avgRoundSpeed",
        "block_dur_s": "block_dur_s",
        "totDistBlock": "totDistBlock",
        "avgBlockSpeed": "avgBlockSpeed",

    },
    "Adjusted_1st_Walk_PinDrop": {
        "totEventDur": "Walk_Dur_Adj",
        "totEventDist": "WalkDist_Adj",
        "avgEventSpeed": "WalkAvgSpeed_Adj",
        "origRow_start": "origRow_start_adjwalk",
        "origRow_end": "origRow_end_adjwalk",
        "testingOrder": "testingOrder",
        "source_file": "source_file",
        "participantID": "participantID",
        "pairID": "pairID",
        "testingDate": "testingDate",
        "sessionType": "sessionType",
        "ptIsAorB" : "ptIsAorB",
        "device" : "device",
        "main_RR" : "main_RR",
        "currentRole" : "currentRole",
        "taskNaive": "taskNaive",
        "BlockStatus": "BlockStatus",
        "CoinSetID": "CoinSetID",
        "blockElapsed_s_start": "blockElapsed_s_adj",
        "blockFrac_start": "blockFrac_adj",
        "sessionID": "sessionID",
    },
}


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


def build_source_table(
    events_df: pd.DataFrame,
    *,
    event_value: str,
    nkeys: list[str],
    src_to_dst: dict[str, str],
    keep: str,
) -> pd.DataFrame:
    src_cols = list(src_to_dst.keys())
    sub = events_df[events_df[EVENTTYPE_COL].astype("string").str.strip() == event_value].copy()
    if sub.empty:
        return pd.DataFrame(columns=nkeys + list(src_to_dst.values()))
    sub = sub.dropna(subset=nkeys)
    sub = sub[nkeys + src_cols].drop_duplicates(subset=nkeys, keep=keep)
    return sub.rename(columns=src_to_dst)


def parse_star_key(filename: str, pattern: str) -> str | None:
    """
    If pattern contains exactly one '*', return the substring of filename matched by '*'
    (based on prefix/suffix around the '*'). Works on basename only.
    """
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
            # fallback: stem
            k = p.stem
        idx[k] = p
    return idx


def process_one(interval_path: Path, events_path: Path, out_path: Path, *, keep: str, fill_only: bool, quiet: bool, debug_keys: bool) -> None:
    interval_df = pd.read_csv(interval_path)
    events_df = pd.read_csv(events_path)

    require_cols(interval_df, KEYS, label=f"INTERVAL {interval_path.name}")
    all_src_cols = sorted({c for spec in ATTACH_SPECS.values() for c in spec.keys()})
    require_cols(events_df, KEYS + [EVENTTYPE_COL] + all_src_cols, label=f"EVENTS {events_path.name}")

    interval_df = add_norm_keys(interval_df, KEYS)
    events_df = add_norm_keys(events_df, KEYS)
    NKEYS = [f"_{k}" for k in KEYS]

    all_dst_cols = sorted({dst for spec in ATTACH_SPECS.values() for dst in spec.values()})
    for dst in all_dst_cols:
        if dst not in interval_df.columns:
            interval_df[dst] = pd.NA

    merged = interval_df
    debug_info = {}

    for event_value, src_to_dst in ATTACH_SPECS.items():
        src_table = build_source_table(events_df, event_value=event_value, nkeys=NKEYS, src_to_dst=src_to_dst, keep=keep)
        debug_info[event_value] = len(src_table)

        merged = merged.merge(src_table, on=NKEYS, how="left", validate="m:1", suffixes=("", "__new"))

        for dst in src_to_dst.values():
            incoming = merged[f"{dst}__new"] if f"{dst}__new" in merged.columns else merged[dst]
            if fill_only:
                mask = merged[dst].isna() & incoming.notna()
            else:
                mask = incoming.notna()
            merged.loc[mask, dst] = incoming[mask].values
            if f"{dst}__new" in merged.columns:
                merged = merged.drop(columns=[f"{dst}__new"])

    merged = merged.drop(columns=NKEYS, errors="ignore")

    if debug_keys and not quiet:
        print("\n=== DEBUG KEYS ===")
        print("interval unique composite keys:", interval_df[[f"_{k}" for k in KEYS]].drop_duplicates().shape[0])
        for ev, n in debug_info.items():
            print(f"{ev} unique keys in events: {n:,}")
        print("=== END DEBUG KEYS ===\n")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Attach pindrop walk metrics onto interval rows (single or batch).")

    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--interval", type=Path, help="Single interval CSV (target).")
    mode.add_argument("--interval-dir", type=Path, help="Directory containing interval CSVs (targets).")

    p.add_argument("--events", type=Path, help="Single events CSV (source). Required in single-file mode.")
    p.add_argument("--events-dir", type=Path, help="Directory containing events CSVs (sources). Required in dir mode.")

    p.add_argument("--output", type=Path, help="Single output CSV path (single-file mode).")
    p.add_argument("--outdir", type=Path, help="Output directory (dir mode).")

    p.add_argument("--interval-pattern", type=str, default="*_filledIntervals.csv", help="(dir mode) glob for intervals.")
    p.add_argument("--events-pattern", type=str, default="*_filled_intervalProps.csv", help="(dir mode) glob for events.")
    p.add_argument("--out-suffix", type=str, default="_withPindropWalks", help="(dir mode) suffix added to interval stem.")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing output file(s).")

    p.add_argument("--keep", choices=["first", "last"], default="last")
    p.add_argument("--fill-only", action="store_true")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--debug-keys", action="store_true")
    p.add_argument("--fail-fast", action="store_true")

    return p.parse_args()


def main() -> int:
    args = parse_args()

    # --- single-file mode ---
    if args.interval is not None:
        if args.events is None or args.output is None:
            print("Single-file mode requires --events and --output.", file=sys.stderr)
            return 2
        if not args.interval.exists():
            print(f"Interval not found: {args.interval}", file=sys.stderr)
            return 2
        if not args.events.exists():
            print(f"Events not found: {args.events}", file=sys.stderr)
            return 2
        if args.output.exists() and not args.overwrite:
            print(f"Output exists (use --overwrite): {args.output}", file=sys.stderr)
            return 3

        if not args.quiet:
            print("=== RUNNING: attach_walk_pindrop_metrics.py ===")
            print("interval:", args.interval)
            print("events:  ", args.events)
            print("output:  ", args.output)

        process_one(
            args.interval, args.events, args.output,
            keep=args.keep, fill_only=args.fill_only,
            quiet=args.quiet, debug_keys=args.debug_keys
        )
        if not args.quiet:
            print(f"✅ Wrote: {args.output}")
        return 0

    # --- directory mode ---
    assert args.interval_dir is not None
    if args.events_dir is None or args.outdir is None:
        print("Dir mode requires --events-dir and --outdir.", file=sys.stderr)
        return 2
    if not args.interval_dir.exists():
        print(f"Interval dir not found: {args.interval_dir}", file=sys.stderr)
        return 2
    if not args.events_dir.exists():
        print(f"Events dir not found: {args.events_dir}", file=sys.stderr)
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

    if not args.quiet:
        print("=== RUNNING (BATCH): attach_walk_pindrop_metrics.py ===")
        print("interval-dir:", args.interval_dir, "pattern:", args.interval_pattern)
        print("events-dir:  ", args.events_dir, "pattern:", args.events_pattern)
        print("outdir:      ", args.outdir)

    for interval_path in intervals:
        key = parse_star_key(interval_path.name, args.interval_pattern) or interval_path.stem
        events_path = event_index.get(key)
        if events_path is None:
            failures += 1
            print(f"❌ No matching events file for interval={interval_path.name} (key={key!r})", file=sys.stderr)
            if args.fail_fast:
                return 1
            continue
        patternStr = str(args.interval_pattern)
        patternStr = patternStr.split('*')[1]
        base = interval_path.name.removesuffix(patternStr)
        out_path = args.outdir / f"{base}{args.out_suffix}"
        if out_path.exists() and not args.overwrite:
            failures += 1
            print(f"❌ Output exists (use --overwrite): {out_path}", file=sys.stderr)
            if args.fail_fast:
                return 1
            continue

        try:
            process_one(
                interval_path, events_path, out_path,
                keep=args.keep, fill_only=args.fill_only,
                quiet=args.quiet, debug_keys=args.debug_keys
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
