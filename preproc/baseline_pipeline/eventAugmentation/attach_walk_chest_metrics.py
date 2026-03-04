#!/usr/bin/env python3
"""
attach_walk_chest_metrics.py (batchable)

Expands BlockType == 'collecting' interval rows into 1 row per chestPin_num found in events,
and attaches:
  - Walk_ChestOpen metrics
  - ChestOpen_Moment fields

Directory mode:
  --interval-dir, --events-dir, --outdir plus patterns (same pairing by '*').

Single-file mode:
  --interval, --events, --output
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
import pandas as pd


BASE_KEYS = ["BlockNum", "BlockInstance", "RoundNum"]
PIN_KEY = "chestPin_num"
ALL_KEYS = BASE_KEYS + [PIN_KEY]

INTERVAL_BLOCKTYPE_COL = "BlockType"
COLLECTING_VALUE = "collecting"

EVENTTYPE_COL = "lo_eventType"
WALK_TYPES = {"Walk_ChestOpen"}  # add "Walk_Chest" if needed
CHESTOPEN_TYPE = "ChestOpen_Moment"

WALK_SRC_TO_DST = {
    "totEventDur": "Walk_Dur",
    "totEventDist": "WalkDist",
    "avgEventSpeed": "WalkAvgSpeed",
    "origRow_start": "origRow_start_walk",
    "origRow_end": "origRow_end_walk",
}

CHESTOPEN_SRC_TO_DST = {
    "chestPin_num": "chestPin_num",
    "dropDist": "dropDist",
    "path_step_in_round": "path_step_in_round",
    "coinLabel": "coinLabel",
    "actualClosestCoinLabel": "actualClosestCoinLabel",
    "actualClosestCoinDist": "actualClosestCoinDist",
    "origRow_start": "origRow_start",
    "origRow_end": "origRow_end",
    "start_AppTime": "start_AppTime",
    "end_AppTime": "end_AppTime",
    "totDistRound": "totDistRound",
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
    "dropQual": "dropQual",

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


def build_event_table(
    events_df: pd.DataFrame,
    *,
    event_filter_mask: pd.Series,
    nkeys: list[str],
    src_to_dst: dict[str, str],
    keep: str,
) -> pd.DataFrame:
    src_cols = list(src_to_dst.keys())
    sub = events_df.loc[event_filter_mask].copy()
    if sub.empty:
        return pd.DataFrame(columns=nkeys + list(src_to_dst.values()))
    sub = sub.dropna(subset=nkeys)
    sub = sub[nkeys + src_cols].drop_duplicates(subset=nkeys, keep=keep)
    return sub.rename(columns=src_to_dst)


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
        print(idx)
    return idx


def process_one(interval_path: Path, events_path: Path, out_path: Path, *, keep: str, fill_only: bool, quiet: bool, debug: bool) -> None:
    interval_df = pd.read_csv(interval_path)
    events_df = pd.read_csv(events_path)

    require_cols(interval_df, BASE_KEYS + [INTERVAL_BLOCKTYPE_COL], label=f"INTERVAL {interval_path.name}")
    require_cols(events_df, ALL_KEYS + [EVENTTYPE_COL], label=f"EVENTS {events_path.name}")

    for col in set(WALK_SRC_TO_DST.keys()) | set(CHESTOPEN_SRC_TO_DST.keys()):
        if col not in events_df.columns:
            raise ValueError(f"EVENTS {events_path.name}: missing required column: {col}")

    interval_df = add_norm_keys(interval_df, BASE_KEYS + [PIN_KEY])
    events_df = add_norm_keys(events_df, ALL_KEYS)

    N_BASE = [f"_{k}" for k in BASE_KEYS]
    N_PIN = f"_{PIN_KEY}"
    N_ALL = N_BASE + [N_PIN]

    is_collecting = (
        interval_df[INTERVAL_BLOCKTYPE_COL]
        .astype("string").str.strip().str.lower()
        .eq(COLLECTING_VALUE)
    )
    interval_collecting = interval_df.loc[is_collecting].copy()
    interval_other = interval_df.loc[~is_collecting].copy()

    if interval_collecting.empty:
        out = interval_df.drop(columns=[c for c in interval_df.columns if c.startswith("_")], errors="ignore")
        out.to_csv(out_path, index=False)
        return

    chest_mask = events_df[EVENTTYPE_COL].astype("string").str.strip().eq(CHESTOPEN_TYPE)
    pin_table = (
        events_df.loc[chest_mask, N_BASE + [N_PIN]]
        .dropna(subset=N_BASE + [N_PIN])
        .drop_duplicates()
    )

    if pin_table.empty:
        out = pd.concat([interval_other, interval_collecting], ignore_index=True)
        out = out.drop(columns=[c for c in out.columns if c.startswith("_")], errors="ignore")
        out.to_csv(out_path, index=False)
        return

    # EXPAND (collision-safe): keep suffix and read the correct pin column
    expanded = interval_collecting.merge(pin_table, on=N_BASE, how="left", validate="m:m", suffixes=("", "__pin"))
    pin_col = f"{N_PIN}__pin" if f"{N_PIN}__pin" in expanded.columns else N_PIN

    expanded[PIN_KEY] = pd.to_numeric(expanded[pin_col], errors="coerce")
    expanded[N_PIN] = norm_key_series(expanded[PIN_KEY])

    # Ensure destination cols exist
    for dst in list(WALK_SRC_TO_DST.values()) + list(CHESTOPEN_SRC_TO_DST.values()):
        if dst not in expanded.columns:
            expanded[dst] = pd.NA

    walk_mask = events_df[EVENTTYPE_COL].astype("string").str.strip().isin(WALK_TYPES)
    walk_tbl = build_event_table(events_df, event_filter_mask=walk_mask, nkeys=N_ALL, src_to_dst=WALK_SRC_TO_DST, keep=keep)
    chest_tbl = build_event_table(events_df, event_filter_mask=chest_mask, nkeys=N_ALL, src_to_dst=CHESTOPEN_SRC_TO_DST, keep=keep)

    merged = expanded.merge(walk_tbl, on=N_ALL, how="left", validate="m:1", suffixes=("", "__walk"))
    merged = merged.merge(chest_tbl, on=N_ALL, how="left", validate="m:1", suffixes=("", "__chest"))

    def apply_attach(df: pd.DataFrame, dst_cols: list[str]) -> None:
        for dst in dst_cols:
            inc_walk = f"{dst}__walk"
            inc_chest = f"{dst}__chest"

            incoming = None
            if inc_walk in df.columns:
                incoming = df[inc_walk]
            if inc_chest in df.columns and (incoming is None or incoming.isna().all()):
                incoming = df[inc_chest]
            if incoming is None:
                continue

            if fill_only:
                mask = df[dst].isna() & incoming.notna()
            else:
                mask = incoming.notna()
            df.loc[mask, dst] = incoming[mask].values

        df.drop(columns=[c for c in df.columns if c.endswith("__walk") or c.endswith("__chest")], inplace=True, errors="ignore")

    apply_attach(merged, list(WALK_SRC_TO_DST.values()) + list(CHESTOPEN_SRC_TO_DST.values()))

    merged = merged.drop(columns=[c for c in merged.columns if c.startswith("_")], errors="ignore")
    interval_other = interval_other.drop(columns=[c for c in interval_other.columns if c.startswith("_")], errors="ignore")

    out = pd.concat([interval_other, merged], ignore_index=True)

    # Optional: sort if you want
    if "start_AppTime" in out.columns:
        out = out.sort_values(by=["start_AppTime"])

    if debug and not quiet:
        print(f"[collecting] {interval_path.name}: collecting rows before={len(interval_collecting)}, after={len(merged)}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Expand collecting intervals + attach chest metrics (single or batch).")

    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--interval", type=Path)
    mode.add_argument("--interval-dir", type=Path)

    p.add_argument("--events", type=Path)
    p.add_argument("--events-dir", type=Path)

    p.add_argument("--output", type=Path)
    p.add_argument("--outdir", type=Path)

    p.add_argument("--interval-pattern", type=str, default="*_filledIntervals.csv")
    p.add_argument("--events-pattern", type=str, default="*_filled_intervalProps.csv")
    p.add_argument("--out-suffix", type=str, default="_withCollectingChest", help="(dir mode) suffix added to interval stem.")
    p.add_argument("--overwrite", action="store_true")

    p.add_argument("--keep", choices=["first", "last"], default="last")
    p.add_argument("--fill-only", action="store_true")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--fail-fast", action="store_true")

    return p.parse_args()


def main() -> int:
    args = parse_args()

    # single-file mode
    if args.interval is not None:
        if args.events is None or args.output is None:
            print("Single-file mode requires --events and --output.", file=sys.stderr)
            return 2
        if args.output.exists() and not args.overwrite:
            print(f"Output exists (use --overwrite): {args.output}", file=sys.stderr)
            return 3

        if not args.quiet:
            print("=== RUNNING: attach_walk_chest_metrics.py ===")
            print("interval:", args.interval)
            print("events:  ", args.events)
            print("output:  ", args.output)

        process_one(args.interval, args.events, args.output, keep=args.keep, fill_only=args.fill_only, quiet=args.quiet, debug=args.debug)
        if not args.quiet:
            print(f"✅ Wrote: {args.output}")
        return 0

    # directory mode
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

    if not args.quiet:
        print("=== RUNNING (BATCH): attach_walk_chest_metrics.py ===")
        print("interval-dir:", args.interval_dir, "pattern:", args.interval_pattern)
        print("events-dir:  ", args.events_dir, "pattern:", args.events_pattern)
        print("outdir:      ", args.outdir)

    for interval_path in intervals:
        key = parse_star_key(interval_path.name, args.interval_pattern) or interval_path.stem
        print(key)
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
            process_one(interval_path, events_path, out_path, keep=args.keep, fill_only=args.fill_only, quiet=args.quiet, debug=args.debug)
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
