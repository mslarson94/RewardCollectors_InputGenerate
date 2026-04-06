#!/usr/bin/env python3
"""
report_roundnums_lt100.py

Report RoundNum values (< 100) per BlockNum (and optionally BlockInstance)
for all CSV files in a directory.

Typical use:
  python report_roundnums_lt100.py \
    --input-dir "/path/to/EventSegmentation/Events_*" \
    --pattern "*_events*.csv" \
    --round-col RoundNum \
    --block-col BlockNum \
    --event-col lo_eventType \
    --only-event RoundEnd \
    --max-round 99 \
    --out "/path/to/roundnums_report.csv"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


def _iter_files(input_dir: Path, pattern: str, recursive: bool) -> list[Path]:
    return sorted(input_dir.rglob(pattern) if recursive else input_dir.glob(pattern))


def _read_csv_safely(p: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(p)
    except Exception as e:
        raise RuntimeError(f"Failed to read {p}: {e}") from e


def summarize_file(
    df: pd.DataFrame,
    *,
    file_name: str,
    round_col: str,
    block_col: str,
    blockinst_col: str | None,
    event_col: str | None,
    only_event: str | None,
    max_round: int,
    include_counts: bool,
) -> pd.DataFrame:
    # Required columns
    for c in [round_col, block_col]:
        if c not in df.columns:
            raise KeyError(f"{file_name}: missing required column {c!r}")

    if blockinst_col and blockinst_col not in df.columns:
        # If user requests blockinstance but it isn't present, just ignore it
        blockinst_col = None

    if event_col and event_col not in df.columns:
        # If user requests event filter but event_col missing, ignore filter
        event_col = None
        only_event = None

    d = df.copy()

    # Normalize
    d[round_col] = pd.to_numeric(d[round_col], errors="coerce")
    d[block_col] = pd.to_numeric(d[block_col], errors="coerce")

    if blockinst_col:
        d[blockinst_col] = pd.to_numeric(d[blockinst_col], errors="coerce")

    if event_col:
        d[event_col] = d[event_col].astype("string").str.strip()

    # Filter: only RoundNum < 100 (<= max_round)
    mask = d[round_col].notna() & (d[round_col] <= max_round)
    if only_event and event_col:
        mask &= d[event_col].eq(only_event)

    d = d.loc[mask, :].copy()
    if d.empty:
        return pd.DataFrame(
            [{
                "file": file_name,
                "BlockNum": pd.NA,
                "BlockInstance": pd.NA,
                "roundnums_lt100": "",
                "n_rows": 0,
            }]
        )

    # Group keys
    group_cols = [block_col]
    out_cols = ["file", "BlockNum", "roundnums_lt100"]
    if blockinst_col:
        group_cols.append(blockinst_col)
        out_cols.insert(2, "BlockInstance")  # after BlockNum
    if include_counts:
        out_cols.append("n_rows")

    # Aggregate unique RoundNums per BlockNum(/BlockInstance)
    def _join_rounds(x: pd.Series) -> str:
        vals = sorted(set(int(v) for v in x.dropna().tolist()))
        return ",".join(map(str, vals))

    agg = d.groupby(group_cols, dropna=False)[round_col].apply(_join_rounds).reset_index(name="roundnums_lt100")

    if include_counts:
        counts = d.groupby(group_cols, dropna=False).size().reset_index(name="n_rows")
        agg = agg.merge(counts, on=group_cols, how="left")

    # Rename to stable output names
    agg.insert(0, "file", file_name)
    agg = agg.rename(columns={block_col: "BlockNum"})
    if blockinst_col:
        agg = agg.rename(columns={blockinst_col: "BlockInstance"})
    else:
        agg["BlockInstance"] = pd.NA  # keep column for easy concat if desired

    # Ensure consistent column ordering
    agg = agg[out_cols]
    return agg


def main() -> None:
    ap = argparse.ArgumentParser(description="Report RoundNum values < 100 per BlockNum across all CSV files in a directory.")
    ap.add_argument("--input-dir", type=Path, required=True, help="Directory containing event CSV files")
    ap.add_argument("--pattern", type=str, default="*.csv", help="Glob pattern (default: *.csv)")
    ap.add_argument("--recursive", action="store_true", help="Recurse into subdirectories")

    ap.add_argument("--round-col", type=str, default="RoundNum", help="Round number column name (default: RoundNum)")
    ap.add_argument("--block-col", type=str, default="BlockNum", help="Block number column name (default: BlockNum)")
    ap.add_argument("--blockinstance-col", type=str, default="BlockInstance", help="Block instance column name (default: BlockInstance)")

    ap.add_argument("--event-col", type=str, default="lo_eventType", help="Event type column name (default: lo_eventType)")
    ap.add_argument("--only-event", type=str, default=None, help="If set, only consider rows where event_col == this value (e.g., RoundEnd)")

    ap.add_argument("--max-round", type=int, default=99, help="Max round treated as 'true round' (default: 99)")
    ap.add_argument("--include-counts", action="store_true", help="Include n_rows contributing to each group")
    ap.add_argument("--out", type=Path, default=None, help="Optional output CSV path")
    ap.add_argument("--fail-fast", action="store_true", help="Stop on first file error (default: continue)")

    args = ap.parse_args()

    in_dir = args.input_dir.expanduser()
    files = _iter_files(in_dir, args.pattern, args.recursive)
    if not files:
        print(f"No files matched in {in_dir} with pattern={args.pattern!r}", file=sys.stderr)
        raise SystemExit(2)

    rows = []
    failures = 0

    for p in files:
        try:
            df = _read_csv_safely(p)
            rep = summarize_file(
                df,
                file_name=p.name,
                round_col=args.round_col,
                block_col=args.block_col,
                blockinst_col=args.blockinstance_col,
                event_col=args.event_col,
                only_event=args.only_event,
                max_round=args.max_round,
                include_counts=args.include_counts,
            )
            rows.append(rep)
        except Exception as e:
            failures += 1
            msg = f"❌ {p.name}: {e}"
            if args.fail_fast:
                raise
            print(msg, file=sys.stderr)

    out_df = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

    # If only-event specified, it's useful to keep it in the report metadata
    if args.only_event:
        out_df.insert(1, "only_event", args.only_event)

    # Sort for readability
    sort_cols = [c for c in ["file", "BlockNum", "BlockInstance"] if c in out_df.columns]
    if sort_cols:
        out_df = out_df.sort_values(sort_cols, kind="mergesort", na_position="last").reset_index(drop=True)

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(args.out, index=False)
        print(f"✅ Wrote report: {args.out}")
    else:
        # Print a compact view to stdout
        with pd.option_context("display.max_rows", 200, "display.max_colwidth", 200):
            print(out_df)

    if failures:
        print(f"⚠️ Completed with {failures} file(s) failing to parse.", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
