#!/usr/bin/env python3
# 06b_propagate_pindrop_startpos.py
#
# Step B:
#   1) Read events that already have startPos + effectiveRoundNum
#   2) Build per-round startPos table keyed on:
#        (BlockNum, BlockInstance, effectiveRoundNum)
#      Preferred source: InterRound_PostCylinderWalk_segment row
#      Fallback source (if InterRound startPos missing): TrueContentStart row
#   3) FORCE overwrite startPos/startPos_dist across all rows in that effective round
#      (rows with NaN effectiveRoundNum are left untouched)
#   4) Optionally update interval tables by merging on:
#        (BlockNum, BlockInstance, RoundNum) == effectiveRoundNum

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd


INTER_EVENT = "InterRound_PostCylinderWalk_segment"
ROUNDSTART_EVENT = "TrueContentStart"

_TRAILING_TOKENS = [
    "_event_reproc_withStartPos_withEffectiveRound_startPosPropagated",
    "_event_reproc_withStartPos_withEffectiveRound",
    "_event_reproc_withStartPos",
    "_event_reproc",
    "_withStartPos",
    "_withEffectiveRound",
    "_startPosPropagated",
]


def _strip_known_trailing_tokens(stem: str) -> str:
    base = stem
    changed = True
    while changed:
        changed = False
        for tok in _TRAILING_TOKENS:
            if base.endswith(tok):
                base = base[: -len(tok)]
                changed = True
                break
    return base


def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _infer_out_path(in_path: Path, out_root: Optional[Path], suffix: str) -> Path:
    base = _strip_known_trailing_tokens(in_path.stem)
    out_name = base + suffix + in_path.suffix
    if out_root is None:
        return in_path.with_name(out_name)
    return out_root / out_name


def _pick_time_col(df: pd.DataFrame) -> Optional[str]:
    for c in ("start_AppTime", "AppTime", "end_AppTime"):
        if c in df.columns:
            return c
    return None


def _infer_interval_paths(events_path: Path, interval_root: Optional[Path]) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Find interval files for base stem.
    Supports both withStartPos and non-withStartPos names.
    """
    base = _strip_known_trailing_tokens(events_path.stem)

    candidates = [
        (base + "_finalInterval_vert_withStartPos.csv", base + "_finalInterval_horz_withStartPos.csv"),
        (base + "_finalInterval_vert.csv",             base + "_finalInterval_horz.csv"),
    ]

    for vname, hname in candidates:
        v = events_path.with_name(vname)
        h = events_path.with_name(hname)
        if interval_root is not None:
            v = interval_root / v.name
            h = interval_root / h.name
        if v.exists() and h.exists():
            return v, h

    return None, None


def process_one_file(
    in_events: Path,
    *,
    out_root: Optional[Path],
    interval_root: Optional[Path],
    also_update_intervals: bool,
    max_round: int,
    dry_run: bool,
    verbose: bool,
) -> None:
    df = pd.read_csv(in_events).copy()

    required = {
        "lo_eventType",
        "BlockNum",
        "BlockInstance",
        "effectiveRoundNum",
        "startPos",
        "startPos_dist",
    }
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{in_events.name} missing required columns: {missing} (did you run 05 + 06a?)")

    # Normalize numeric keys
    for c in ("BlockNum", "BlockInstance", "effectiveRoundNum"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Leave NaNs alone; keep only valid <= max_round for matching
    df["effectiveRoundNum"] = df["effectiveRoundNum"].where(
        df["effectiveRoundNum"].notna() & (df["effectiveRoundNum"] <= max_round)
    )

    # Normalize event types
    df["lo_eventType"] = df["lo_eventType"].astype("string").str.strip()

    # Optional debug summary
    if verbose:
        ir_has = ((df["lo_eventType"] == INTER_EVENT) & df["startPos"].notna()).sum()
        rs_has = ((df["lo_eventType"] == ROUNDSTART_EVENT) & df["startPos"].notna()).sum()
        eff_n = df["effectiveRoundNum"].notna().sum()
        print(
            f"[{in_events.name}] rows={len(df)} effRound(notna)={eff_n} "
            f"IR(startPos)={int(ir_has)} RS(startPos)={int(rs_has)}"
        )

    # Choose ordering time for InterRound duplicates: end_AppTime preferred, else AppTime, else start_AppTime
    inter_sort_time = None
    for c in ("end_AppTime", "AppTime", "start_AppTime"):
        if c in df.columns:
            inter_sort_time = c
            break
    if inter_sort_time is not None:
        df[inter_sort_time] = pd.to_numeric(df[inter_sort_time], errors="coerce")

    keys = ["BlockNum", "BlockInstance", "effectiveRoundNum"]

    # ------------------------------
    # Build per-round source table
    # ------------------------------
    # Preferred: InterRound rows with startPos
    ir = df[
        df["effectiveRoundNum"].notna()
        & (df["lo_eventType"] == INTER_EVENT)
        & df["startPos"].notna()
    ].copy()

    if not ir.empty:
        if inter_sort_time is not None:
            ir = ir.sort_values(keys + [inter_sort_time], kind="mergesort", na_position="last")
        else:
            ir = ir.sort_values(keys, kind="mergesort", na_position="last")

        per_ir = (
            ir.drop_duplicates(subset=keys, keep="last")[keys + ["startPos", "startPos_dist"]]
            .rename(columns={"startPos": "startPos__src", "startPos_dist": "startPos_dist__src"})
            .copy()
        )
    else:
        per_ir = pd.DataFrame(columns=keys + ["startPos__src", "startPos_dist__src"])

    # Fallback: TrueContentStart rows with startPos
    rs = df[
        df["effectiveRoundNum"].notna()
        & (df["lo_eventType"] == ROUNDSTART_EVENT)
        & df["startPos"].notna()
    ].copy()

    per_rs = (
        rs.drop_duplicates(subset=keys, keep="last")[keys + ["startPos", "startPos_dist"]]
        .rename(columns={"startPos": "startPos__fallback", "startPos_dist": "startPos_dist__fallback"})
        .copy()
    )

    # Combine (InterRound wins; else TrueContentStart)
    per_round = per_rs.merge(per_ir, on=keys, how="outer")

    per_round["startPos__final"] = per_round["startPos__src"].combine_first(per_round["startPos__fallback"])
    per_round["startPos_dist__final"] = per_round["startPos_dist__src"].combine_first(per_round["startPos_dist__fallback"])

    per_round = per_round[keys + ["startPos__final", "startPos_dist__final"]]

    # ------------------------------
    # FORCE overwrite across round
    # ------------------------------
    df = df.merge(per_round, on=keys, how="left")
    mask = df["startPos__final"].notna()

    df.loc[mask, "startPos"] = df.loc[mask, "startPos__final"]
    df.loc[mask, "startPos_dist"] = df.loc[mask, "startPos_dist__final"]

    df = df.drop(columns=["startPos__final", "startPos_dist__final"], errors="ignore")

    # Final sort before saving (AppTime preferred)
    if "AppTime" in df.columns:
        df["AppTime"] = pd.to_numeric(df["AppTime"], errors="coerce")
        df = df.sort_values("AppTime", kind="mergesort", na_position="last")
    else:
        tcol = _pick_time_col(df)
        if tcol is not None:
            df[tcol] = pd.to_numeric(df[tcol], errors="coerce")
            df = df.sort_values(tcol, kind="mergesort", na_position="last")

    # Write events output
    out_path = _infer_out_path(in_events, out_root, suffix="_startPosPropagated")
    _ensure_parent(out_path)

    if dry_run:
        print(f"[dry-run] would write events: {out_path}")
    else:
        df.to_csv(out_path, index=False)
        print(f"[ok] wrote events: {out_path}")

    # ------------------------------
    # Optional: update interval tables
    # ------------------------------
    if not also_update_intervals:
        return

    vert_path, horz_path = _infer_interval_paths(in_events, interval_root)
    if vert_path is None or horz_path is None:
        print(f"[skip] intervals not found for {in_events.name}")
        return

    vert = pd.read_csv(vert_path)
    horz = pd.read_csv(horz_path)

    # Build interval merge table where RoundNum = effectiveRoundNum
    per_round_for_intervals = per_round.rename(columns={"effectiveRoundNum": "RoundNum"}).copy()
    per_round_for_intervals["RoundNum"] = pd.to_numeric(per_round_for_intervals["RoundNum"], errors="coerce")

    merge_keys = ["BlockNum", "BlockInstance", "RoundNum"]

    def _update_one_interval(interval_df: pd.DataFrame, interval_path: Path, label: str) -> None:
        out_df = interval_df.copy()
        for c in merge_keys:
            if c in out_df.columns:
                out_df[c] = pd.to_numeric(out_df[c], errors="coerce")

        out_df = out_df.merge(per_round_for_intervals, on=merge_keys, how="left", suffixes=("", "__fromEvents"))

        # Overwrite only where we have a value from events
        if "startPos__final__fromEvents" in out_df.columns:
            m = out_df["startPos__final__fromEvents"].notna()
            out_df.loc[m, "startPos"] = out_df.loc[m, "startPos__final__fromEvents"]
        if "startPos_dist__final__fromEvents" in out_df.columns:
            m = out_df["startPos_dist__final__fromEvents"].notna()
            out_df.loc[m, "startPos_dist"] = out_df.loc[m, "startPos_dist__final__fromEvents"]

        out_df = out_df.drop(
            columns=["startPos__final__fromEvents", "startPos_dist__final__fromEvents"],
            errors="ignore",
        )

        out_int_path = _infer_out_path(interval_path, interval_root, suffix="_startPosPropagated")
        _ensure_parent(out_int_path)

        if dry_run:
            print(f"[dry-run] would write interval {label}: {out_int_path}")
        else:
            out_df.to_csv(out_int_path, index=False)
            print(f"[ok] wrote interval {label}: {out_int_path}")

    _update_one_interval(vert, vert_path, "vert")
    _update_one_interval(horz, horz_path, "horz")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Step B: propagate startPos within effective rounds (InterRound preferred; TrueContentStart fallback) and optionally update intervals."
    )
    ap.add_argument("--root", required=True, help="Root dir to search for events CSVs")
    ap.add_argument("--pattern", default="*_withEffectiveRound.csv", help="Glob for events files produced by 06a")
    ap.add_argument("--out-root", default="", help="Optional flat output dir; otherwise next to inputs")
    ap.add_argument("--interval-root", default="", help="Optional where interval files live")
    ap.add_argument("--also-update-intervals", action="store_true")
    ap.add_argument("--max-round", type=int, default=100)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", action="store_true", help="Print per-file diagnostics")
    args = ap.parse_args()

    root = Path(args.root).expanduser()
    out_root = Path(args.out_root).expanduser() if args.out_root else None
    interval_root = Path(args.interval_root).expanduser() if args.interval_root else None

    files = sorted(root.rglob(args.pattern))
    print(f"[scan] found {len(files)} files under {root} matching {args.pattern}")

    for f in files:
        try:
            process_one_file(
                f,
                out_root=out_root,
                interval_root=interval_root,
                also_update_intervals=args.also_update_intervals,
                max_round=args.max_round,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )
        except Exception as e:
            print(f"[fail] {f}: {e}")

    print("[done]")


if __name__ == "__main__":
    main()
