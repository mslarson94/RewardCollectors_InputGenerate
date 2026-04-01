#!/usr/bin/env python3
# 06_fix_pindrop_startpos.py

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd


INTER_EVENT = "InterRound_PostCylinderWalk_segment"
ROUNDSTART_EVENT = "RoundStart"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _infer_out_path(in_path: Path, out_root: Optional[Path], suffix: str = "_pindropFixed") -> Path:
    if out_root is None:
        return in_path.with_name(in_path.stem + suffix + in_path.suffix)
    return out_root / (in_path.stem + suffix + in_path.suffix)


def _infer_interval_paths(events_path: Path, interval_root: Optional[Path]) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Try to locate interval files for a given events file.
    Accepts both *_withStartPos and non-withStartPos variants.
    """
    stem = events_path.stem

    suffix_tokens = [
        "_event_reproc_withStartPos_pindropFixed",
        "_event_reproc_withStartPos",
        "_event_reproc",
        "_events_pre_reproc_withStartPos",
        "_events_pre_reproc",
        "_events_withStartPos",
        "_events",
    ]
    base = stem
    for tok in suffix_tokens:
        if base.endswith(tok):
            base = base[: -len(tok)]
            break

    candidates = [
        (base + "_finalInterval_vert_withStartPos.csv", base + "_finalInterval_horz_withStartPos.csv"),
        (base + "_finalInterval_vert.csv",             base + "_finalInterval_horz.csv"),
    ]

    for vert_name, horz_name in candidates:
        vert = events_path.with_name(vert_name)
        horz = events_path.with_name(horz_name)

        if interval_root is not None:
            vert = interval_root / vert.name
            horz = interval_root / horz.name

        if vert.exists() and horz.exists():
            return vert, horz

    return None, None


def _pick_time_col(df: pd.DataFrame) -> Optional[str]:
    # prefer start_AppTime, then AppTime, then end_AppTime
    for c in ("start_AppTime", "AppTime", "end_AppTime"):
        if c in df.columns:
            return c
    return None


def _coerce_numeric(df: pd.DataFrame, cols: List[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def _next_roundnum_from_next_roundstart(
    df: pd.DataFrame,
    *,
    time_col: str,
    block_keys: Tuple[str, str] = ("BlockNum", "BlockInstance"),
) -> pd.Series:
    """
    For each InterRound row, find the NEXT RoundStart in the same (BlockNum, BlockInstance) by time,
    and return that RoundStart's RoundNum.

    Returns: Series aligned to df.index (values ONLY for InterRound rows; others NA).
    """
    d = df.copy()

    # stable local row id
    if "__rowid" not in d.columns:
        d["__rowid"] = np.arange(len(d), dtype=int)

    # normalize strings
    d["lo_eventType"] = d["lo_eventType"].astype("string").str.strip()

    # coerce numeric keys + time + roundnum
    _coerce_numeric(d, [*block_keys, "RoundNum", time_col])

    # must be sorted for merge_asof
    d = d.sort_values([*block_keys, time_col], kind="mergesort", na_position="last")

    # RoundStart lookup table
    rs = d[d["lo_eventType"] == ROUNDSTART_EVENT][
        ["__rowid", *block_keys, time_col, "RoundNum"]
    ].dropna(subset=[*block_keys, time_col, "RoundNum"]).copy()

    rs = rs.rename(
        columns={
            "__rowid": "__rs_rowid",
            time_col: "__rs_time",
            "RoundNum": "__rs_round",
        }
    ).sort_values([*block_keys, "__rs_time"], kind="mergesort", na_position="last")

    # InterRound table
    inter = d[d["lo_eventType"] == INTER_EVENT][
        ["__rowid", *block_keys, time_col]
    ].dropna(subset=[*block_keys, time_col]).copy()

    inter = inter.rename(
        columns={
            "__rowid": "__inter_rowid",
            time_col: "__inter_time",
        }
    ).sort_values([*block_keys, "__inter_time"], kind="mergesort", na_position="last")

    out = pd.Series(pd.NA, index=df.index, dtype="Float64")
    if inter.empty or rs.empty:
        return out

    # asof forward: next RoundStart after inter time
    linked = pd.merge_asof(
        inter,
        rs,
        left_on="__inter_time",
        right_on="__rs_time",
        by=list(block_keys),
        direction="forward",
        allow_exact_matches=False,
    )

    # Keep only successful matches (where __rs_round is notna)
    linked_ok = linked[linked["__rs_round"].notna()].copy()
    if linked_ok.empty:
        return out

    # Build mapping: inter_rowid -> rs_round
    # (avoid astype(int) on NA by filtering above)
    m = dict(zip(linked_ok["__inter_rowid"].astype(int), linked_ok["__rs_round"].astype(float)))

    # Map back to original df rows (by their __rowid)
    if "__rowid" in df.columns:
        rowid = pd.to_numeric(df["__rowid"], errors="coerce").astype("Int64")
    else:
        rowid = pd.Series(np.arange(len(df), dtype=int), index=df.index).astype("Int64")

    is_inter_orig = df["lo_eventType"].astype("string").str.strip() == INTER_EVENT
    out.loc[is_inter_orig] = rowid.loc[is_inter_orig].map(m).astype("Float64")
    return out


def fix_one_events_file(
    in_events: Path,
    *,
    out_root: Optional[Path],
    interval_root: Optional[Path],
    also_update_intervals: bool,
    max_round: int,
    dry_run: bool,
) -> None:
    df = pd.read_csv(in_events).copy()

    # Hard guard: only process events files produced by your pipeline
    required_cols = {"lo_eventType", "BlockType", "BlockNum", "BlockInstance", "RoundNum", "startPos", "startPos_dist"}
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"[skip] {in_events.name}: missing {missing} (not an events file we can fix).")
        return

    # Ensure rowid exists and is stable
    if "__rowid" not in df.columns:
        df["__rowid"] = np.arange(len(df), dtype=int)

    # normalize text cols
    df["BlockType"] = df["BlockType"].astype("string").str.strip().str.lower()
    df["lo_eventType"] = df["lo_eventType"].astype("string").str.strip()

    is_pindrop = df["BlockType"] == "pindropping"
    is_inter = is_pindrop & (df["lo_eventType"] == INTER_EVENT)

    if not is_pindrop.any():
        out_path = _infer_out_path(in_events, out_root)
        _ensure_parent(out_path)
        if dry_run:
            print(f"[dry-run] no pindropping in {in_events.name}; would copy -> {out_path}")
        else:
            df.drop(columns=["__rowid"], errors="ignore").to_csv(out_path, index=False)
            print(f"[ok] no pindropping in {in_events.name}; wrote -> {out_path}")
        return

    # Must have at least one InterRound with a startPos to propagate
    inter_with_pos = is_inter & df["startPos"].notna()
    if not inter_with_pos.any():
        out_path = _infer_out_path(in_events, out_root)
        _ensure_parent(out_path)
        if dry_run:
            print(f"[dry-run] no usable InterRound startPos rows in {in_events.name}; would copy -> {out_path}")
        else:
            df.drop(columns=["__rowid"], errors="ignore").to_csv(out_path, index=False)
            print(f"[ok] no usable InterRound startPos rows in {in_events.name}; wrote -> {out_path}")
        return

    # pick time column
    time_col = _pick_time_col(df)
    if time_col is None:
        raise ValueError(f"{in_events.name}: no usable time column found (start_AppTime/AppTime/end_AppTime).")

    # coerce numeric for time + keys + roundnum
    _coerce_numeric(df, ["BlockNum", "BlockInstance", "RoundNum", time_col])

    # Fix InterRound RoundNum when it's missing OR sentinel/out-of-range (> max_round)
    needs_round_fix = is_inter & (df["RoundNum"].isna() | (df["RoundNum"] > max_round))
    if needs_round_fix.any():
        next_rs_round = _next_roundnum_from_next_roundstart(df, time_col=time_col)
        fillable = needs_round_fix & next_rs_round.notna()
        df.loc[fillable, "RoundNum"] = next_rs_round.loc[fillable]

    # Build per-round startPos map from InterRound rows (after RoundNum fix)
    keys = ["BlockNum", "BlockInstance", "RoundNum"]
    per_round = df.loc[inter_with_pos, keys + ["startPos", "startPos_dist", time_col]].copy()
    per_round = per_round[per_round["RoundNum"].notna() & (per_round["RoundNum"] <= max_round)].copy()

    if per_round.empty:
        out_path = _infer_out_path(in_events, out_root)
        _ensure_parent(out_path)
        if dry_run:
            print(f"[dry-run] after RoundNum fix, no InterRound rows <= max_round in {in_events.name}; would copy -> {out_path}")
        else:
            df.drop(columns=["__rowid"], errors="ignore").to_csv(out_path, index=False)
            print(f"[ok] after RoundNum fix, no InterRound rows <= max_round in {in_events.name}; wrote -> {out_path}")
        return

    # Dedup: keep earliest InterRound per (BlockNum, BlockInstance, RoundNum)
    per_round = per_round.sort_values(keys + [time_col], kind="mergesort", na_position="last")
    per_round = per_round.drop_duplicates(subset=keys, keep="first")[keys + ["startPos", "startPos_dist"]].copy()

    # Broadcast fill ONLY within pindropping rows
    df = df.merge(per_round, on=keys, how="left", suffixes=("", "__pindrop"))
    df.loc[is_pindrop, "startPos"] = df.loc[is_pindrop, "startPos"].fillna(df.loc[is_pindrop, "startPos__pindrop"])
    df.loc[is_pindrop, "startPos_dist"] = df.loc[is_pindrop, "startPos_dist"].fillna(df.loc[is_pindrop, "startPos_dist__pindrop"])
    df = df.drop(columns=["startPos__pindrop", "startPos_dist__pindrop", "__rowid"], errors="ignore")

    # Write events output
    out_path = _infer_out_path(in_events, out_root, suffix="_pindropFixed")
    _ensure_parent(out_path)
    if dry_run:
        print(f"[dry-run] would write events: {out_path}")
    else:
        df.to_csv(out_path, index=False)
        print(f"[ok] wrote events: {out_path}")

    if not also_update_intervals:
        return

    # Update interval tables (no BlockType/lo_eventType needed there)
    vert_path, horz_path = _infer_interval_paths(in_events, interval_root)
    if vert_path is None or horz_path is None:
        print(f"[skip] intervals not found for {in_events.name}")
        return

    vert = pd.read_csv(vert_path)
    horz = pd.read_csv(horz_path)

    vert_out = vert.merge(per_round, on=keys, how="left", suffixes=("", "__pindrop"))
    horz_out = horz.merge(per_round, on=keys, how="left", suffixes=("", "__pindrop"))

    for out_df in (vert_out, horz_out):
        if "startPos__pindrop" in out_df.columns:
            if "startPos" in out_df.columns:
                out_df["startPos"] = out_df["startPos"].fillna(out_df["startPos__pindrop"])
            else:
                out_df["startPos"] = out_df["startPos__pindrop"]
        if "startPos_dist__pindrop" in out_df.columns:
            if "startPos_dist" in out_df.columns:
                out_df["startPos_dist"] = out_df["startPos_dist"].fillna(out_df["startPos_dist__pindrop"])
            else:
                out_df["startPos_dist"] = out_df["startPos_dist__pindrop"]
        out_df.drop(columns=["startPos__pindrop", "startPos_dist__pindrop"], inplace=True, errors="ignore")

    vert_out_path = _infer_out_path(vert_path, out_root, suffix="_pindropFixed")
    horz_out_path = _infer_out_path(horz_path, out_root, suffix="_pindropFixed")
    _ensure_parent(vert_out_path)
    _ensure_parent(horz_out_path)

    if dry_run:
        print(f"[dry-run] would write intervals: {vert_out_path}")
        print(f"[dry-run] would write intervals: {horz_out_path}")
    else:
        vert_out.to_csv(vert_out_path, index=False)
        horz_out.to_csv(horz_out_path, index=False)
        print(f"[ok] wrote intervals: {vert_out_path}")
        print(f"[ok] wrote intervals: {horz_out_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Fix pindropping startPos: match InterRound to next RoundStart RoundNum, then propagate within (BlockNum, BlockInstance, RoundNum).")
    ap.add_argument("--root", required=True, help="Root dir to search for events CSVs")
    ap.add_argument("--pattern", default="*_event_reproc_withStartPos.csv", help="Glob for events files (default: *_event_reproc_withStartPos.csv)")
    ap.add_argument("--out-root", default="", help="Optional flat output dir (otherwise next to inputs)")
    ap.add_argument("--interval-root", default="", help="Optional where interval files live")
    ap.add_argument("--also-update-intervals", action="store_true")
    ap.add_argument("--max-round", type=int, default=100)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = Path(args.root).expanduser()
    out_root = Path(args.out_root).expanduser() if args.out_root else None
    interval_root = Path(args.interval_root).expanduser() if args.interval_root else None

    files = sorted(root.rglob(args.pattern))
    print(f"[scan] found {len(files)} files under {root} matching {args.pattern}")

    for f in files:
        try:
            fix_one_events_file(
                f,
                out_root=out_root,
                interval_root=interval_root,
                also_update_intervals=args.also_update_intervals,
                max_round=args.max_round,
                dry_run=args.dry_run,
            )
        except Exception as e:
            print(f"[fail] {f}: {e}")

    print("[done]")


if __name__ == "__main__":
    main()
