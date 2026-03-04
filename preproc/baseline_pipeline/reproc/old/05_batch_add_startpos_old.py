#!/usr/bin/env python3
# 05_batch_add_startpos.py

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Adjust this import to match wherever you placed startPosAssignment.py
# (e.g., RC_utilities/reProcHelpers/startPosAssignment.py)
from RC_utilities.reProcHelpers.startPosAssignment import compute_startpos_for_events_flexible


DEFAULT_EVENT_PRIORITY = ["TrueContentStart", "RoundStart"]  # prefer TrueContentStart if both exist


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _infer_out_path(in_path: Path, out_root: Optional[Path], suffix: str = "_withStartPos") -> Path:
    if out_root is None:
        return in_path.with_name(in_path.stem + suffix + in_path.suffix)
    rel = in_path.name
    return out_root / (Path(rel).stem + suffix + Path(rel).suffix)

def _label_subset(
    df: pd.DataFrame,
    events_of_interest: Dict[str, str],
    *,
    role_col: str,
    x_start_col: str,
    z_start_col: str,
    x_end_col: str,
    z_end_col: str,
    add_used_xy_cols: bool,
    strict: bool,
    strict_roles: bool,
) -> pd.DataFrame:
    # Only keep rows that can be labeled by the mapping
    mask = df["lo_eventType"].isin(events_of_interest.keys())
    if not mask.any():
        return df[["__rowid"]].assign(startPos=pd.NA, startPos_dist=pd.NA)

    sub = df.loc[mask, ["__rowid", "lo_eventType", role_col, x_start_col, z_start_col, x_end_col, z_end_col]].copy()

    labeled = compute_startpos_for_events_flexible(
        sub,
        events_of_interest,
        role_col=role_col,
        x_start_col=x_start_col,
        z_start_col=z_start_col,
        x_end_col=x_end_col,
        z_end_col=z_end_col,
        startpos_label_col="startPos",
        startpos_dist_col="startPos_dist",
        add_used_xy_cols=add_used_xy_cols,
        strict=strict,
        strict_roles=strict_roles,
    )
    return labeled[["__rowid", "startPos", "startPos_dist"]]


def _infer_interval_paths(events_path: Path, interval_root: Optional[Path]) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Best-effort inference:
      <stem>_event_reproc.csv or <stem>_events_pre_reproc.csv
      -> <stem>_finalInterval_vert.csv and <stem>_finalInterval_horz.csv
    """
    stem = events_path.stem
    # common variants
    for token in ["_event_reproc", "_events_pre_reproc", "_events_final", "_events"]:
        if stem.endswith(token):
            base = stem[: -len(token)]
            break
    else:
        base = stem

    vert = events_path.with_name(base + "_finalInterval_vert.csv")
    horz = events_path.with_name(base + "_finalInterval_horz.csv")

    if interval_root is not None:
        vert = interval_root / vert.name
        horz = interval_root / horz.name

    if not vert.exists():
        vert = None
    if not horz.exists():
        horz = None
    return vert, horz


# def _build_events_of_interest(event_types: List[str], direction: str = "start") -> Dict[str, str]:
#     direction = direction.lower().strip()
#     if direction not in ("start", "end"):
#         raise ValueError("direction must be 'start' or 'end'")
#     return {et: direction for et in event_types}


def _one_startpos_per_round(
    events_labeled: pd.DataFrame,
    *,
    key_cols: Tuple[str, str, str] = ("BlockNum", "BlockInstance", "RoundNum"),
    event_type_col: str = "lo_eventType",
    time_col: str = "start_AppTime",
    startpos_col: str = "startPos",
    dist_col: str = "startPos_dist",
    priority: List[str] = DEFAULT_EVENT_PRIORITY,
    max_round: int = 100,
) -> pd.DataFrame:
    ev = events_labeled.copy()

    # Keep only rounds <= max_round when present
    if "RoundNum" in ev.columns:
        ev["RoundNum"] = pd.to_numeric(ev["RoundNum"], errors="coerce")
        ev = ev[ev["RoundNum"].notna() & (ev["RoundNum"] <= max_round)].copy()

    # only keep events that actually have a startPos computed
    ev = ev[ev[startpos_col].notna()].copy()
    if ev.empty:
        return pd.DataFrame(list(key_cols) + [startpos_col, dist_col])

    rank_map = {k: i for i, k in enumerate(priority)}
    ev["__rank"] = ev[event_type_col].map(rank_map).fillna(999).astype(int)

    sort_cols = list(key_cols) + ["__rank"]
    if time_col in ev.columns:
        ev[time_col] = pd.to_numeric(ev[time_col], errors="coerce")
        sort_cols.append(time_col)

    ev = ev.sort_values(sort_cols, kind="mergesort")
    out = ev.drop_duplicates(subset=list(key_cols), keep="first")[
        list(key_cols) + [startpos_col, dist_col]
    ].copy()

    return out


def process_one_events_file(
    events_csv: Path,
    *,
    out_root: Optional[Path],
    event_types: List[str],
    role_col: str = "currentRole",
    x_start_col: str,
    z_start_col: str,
    x_end_col: str,
    z_end_col: str,
    add_used_xy_cols: bool,
    strict: bool,
    strict_roles: bool,
    also_update_intervals: bool,
    interval_root: Optional[Path],
    max_round: int,
    dry_run: bool,
) -> None:
    df = pd.read_csv(events_csv)

    # Stable join key so we can label a subset and merge back without reindex issues
    df = df.copy()
    df["__rowid"] = np.arange(len(df), dtype=int)

    #events_of_interest differs for the two different BlockTypes 
    eventsOfInterest_collecting = {
        "TrueContentStart": "start", # collecting
    }
    eventsOfInterest_pinDropping = {
        "InterRound_PostCylinderWalk_segment": "start", # pindropping
    }

    # Ensure we have BlockType
    if "BlockType" not in df.columns:
        raise ValueError(f"{events_csv.name} is missing BlockType; can't split collecting vs pindropping.")

    # Compute labels separately, then roll up
    collecting_df = df[df["BlockType"] == "collecting"].copy()
    pindrop_df   = df[df["BlockType"] == "pindropping"].copy()

    labeled_collecting = _label_subset(
        collecting_df,
        eventsOfInterest_collecting,
        role_col=role_col,
        x_start_col=x_start_col,
        z_start_col=z_start_col,
        x_end_col=x_end_col,
        z_end_col=z_end_col,
        add_used_xy_cols=add_used_xy_cols,
        strict=strict,
        strict_roles=strict_roles,
    )

    labeled_pindrop = _label_subset(
        pindrop_df,
        eventsOfInterest_pinDropping,
        role_col=role_col,
        x_start_col=x_start_col,
        z_start_col=z_start_col,
        x_end_col=x_end_col,
        z_end_col=z_end_col,
        add_used_xy_cols=add_used_xy_cols,
        strict=strict,
        strict_roles=strict_roles,
    )

    labeled_subset = pd.concat([labeled_collecting, labeled_pindrop], ignore_index=True)

    # Merge startPos back into full events
    df_out = df.merge(labeled_subset, on="__rowid", how="left").drop(columns=["__rowid"], errors="ignore")


    # Merge startPos back into full events; only selected event types will be filled
    df_out = df.merge(
        labeled_subset[["__rowid", "startPos", "startPos_dist"]],
        on="__rowid",
        how="left",
    ).drop(columns=["__rowid"], errors="ignore")

    out_path = _infer_out_path(events_csv, out_root, suffix="_withStartPos")
    _ensure_parent(out_path)

    if dry_run:
        print(f"[dry-run] would write: {out_path}")
    else:
        df_out.to_csv(out_path, index=False)
        print(f"[ok] wrote events: {out_path}")

    if not also_update_intervals:
        return

    vert_path, horz_path = _infer_interval_paths(events_csv, interval_root)
    if vert_path is None or horz_path is None:
        print(f"[skip] intervals not found for {events_csv.name} (expected *_finalInterval_vert/horz.csv)")
        return

    vert = pd.read_csv(vert_path)
    horz = pd.read_csv(horz_path)

    # Build one row per round for merging into intervals
    per_round = _one_startpos_per_round(
        df_out,
        priority=DEFAULT_EVENT_PRIORITY,
        max_round=max_round,
    )

    merge_keys = ["BlockNum", "BlockInstance", "RoundNum"]
    vert_out = vert.merge(per_round, on=merge_keys, how="left")
    horz_out = horz.merge(per_round, on=merge_keys, how="left")

    vert_out_path = _infer_out_path(vert_path, out_root, suffix="_withStartPos")
    horz_out_path = _infer_out_path(horz_path, out_root, suffix="_withStartPos")
    _ensure_parent(vert_out_path)
    _ensure_parent(horz_out_path)

    if dry_run:
        print(f"[dry-run] would write: {vert_out_path}")
        print(f"[dry-run] would write: {horz_out_path}")
    else:
        vert_out.to_csv(vert_out_path, index=False)
        horz_out.to_csv(horz_out_path, index=False)
        print(f"[ok] wrote intervals: {vert_out_path}")
        print(f"[ok] wrote intervals: {horz_out_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Batch add startPos to events (selected lo_eventType only) and optionally interval tables.")
    ap.add_argument("--root", required=True, help="Root directory to search under")
    ap.add_argument("--pattern", default="*_events*.csv", help="Glob pattern to find events CSVs (default: '*_events*.csv')")
    ap.add_argument("--out-root", default="", help="If set, write outputs here (flat). Otherwise next to inputs.")
    ap.add_argument("--interval-root", default="", help="Optional: where interval files live (if not alongside events).")

    ap.add_argument("--event-types", default="TrueContentStart,RoundStart",
                    help="Comma-separated lo_eventTypes to label (default: TrueContentStart,RoundStart)")
    # ap.add_argument("--direction", default="start", choices=["start", "end"],
    #                 help="Whether those event types should use start coords or end coords (default: start)")

    ap.add_argument("--role-col", default="currentRole")
    # IMPORTANT: defaults match your reproc-augmented events naming
    ap.add_argument("--x-start-col", default="HeadPosAnchored_x_start")
    ap.add_argument("--z-start-col", default="HeadPosAnchored_z_start")
    ap.add_argument("--x-end-col", default="HeadPosAnchored_x_end") # legacy, not actually used anymore
    ap.add_argument("--z-end-col", default="HeadPosAnchored_z_end") # legacy, not actually used anymore

    ap.add_argument("--add-used-xy-cols", action="store_true",
                    help="If set, keep debug cols like startPos_x_used/startPos_z_used in labeled subset")
    ap.add_argument("--no-strict", action="store_true",
                    help="If set, missing cols will not error; startPos will be NA where needed.")
    ap.add_argument("--no-strict-roles", action="store_true",
                    help="If set, role values outside AN/PO won't error; those rows will be NA.")

    ap.add_argument("--also-update-intervals", action="store_true",
                    help="If set, merges one startPos per round into *_finalInterval_vert/horz.csv")
    ap.add_argument("--max-round", type=int, default=100,
                    help="Exclude RoundNum > max-round when building per-round startPos for intervals (default: 100)")

    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = Path(args.root).expanduser()
    out_root = Path(args.out_root).expanduser() if args.out_root else None
    interval_root = Path(args.interval_root).expanduser() if args.interval_root else None

    event_types = [s.strip() for s in args.event_types.split(",") if s.strip()]

    strict = not args.no_strict
    strict_roles = not args.no_strict_roles

    files = sorted(root.rglob(args.pattern))
    print(f"[scan] found {len(files)} files under {root} matching {args.pattern}")

    for f in files:
        try:
            process_one_events_file(
                f,
                out_root=out_root,
                event_types=event_types,
                role_col=args.role_col,
                x_start_col=args.x_start_col,
                z_start_col=args.z_start_col,
                x_end_col=args.x_end_col,
                z_end_col=args.z_end_col,
                add_used_xy_cols=args.add_used_xy_cols,
                strict=strict,
                strict_roles=strict_roles,
                also_update_intervals=args.also_update_intervals,
                interval_root=interval_root,
                max_round=args.max_round,
                dry_run=args.dry_run,
            )
        except Exception as e:
            print(f"[fail] {f}: {e}")

    print("[done]")


if __name__ == "__main__":
    main()
