#!/usr/bin/env python3
# 00_batch_reproc_steps_01_04.py

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd

from RC_utilities.reProcHelpers.helpers_reproc import (
    build_block_intervals,
    build_round_intervals,
    merge_block_and_round_intervals,
    cleanup_merge_suffixes,
    augment_processed_with_intervals,
    compute_step_distance,
    aggregate_total_distance,
    compute_speed,
    augment_events_from_reprocessed,
    extract_pindrop_moments,
    intervals_add_pindrops_wide,
    compute_cumulative_path,
    safe_merge,
    normalize_keys,
    DEFAULT_MAX_TRUE_ROUNDNUM,
    ensure_or_validate_origRow,
    choose_round_mode,
)

# Optional canonicalizer (only used if import succeeds)
def _try_canonicalize(events_df: pd.DataFrame) -> pd.DataFrame:
    try:
        from makeItCannonical import canonicalize_event_order
        return canonicalize_event_order(
            events_df,
            start_col="start_AppTime",
            end_col="end_AppTime",
            group_first=False,
        )
    except Exception:
        if "start_AppTime" in events_df.columns:
            return events_df.sort_values("start_AppTime", kind="mergesort", na_position="last")
        return events_df


@dataclass(frozen=True)
class OutRoots:
    intervals_dir: Path
    prelim_reproc_dir: Path
    reproc_dir: Path
    events_pre_dir: Path
    events_reproc_dir: Path


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _infer_base_from_events(events_path: Path) -> str:
    """
    Converts:
      ObsReward_A_..._events_pos.csv  -> ObsReward_A_...
      ObsReward_A_..._events.csv      -> ObsReward_A_...
      ObsReward_A_..._processed_events.csv -> ObsReward_A_...
    """
    stem = events_path.stem
    for suf in (
        "_events_pos",
        "_events_pre_reproc",
        "_event_reproc",
        "_processed_events_pos",
        "_processed_events",
        "_events",
        "_events_coinLabel",
    ):
        if stem.endswith(suf):
            return stem[: -len(suf)]
    return stem


def _paths_for_one(base: str, roots: OutRoots) -> dict[str, Path]:
    return {
        "blocks": roots.intervals_dir / f"{base}_blockIntervals.csv",
        "rounds": roots.intervals_dir / f"{base}_roundIntervals.csv",
        "combined": roots.intervals_dir / f"{base}_prelim_blkRndInt.csv",
        "prelim_reproc": roots.prelim_reproc_dir / f"{base}_prelim_reproc.csv",
        "reprocessed": roots.reproc_dir / f"{base}_reprocessed.csv",
        "events_pre": roots.events_pre_dir / f"{base}_events_pre_reproc.csv",
        "interval_horz": roots.intervals_dir / f"{base}_finalInterval_horz.csv",
        "interval_vert": roots.intervals_dir / f"{base}_finalInterval_vert.csv",
        "events_final": roots.events_reproc_dir / f"{base}_event_reproc.csv",
    }


def _maybe_skip(path: Path, skip_existing: bool) -> bool:
    return skip_existing and path.exists()


def run_steps_01_to_04_for_file(
    *,
    events_csv: Path,
    processed_csv: Path,
    roots: OutRoots,
    max_round: int,
    round_mode: str,
    pos_cols: Sequence[str],
    group_for_diff: Sequence[str],
    skip_existing: bool,
) -> None:
    base = _infer_base_from_events(events_csv)
    outs = _paths_for_one(base, roots)

    # Make dirs
    for d in (
        roots.intervals_dir,
        roots.prelim_reproc_dir,
        roots.reproc_dir,
        roots.events_pre_dir,
        roots.events_reproc_dir,
    ):
        _ensure_dir(d)
        print(d)

    # -------------------
    # 01_build_intervals
    # -------------------
    print("✨"*30)
    print("starting 01_build_intervals")
    if _maybe_skip(outs["combined"], skip_existing) and _maybe_skip(outs["blocks"], skip_existing) and _maybe_skip(outs["rounds"], skip_existing):
        print(f"[skip 01] {base} (interval outputs exist)")
        blocks = pd.read_csv(outs["blocks"])
        rounds = pd.read_csv(outs["rounds"])
        combined = pd.read_csv(outs["combined"])
    else:
        events = pd.read_csv(events_csv)
        if "origRow_start" not in events.columns:
            raise ValueError(f"{events_csv.name} is missing origRow_start; cannot build origRow-anchored intervals")

        processed = pd.read_csv(processed_csv)


        blocks = build_block_intervals(events, processed_df=processed)
        #mode = args.round_mode
        if round_mode == "auto":
            round_mode = choose_round_mode(events, blocks, max_round=max_round)
        rounds = build_round_intervals(events, blocks, mode=round_mode, max_round=max_round)

        #rounds = build_round_intervals(events, blocks, mode=auto_mode, max_round=max_round)
        #rounds = build_round_intervals(events, blocks, mode=round_mode, max_round=max_round)
        combined = merge_block_and_round_intervals(blocks, rounds)

        combined = cleanup_merge_suffixes(combined, suffixes=("_r",), numeric_tol=0.0)
        blocks = cleanup_merge_suffixes(blocks, suffixes=("_r",), numeric_tol=0.0)
        rounds = cleanup_merge_suffixes(rounds, suffixes=("_r",), numeric_tol=0.0)

        blocks.to_csv(outs["blocks"], index=False)
        rounds.to_csv(outs["rounds"], index=False)
        combined.to_csv(outs["combined"], index=False)
        print(f"[ok 01] {base}")

    # -------------------------------------------
    # 02_reproc_processed_add_elapsed_and_distance
    # -------------------------------------------
    print("✨"*30)
    print("02_reproc_processed_add_elapsed_and_distance")
    if _maybe_skip(outs["prelim_reproc"], skip_existing):
        print(f"[skip 02] {base} (prelim_reproc exists)")
        prelim = pd.read_csv(outs["prelim_reproc"])
    else:
        proc = pd.read_csv(processed_csv)
        proc = ensure_or_validate_origRow(proc, strict_sequential=True)
        print("[debug] blocks cols:", [repr(c) for c in blocks.columns])
        print("[debug] rounds cols:", [repr(c) for c in rounds.columns])
        print("[debug] rounds shape:", rounds.shape)

        df = augment_processed_with_intervals(proc, blocks, rounds, max_round=max_round)

        df = compute_step_distance(
            df,
            pos_cols=list(pos_cols),
            time_col="AppTime",
            group_cols=list(group_for_diff),
            out_dt="dt",
            out_step="stepDist",
        )

        df = aggregate_total_distance(
            df,
            group_keys=["BlockInstance", "BlockNum"],
            step_col="stepDist",
            out_col="totDistBlock",
            how="sum",
        )

        df = aggregate_total_distance(
            df,
            group_keys=["BlockInstance", "BlockNum", "RoundNum_interval_assigned"],
            step_col="stepDist",
            out_col="totDistRound",
            how="sum",
        )

        # Running cumulative distances
        df["totDistBlock_current"] = df.groupby(["BlockInstance", "BlockNum"], dropna=False)["stepDist"].cumsum()
        df["totDistRound_current"] = df.groupby(
            ["BlockInstance", "BlockNum", "RoundNum_interval_assigned"], dropna=False
        )["stepDist"].cumsum()

        df["inTrueRound"] = df["RoundNum_interval_assigned"].notna()

        df.to_csv(outs["prelim_reproc"], index=False)
        prelim = df
        print(f"[ok 02] {base}")

    # -----------------
    # 03_compute_speed
    # -----------------
    print("✨"*30)
    print("03_compute_speed")
    if _maybe_skip(outs["reprocessed"], skip_existing):
        print(f"[skip 03] {base} (reprocessed exists)")
        reproc = pd.read_csv(outs["reprocessed"])
    else:
        df = prelim.copy()
        df = compute_speed(df, step_col="stepDist", dt_col="dt", out_col="currSpeed")
        df.to_csv(outs["reprocessed"], index=False)
        reproc = df
        print(f"[ok 03] {base}")

    # ---------------------------------------------------
    # 04_finalize_events_and_intervals_with_pindrops (no startPos here)
    # ---------------------------------------------------
    print("✨"*30)
    print("04_finalize_events_and_intervals_with_pindrops")
    if _maybe_skip(outs["events_final"], skip_existing) and _maybe_skip(outs["interval_vert"], skip_existing) and _maybe_skip(outs["interval_horz"], skip_existing):
        print(f"[skip 04] {base} (final outputs exist)")
        return

    events = pd.read_csv(events_csv)
    intervals = pd.read_csv(outs["combined"])

    metrics = [
        "totDistRound", "totDistBlock", "currSpeed",
        "roundElapsed_s", "blockElapsed_s", "totalSessionElapsed_s",
        "roundFrac", "blockFrac",
        "dt", "stepDist",
        "totDistBlock_current", "totDistRound_current",
    ]
    pos_cols_events = ["HeadPosAnchored_x", "HeadPosAnchored_y", "HeadPosAnchored_z"]

    events_pre = augment_events_from_reprocessed(events, reproc, metrics + pos_cols_events)
    events_pre = _try_canonicalize(events_pre)
    events_pre = cleanup_merge_suffixes(events_pre, suffixes=("_int", "_r"))
    events_pre.to_csv(outs["events_pre"], index=False)

    pins = extract_pindrop_moments(events_pre)
    if "RoundNum" in pins.columns:
        pins["RoundNum"] = pd.to_numeric(pins["RoundNum"], errors="coerce")
        pins = pins[(pins["RoundNum"] >= 1) & (pins["RoundNum"] <= max_round)].copy()

    interval_horz = intervals_add_pindrops_wide(intervals, pins)

    keys = ["BlockInstance", "BlockNum", "RoundNum"]
    interval_vert = safe_merge(
        normalize_keys(intervals.copy(), keys, inplace=False),
        normalize_keys(pins.copy(), keys + ["chestPin_num"], inplace=False),
        keys,
        how="left",
        validate="1:m",
        indicator=False,
    )

    # Path order
    if "chestPin_num" in interval_vert.columns:
        long_with_steps, path_strings = compute_cumulative_path(
            interval_vert[interval_vert["chestPin_num"].notna()].copy(),
            group_keys=tuple(keys),
            order_key="chestPin_num",
            label_col="coinLabel",
            out_step_col="path_step_in_round",
            out_path_col="path_order_round",
        )
        interval_vert = safe_merge(interval_vert, path_strings, keys, how="left", validate="m:1", indicator=False)
        interval_horz = safe_merge(interval_horz, path_strings, keys, how="left", validate="1:1", indicator=False)

        step_cols = keys + ["chestPin_num", "path_step_in_round"]
        interval_vert = safe_merge(
            interval_vert, long_with_steps[step_cols], keys + ["chestPin_num"],
            how="left", validate="m:1", indicator=False, suffixes=("", "_step")
        )
        if "path_step_in_round_step" in interval_vert.columns:
            interval_vert["path_step_in_round"] = interval_vert["path_step_in_round"].fillna(interval_vert["path_step_in_round_step"])
            interval_vert = interval_vert.drop(columns=["path_step_in_round_step"], errors="ignore")

    interval_horz = cleanup_merge_suffixes(interval_horz, suffixes=("_int", "_r"))
    interval_vert = cleanup_merge_suffixes(interval_vert, suffixes=("_int", "_r"))
    interval_horz.to_csv(outs["interval_horz"], index=False)
    interval_vert.to_csv(outs["interval_vert"], index=False)

    # Re-augment PinDrop rows onto events
    ev_final = events_pre.copy()
    is_pin = (ev_final["lo_eventType"].astype("string") == "PinDrop_Moment") if "lo_eventType" in ev_final.columns else False

    pin_rows = ev_final[is_pin].copy()
    other_rows = ev_final[~is_pin].copy()

    join_keys = keys + (["chestPin_num"] if ("chestPin_num" in pin_rows.columns and "chestPin_num" in interval_vert.columns) else [])
    if join_keys:
        pin_aug = safe_merge(
            normalize_keys(pin_rows, join_keys, inplace=False),
            normalize_keys(interval_vert, join_keys, inplace=False),
            join_keys,
            how="left",
            validate="m:1",
            suffixes=("", "_int"),
            indicator=False,
        )
        ev_final = pd.concat([other_rows, pin_aug], ignore_index=True)

    ev_final = _try_canonicalize(ev_final)
    ev_final = cleanup_merge_suffixes(ev_final, suffixes=("_int", "_r"))
    ev_final.to_csv(outs["events_final"], index=False)
    print(f"[ok 04] {base}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Batch-run reproc steps 01–04 over all events/processed pairs.")
    ap.add_argument("--events-root", required=True, help="Directory containing last viable events (e.g. .../EventSegmentation/Events_Pos)")
    ap.add_argument("--processed-root", required=True, help="Directory containing *_processed.csv (e.g. .../ProcessedData_Flat)")

    ap.add_argument("--intervals-dir", required=True, help="Output dir for interval tables")
    ap.add_argument("--prelim-reproc-dir", required=True, help="Output dir for *_prelim_reproc.csv")
    ap.add_argument("--reproc-dir", required=True, help="Output dir for *_reprocessed.csv")
    ap.add_argument("--events-pre-dir", required=True, help="Output dir for *_events_pre_reproc.csv")
    ap.add_argument("--events-reproc-dir", required=True, help="Output dir for *_event_reproc.csv")

    ap.add_argument("--pattern", default="*_events_pos.csv", help="Glob under events-root (default: *_events_pos.csv)")
    ap.add_argument("--max-round", type=int, default=DEFAULT_MAX_TRUE_ROUNDNUM)
    #ap.add_argument("--round-mode", default="truecontent", choices=["truecontent", "roundstartend"])

    ap.add_argument("--pos-cols", nargs="+", default=["HeadPosAnchored_x", "HeadPosAnchored_y", "HeadPosAnchored_z"])
    ap.add_argument("--group-for-diff", nargs="+", default=["BlockInstance", "BlockNum"])

    ap.add_argument("--skip-existing", action="store_true", help="Skip files whose outputs already exist.")
    ap.add_argument("--round-mode", default="auto", choices=["auto","truecontent","roundstartend"])

    args = ap.parse_args()

    events_root = Path(args.events_root).expanduser()
    processed_root = Path(args.processed_root).expanduser()

    roots = OutRoots(
        intervals_dir=Path(args.intervals_dir).expanduser(),
        prelim_reproc_dir=Path(args.prelim_reproc_dir).expanduser(),
        reproc_dir=Path(args.reproc_dir).expanduser(),
        events_pre_dir=Path(args.events_pre_dir).expanduser(),
        events_reproc_dir=Path(args.events_reproc_dir).expanduser(),
    )

    files = sorted(events_root.rglob(args.pattern))
    print(f"[scan] {len(files)} events files under {events_root} matching {args.pattern}")

    for ev_path in files:
        base = _infer_base_from_events(ev_path)
        proc_path = processed_root / f"{base}_processed.csv"
        if not proc_path.exists():
            print(f"[skip] missing processed for {ev_path.name} -> {proc_path.name}")
            continue

        try:
            print(f"\n[run] {base}")
            run_steps_01_to_04_for_file(
                events_csv=ev_path,
                processed_csv=proc_path,
                roots=roots,
                max_round=args.max_round,
                round_mode=args.round_mode,
                pos_cols=args.pos_cols,
                group_for_diff=args.group_for_diff,
                skip_existing=args.skip_existing,
            )
        except Exception as e:
            print(f"[FAIL] {base}: {e}")

    print("\n[done]")


if __name__ == "__main__":
    main()
