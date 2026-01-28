#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import pandas as pd

## helpers_reproc
from RC_utilities.reProcHelpers.helpers_reproc import (
    augment_events_from_reprocessed,
    extract_pindrop_moments,
    intervals_add_pindrops_wide,
    compute_cumulative_path,
    safe_merge,
    normalize_keys,
    DEFAULT_MAX_TRUE_ROUNDNUM,
    cleanup_merge_suffixes,
)
from RC_utilities.reProcHelpers.startPosAssignment import compute_startpos_for_events_flexible

def _try_canonicalize(events_df: pd.DataFrame) -> pd.DataFrame:
    # Optional: only if makeItCannonical.py is present in your PYTHONPATH/working dir
    try:
        from makeItCannonical import canonicalize_event_order
        return canonicalize_event_order(
            events_df,
            start_col="start_AppTime",
            end_col="end_AppTime",
            group_first=False,
        )
    except Exception:
        # fallback: stable chronological sort by start_AppTime if present
        if "start_AppTime" in events_df.columns:
            return events_df.sort_values("start_AppTime", kind="mergesort", na_position="last")
        return events_df

def _add_startpos_and_merge_into_intervals(
    events_pre: pd.DataFrame,
    intervals: pd.DataFrame,
    *,
    max_round: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute startPos for:
      - collecting blocks: lo_eventType == TrueContentStart
      - pindropping blocks: lo_eventType == InterRound_PostCylinderWalk_segment

    Then collapse to one row per (BlockInstance, BlockNum, RoundNum) and merge into intervals (1:1).
    Leaves startPos mostly NA on non-target event rows.
    """
    df = events_pre.copy()
    df["__rowid"] = np.arange(len(df), dtype=int)

    # Required columns produced by augment_events_from_reprocessed (pos_cols -> *_start/*_end)
    role_col = "currentRole"
    x_start_col = "HeadPosAnchored_x_start"
    z_start_col = "HeadPosAnchored_z_start"
    x_end_col   = "HeadPosAnchored_x_end"
    z_end_col   = "HeadPosAnchored_z_end"

    missing = [c for c in [role_col, x_start_col, z_start_col, x_end_col, z_end_col, "lo_eventType", "BlockType"] if c not in df.columns]
    if missing:
        raise ValueError(f"Cannot compute startPos; missing columns: {missing}")

    def _label_subset(sub_df: pd.DataFrame, events_of_interest: dict[str, str]) -> pd.DataFrame:
        mask = sub_df["lo_eventType"].isin(events_of_interest.keys())
        if not mask.any():
            return sub_df[["__rowid"]].assign(startPos=pd.NA, startPos_dist=pd.NA)

        labeled = compute_startpos_for_events_flexible(
            sub_df.loc[mask, ["__rowid", "lo_eventType", role_col, x_start_col, z_start_col, x_end_col, z_end_col]].copy(),
            events_of_interest,
            role_col=role_col,
            x_start_col=x_start_col,
            z_start_col=z_start_col,
            x_end_col=x_end_col,
            z_end_col=z_end_col,
            startpos_label_col="startPos",
            startpos_dist_col="startPos_dist",
            add_used_xy_cols=False,
            strict=True,
            strict_roles=False,
        )
        return labeled[["__rowid", "startPos", "startPos_dist"]]

    # Event types differ by BlockType
    events_collecting = {"TrueContentStart": "start"}
    events_pindropping = {"InterRound_PostCylinderWalk_segment": "start"}

    collecting_df = df[df["BlockType"] == "collecting"].copy()
    pindrop_df    = df[df["BlockType"] == "pindropping"].copy()

    labeled = pd.concat(
        [
            _label_subset(collecting_df, events_collecting),
            _label_subset(pindrop_df, events_pindropping),
        ],
        ignore_index=True,
    )

    # attach to events_pre (only target rows get non-NA startPos)
    df = df.merge(labeled, on="__rowid", how="left").drop(columns=["__rowid"], errors="ignore")

    # Collapse to one row per round for interval merge
    keys = ["BlockInstance", "BlockNum", "RoundNum"]
    per_round = df[df["startPos"].notna()].copy()

    if "RoundNum" in per_round.columns:
        per_round["RoundNum"] = pd.to_numeric(per_round["RoundNum"], errors="coerce")
        per_round = per_round[per_round["RoundNum"].notna() & (per_round["RoundNum"] <= max_round)].copy()

    # prefer earliest start in the round (stable)
    if "start_AppTime" in per_round.columns:
        per_round["start_AppTime"] = pd.to_numeric(per_round["start_AppTime"], errors="coerce")
        per_round = per_round.sort_values(["BlockInstance", "BlockNum", "RoundNum", "start_AppTime"], kind="mergesort")
    per_round = per_round.drop_duplicates(subset=keys, keep="first")[keys + ["startPos", "startPos_dist"]].copy()

    # Merge into intervals (1 row per round)
    intervals_out = safe_merge(
        normalize_keys(intervals.copy(), keys, inplace=False),
        normalize_keys(per_round, keys, inplace=False),
        keys,
        how="left",
        validate="1:1",
        indicator=False,
    )

    return df, intervals_out


def main():
    ap = argparse.ArgumentParser(description="Augment events with metrics + build final interval tables + re-augment PinDrop events.")
    ap.add_argument("--events", required=True, help="Input last viable *_events*.csv")
    ap.add_argument("--reprocessed", required=True, help="Input *_reprocessed.csv")
    ap.add_argument("--prelim_intervals", required=True, help="Input *_prelim_blkRndInt.csv")
    ap.add_argument("--out_events_pre", required=True, help="Output *_events_pre_reproc.csv")
    ap.add_argument("--out_interval_horz", required=True, help="Output *_finalInterval_horz.csv")
    ap.add_argument("--out_interval_vert", required=True, help="Output *_finalInterval_vert.csv")
    ap.add_argument("--out_events_final", required=True, help="Output *_event_reproc.csv")
    ap.add_argument("--max_round", type=int, default=DEFAULT_MAX_TRUE_ROUNDNUM)
    args = ap.parse_args()
    # Ensure output dirs exist BEFORE writing
    for p in [args.out_events_pre, args.out_interval_horz, args.out_interval_vert, args.out_events_final]:
        Path(p).parent.mkdir(parents=True, exist_ok=True)
    events = pd.read_csv(args.events)
    reproc = pd.read_csv(args.reprocessed)
    intervals = pd.read_csv(args.prelim_intervals)

    # Step 5: augment events using origRow_start/end
    #metrics = ["totDistRound", "totDistBlock", "currSpeed"]
    metrics = [
        # core distances + speed
        "totDistRound", "totDistBlock", "currSpeed",
        # elapsed + fractions
        "roundElapsed_s", "blockElapsed_s", "totalSessionElapsed_s",
        "roundFrac", "blockFrac",
        # sanity columns (optional but useful)
        "dt", "stepDist",
    ]

    # also pull start/end positions for events (simple + robust)
    pos_cols = ["HeadPosAnchored_x", "HeadPosAnchored_y", "HeadPosAnchored_z"]

    events_pre = augment_events_from_reprocessed(events, reproc, metrics + pos_cols)


    #events_pre = augment_events_from_reprocessed(events, reproc, metrics)

    # Canonicalize (optional)
    events_pre = _try_canonicalize(events_pre)
    events_pre    = cleanup_merge_suffixes(events_pre, suffixes=("_int","_r"))
    events_pre.to_csv(args.out_events_pre, index=False)

    # Step 6: extract PinDrop_Moment rows
    pins = extract_pindrop_moments(events_pre)

    # Exclude special rounds (>max_round) from pin drops too
    if "RoundNum" in pins.columns:
        pins["RoundNum"] = pd.to_numeric(pins["RoundNum"], errors="coerce")
        pins = pins[(pins["RoundNum"] >= 1) & (pins["RoundNum"] <= args.max_round)].copy()

    # Build wide interval enrichment (horz)
    interval_horz = intervals_add_pindrops_wide(intervals, pins)

    # Build long (vert): intervals 1:1 per round, pins up to 3 per round => left join yields long
    keys = ["BlockInstance","BlockNum","RoundNum"]
    interval_vert = safe_merge(
        normalize_keys(intervals.copy(), keys, inplace=False),
        normalize_keys(pins.copy(), keys + ["chestPin_num"], inplace=False),
        keys,
        how="left",
        validate="1:m",
        indicator=False,
    )

    # Compute path order per round (using chestPin_num ordering)
    if "chestPin_num" in interval_vert.columns:
        long_with_steps, path_strings = compute_cumulative_path(
            interval_vert[interval_vert["chestPin_num"].notna()].copy(),
            group_keys=tuple(keys),
            order_key="chestPin_num",
            label_col="coinLabel",
            out_step_col="path_step_in_round",
            out_path_col="path_order_round",
        )
        # merge path strings back to both interval outputs
        interval_vert = safe_merge(interval_vert, path_strings, keys, how="left", validate="m:1", indicator=False)
        interval_horz = safe_merge(interval_horz, path_strings, keys, how="left", validate="1:1", indicator=False)

        # also keep per-pin step col where it exists
        step_cols = keys + ["chestPin_num","path_step_in_round"]
        interval_vert = safe_merge(interval_vert, long_with_steps[step_cols], keys + ["chestPin_num"], how="left",
                                   validate="m:1", indicator=False, suffixes=("", "_step"))
        # if merge created duplicates, prefer existing
        if "path_step_in_round_step" in interval_vert.columns:
            interval_vert["path_step_in_round"] = interval_vert["path_step_in_round"].fillna(interval_vert["path_step_in_round_step"])
            interval_vert = interval_vert.drop(columns=["path_step_in_round_step"], errors="ignore")

    interval_horz = cleanup_merge_suffixes(interval_horz, suffixes=("_int","_r"))
    interval_vert = cleanup_merge_suffixes(interval_vert, suffixes=("_int","_r"))
    interval_horz.to_csv(args.out_interval_horz, index=False)
    interval_vert.to_csv(args.out_interval_vert, index=False)

    # Step 7: augment events_pre for PinDrop_Moment rows using finalInterval_vert (join on keys + chestPin_num)
    ev_final = events_pre.copy()
    if "lo_eventType" in ev_final.columns:
        is_pin = ev_final["lo_eventType"].astype(str) == "PinDrop_Moment"
    else:
        is_pin = pd.Series(False, index=ev_final.index)

    pin_rows = ev_final[is_pin].copy()
    other_rows = ev_final[~is_pin].copy()

    join_keys = keys + (["chestPin_num"] if "chestPin_num" in pin_rows.columns and "chestPin_num" in interval_vert.columns else [])
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
    else:
        ev_final = events_pre

    ev_final = _try_canonicalize(ev_final)
    ev_final      = cleanup_merge_suffixes(ev_final, suffixes=("_int","_r"))
    ev_final.to_csv(args.out_events_final, index=False)

if __name__ == "__main__":
    main()
