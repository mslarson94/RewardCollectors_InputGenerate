#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd

from RC_utilities.alignHelpers.batchAlignHelpers import (
    _automatic_affine_alignment,
    _normalize_ml_stem,
    _select_mark_rows,
)


def _choose_rpi_times_and_sources(rpi_df: pd.DataFrame, rpi_time_type: str) -> Tuple[pd.Series, pd.Series, pd.Series]:
    if rpi_time_type not in rpi_df.columns:
        raise KeyError(f"RPi timestamp column not found: {rpi_time_type!r}")

    if "timezone_offset_hours" in rpi_df.columns:
        timezone_offsets = pd.to_numeric(rpi_df["timezone_offset_hours"], errors="coerce")
    else:
        timezone_offsets = pd.Series(np.nan, index=rpi_df.index, dtype=float)

    return (
        pd.to_datetime(rpi_df[rpi_time_type], errors="coerce"),
        pd.Series(rpi_time_type, index=rpi_df.index, dtype="string"),
        timezone_offsets
    )

@dataclass
class Args:
    ml_csv_file: str
    rpi_marks_csv: str
    csv_timestamp_column: str
    event_type_column: str
    event_type_values: str
    label: str
    device: str
    rpi_time_type: str
    initial_match_gap_s: float
    final_match_gap_s: float
    coarse_search_window_s: float
    sigma_clip: float
    out_dir: str
    strip_ml_suffixes: str
    burst_gap_s: float


def _parse_args() -> Args:
    ap = argparse.ArgumentParser(
        description="Automatically match ML marks to RPi marks and freeze those correspondences for downstream alignment models."
    )

    ap.add_argument( "--ml_csv_file", required=True)
    ap.add_argument("--rpi_marks_csv", required=True)
    ap.add_argument("--csv_timestamp_column", default="mLT_orig")
    ap.add_argument("--event_type_column", default="lo_eventType")
    ap.add_argument("--event_type_values", default="Mark")
    ap.add_argument("--label", required=True)
    ap.add_argument("--device", required=True)
    ap.add_argument("--rpi_time_type", required=True, help="Exact RPi timestamp column to use.")
    ap.add_argument("--burst_gap_s", type=float, default=30.0, help="Gap in seconds between consecutive matched ML marks that starts a new mark burst.")
    ap.add_argument("--initial_match_gap_s", type=float, default=0.75)
    ap.add_argument("--final_match_gap_s", type=float, default=0.35)
    ap.add_argument("--coarse_search_window_s", type=float, default=30.0)
    ap.add_argument("--sigma_clip", type=float, default=4.0)
    ap.add_argument("--out_dir", default="")
    ap.add_argument("--strip_ml_suffixes", default="_events_final,_events,_final")

    ns = ap.parse_args()

    return Args(
        ml_csv_file=ns.ml_csv_file,
        rpi_marks_csv=ns.rpi_marks_csv,
        csv_timestamp_column=ns.csv_timestamp_column,
        event_type_column=ns.event_type_column,
        event_type_values=ns.event_type_values,
        label=ns.label,
        device=ns.device,
        rpi_time_type=ns.rpi_time_type,
        initial_match_gap_s=ns.initial_match_gap_s,
        final_match_gap_s=ns.final_match_gap_s,
        coarse_search_window_s=ns.coarse_search_window_s,
        sigma_clip=ns.sigma_clip,
        out_dir=ns.out_dir,
        strip_ml_suffixes=ns.strip_ml_suffixes,
        burst_gap_s=ns.burst_gap_s,
    )


def main() -> None:
    args = _parse_args()

    ml_path = Path(args.ml_csv_file)
    rpi_path = Path(args.rpi_marks_csv)

    if not ml_path.exists():
        raise FileNotFoundError(
            f"Missing ML CSV: {ml_path}"
        )

    if not rpi_path.exists():
        raise FileNotFoundError(
            f"Missing RPi marks CSV: {rpi_path}"
        )

    # ---------------------------------------------------------
    # Load ML marks.
    # ---------------------------------------------------------

    ml_df = pd.read_csv(ml_path)

    if args.csv_timestamp_column not in ml_df.columns:
        raise KeyError(f"ML CSV missing timestamp column {args.csv_timestamp_column!r}")

    if args.event_type_column not in ml_df.columns:
        raise KeyError(f"ML CSV missing event type column {args.event_type_column!r}")

    type_values = [
        value.strip()
        for value in args.event_type_values.split(",")
        if value.strip()
    ]

    ml_marks = _select_mark_rows(ml_df, args.event_type_column, type_values).copy()

    # Preserve row position in the original ML dataframe.
    ml_marks["_ml_source_index"] = ml_marks.index

    ml_marks = ml_marks.reset_index(drop=True)

    ml_times = pd.to_datetime(ml_marks[args.csv_timestamp_column], errors="coerce").reset_index(drop=True)

    # ---------------------------------------------------------
    # Load selected RPi timestamp source.
    # ---------------------------------------------------------

    rpi_df = pd.read_csv(rpi_path).reset_index(drop=True)

    rpi_times, rpi_sources, _ = _choose_rpi_times_and_sources(rpi_df,args.rpi_time_type)

    rpi_times = rpi_times.reset_index(drop=True)
    rpi_sources = rpi_sources.reset_index(drop=True)


    if rpi_times.isna().any():
        bad = int(rpi_times.isna().sum())
        raise ValueError(f"Found {bad} RPi mark rows with invalid timestamps in '{args.rpi_time_type}'")

    if ml_times.isna().any():
        bad = int(ml_times.isna().sum())
        raise ValueError(f"Found {bad} ML Mark rows with invalid timestamps in '{args.csv_timestamp_column}'")
    # ---------------------------------------------------------
    # Automatic matching.
    # ---------------------------------------------------------

    (
        match_idx,
        observed_offsets_s,
        matcher_reasons,
        temporary_model,
        temporary_fit_inlier,
        temporary_residual_s,
    ) = _automatic_affine_alignment(
        ml_times,
        rpi_times,
        initial_match_gap_s=args.initial_match_gap_s,
        final_match_gap_s=args.final_match_gap_s,
        coarse_search_window_s=args.coarse_search_window_s,
        sigma_clip=args.sigma_clip,
    )

    source_indices = ml_marks["_ml_source_index"].to_numpy(dtype=int)

    matched = match_idx >= 0

    used_rpi = set(match_idx[matched].astype(int).tolist())

    # =========================================================
    # BURST CHARACTERIZATION
    #
    # Burst membership is defined only from successfully matched
    # ML marks.
    #
    # A new burst starts when the elapsed time between consecutive
    # matched ML marks exceeds args.burst_gap_s.
    #
    # For every matched mark we record:
    #
    #   matched_burst_id
    #       1-based burst number
    #
    #   matched_burst_position
    #       1-based position within the burst
    #
    #   is_first_in_burst
    #       True when matched_burst_position == 1
    #
    # This is structural metadata only. No marks are excluded here.
    # =========================================================

    burst_id_by_ml_mark = np.full(len(ml_marks), np.nan, dtype=float)

    burst_position_by_ml_mark = np.full(len(ml_marks), np.nan, dtype=float)

    matched_positions = np.flatnonzero(matched)

    matched_times = ml_times.iloc[matched_positions]

    current_burst = 0
    current_position = 0
    previous_time = None

    for pos, mark_time in zip(matched_positions, matched_times):
        if pd.isna(mark_time):
            continue

        if previous_time is None:
            current_burst = 1
            current_position = 1

        else:
            gap_s = (mark_time - previous_time).total_seconds()

            if gap_s > args.burst_gap_s:
                current_burst += 1
                current_position = 1

            else:
                current_position += 1

        burst_id_by_ml_mark[pos] = (current_burst)
        burst_position_by_ml_mark[pos] = (current_position)
        previous_time = mark_time

    n_matched_bursts = current_burst
    single_burst_only = (n_matched_bursts == 1)

    # ---------------------------------------------------------
    # Burst boundary reporting.
    # ---------------------------------------------------------

    first_burst_start_time = pd.NaT
    first_burst_end_time = pd.NaT

    last_burst_start_time = pd.NaT
    last_burst_end_time = pd.NaT

    if n_matched_bursts > 0:
        burst_table = pd.DataFrame(
            {
                "matched_burst_id": burst_id_by_ml_mark[matched_positions],
                "matched_burst_position": burst_position_by_ml_mark[matched_positions],
                "ml_time": matched_times.to_numpy(),
            }
        )

        burst_table = burst_table.dropna(subset=["matched_burst_id", "ml_time"])

        burst_summary = burst_table.groupby("matched_burst_id",sort=True,)["ml_time"].agg(burst_start="min", burst_end="max").reset_index()
        

        if not burst_summary.empty:
            first_burst_start_time = burst_summary.iloc[0]["burst_start"]
            first_burst_end_time = burst_summary.iloc[0]["burst_end"]
            last_burst_start_time = burst_summary.iloc[-1]["burst_start"]
            last_burst_end_time = burst_summary.iloc[-1]["burst_end"]
            

    # ---------------------------------------------------------
    # Inter-burst gap reporting.
    # ---------------------------------------------------------

    matched_gaps_s = matched_times.diff().dt.total_seconds().dropna()
    

    interburst_gaps_s = matched_gaps_s.loc[matched_gaps_s > args.burst_gap_s]
    

    max_interburst_gap_s = (
        float(interburst_gaps_s.max())
        if len(interburst_gaps_s)
        else np.nan
    )

    # ---------------------------------------------------------
    # Additional structural burst counts.
    # ---------------------------------------------------------

    first_in_burst_mask = np.isfinite(burst_position_by_ml_mark) & (burst_position_by_ml_mark == 1)
    stable_mark_mask = np.isfinite(burst_position_by_ml_mark) & (burst_position_by_ml_mark > 1)
    n_first_in_burst_marks = int(first_in_burst_mask.sum())
    n_stable_marks = int(stable_mark_mask.sum())

    # =========================================================
    # WRITE FROZEN MATCH RECORDS
    # =========================================================

    rows: list[dict] = []

    # ---------------------------------------------------------
    # One record for every ML mark.
    # ---------------------------------------------------------

    for i in range(len(ml_marks)):
        j = (
            int(match_idx[i])
            if match_idx[i] >= 0
            else None
        )

        burst_id = (
            int(burst_id_by_ml_mark[i])
            if np.isfinite(burst_id_by_ml_mark[i])
            else pd.NA
        )

        burst_position = (
            int(burst_position_by_ml_mark[i])
            if np.isfinite(burst_position_by_ml_mark[i])
            else pd.NA
        )

        is_first_in_burst = (
            bool(burst_position == 1)
            if pd.notna(burst_position)
            else pd.NA
        )

        rows.append(
            {
                "record_type": "ml_mark",
                "matching_method": "automatic",
                "ml_mark_index": int(i),

                "ml_source_index": int(source_indices[i]),

                "ml_time": ml_times.iloc[i],

                "rpi_index": j,

                "rpi_time": (
                    rpi_times.iloc[j]
                    if j is not None
                    else pd.NaT
                ),

                "rpi_timestamp_source": (
                    args.rpi_time_type
                    if j is not None
                    else ""
                ),

                "matched": bool(j is not None),
                "excluded": False,
                "exclusion_reason": "",

                # Matcher diagnostic only.
                "matcher_reason_internal": matcher_reasons[i],

                "raw_offset_s": observed_offsets_s[i],

                # Temporary affine model used only internally by the matcher to refine correspondence.
                "matcher_temp_affine_inlier": bool(temporary_fit_inlier[i]),
                "matcher_temp_affine_residual_s": temporary_residual_s[i],

                "matcher_initial_match_gap_s": args.initial_match_gap_s,
                "matcher_final_match_gap_s": args.final_match_gap_s,
                "matcher_coarse_search_window_s": args.coarse_search_window_s,
                "matcher_sigma_clip": args.sigma_clip,

                # Burst metadata.
                "matched_burst_id": burst_id,
                "matched_burst_position": burst_position,
                "is_first_in_burst": is_first_in_burst,
                "matcher_burst_gap_s": args.burst_gap_s,

            }
        )

    # ---------------------------------------------------------
    # Preserve unmatched RPi marks.
    # ---------------------------------------------------------

    unused_rpi_indices = sorted(
        set(range(len(rpi_df)))
        - used_rpi
    )

    for j in unused_rpi_indices:
        rows.append(
            {
                "record_type": "rpi_only",
                "matching_method": "automatic",
                "ml_mark_index": pd.NA,
                "ml_source_index": pd.NA,
                "ml_time": pd.NaT,
                "rpi_index": int(j),
                "rpi_time": rpi_times.iloc[j],
                "rpi_timestamp_source": args.rpi_time_type,
                "matched": False,
                "excluded": False,
                "exclusion_reason": "",
                "matcher_reason_internal": "unmatched_rpi",
                "raw_offset_s": np.nan,
                "matcher_temp_affine_inlier": False,
                "matcher_temp_affine_residual_s": np.nan,
                "matcher_initial_match_gap_s": args.initial_match_gap_s,
                "matcher_final_match_gap_s": args.final_match_gap_s,
                "matcher_coarse_search_window_s": args.coarse_search_window_s,
                "matcher_sigma_clip": args.sigma_clip,
                "matched_burst_id": pd.NA,
                "matched_burst_position": pd.NA,
                "is_first_in_burst": pd.NA,
                "matcher_burst_gap_s": args.burst_gap_s,
            }
        )

    matches_df = pd.DataFrame(
        rows
    )

    # ---------------------------------------------------------
    # Output paths.
    # ---------------------------------------------------------

    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else ml_path.parent
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    stem = _normalize_ml_stem(
        ml_path.stem,
        [
            suffix.strip()
            for suffix
            in args.strip_ml_suffixes.split(",")
            if suffix.strip()
        ],
    )

    matches_csv = out_dir/ f"{stem}_{args.label}_{args.device}_matched_marks.csv"

    summary_csv = out_dir / f"{stem}_{args.label}_{args.device}_matching_summary.csv"
        

    # ---------------------------------------------------------
    # Matching summary.
    # ---------------------------------------------------------

    n_ml = len(ml_marks)

    n_rpi = len(rpi_df)

    n_matched = int(matched.sum())


    summary = pd.DataFrame(
        [
            {
                "ml_file": ml_path.name,
                "rpi_file": rpi_path.name,
                "label": args.label,
                "device": args.device,
                "rpi_timestamp_column": args.rpi_time_type,
                "matching_method": "automatic",

                "n_ml_marks": n_ml,
                "n_rpi_marks": n_rpi,
                "n_matched_marks": n_matched,
                "n_unmatched_ml_marks": int(n_ml - n_matched),
                "n_unmatched_rpi_marks": int(n_rpi - n_matched),

                "initial_match_gap_s": args.initial_match_gap_s,
                "final_match_gap_s": args.final_match_gap_s,
                "coarse_search_window_s": args.coarse_search_window_s,
                "sigma_clip": args.sigma_clip,

                "temporary_matcher_offset_at_reference_s": temporary_model.offset_at_reference_s,
                "temporary_matcher_drift_ppm": temporary_model.drift_ppm,
                "temporary_matcher_residual_rmse_s": temporary_model.residual_rmse_s,

                # Burst structural metadata.
                "burst_gap_s": args.burst_gap_s,
                "n_matched_bursts": n_matched_bursts,
                "single_burst_only": single_burst_only,
                "n_first_in_burst_marks": n_first_in_burst_marks,
                "n_stable_marks": n_stable_marks,
                "first_burst_start_time": first_burst_start_time,
                "first_burst_end_time": first_burst_end_time,
                "last_burst_start_time": last_burst_start_time,
                "last_burst_end_time": last_burst_end_time,
                "max_interburst_gap_s": max_interburst_gap_s,
            }
        ]
    )

    # ---------------------------------------------------------
    # Write outputs.
    # ---------------------------------------------------------

    matches_df.to_csv(matches_csv, index=False)
    summary.to_csv(summary_csv, index=False)

    print(f"[ok] wrote frozen mark matches -> {matches_csv}")
    print(f"[ok] wrote matching summary     -> {summary_csv}")

    print(
        "[match] "
        f"matched={n_matched}/{n_ml} ML marks; "
        f"unmatched RPi={n_rpi - n_matched}; "
        f"bursts={n_matched_bursts}; "
        f"first-burst-marks={n_first_in_burst_marks}; "
        f"stable-marks={n_stable_marks}; "
        f"timestamp={args.rpi_time_type}"
    )


if __name__ == "__main__":
    main()