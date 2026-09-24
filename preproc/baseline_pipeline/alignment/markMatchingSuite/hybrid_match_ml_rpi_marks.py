#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path


import numpy as np
import pandas as pd

from RC_utilities.alignHelpers.batchAlignHelpers import (
    _automatic_affine_alignment,
    _normalize_ml_stem,
    _select_mark_rows,
)



def _as_bool(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    truthy = {"1", "true", "t", "yes", "y"}
    return series.fillna("").astype(str).str.strip().str.lower().isin(truthy)


def _choose_rpi_times_and_sources(rpi_df: pd.DataFrame, rpi_time_type: str,) -> tuple[pd.Series, pd.Series]:
    if rpi_time_type not in rpi_df.columns:
        raise KeyError(f"RPi timestamp column not found: {rpi_time_type!r}")

    rpi_times = pd.to_datetime(rpi_df[rpi_time_type], errors="coerce")

    rpi_sources = pd.Series(rpi_time_type, index=rpi_df.index, dtype="string")

    return rpi_times, rpi_sources


def _validate_and_get_exclusions(
    singles_df: pd.DataFrame,
    stream: str,
    candidate_times: pd.Series,
    *,
    manual_time_column: str,
    tolerance_s: float) -> tuple[set[int], pd.DataFrame]:

    required = {
        "stream",
        "ordinal",
        "exclude",
        manual_time_column,
    }

    missing = sorted(required - set(singles_df.columns))

    if missing:
        raise KeyError(f"mark_singles CSV missing required columns: {missing}")

    rows = singles_df.loc[
        singles_df["stream"].astype(str).str.strip().str.lower().eq(stream.lower())].copy()

    if rows.empty:
        raise ValueError(f"mark_singles CSV contains no stream={stream!r} rows")

    rows["ordinal"] = pd.to_numeric(rows["ordinal"], errors="raise").astype(int)

    rows["manual_mark_time"] = pd.to_datetime(rows[manual_time_column], errors="coerce")

    rows["manual_exclude"] = _as_bool(rows["exclude"])

    duplicates = rows["ordinal"].duplicated(keep=False)

    if duplicates.any():
        vals = sorted(rows.loc[duplicates, "ordinal"].unique().tolist())

        raise ValueError( f"Duplicate {stream} ordinals in mark_singles CSV: {vals}")

    n = len(candidate_times)

    bad_ord = rows.loc[(rows["ordinal"] < 0) | (rows["ordinal"] >= n), "ordinal"]

    if len(bad_ord):
        raise ValueError(f"{stream} ordinal(s) outside raw candidate range 0..{n - 1}: {sorted(bad_ord.tolist())}")

    raw_times = pd.to_datetime(candidate_times, errors="coerce").reset_index(drop=True)

    audit_rows = []

    for _, row in rows.sort_values("ordinal").iterrows():

        ordinal = int(row["ordinal"])

        manual_time = row["manual_mark_time"]
        raw_time = raw_times.iloc[ordinal]

        delta_s = np.nan
        time_ok = False

        if (pd.notna(manual_time) and pd.notna(raw_time)):
            delta_s = abs((raw_time - manual_time).total_seconds())

            time_ok = (delta_s <= float(tolerance_s))

        audit_rows.append(
            {
                "stream": stream,
                "ordinal": ordinal,
                "manual_time_column": manual_time_column,
                "manual_mark_time": manual_time,
                "raw_mark_time": raw_time,
                "time_difference_s": delta_s,
                "time_check_ok": time_ok,
                "manual_exclude": bool(row["manual_exclude"]),
                "reason": row.get("reason", np.nan),
                "mark_id": row.get("mark_id", np.nan),
            }
        )

    audit = pd.DataFrame(
        audit_rows
    )

    failures = audit.loc[
        ~audit["time_check_ok"]
    ]

    if len(failures):
        preview = failures.head(8)[
            [
                "stream",
                "ordinal",
                "manual_time_column",
                "manual_mark_time",
                "raw_mark_time",
                "time_difference_s",
            ]
        ].to_dict("records")

        raise ValueError(
            f"mark_singles does not match the raw "
            f"{stream} mark train using "
            f"{manual_time_column!r} within "
            f"{tolerance_s:.6f}s. "
            f"First mismatches: {preview}"
        )

    exclusions = set(audit.loc[audit["manual_exclude"], "ordinal"].astype(int).tolist())

    return exclusions, audit


@dataclass
class Args:
    ml_csv_file: str
    rpi_marks_csv: str
    rpi_time_type: str
    mark_singles_csv: str
    csv_timestamp_column: str
    event_type_column: str
    event_type_values: str
    label: str
    device: str
    initial_match_gap_s: float
    final_match_gap_s: float
    coarse_search_window_s: float
    sigma_clip: float
    manual_filter_time_tolerance_s: float
    out_dir: str
    strip_ml_suffixes: str
    burst_gap_s: float


def _parse_args() -> Args:
    ap = argparse.ArgumentParser(
        description=(
            "Manual-exclusion-assisted automatic ML/RPi mark matching. Uses mark_singles.csv only as a pre-match exclusion mask; "
            "manual pair assignments are ignored. Produces frozen mark correspondences for downstream affine fitting."
        )
    )
    ap.add_argument("--ml_csv_file", required=True)
    ap.add_argument("--rpi_marks_csv", required=True)
    ap.add_argument("--rpi_time_type", required=True)
    ap.add_argument("--mark_singles_csv", required=True)
    ap.add_argument("--csv_timestamp_column", default="mLT_orig")
    ap.add_argument("--event_type_column", default="lo_eventType")
    ap.add_argument("--event_type_values", default="Mark")
    ap.add_argument("--label", required=True)
    ap.add_argument("--device", required=True)

    ap.add_argument("--initial_match_gap_s", type=float, default=1.0)
    ap.add_argument("--final_match_gap_s", type=float, default=0.35)
    ap.add_argument("--coarse_search_window_s", type=float, default=30.0)
    ap.add_argument("--sigma_clip", type=float, default=4.0)
    ap.add_argument("--burst_gap_s", type=float, default=30.0)

    ap.add_argument("--manual_filter_time_tolerance_s", type=float, default=0.005,
        help="Max raw-vs-mark_singles timestamp discrepancy allowed for ordinal validation.")
    ap.add_argument("--out_dir", default="")
    ap.add_argument("--strip-ml-suffixes", default="_events_final,_events,_final")
    ap.add_argument("--strip_ml_suffixes", default="_events_final,_events,_final")
    
    ns = ap.parse_args()
    return Args(**vars(ns))


def main() -> None:
    args = _parse_args()
    ml_path = Path(args.ml_csv_file)
    rpi_path = Path(args.rpi_marks_csv)
    singles_path = Path(args.mark_singles_csv)

    for path, description in (
        (ml_path, "ML CSV"),
        (rpi_path, "RPi marks CSV"),
        (singles_path, "mark_singles CSV"),
    ):
        if not path.exists():
            raise FileNotFoundError(f"Missing {description}: {path}")

    ml_df_all = pd.read_csv(ml_path)
    ts_col = args.csv_timestamp_column
    type_col = args.event_type_column
    type_vals = [v.strip() for v in args.event_type_values.split(",") if v.strip()]

    if ts_col not in ml_df_all.columns:
        raise KeyError(f"ML CSV missing timestamp column '{ts_col}'")
    if type_col not in ml_df_all.columns:
        raise KeyError(f"ML CSV missing event type column '{type_col}'")

    ml_marks = _select_mark_rows(ml_df_all, type_col, type_vals).copy()
    ml_marks["_ml_source_index"] = ml_marks.index
    ml_marks = ml_marks.reset_index(drop=True)
    ml_mark_times_raw = pd.to_datetime(ml_marks[ts_col], errors="coerce").reset_index(drop=True)

    rpi_df = pd.read_csv(rpi_path)
    if args.rpi_time_type not in rpi_df.columns:
        raise KeyError(f"RPi marks CSV missing requested timestamp column '{args.rpi_time_type}'")

    rpi_times_raw = pd.to_datetime(rpi_df[args.rpi_time_type], errors="coerce").reset_index(drop=True)

    if ml_mark_times_raw.isna().any():
        bad = int(ml_mark_times_raw.isna().sum())
        raise ValueError(f"Found {bad} ML Mark rows with invalid timestamps in '{ts_col}'")

    if rpi_times_raw.isna().any():
        bad = int(rpi_times_raw.isna().sum())
        raise ValueError(f"Found {bad} RPi mark rows with invalid timestamps in '{args.rpi_time_type}'")

    singles_df = pd.read_csv(singles_path)

    # Validate the exclusion file against the unfiltered raw mark trains.
    ml_excluded_ordinals, ml_audit = _validate_and_get_exclusions(
        singles_df,
        "events",
        ml_mark_times_raw,
        manual_time_column="mark_time_orig",
        tolerance_s=args.manual_filter_time_tolerance_s,
    )
    rpi_excluded_ordinals, rpi_audit = _validate_and_get_exclusions(
        singles_df,
        "rpi",
        rpi_times_raw,
        manual_time_column=args.rpi_time_type,
        tolerance_s=args.manual_filter_time_tolerance_s,
    )

    # Filter before any automatic pairing.
    ml_keep = np.array([i not in ml_excluded_ordinals for i in range(len(ml_marks))], dtype=bool)
    rpi_keep = np.array([i not in rpi_excluded_ordinals for i in range(len(rpi_df))], dtype=bool)

    ml_times_fit = ml_mark_times_raw.loc[ml_keep].reset_index(drop=True)
    rpi_times_fit = rpi_times_raw.loc[rpi_keep].reset_index(drop=True)

    # Keep original ordinals so output pair tables remain auditable.
    ml_original_ordinals = np.flatnonzero(ml_keep)
    rpi_original_ordinals = np.flatnonzero(rpi_keep)

    (
        match_idx,
        observed_offsets_s,
        reasons,
        matcher_model,
        matcher_inlier,
        matcher_residual_s,
    ) = _automatic_affine_alignment(
        ml_times_fit,
        rpi_times_fit,
        initial_match_gap_s=args.initial_match_gap_s,
        final_match_gap_s=args.final_match_gap_s,
        coarse_search_window_s=args.coarse_search_window_s,
        sigma_clip=args.sigma_clip,
    )

    source_indices = ml_marks["_ml_source_index"].to_numpy(dtype=int)

    matched_fit = match_idx >= 0

    # Map original ML ordinal -> filtered-fit index.
    fit_index_by_original_ml = {
        int(original_ordinal): int(fit_index)
        for fit_index, original_ordinal in enumerate(ml_original_ordinals)
    }

    # Original RPi ordinals that were successfully matched.
    used_rpi_original = {
        int(rpi_original_ordinals[int(j_fit)])
        for j_fit in match_idx[matched_fit]
    }
    

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

    matched_fit_positions = np.flatnonzero(matched_fit)

    matched_original_ml_ordinals = ml_original_ordinals[matched_fit_positions].astype(int)

    matched_times = ml_times_fit.iloc[matched_fit_positions].reset_index(drop=True)

    current_burst = 0
    current_position = 0
    previous_time = None

    for original_ml_ordinal, mark_time in zip(matched_original_ml_ordinals, matched_times):
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

        burst_id_by_ml_mark[original_ml_ordinal] = current_burst
        burst_position_by_ml_mark[original_ml_ordinal] = current_position

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
                "matched_burst_id": burst_id_by_ml_mark[matched_original_ml_ordinals],
                "matched_burst_position": burst_position_by_ml_mark[matched_original_ml_ordinals],
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

    # Convenient lookup of manual exclusion reasons.
    ml_exclusion_reason = {
        int(row["ordinal"]): (
            ""
            if pd.isna(row["reason"])
            else str(row["reason"])
        )
        for _, row in ml_audit.loc[ml_audit["manual_exclude"]].iterrows()
    }

    rpi_exclusion_reason = {
        int(row["ordinal"]): (
            ""
            if pd.isna(row["reason"])
            else str(row["reason"])
        )
        for _, row in rpi_audit.loc[rpi_audit["manual_exclude"]].iterrows()
    }


    for original_ml_ordinal in range(len(ml_marks)):
        excluded = original_ml_ordinal in ml_excluded_ordinals

        burst_id = (
            int(burst_id_by_ml_mark[original_ml_ordinal])
            if np.isfinite(burst_id_by_ml_mark[original_ml_ordinal])
            else pd.NA
        )

        burst_position = (
            int(burst_position_by_ml_mark[original_ml_ordinal])
            if np.isfinite(burst_position_by_ml_mark[original_ml_ordinal])
            else pd.NA
        )

        is_first_in_burst = (
            bool(burst_position == 1)
            if pd.notna(burst_position)
            else pd.NA
        )

        if excluded:
            rows.append(
                {
                    "record_type": "ml_mark",
                    "matching_method": "manual_exclusion_auto",
                    "ml_mark_index": original_ml_ordinal,
                    "ml_source_index": int(source_indices[original_ml_ordinal]),
                    "ml_time": ml_mark_times_raw.iloc[original_ml_ordinal],
                    "rpi_index": pd.NA,
                    "rpi_time": pd.NaT,
                    "rpi_timestamp_source": "",
                    "matched": False,
                    "excluded": True,
                    "exclusion_reason": ml_exclusion_reason.get(original_ml_ordinal, "",),
                    "matcher_reason_internal": "manual_pre_match_exclusion",
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
            continue

        fit_i = fit_index_by_original_ml[original_ml_ordinal]

        j_fit = (
            int(match_idx[fit_i])
            if match_idx[fit_i] >= 0
            else None
        )

        j_original = (
            int(rpi_original_ordinals[j_fit])
            if j_fit is not None
            else None
        )

        rows.append(
            {
                "record_type": "ml_mark",
                "matching_method": "manual_exclusion_auto",
                "ml_mark_index": original_ml_ordinal,
                "ml_source_index": int(source_indices[original_ml_ordinal]),
                "ml_time": ml_mark_times_raw.iloc[original_ml_ordinal],
                "rpi_index": (
                    j_original
                    if j_original is not None
                    else pd.NA),
                "rpi_time": (
                    rpi_times_raw.iloc[j_original]
                    if j_original is not None
                    else pd.NaT),
                "rpi_timestamp_source": (
                    args.rpi_time_type
                    if j_original is not None
                    else ""),
                "matched": bool(j_original is not None),
                "excluded": False,
                "exclusion_reason": "",
                "matcher_reason_internal": reasons[fit_i],
                "raw_offset_s": observed_offsets_s[fit_i],
                "matcher_temp_affine_inlier": bool(matcher_inlier[fit_i]),
                "matcher_temp_affine_residual_s": matcher_residual_s[fit_i],
                "matcher_initial_match_gap_s": args.initial_match_gap_s,
                "matcher_final_match_gap_s": args.final_match_gap_s,
                "matcher_coarse_search_window_s": args.coarse_search_window_s,
                "matcher_sigma_clip": args.sigma_clip,
                "matched_burst_id": burst_id,
                "matched_burst_position": burst_position,
                "is_first_in_burst": is_first_in_burst,
                "matcher_burst_gap_s": args.burst_gap_s,
            }
        )

    # ---------------------------------------------------------
    # Preserve unmatched RPi marks.
    # ---------------------------------------------------------

    unused_rpi_indices = sorted(set(range(len(rpi_df))) - used_rpi_original)

    for j_original in unused_rpi_indices:
        excluded = j_original in rpi_excluded_ordinals

        rows.append(
            {
                "record_type": "rpi_only",
                "matching_method": "manual_exclusion_auto",
                "ml_mark_index": pd.NA,
                "ml_source_index": pd.NA,
                "ml_time": pd.NaT,
                "rpi_index": int(j_original),
                "rpi_time": rpi_times_raw.iloc[j_original],
                "rpi_timestamp_source": args.rpi_time_type,
                "matched": False,
                "excluded": bool(excluded),
                "exclusion_reason": (
                    rpi_exclusion_reason.get(j_original, "",)
                    if excluded
                    else ""),
                "matcher_reason_internal": (
                    "manual_pre_match_exclusion"
                    if excluded
                    else "unmatched_rpi"),
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


    matches_df = pd.DataFrame(rows)

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

    audit_csv = out_dir / f"{stem}_{args.label}_{args.device}_manual_exclusion_audit.csv"

    # ---------------------------------------------------------
    # Matching summary.
    # ---------------------------------------------------------

    n_ml = len(ml_marks)
    n_rpi = len(rpi_df)

    n_ml_excluded = len(ml_excluded_ordinals)
    n_rpi_excluded = len(rpi_excluded_ordinals)

    n_ml_eligible = len(ml_times_fit)
    n_rpi_eligible = len(rpi_times_fit)

    n_matched = int(matched_fit.sum())

    n_unmatched_eligible_ml = n_ml_eligible - n_matched
    n_unmatched_eligible_rpi = n_rpi_eligible - n_matched
    

    summary = pd.DataFrame(
        [
            {
                "ml_file": ml_path.name,
                "rpi_file": rpi_path.name,
                "label": args.label,
                "device": args.device,
                "rpi_timestamp_column": args.rpi_time_type,

                "matching_method": "manual_exclusion_auto",
                "manual_filter_time_tolerance_s": args.manual_filter_time_tolerance_s,

                "n_ml_marks_raw": n_ml,
                "n_rpi_marks_raw": n_rpi,

                "n_manual_excluded_ml_marks": n_ml_excluded,
                "n_manual_excluded_rpi_marks": n_rpi_excluded,

                "n_ml_marks_eligible": n_ml_eligible,
                "n_rpi_marks_eligible": n_rpi_eligible,

                "n_matched_marks": n_matched,
                "n_unmatched_eligible_ml_marks": n_unmatched_eligible_ml,
                "n_unmatched_eligible_rpi_marks": n_unmatched_eligible_rpi,

                "initial_match_gap_s": args.initial_match_gap_s,
                "final_match_gap_s": args.final_match_gap_s,
                "coarse_search_window_s": args.coarse_search_window_s,
                "sigma_clip": args.sigma_clip,

                "temporary_matcher_offset_at_reference_s": matcher_model.offset_at_reference_s,
                "temporary_matcher_drift_ppm": matcher_model.drift_ppm,
                "temporary_matcher_residual_rmse_s": matcher_model.residual_rmse_s,

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

    audit_df = pd.concat([ml_audit, rpi_audit], ignore_index=True)

    matches_df.to_csv(matches_csv, index=False)
    summary.to_csv(summary_csv, index=False)
    audit_df.to_csv(audit_csv, index=False)

    print(f"[ok] wrote frozen mark matches      -> {matches_csv}")
    print(f"[ok] wrote matching summary         -> {summary_csv}")
    print(f"[ok] wrote manual exclusion audit   -> {audit_csv}")

    print(
        "[match] "
        f"matched={n_matched}/{n_ml_eligible} eligible ML marks; "
        f"ML excluded={n_ml_excluded}; "
        f"RPi excluded={n_rpi_excluded}; "
        f"unmatched eligible ML={n_unmatched_eligible_ml}; "
        f"unmatched eligible RPi={n_unmatched_eligible_rpi}; "
        f"bursts={n_matched_bursts}; "
        f"first-burst-marks={n_first_in_burst_marks}; "
        f"stable-marks={n_stable_marks}; "
        f"timestamp={args.rpi_time_type}"
    )
if __name__ == "__main__":
    main()