#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from batchAlignHelpers import (
    _auto_offset_hours,
    _automatic_affine_alignment,
    _normalize_ml_stem,
    _predict_ml_from_rpi,
    _predict_rpi_from_ml,
    _select_mark_rows,
)


def _choose_rpi_times_and_sources(
    rpi_df: pd.DataFrame,
    rpi_time_type: str,
) -> Tuple[pd.Series, pd.Series]:
    if rpi_time_type not in rpi_df.columns:
        raise KeyError(f"RPi timestamp column not found: {rpi_time_type!r}")

    return (
        pd.to_datetime(rpi_df[rpi_time_type], errors="coerce"),
        pd.Series(
            rpi_time_type,
            index=rpi_df.index,
            dtype="string",
        ),
    )


@dataclass
class Args:
    ml_csv_file: str
    rpi_marks_csv: str
    blankRowTemplate: Optional[str]
    csv_timestamp_column: str
    event_type_column: str
    event_type_values: str
    label: str
    device: str
    timezone_offset_hours: str
    rpi_time_type: str
    initial_match_gap_s: float
    final_match_gap_s: float
    coarse_search_window_s: float
    sigma_clip: float
    out_dir: str
    strip_ml_suffixes: str


def _parse_args() -> Args:
    ap = argparse.ArgumentParser(
        description=(
            "Automatically pair ML/RPi marks, estimate clock offset+drift, "
            "and align timestamps."
        )
    )

    ap.add_argument("--ml_csv_file", required=True)
    ap.add_argument("--rpi_marks_csv", required=True)
    ap.add_argument("--blankRowTemplate")
    ap.add_argument("--csv_timestamp_column", default="mLT_orig")
    ap.add_argument("--event_type_column", default="lo_eventType")
    ap.add_argument("--event_type_values", default="Mark")
    ap.add_argument("--label", required=True)
    ap.add_argument("--device", required=True)

    ap.add_argument(
        "--timezone_offset_hours",
        default="auto",
        help=(
            "Whole-hour wall-clock normalization, or 'auto'. "
            "Sub-hour offset is fit, not removed here."
        ),
    )

    ap.add_argument("--rpi_time_type",default="RPi_Time_simple",help="Exact RPi timestamp column to use for alignment.")

    ap.add_argument("--initial_match_gap_s", type=float, default=0.75)
    ap.add_argument("--final_match_gap_s", type=float, default=0.35)
    ap.add_argument("--coarse_search_window_s", type=float, default=30.0)
    ap.add_argument("--sigma_clip", type=float, default=4.0)
    ap.add_argument("--out_dir", default="")
    ap.add_argument(
        "--strip_ml_suffixes",
        default="_events_final,_events,_final",
    )

    ns = ap.parse_args()

    return Args(
        ml_csv_file=ns.ml_csv_file,
        rpi_marks_csv=ns.rpi_marks_csv,
        blankRowTemplate=ns.blankRowTemplate,
        csv_timestamp_column=ns.csv_timestamp_column,
        event_type_column=ns.event_type_column,
        event_type_values=ns.event_type_values,
        label=ns.label,
        device=ns.device,
        timezone_offset_hours=ns.timezone_offset_hours,
        rpi_time_type=ns.rpi_time_type,
        initial_match_gap_s=ns.initial_match_gap_s,
        final_match_gap_s=ns.final_match_gap_s,
        coarse_search_window_s=ns.coarse_search_window_s,
        sigma_clip=ns.sigma_clip,
        out_dir=ns.out_dir,
        strip_ml_suffixes=ns.strip_ml_suffixes,
    )


def main() -> None:
    args = _parse_args()

    ml_path = Path(args.ml_csv_file)
    rpi_path = Path(args.rpi_marks_csv)

    if not ml_path.exists():
        raise FileNotFoundError(f"Missing ML CSV: {ml_path}")

    if not rpi_path.exists():
        raise FileNotFoundError(f"Missing RPi marks CSV: {rpi_path}")

    ml_df_all = pd.read_csv(ml_path)

    ts_col = args.csv_timestamp_column
    type_col = args.event_type_column
    type_vals = [
        v.strip()
        for v in args.event_type_values.split(",")
        if v.strip()
    ]

    if ts_col not in ml_df_all.columns:
        raise KeyError(
            f"ML CSV missing timestamp column {ts_col!r}"
        )

    if type_col not in ml_df_all.columns:
        raise KeyError(
            f"ML CSV missing event type column {type_col!r}"
        )

    # Marks are used to estimate the clock model.
    # No manual matching/exclusion data is consumed.
    ml_marks = _select_mark_rows(
        ml_df_all,
        type_col,
        type_vals,
    ).copy()

    ml_marks["_ml_source_index"] = ml_marks.index
    ml_marks = ml_marks.reset_index(drop=True)

    ml_mark_times = pd.to_datetime(
        ml_marks[ts_col],
        errors="coerce",
    )

    # Explicitly use the RPi timestamp column requested by the caller.
    rpi_df = pd.read_csv(rpi_path)

    rpi_times, rpi_sources = _choose_rpi_times_and_sources(
        rpi_df,
        args.rpi_time_type,
    )

    if rpi_times.isna().all():
        raise ValueError(
            f"RPi timestamp column {args.rpi_time_type!r} "
            "contains no parseable timestamps"
        )

    # Normalize only whole-hour wall-clock differences.
    if str(args.timezone_offset_hours).strip().lower() == "auto":
        tz_hours = _auto_offset_hours(
            ml_mark_times,
            rpi_times,
        )
    else:
        tz_hours = float(args.timezone_offset_hours)

    # Intentional behavior:
    # verbose RPi timestamps are already in the appropriate wall-clock frame,
    # while non-verbose timestamps receive the whole-hour correction.
    src_lower = rpi_sources.fillna("").str.lower()
    is_verb = src_lower.str.contains("_verb")

    apply_tz = (
        (~rpi_times.isna())
        & (~is_verb)
    )

    rpi_effective = rpi_times.copy()

    if tz_hours != 0:
        rpi_effective.loc[apply_tz] = (
            rpi_effective.loc[apply_tz]
            + timedelta(hours=float(tz_hours))
        )

    (
        match_idx,
        observed_offsets_s,
        reasons,
        model,
        fit_inlier,
        residual_s,
    ) = _automatic_affine_alignment(
        ml_mark_times,
        rpi_effective,
        initial_match_gap_s=args.initial_match_gap_s,
        final_match_gap_s=args.final_match_gap_s,
        coarse_search_window_s=args.coarse_search_window_s,
        sigma_clip=args.sigma_clip,
    )

    matched = match_idx >= 0

    rp_j = match_idx[matched].astype(int)

    # Apply the fitted clock model to EVERY ML row, not only Mark rows.
    all_ml_times = pd.to_datetime(
        ml_df_all[ts_col],
        errors="coerce",
    )

    aligned_rpi_for_all_ml = _predict_rpi_from_ml(
        all_ml_times,
        model,
    )

    predicted_offset_all = (
        aligned_rpi_for_all_ml - all_ml_times
    ).dt.total_seconds()

    label = args.label

    aligned_col = f"{label}_Aligned_RPi_Time"
    predicted_offset_col = (
        f"{label}_Predicted_Clock_Offset_s"
    )

    ml_df_all[aligned_col] = aligned_rpi_for_all_ml
    ml_df_all[predicted_offset_col] = predicted_offset_all
    ml_df_all[f"{label}_Clock_Drift_ppm"] = model.drift_ppm
    ml_df_all[f"{label}_Clock_Rate"] = model.rate

    # Mark-level diagnostics live on the original ML rows.
    ml_df_all[f"{label}_RPi_Matched"] = False
    ml_df_all[f"{label}_RPi_MatchReason"] = ""
    ml_df_all[f"{label}_RPi_Timestamp"] = pd.NaT
    ml_df_all[f"{label}_Observed_Offset_s"] = np.nan
    ml_df_all[f"{label}_Alignment_Residual_s"] = np.nan
    ml_df_all[f"{label}_Affine_Inlier"] = False

    ml_df_all[f"{label}_RPi_Index"] = pd.Series(
        [pd.NA] * len(ml_df_all),
        dtype="Int64",
    )

    source_indices = ml_marks[
        "_ml_source_index"
    ].to_numpy(dtype=int)

    for k, original_i in enumerate(source_indices):
        ml_df_all.at[
            original_i,
            f"{label}_RPi_MatchReason",
        ] = reasons[k]

        if match_idx[k] >= 0:
            j = int(match_idx[k])

            ml_df_all.at[
                original_i,
                f"{label}_RPi_Matched",
            ] = True

            ml_df_all.at[
                original_i,
                f"{label}_RPi_Timestamp",
            ] = rpi_effective.iloc[j]

            ml_df_all.at[
                original_i,
                f"{label}_Observed_Offset_s",
            ] = observed_offsets_s[k]

            ml_df_all.at[
                original_i,
                f"{label}_Alignment_Residual_s",
            ] = residual_s[k]

            ml_df_all.at[
                original_i,
                f"{label}_Affine_Inlier",
            ] = bool(fit_inlier[k])

            ml_df_all.at[
                original_i,
                f"{label}_RPi_Index",
            ] = j

    # Compact pair table for auditing.
    pair_rows = []

    for k in range(len(ml_marks)):
        j = (
            int(match_idx[k])
            if match_idx[k] >= 0
            else None
        )

        if pd.notna(ml_mark_times.iloc[k]):
            predicted_rpi = _predict_rpi_from_ml(
                pd.Series([ml_mark_times.iloc[k]]),
                model,
            ).iloc[0]

            predicted_offset_s = (
                predicted_rpi
                - ml_mark_times.iloc[k]
            ).total_seconds()
        else:
            predicted_offset_s = np.nan

        row = {
            "ml_mark_index": int(k),
            "ml_source_index": int(source_indices[k]),
            "ml_time": ml_mark_times.iloc[k],
            "matched": bool(match_idx[k] >= 0),
            "match_reason": reasons[k],
            "rpi_index": j,
            "rpi_time": (
                rpi_effective.iloc[j]
                if j is not None
                else pd.NaT
            ),
            "rpi_timestamp_source": (
                args.rpi_time_type
                if j is not None
                else ""
            ),
            "observed_offset_s": observed_offsets_s[k],
            "predicted_offset_s": predicted_offset_s,
            "alignment_residual_s": residual_s[k],
            "affine_inlier": bool(fit_inlier[k]),
        }

        pair_rows.append(row)

    pair_df = pd.DataFrame(pair_rows)

    # Optionally append unmatched RPi marks as synthetic rows,
    # preserving prior suite behavior.
    used_rpi = set(rp_j.tolist())

    unmatched_rpi = sorted(
        set(range(len(rpi_df)))
        - used_rpi
    )

    synth_rows: List[pd.Series] = []
    template_row = None

    if args.blankRowTemplate:
        tmpl = Path(args.blankRowTemplate)

        if tmpl.exists():
            tdf = pd.read_csv(tmpl)

            if not tdf.empty:
                template_row = tdf.iloc[0].copy()

    if template_row is not None:
        for j in unmatched_rpi:
            row = template_row.copy()

            rpi_t = rpi_effective.iloc[j]

            aligned_ml_t = _predict_ml_from_rpi(
                pd.Series([rpi_t]),
                model,
            ).iloc[0]

            if ts_col in row.index:
                row[ts_col] = aligned_ml_t

            row["_synthetic_from_rpi"] = True
            row[f"{label}_RPi_Timestamp"] = rpi_t
            row[f"{label}_RPi_Matched"] = False
            row[f"{label}_RPi_MatchReason"] = (
                "unmatched_rpi_synthetic"
            )
            row[f"{label}_Aligned_RPi_Time"] = rpi_t
            row[f"{label}_Clock_Drift_ppm"] = model.drift_ppm
            row[f"{label}_Clock_Rate"] = model.rate
            row[f"{label}_RPi_Index"] = j

            synth_rows.append(row)

    final_df = ml_df_all.copy()

    if synth_rows:
        final_df = pd.concat(
            [
                final_df,
                pd.DataFrame(synth_rows),
            ],
            ignore_index=True,
            sort=False,
        )

    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else ml_path.parent
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    stem = _normalize_ml_stem(
        ml_path.stem,
        [
            s.strip()
            for s in args.strip_ml_suffixes.split(",")
            if s.strip()
        ],
    )

    out_csv = (
        out_dir
        / f"{stem}_{label}_{args.device}_aligned_with_RPi.csv"
    )

    pair_csv = (
        out_dir
        / f"{stem}_{label}_{args.device}_clock_pairs.csv"
    )

    summary_csv = (
        out_dir
        / f"{stem}_{label}_{args.device}_alignment_summary.csv"
    )

    valid_all_ml_times = all_ml_times.dropna()

    if len(valid_all_ml_times) >= 2:
        elapsed_seconds = float(
            (
                valid_all_ml_times.max()
                - valid_all_ml_times.min()
            ).total_seconds()
        )
    else:
        elapsed_seconds = float("nan")

    elapsed_minutes = (
        elapsed_seconds / 60.0
        if np.isfinite(elapsed_seconds)
        else np.nan
    )

    elapsed_hours = (
        elapsed_seconds / 3600.0
        if np.isfinite(elapsed_seconds)
        else np.nan
    )

    drift_accumulated_s = (
        model.drift_ppm
        * 1e-6
        * elapsed_seconds
        if np.isfinite(elapsed_seconds)
        else np.nan
    )

    drift_accumulated_ms = (
        drift_accumulated_s * 1000.0
        if np.isfinite(drift_accumulated_s)
        else np.nan
    )

    summary = pd.DataFrame(
        [
            {
                "ml_file": ml_path.name,
                "rpi_file": rpi_path.name,
                "rpi_timestamp_column": args.rpi_time_type,
                "n_ml_rows": len(ml_df_all),
                "n_ml_marks": len(ml_marks),
                "n_rpi_marks": len(rpi_df),
                "n_matched_marks": int(matched.sum()),
                "n_affine_inliers": int(fit_inlier.sum()),
                "n_unmatched_ml_marks": int((~matched).sum()),
                "n_unmatched_rpi_marks": len(unmatched_rpi),
                "timezone_offset_hours": float(tz_hours),
                "clock_reference_time": model.reference_time,
                "clock_offset_at_reference_s": (
                    model.offset_at_reference_s
                ),
                "clock_rate": model.rate,
                "clock_drift_ppm": model.drift_ppm,
                "elapsed_time_s": elapsed_seconds,
                "elapsed_time_min": elapsed_minutes,
                "elapsed_time_hours": elapsed_hours,
                "drift_accumulated_over_elapsed_s": (
                    drift_accumulated_s
                ),
                "drift_accumulated_over_elapsed_ms": (
                    drift_accumulated_ms
                ),
                "residual_median_s": model.residual_median_s,
                "residual_mad_s": model.residual_mad_s,
                "residual_rmse_s": model.residual_rmse_s,
                "initial_match_gap_s": args.initial_match_gap_s,
                "final_match_gap_s": args.final_match_gap_s,
                "coarse_search_window_s": (
                    args.coarse_search_window_s
                ),
                "sigma_clip": args.sigma_clip,
            }
        ]
    )

    final_df.to_csv(
        out_csv,
        index=False,
    )

    pair_df.to_csv(
        pair_csv,
        index=False,
    )

    summary.to_csv(
        summary_csv,
        index=False,
    )

    print(f"[ok] wrote aligned events  -> {out_csv}")
    print(f"[ok] wrote clock pairs     -> {pair_csv}")
    print(f"[ok] wrote alignment model -> {summary_csv}")
    print(f"[info] RPi timestamp column -> {args.rpi_time_type}")

    print(
        "[fit] "
        f"offset@ref={model.offset_at_reference_s:+.6f}s, "
        f"drift={model.drift_ppm:+.3f}ppm, "
        f"inliers={model.n_inliers}/{model.n_input_pairs}, "
        f"RMSE={model.residual_rmse_s:.6f}s"
    )


if __name__ == "__main__":
    main()