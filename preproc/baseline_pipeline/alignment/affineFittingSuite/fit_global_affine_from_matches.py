#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from RC_utilities.alignHelpers.batchAlignHelpers import (
    _fit_affine_clock,
    _normalize_ml_stem,
    _predict_ml_from_rpi,
    _predict_rpi_from_ml,
)


def _as_bool(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)

    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .isin({"1", "true", "t", "yes", "y"})
    )


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Fit and apply a global robust affine ML->RPi clock model using a frozen matched-mark table. This script never rematches marks.")

    ap.add_argument("--ml_csv_file", required=True)
    ap.add_argument("--matched_marks_csv", required=True)
    ap.add_argument("--blankRowTemplate")
    ap.add_argument("--csv_timestamp_column", default="mLT_orig")
    ap.add_argument("--label", required=True)
    ap.add_argument("--device", required=True)
    ap.add_argument("--sigma_clip", type=float, default=4.0)
    ap.add_argument("--out_dir", default="")
    ap.add_argument("--strip_ml_suffixes", default="_events_final,_events,_final")
    ap.add_argument("--max_abs_residual_s", default="auto", 
        help="Absolute residual cap used by the robust fitter. 'auto' reproduces the prior behavior: 2 * max(matcher initial gap, matcher final gap). Use 'none' to disable.")

    return ap.parse_args()


def _resolve_max_abs_residual(matched_records: pd.DataFrame, arg: str,) -> Optional[float]:
    value = str(arg).strip().lower()

    if value in {"none", "off", "disabled", ""}:
        return None

    if value != "auto":
        return float(arg)

    candidates: list[float] = []

    for col in ("matcher_initial_match_gap_s", "matcher_final_match_gap_s",):
        if col in matched_records.columns:
            vals = pd.to_numeric(matched_records[col], errors="coerce").dropna()

            if not vals.empty:
                candidates.append(float(vals.iloc[0]))

    if not candidates:
        return None

    return 2.0 * max(candidates)


def main() -> None:
    args = _parse_args()

    ml_path = Path(args.ml_csv_file)
    matches_path = Path(args.matched_marks_csv)

    if not ml_path.exists():
        raise FileNotFoundError(f"Missing ML CSV: {ml_path}")

    if not matches_path.exists():
        raise FileNotFoundError(f"Missing matched-mark CSV: {matches_path}")

    ml_df = pd.read_csv(ml_path)

    if args.csv_timestamp_column not in ml_df.columns:
        raise KeyError(f"ML CSV missing timestamp column {args.csv_timestamp_column!r}")

    matches = pd.read_csv(matches_path)

    required = {
        "record_type",
        "ml_source_index",
        "ml_time",
        "rpi_index",
        "rpi_time",
        "matched",
    }

    missing = sorted(required - set(matches.columns))

    if missing:
        raise KeyError(f"Matched-mark CSV missing required columns: {missing}")

    ml_mark_records = matches.loc[matches["record_type"].astype(str).eq("ml_mark")].copy()

    matched_mask = _as_bool(ml_mark_records["matched"])

    fit_rows = ml_mark_records.loc[matched_mask].copy()

    if len(fit_rows) < 3:
        raise ValueError(f"Need at least 3 frozen matched pairs; got {len(fit_rows)}")

    # ---------------------------------------------------------
    # BURST REPORTING
    # Read burst structure already determined by the matcher.
    # Do not independently redefine bursts here.
    # ---------------------------------------------------------

    if "matched_burst_id" in fit_rows.columns:
        burst_ids = pd.to_numeric(fit_rows["matched_burst_id"], errors="coerce",).dropna()
        n_matched_bursts = int(burst_ids.nunique())

    else:
        n_matched_bursts = np.nan

    single_burst_only = (bool(n_matched_bursts == 1) if np.isfinite(n_matched_bursts) else pd.NA)
    burst_gap_s = np.nan

    if "matcher_burst_gap_s" in fit_rows.columns:
        burst_gap_values = pd.to_numeric(fit_rows["matcher_burst_gap_s"], errors="coerce").dropna()

        if not burst_gap_values.empty:
            burst_gap_s = float(burst_gap_values.iloc[0])

    fit_ml_times = pd.to_datetime(fit_rows["ml_time"], errors="coerce",)
    fit_rpi_times = pd.to_datetime(fit_rows["rpi_time"], errors="coerce",)

    # BURST REPORTING:
    # Timing of the first and last matched bursts.
    first_burst_start_time = pd.NaT
    first_burst_end_time = pd.NaT
    last_burst_start_time = pd.NaT
    last_burst_end_time = pd.NaT
    max_interburst_gap_s = np.nan

    if ("matched_burst_id" in fit_rows.columns and np.isfinite(n_matched_bursts) and n_matched_bursts > 0):
        
        burst_df = fit_rows[["matched_burst_id", "ml_time"]].copy()
        burst_df["matched_burst_id"] = pd.to_numeric(burst_df["matched_burst_id"],errors="coerce")
        burst_df["ml_time"] = pd.to_datetime(burst_df["ml_time"], errors="coerce",)
        burst_df = burst_df.dropna(subset=["matched_burst_id", "ml_time",])

        if not burst_df.empty:
            burst_summary = burst_df.groupby("matched_burst_id",sort=True,)["ml_time"].agg(burst_start="min", burst_end="max",).reset_index()
            first_burst_start_time = burst_summary.iloc[0]["burst_start"]
            first_burst_end_time = burst_summary.iloc[0]["burst_end"]
            last_burst_start_time = burst_summary.iloc[-1]["burst_start"]
            last_burst_end_time = burst_summary.iloc[-1]["burst_end"]

            if len(burst_summary) >= 2:
                interburst_gaps = (

                    burst_summary["burst_start"].iloc[1:].reset_index(drop=True) - burst_summary["burst_end"].iloc[:-1].reset_index(drop=True)

                    ).dt.total_seconds()

                if len(interburst_gaps):
                    max_interburst_gap_s = float(interburst_gaps.max())

    max_abs_residual_s = _resolve_max_abs_residual(fit_rows, args.max_abs_residual_s,)

    model, fit_inlier, residual_s = (
        _fit_affine_clock(
            fit_ml_times,
            fit_rpi_times,
            sigma_clip=args.sigma_clip,
            min_pairs=3,
            max_abs_residual_s=(
                max_abs_residual_s
            ),
        )
    )

    # Apply final global model to every original ML row.
    all_ml_times = pd.to_datetime(ml_df[args.csv_timestamp_column], errors="coerce",)

    aligned_rpi = _predict_rpi_from_ml(all_ml_times, model)

    predicted_offset = (aligned_rpi - all_ml_times).dt.total_seconds()

    label = args.label

    ml_df[f"{label}_Aligned_RPi_Time"] = aligned_rpi
    ml_df[f"{label}_Predicted_Clock_Offset_s"] = predicted_offset
    ml_df[f"{label}_Clock_Drift_ppm"] = model.drift_ppm
    ml_df[f"{label}_Clock_Rate"] = model.rate

    # Mark-level diagnostics on the original ML rows.
    ml_df[f"{label}_RPi_Matched"] = False
    ml_df[f"{label}_RPi_Timestamp"] = pd.NaT
    ml_df[f"{label}_Observed_Offset_s"] = np.nan
    ml_df[f"{label}_Alignment_Residual_s"] = np.nan
    ml_df[f"{label}_Affine_Inlier"] = False
    ml_df[f"{label}_RPi_Index"] = pd.Series([pd.NA] * len(ml_df), dtype="Int64")

    diagnostics = (
        ml_mark_records.copy()
    )

    diagnostics["global_predicted_rpi_time"] = pd.NaT
    diagnostics["global_predicted_offset_s"] = np.nan
    diagnostics["global_alignment_residual_s"] = np.nan
    diagnostics["global_affine_inlier"] = False

    # Map fitted diagnostics back to rows in the frozen match table.
    fit_positions = fit_rows.index.to_list()

    for local_k, match_table_idx in enumerate(fit_positions):
        row = fit_rows.loc[match_table_idx]

        ml_t = pd.to_datetime(row["ml_time"], errors="coerce",)

        rpi_t = pd.to_datetime(row["rpi_time"], errors="coerce",)

        pred = _predict_rpi_from_ml(pd.Series([ml_t]),model, ).iloc[0]

        diagnostics.loc[match_table_idx, "global_predicted_rpi_time",] = pred
        diagnostics.loc[match_table_idx, "global_predicted_offset_s",] = ((pred - ml_t).total_seconds() if pd.notna(pred) and pd.notna(ml_t) else np.nan)
        diagnostics.loc[match_table_idx, "global_alignment_residual_s",] = residual_s[local_k]
        diagnostics.loc[match_table_idx,"global_affine_inlier",] = bool(fit_inlier[local_k])

        source_index = int(row["ml_source_index"])

        rpi_index = int(row["rpi_index"])

        if 0 <= source_index < len(ml_df):

            ml_df.at[source_index, f"{label}_RPi_Matched"] = True
            ml_df.at[source_index, f"{label}_RPi_Timestamp"] = rpi_t
            ml_df.at[source_index, f"{label}_Observed_Offset_s"] = ((rpi_t - ml_t).total_seconds() if pd.notna(rpi_t) and pd.notna(ml_t) else np.nan)
            ml_df.at[source_index,f"{label}_Alignment_Residual_s"] = residual_s[local_k]
            ml_df.at[source_index, f"{label}_Affine_Inlier"] = bool(fit_inlier[local_k])
            ml_df.at[source_index, f"{label}_RPi_Index"] = rpi_index

    # Preserve unmatched RPi marks as synthetic event rows if a blank-row template is provided, matching the legacy pipeline behavior.
    rpi_only = matches.loc[matches["record_type"].astype(str).eq("rpi_only")].copy()

    synth_rows: List[pd.Series] = []

    template_row = None

    if args.blankRowTemplate:
        template_path = Path(args.blankRowTemplate)

        if template_path.exists():
            template_df = pd.read_csv(template_path)

            if not template_df.empty:
                template_row = template_df.iloc[0].copy()
                

    if template_row is not None:
        for _, unmatched in rpi_only.iterrows():
            rpi_t = pd.to_datetime(unmatched["rpi_time"], errors="coerce",)

            if pd.isna(rpi_t):
                continue

            row = template_row.copy()

            aligned_ml_t = (
                _predict_ml_from_rpi(
                    pd.Series([rpi_t]),
                    model,
                ).iloc[0]
            )

            if (args.csv_timestamp_column in row.index):
                row[args.csv_timestamp_column] = aligned_ml_t

            row["_synthetic_from_rpi"] = True
            row[f"{label}_RPi_Timestamp"] = rpi_t
            row[f"{label}_RPi_Matched"] = False
            row[f"{label}_Aligned_RPi_Time"] = rpi_t
            row[f"{label}_Clock_Drift_ppm"] = model.drift_ppm
            row[f"{label}_Clock_Rate"] = model.rate

            if pd.notna(unmatched.get("rpi_index")):
                row[f"{label}_RPi_Index"] = int(unmatched["rpi_index"])

            synth_rows.append(row)

    final_df = ml_df.copy()

    if synth_rows:
        final_df = pd.concat([final_df, pd.DataFrame(synth_rows)], ignore_index=True, sort=False,)

    valid_all_ml_times = all_ml_times.dropna()

    if len(valid_all_ml_times) >= 2:
        elapsed_seconds = float((valid_all_ml_times.max() - valid_all_ml_times.min()).total_seconds())
    else:
        elapsed_seconds = np.nan

    elapsed_minutes = (elapsed_seconds / 60.0
        if np.isfinite(elapsed_seconds)
        else np.nan)

    elapsed_hours = (elapsed_seconds / 3600.0
        if np.isfinite(elapsed_seconds)
        else np.nan)

    drift_accumulated_s = (model.drift_ppm * 1e-6 * elapsed_seconds
        if np.isfinite(elapsed_seconds)
        else np.nan)

    drift_accumulated_ms = (drift_accumulated_s * 1000.0
        if np.isfinite(drift_accumulated_s)
        else np.nan)

    n_matched = len(fit_rows)

    n_inliers = int(np.sum(fit_inlier))

    inlier_fraction = (n_inliers / n_matched
        if n_matched
        else np.nan)

    valid_fit_ml_times = fit_ml_times.dropna()

    if len(valid_fit_ml_times) >= 2:
        matched_mark_span_s = float((valid_fit_ml_times.max() - valid_fit_ml_times.min()).total_seconds())
    else:
        matched_mark_span_s = np.nan

    matched_mark_span_min = (matched_mark_span_s / 60.0
        if np.isfinite(matched_mark_span_s)
        else np.nan)

    rpi_timestamp_column = ""

    if ("rpi_timestamp_source"
        in fit_rows.columns):

        vals = (
            fit_rows["rpi_timestamp_source"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

        if len(vals) == 1:
            rpi_timestamp_column = vals[0]

        elif vals:
            rpi_timestamp_column = (
                "|".join(
                    sorted(vals)
                )
            )

    summary = pd.DataFrame(
        [
            {
                "ml_file": ml_path.name,
                "matched_marks_file": matches_path.name,
                "label": args.label,
                "device": args.device,
                "rpi_timestamp_column": rpi_timestamp_column,
                "n_ml_rows": len(ml_df),
                "n_ml_marks": len(ml_mark_records),
                "n_matched_marks": n_matched,
                "n_affine_inliers": n_inliers,
                "n_affine_outliers": int(n_matched- n_inliers),
                "inlier_fraction_of_matched": inlier_fraction,
                "n_unmatched_ml_marks": int(len( ml_mark_records) - n_matched),
                "n_unmatched_rpi_marks": len(rpi_only),

                # BURST REPORTING
                "burst_gap_s": burst_gap_s,
                "n_matched_bursts": n_matched_bursts,
                "single_burst_only": single_burst_only,
                "first_burst_start_time": first_burst_start_time,
                "first_burst_end_time": first_burst_end_time,
                "last_burst_start_time": last_burst_start_time,
                "last_burst_end_time": last_burst_end_time,
                "max_interburst_gap_s": max_interburst_gap_s,
                "matched_mark_span_s": matched_mark_span_s,
                "matched_mark_span_min": matched_mark_span_min,
                "clock_reference_time": model.reference_time,
                "clock_offset_at_reference_s": model.offset_at_reference_s,
                "clock_rate": model.rate,
                "clock_drift_ppm": model.drift_ppm,
                "elapsed_time_s": elapsed_seconds,
                "elapsed_time_min": elapsed_minutes,
                "elapsed_time_hours": elapsed_hours,
                "drift_accumulated_over_elapsed_s": drift_accumulated_s,
                "drift_accumulated_over_elapsed_ms": drift_accumulated_ms,
                "residual_median_s": model.residual_median_s,
                "residual_mad_s": model.residual_mad_s,
                "residual_rmse_s": model.residual_rmse_s,
                "sigma_clip": args.sigma_clip,
                "max_abs_residual_s": max_abs_residual_s,
            }
        ]
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
            for s
            in args.strip_ml_suffixes.split(",")
            if s.strip()
        ],
    )

    aligned_csv = out_dir / f"{stem}_{label}_{args.device}_aligned_with_RPi.csv"

    diagnostics_csv = out_dir / f"{stem}_{label}_{args.device}_global_affine_mark_diagnostics.csv"
    

    summary_csv = out_dir / f"{stem}_{label}_{args.device}_global_affine_summary.csv"

    final_df.to_csv(aligned_csv, index=False)

    diagnostics.to_csv(diagnostics_csv, index=False)

    summary.to_csv(summary_csv, index=False)

    print(f"[ok] wrote aligned events      -> {aligned_csv}")

    print(f"[ok] wrote affine diagnostics  -> {diagnostics_csv}")

    print(f"[ok] wrote global summary      -> {summary_csv}")

    print(
        "[fit] "
        f"offset@ref="
        f"{model.offset_at_reference_s:+.6f}s, "
        f"drift="
        f"{model.drift_ppm:+.3f}ppm, "
        f"inliers="
        f"{n_inliers}/{n_matched}, "
        f"RMSE="
        f"{model.residual_rmse_s:.6f}s"
    )


if __name__ == "__main__":
    main()