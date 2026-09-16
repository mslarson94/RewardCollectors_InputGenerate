#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Classify global-affine alignment summaries using prespecified QC thresholds and distinguish true fit failure from insufficient temporal span for meaningful drift estimation.")

    ap.add_argument("--input_dir", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--recursive", action="store_true")
    ap.add_argument("--use-rmse", action=argparse.BooleanOptionalAction,default=True)
    ap.add_argument("--max-rmse-ms", type=float, default=50.0)
    ap.add_argument("--use-mad", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--max-mad-ms", type=float, default=30.0)
    ap.add_argument("--use-inlier-fraction", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--min-inlier-fraction", type=float, default=0.80)
    ap.add_argument("--max-abs-accumulated-drift-ms", type=float, default=100.0)

    ap.add_argument("--use-accumulated-drift", action=argparse.BooleanOptionalAction, default=False, 
        help="Enable accumulated-drift QC only when matched temporal span is sufficient for drift estimation.")
    
    ap.add_argument("--min-drift-span-s", type=float, default=60.0,
        help="Minimum span between first and last matched ML marks required to interpret clock drift / ppm as meaningful. Sessions below this threshold are labeled insufficient_span_for_drift.")

    ap.add_argument("--insufficient-span-counts-as-fail", action=argparse.BooleanOptionalAction, default=False,
        help="If enabled, insufficient temporal span is treated as a QC failure. Default is false: these sessions are classified separately rather than as bad affine fits.")

    return ap.parse_args()


def _first_row(path: Path) -> pd.Series:
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"Summary is empty: {path}")
    return df.iloc[0]


def _finite_float(value) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return np.nan
    return out if np.isfinite(out) else np.nan

def _optional_bool(value):
    if pd.isna(value):
        return pd.NA

    if isinstance(value, (bool, np.bool_)):
        return bool(value)

    text = str(value).strip().lower()

    if text in {"true", "1", "yes", "y", "t"}:
        return True

    if text in {"false", "0", "no", "n", "f"}:
        return False

    return pd.NA

def main() -> None:
    args = _parse_args()

    root = Path(args.input_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    pattern = (
        "**/*_global_affine_summary.csv"
        if args.recursive
        else "*_global_affine_summary.csv"
    )
    paths = sorted(root.glob(pattern))

    if not paths:
        raise FileNotFoundError(f"No *_global_affine_summary.csv files found under {root}")

    rows: list[dict] = []

    for path in paths:
        try:
            s = _first_row(path)
        except Exception as exc:
            rows.append(
                {
                    "summary_file": str(path),
                    "qc_classification": "summary_read_error",
                    "global_affine_pass": False,
                    "failed_read": True,
                    "failure_reasons": f"summary_read_error:{exc}",
                    "n_failed_criteria": 1,
                }
            )
            continue

        rmse_ms = 1000.0 * _finite_float(s.get("residual_rmse_s"))
        mad_ms = 1000.0 * _finite_float(s.get("residual_mad_s"))
        inlier_fraction = _finite_float(s.get("inlier_fraction_of_matched"))
        accumulated_ms = _finite_float(s.get("drift_accumulated_over_elapsed_ms"))
        matched_span_s = _finite_float(s.get("matched_mark_span_s"))

        sufficient_drift_span = bool(
            np.isfinite(matched_span_s)
            and matched_span_s >= args.min_drift_span_s
        )

        failed_rmse = bool(
            args.use_rmse
            and (
                not np.isfinite(rmse_ms)
                or rmse_ms > args.max_rmse_ms
            )
        )

        failed_mad = bool(
            args.use_mad
            and (
                not np.isfinite(mad_ms)
                or mad_ms > args.max_mad_ms
            )
        )

        failed_inlier_fraction = bool(
            args.use_inlier_fraction
            and (
                not np.isfinite(inlier_fraction)
                or inlier_fraction < args.min_inlier_fraction
            )
        )

        # Drift-based QC only applies when the matched marks span enough time
        # to make slope/ppm interpretation meaningful.
        failed_accumulated_drift = bool(
            args.use_accumulated_drift
            and sufficient_drift_span
            and (
                not np.isfinite(accumulated_ms)
                or abs(accumulated_ms)
                > args.max_abs_accumulated_drift_ms
            )
        )

        insufficient_span = not sufficient_drift_span

        reasons: list[str] = []
        if failed_rmse:
            reasons.append("rmse")
        if failed_mad:
            reasons.append("mad")
        if failed_inlier_fraction:
            reasons.append("inlier_fraction")
        if failed_accumulated_drift:
            reasons.append("accumulated_drift")
        if insufficient_span and args.insufficient_span_counts_as_fail:
            reasons.append("insufficient_drift_span")

        n_failed = len(reasons)
        affine_fit_failed = (
            failed_rmse
            or failed_mad
            or failed_inlier_fraction
            or failed_accumulated_drift
        )

        if affine_fit_failed:
            qc_classification = "bad_global_affine"
            global_affine_pass = False
        elif insufficient_span:
            qc_classification = "insufficient_span_for_drift"
            global_affine_pass = not args.insufficient_span_counts_as_fail
        else:
            qc_classification = "good_global_affine"
            global_affine_pass = True

        row = {
            "summary_file": str(path),
            "ml_file": s.get("ml_file", ""),
            "matched_marks_file": s.get("matched_marks_file", ""),
            "label": s.get("label", ""),
            "device": s.get("device", ""),
            "rpi_timestamp_column": s.get("rpi_timestamp_column", ""),
            "qc_classification": qc_classification,
            "global_affine_pass": global_affine_pass,
            "sufficient_drift_span": sufficient_drift_span,
            "matched_mark_span_s": matched_span_s,
            "matched_mark_span_min": (
                matched_span_s / 60.0
                if np.isfinite(matched_span_s)
                else np.nan
            ),
            
            "burst_gap_s": _finite_float(s.get("burst_gap_s")),
            "n_matched_bursts": _finite_float(s.get("n_matched_bursts")),
            "single_burst_only": _optional_bool(s.get("single_burst_only")),
            "first_burst_start_time": s.get("first_burst_start_time","",),
            "first_burst_end_time": s.get("first_burst_end_time","",),
            "last_burst_start_time": s.get("last_burst_start_time","",),
            "last_burst_end_time": s.get("last_burst_end_time","",),
            "max_interburst_gap_s": _finite_float(s.get("max_interburst_gap_s")),


            "n_failed_criteria": n_failed,
            "failure_reasons": "|".join(reasons),
            "failed_rmse": failed_rmse,
            "failed_mad": failed_mad,
            "failed_inlier_fraction": failed_inlier_fraction,
            "failed_accumulated_drift": failed_accumulated_drift,
            "residual_rmse_ms": rmse_ms,
            "residual_mad_ms": mad_ms,
            "inlier_fraction_of_matched": inlier_fraction,
            "clock_drift_ppm": _finite_float(s.get("clock_drift_ppm")),
            "elapsed_time_hours": _finite_float(s.get("elapsed_time_hours")),
            "drift_accumulated_over_elapsed_ms": accumulated_ms,
            "n_matched_marks": _finite_float(s.get("n_matched_marks")),
            "n_affine_inliers": _finite_float(s.get("n_affine_inliers")),
            "threshold_use_rmse": args.use_rmse,
            "threshold_max_rmse_ms": args.max_rmse_ms,
            "threshold_use_mad": args.use_mad,
            "threshold_max_mad_ms": args.max_mad_ms,
            "threshold_use_inlier_fraction": args.use_inlier_fraction,
            "threshold_min_inlier_fraction": args.min_inlier_fraction,
            "threshold_use_accumulated_drift": args.use_accumulated_drift,
            "threshold_max_abs_accumulated_drift_ms": args.max_abs_accumulated_drift_ms,
            "threshold_min_drift_span_s": args.min_drift_span_s,
            "threshold_insufficient_span_counts_as_fail": args.insufficient_span_counts_as_fail,
        }

        rows.append(row)

    qc_df = pd.DataFrame(rows)

    failed_df = qc_df.loc[
        qc_df["qc_classification"].eq("bad_global_affine")
        | (
            args.insufficient_span_counts_as_fail
            & qc_df["qc_classification"].eq(
                "insufficient_span_for_drift"
            )
        )
        | qc_df["qc_classification"].eq("summary_read_error")
    ].copy()

    insufficient_df = qc_df.loc[
        qc_df["qc_classification"].eq(
            "insufficient_span_for_drift"
        )
    ].copy()

    all_csv = out_dir / "global_affine_qc_all_sessions.csv"
    failed_csv = out_dir / "failed_global_affine_sessions.csv"
    insufficient_csv = out_dir / "insufficient_drift_span_sessions.csv"

    qc_df.to_csv(all_csv, index=False)
    failed_df.to_csv(failed_csv, index=False)
    insufficient_df.to_csv(insufficient_csv, index=False)

    n_total = len(qc_df)
    n_good = int(qc_df["qc_classification"].eq("good_global_affine").sum())
    n_bad = int(qc_df["qc_classification"].eq("bad_global_affine").sum())
    n_insufficient = int(qc_df["qc_classification"].eq("insufficient_span_for_drift").sum())

    print(f"[ok] scanned {n_total} global-affine summaries")
    print(f"[ok] good global affine          = {n_good}")
    print(f"[ok] bad global affine           = {n_bad}")
    print(f"[ok] insufficient drift span     = {n_insufficient}")
    print(f"[ok] wrote all-session QC        -> {all_csv}")
    print(f"[ok] wrote failure manifest      -> {failed_csv}")
    print(f"[ok] wrote insufficient-span list -> {insufficient_csv}")


if __name__ == "__main__":
    main()
