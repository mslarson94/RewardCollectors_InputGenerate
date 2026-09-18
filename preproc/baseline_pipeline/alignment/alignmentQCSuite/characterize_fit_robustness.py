#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#characterize_fit_robustness.py

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


DIAGNOSTIC_SUFFIX = "_global_affine_mark_diagnostics.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Characterize fit robustness and mark-level measurement noise for global-affine ML/RPi alignment sessions.")

    parser.add_argument("--root", required=True, help="Directory containing *_global_affine_mark_diagnostics.csv files.")
    parser.add_argument("--manifest-csv", default="", help="Optional session manifest, such as global_fail_low_local_movement.csv. If omitted, all diagnostics files are analyzed.")
    parser.add_argument("--out-dir", default="", help="Output directory. Defaults to <root>/FitRobustnessQC.")
    parser.add_argument("--recursive", action=argparse.BooleanOptionalAction, default=True)

    # ---------------------------------------------------------
    # Screening thresholds.
    #
    # These only create diagnostic flags. They do NOT alter
    # alignment or global-affine QC classifications.
    # ---------------------------------------------------------

    parser.add_argument("--large-method-offset-delta-ms", type=float, default=25.0)
    parser.add_argument("--large-method-drift-delta-ppm", type=float, default=50.0)
    parser.add_argument("--large-loo-mark-offset-delta-ms", type=float, default=25.0)
    parser.add_argument("--large-loo-mark-drift-delta-ppm", type=float, default=50.0)
    parser.add_argument("--large-loo-burst-offset-delta-ms", type=float, default=50.0)
    parser.add_argument("--large-loo-burst-drift-delta-ppm", type=float, default=50.0)
    parser.add_argument("--first-mark-improvement-ms", type=float, default=20.0, help="Diagnostic threshold for calling removal of first-in-burst marks materially beneficial.")
    parser.add_argument("--debug", action="store_true")

    parser.add_argument("--min-retained-bursts-for-loo-burst", type=int, default=3,
        help="Minimum number of distinct bursts that must remain after holding one burst out. Default=3, so a session generally needs at least 4 bursts for leave-one-burst robustness testing." )

    return parser.parse_args()


def _session_name(path: Path) -> str:
    if path.name.endswith(DIAGNOSTIC_SUFFIX):
        return path.name[:-len(DIAGNOSTIC_SUFFIX)]
    return path.stem


def _to_bool(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})
    


def _finite(values: np.ndarray) -> np.ndarray:
    values = np.asarray(values, dtype=float)
    return values[np.isfinite(values)]


def _rmse(values: np.ndarray) -> float:
    values = _finite(values)

    if values.size == 0:
        return np.nan

    return float(np.sqrt(np.mean(values ** 2)))


def _mad(values: np.ndarray) -> float:
    """
    Unscaled median absolute deviation around the median.
    """

    values = _finite(values)

    if values.size == 0:
        return np.nan
    center = np.median(values)
    return float(np.median(np.abs(values - center)))


def _median_abs(values: np.ndarray) -> float:
    values = _finite(values)

    if values.size == 0:
        return np.nan
    return float(np.median(np.abs(values)))


def _percentile_abs(values: np.ndarray, percentile: float,) -> float:
    values = _finite(values)

    if values.size == 0:
        return np.nan
    return float(np.percentile(np.abs(values), percentile))


def _fit_offset_ols(elapsed_s: np.ndarray, observed_offset_s: np.ndarray) -> tuple[float, float]:
    """
    Fit:

        offset = drift_s_per_s * elapsed + offset_at_reference

    This is equivalent to:

        t_RPi = a * t_ML + b

    where:

        a = 1 + drift_s_per_s
        drift_ppm = drift_s_per_s * 1e6

    Fitting offset against elapsed time avoids numerical problems
    from regressing epoch timestamps directly.
    """

    x = np.asarray(elapsed_s, dtype=float)

    y = np.asarray(observed_offset_s, dtype=float)

    valid = (np.isfinite(x) & np.isfinite(y))

    x = x[valid]
    y = y[valid]

    if len(x) < 2:
        raise ValueError("Need at least two finite points for OLS.")

    if np.ptp(x) <= 0:
        raise ValueError("Elapsed-time span is zero.")

    design = np.column_stack([x, np.ones(len(x))])

    coefficients, *_ = np.linalg.lstsq(design, y, rcond=None)

    slope = float(coefficients[0])
    intercept = float(coefficients[1])

    return slope, intercept


def _fit_offset_huber(
    elapsed_s: np.ndarray,
    observed_offset_s: np.ndarray,
    *,
    delta: float = 1.345,
    max_iter: int = 100,
    tolerance: float = 1e-12,
) -> tuple[float, float]:
    """
    Huber regression via iteratively reweighted least squares.

    No sklearn dependency is required.
    """

    x = np.asarray(elapsed_s, dtype=float)
    y = np.asarray(observed_offset_s, dtype=float)

    valid = (np.isfinite(x) & np.isfinite(y))

    x = x[valid]
    y = y[valid]

    if len(x) < 2:
        raise ValueError("Need at least two finite points for Huber fit.")

    slope, intercept = _fit_offset_ols(x, y)

    for _ in range(max_iter):
        prediction = (slope * x) + intercept

        residual = y - prediction
        residual_center = np.median(residual)

        # MAD? 
        scale = (1.4826 * np.median(np.abs(residual - residual_center)))

        if (not np.isfinite(scale) or scale <= 1e-12):
            break

        cutoff = delta * scale
        

        abs_residual = np.abs(residual)

        weights = np.ones(len(residual), dtype=float)

        high = abs_residual > cutoff
        

        weights[high] = cutoff / abs_residual[high]
        
        design = np.column_stack([x, np.ones(len(x))])

        sqrt_w = np.sqrt(weights)
        weighted_design = design * sqrt_w[:, None]
        weighted_y = y * sqrt_w
        

        coefficients, *_ = np.linalg.lstsq(weighted_design, weighted_y, rcond=None)

        new_slope = float(coefficients[0])
        new_intercept = float(coefficients[1])

        change = max(abs(new_slope - slope), abs(new_intercept - intercept))

        slope = new_slope
        intercept = new_intercept

        if change < tolerance:
            break

    return slope, intercept


def _predict_offset(elapsed_s: np.ndarray, slope: float, intercept: float) -> np.ndarray:
    return ((slope * np.asarray(elapsed_s, dtype=float,)) + intercept)



def _model_residuals(elapsed_s: np.ndarray, observed_offset_s: np.ndarray, slope: float, intercept: float,) -> np.ndarray:
    return (np.asarray(observed_offset_s, dtype=float) - _predict_offset(elapsed_s, slope, intercept))


def _drift_ppm(slope: float) -> float:
    return float(slope * 1e6)


def _fraction_above(values: np.ndarray, threshold_s: float) -> float:
    values = _finite(values)

    if values.size == 0:
        return np.nan

    return float(np.mean(np.abs(values) > threshold_s))


def _fit_metrics(
    elapsed_s: np.ndarray,
    observed_offset_s: np.ndarray,
    slope: float,
    intercept: float,
    prefix: str,
    ) -> dict:
    residual = _model_residuals(
        elapsed_s,
        observed_offset_s,
        slope,
        intercept,
    )

    return {
        f"{prefix}_drift_ppm": _drift_ppm(slope),
        f"{prefix}_offset_at_reference_ms": float(intercept * 1000.0),
        f"{prefix}_rmse_ms": float(_rmse(residual) * 1000.0),
        f"{prefix}_mad_ms": float(_mad(residual) * 1000.0),
        f"{prefix}_median_abs_residual_ms": float(_median_abs(residual) * 1000.0),
        f"{prefix}_p95_abs_residual_ms": float(_percentile_abs(residual, 95) * 1000.0),
    }


def _load_manifest_sessions( path: Path) -> set[str]:
    manifest = pd.read_csv(path)

    if "session" not in manifest.columns:
        raise KeyError(f"{path}: manifest must contain a 'session' column.")

    return set(manifest["session"].dropna().astype(str).str.strip())


def _prepare_session(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    required = [
        "ml_time",
        "rpi_time",
        "matched_burst_id",
        "matched_burst_position",
    ]

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise KeyError(f"{path.name}: missing columns {missing}")

    work = df.copy()

    work["_ml_time"] = pd.to_datetime(work["ml_time"], errors="coerce")
    work["_rpi_time"] = pd.to_datetime(work["rpi_time"], errors="coerce")
    work["_burst_id"] = pd.to_numeric(work["matched_burst_id"], errors="coerce")
    work["_burst_position"] = pd.to_numeric(work["matched_burst_position"], errors="coerce")

    if "matched" in work.columns:
        matched = _to_bool(work["matched"])
    else:
        matched = pd.Series(True, index=work.index)

    valid = (
        matched
        & work["_ml_time"].notna()
        & work["_rpi_time"].notna()
        & work["_burst_id"].notna()
        & work["_burst_position"].notna()
    )

    work = work.loc[valid].copy().reset_index(drop=True)
    
    if len(work) < 3:
        raise ValueError(
            f"{path.name}: fewer than three usable matched marks."
        )

    reference_ml = work["_ml_time"].min()
    

    work["_elapsed_s"] = (work["_ml_time"] - reference_ml).dt.total_seconds()

    work["_observed_offset_s"] = (work["_rpi_time"] - work["_ml_time"]).dt.total_seconds()

    if "global_alignment_residual_s" in work.columns:
        work["_current_residual_s"] = pd.to_numeric(work["global_alignment_residual_s"], errors="coerce")
    else:
        work["_current_residual_s"] = np.nan

    if "global_affine_inlier" in work.columns:
        work["_global_inlier"] = _to_bool(work["global_affine_inlier"])
    else:
        work["_global_inlier"] = True

    return work


def _leave_one_mark_analysis(work: pd.DataFrame, full_slope: float, full_intercept: float) -> tuple[dict, pd.DataFrame]:

    details = []

    held_out_residuals = []

    for index in work.index:
        train = work.drop(index=index)

        test = work.loc[[index]]

        if (len(train) < 3 or np.ptp(train["_elapsed_s"].to_numpy(dtype=float)) <= 0):
            continue

        try:
            slope, intercept = _fit_offset_huber(
                train["_elapsed_s"].to_numpy(dtype=float),
                train["_observed_offset_s"].to_numpy(dtype=float),
            )
        except Exception:
            continue

        test_elapsed = float(test["_elapsed_s"].iloc[0])

        test_offset = float(test["_observed_offset_s"].iloc[0])

        predicted = ((slope * test_elapsed) + intercept)
        held_out_residual = test_offset - predicted

        held_out_residuals.append(held_out_residual)

        details.append(
            {
                "left_out_index": int(index),
                "burst_id": int(test["_burst_id"].iloc[0]),
                "burst_position": int(test["_burst_position"].iloc[0]),
                "held_out_residual_ms": held_out_residual * 1000.0,
                "refit_drift_ppm": _drift_ppm(slope),
                "delta_drift_ppm":_drift_ppm(slope) - _drift_ppm(full_slope),
                "refit_offset_at_reference_ms": intercept * 1000.0,
                "delta_offset_at_reference_ms": (intercept - full_intercept) * 1000.0,
            }
        )

    details_df = pd.DataFrame(details)

    if details_df.empty:
        summary = {
            "loo_mark_n": 0,
            "loo_mark_cv_rmse_ms": np.nan,
            "loo_mark_cv_mad_ms": np.nan,
            "loo_mark_max_abs_delta_drift_ppm": np.nan,
            "loo_mark_max_abs_delta_offset_ms": np.nan,
        }

        return summary, details_df

    residual_array = np.asarray(
        held_out_residuals,
        dtype=float,
    )

    summary = {
        "loo_mark_n": int(len(details_df)),
        "loo_mark_cv_rmse_ms": _rmse(residual_array) * 1000.0,
        "loo_mark_cv_mad_ms": _mad(residual_array) * 1000.0,
        "loo_mark_max_abs_delta_drift_ppm": float(details_df["delta_drift_ppm"].abs().max()),
        "loo_mark_max_abs_delta_offset_ms": float(details_df["delta_offset_at_reference_ms"].abs().max()),
    }

    return summary, details_df


def _leave_one_burst_analysis(work: pd.DataFrame, full_slope: float, full_intercept: float, *, min_retained_bursts: int) -> tuple[dict, pd.DataFrame]:
    details = []

    all_held_out_residuals = []

    for burst_id in sorted(work["_burst_id"].unique()):
        train = work.loc[work["_burst_id"] != burst_id]

        test = work.loc[work["_burst_id"] == burst_id]

        remaining_bursts = int(train["_burst_id"].nunique())

        # ---------------------------------------------------------
        # A drift/slope robustness test is not meaningful if
        # removing one burst leaves too little independent temporal
        # structure.
        # ---------------------------------------------------------

        if remaining_bursts < min_retained_bursts:
            continue

        if len(train) < 3:
            continue

        train_elapsed = train["_elapsed_s"].to_numpy(dtype=float)

        if np.ptp(train_elapsed) <= 0:
            continue

        try:
            slope, intercept = _fit_offset_huber(train_elapsed, train["_observed_offset_s"].to_numpy(dtype=float))
        except Exception:
            continue

        held_out_residual = _model_residuals(
            test["_elapsed_s"].to_numpy(dtype=float),
            test["_observed_offset_s"].to_numpy(dtype=float),
            slope,
            intercept,
        )

        all_held_out_residuals.extend(held_out_residual.tolist())

        details.append(
            {
                "burst_id": int(burst_id),
                "n_held_out_marks": int(len(test)),
                "n_retained_bursts": remaining_bursts,
                "held_out_rmse_ms": _rmse(held_out_residual) * 1000.0,
                "held_out_median_residual_ms": float(np.median(held_out_residual) * 1000.0),

                "refit_drift_ppm": _drift_ppm(slope),
                "delta_drift_ppm": _drift_ppm(slope) - _drift_ppm(full_slope),

                "refit_offset_at_reference_ms": intercept * 1000.0,
                "delta_offset_at_reference_ms": (intercept - full_intercept) * 1000.0,
            }
        )

    details_df = pd.DataFrame(
        details
    )

    if details_df.empty:
        summary = {
            "loo_burst_test_available": False,
            "loo_burst_n": 0,
            "loo_burst_cv_rmse_ms": np.nan,
            "loo_burst_cv_mad_ms": np.nan,
            "loo_burst_max_abs_delta_drift_ppm": np.nan,
            "loo_burst_max_abs_delta_offset_ms": np.nan,
        }

        return summary, details_df

    residual_array = np.asarray(
        all_held_out_residuals,
        dtype=float,
    )

    summary = {
        "loo_burst_test_available": True,
        "loo_burst_n": int(len(details_df)),
        "loo_burst_cv_rmse_ms": _rmse(residual_array) * 1000.0,
        "loo_burst_cv_mad_ms": _mad(residual_array) * 1000.0,
        "loo_burst_max_abs_delta_drift_ppm": float(details_df["delta_drift_ppm"].abs().max()),
        "loo_burst_max_abs_delta_offset_ms": float(details_df["delta_offset_at_reference_ms"].abs().max()),
    }

    return summary, details_df


def _within_burst_noise(work: pd.DataFrame,) -> dict:
    """
    Estimate mark scatter within bursts using current global-affine
    residuals.

    Stable marks only are used here (burst position > 1).
    """

    burst_mads = []

    burst_medians = []

    for _, group in work.groupby("_burst_id"):
        stable = group.loc[group["_burst_position"] > 1]

        values = _finite(stable["_current_residual_s"].to_numpy(dtype=float))

        if values.size >= 2:
            burst_mads.append(_mad(values))

        if values.size >= 1:
            burst_medians.append(float(np.median(values)))

    if burst_mads:
        within_mad_ms = float(np.median(burst_mads)) * 1000.0
        
    else:
        within_mad_ms = np.nan

    burst_medians_array = np.asarray(burst_medians, dtype=float)

    if burst_medians_array.size >= 2:
        between_range_ms = float(np.max(burst_medians_array) - np.min(burst_medians_array)) * 1000.0
        between_mad_ms =  _mad(burst_medians_array) * 1000.0
        
    else:
        between_range_ms = np.nan
        between_mad_ms = np.nan

    return {
        "within_burst_median_mad_ms": within_mad_ms,
        "between_burst_median_range_ms": between_range_ms,
        "between_burst_mad_of_medians_ms": between_mad_ms,
        "n_bursts_with_stable_residuals": int(len(burst_medians)),
    }


def characterize_session(path: Path, args: argparse.Namespace) -> tuple[dict, pd.DataFrame, pd.DataFrame]:

    session = _session_name(path)
    work = _prepare_session(path)

    elapsed = work["_elapsed_s"].to_numpy(dtype=float)
    offset = work["_observed_offset_s"].to_numpy(dtype=float)
    current_residual = work["_current_residual_s"].to_numpy(dtype=float)
    stable_mask = work["_burst_position"].to_numpy(dtype=float) > 1

    # =========================================================
    # Current pipeline residuals
    # =========================================================

    current_finite = _finite(current_residual)
    current_stable = _finite(current_residual[stable_mask])

    # =========================================================
    # OLS and Huber full-session fits
    # =========================================================

    ols_slope, ols_intercept = _fit_offset_ols(elapsed, offset)

    huber_slope, huber_intercept = _fit_offset_huber(elapsed, offset)
    

    summary = {
        "session": session,
        "diagnostics_file": str(path),
        "n_marks": int(len(work)),
        "n_bursts": int(work["_burst_id"].nunique()),
        "n_first_marks": int(np.sum(~stable_mask)),
        "n_stable_marks": int(np.sum(stable_mask)),
        "matched_span_s": float(np.max(elapsed) - np.min(elapsed)),

        "current_rmse_ms": _rmse(current_finite) * 1000.0,
        "current_mad_ms": _mad(current_finite) * 1000.0,

        "current_stable_rmse_ms": _rmse(current_stable) * 1000.0,
        "current_stable_mad_ms": _mad(current_stable) * 1000.0,

        "current_p95_abs_residual_ms": _percentile_abs(current_finite, 95) * 1000.0,

        "current_fraction_abs_gt_50ms": _fraction_above(current_finite, 0.050),
        "current_fraction_abs_gt_100ms": _fraction_above(current_finite, 0.100),
        "current_fraction_abs_gt_150ms": _fraction_above(current_finite, 0.150),
    }

    summary.update(
        _fit_metrics(
            elapsed,
            offset,
            ols_slope,
            ols_intercept,
            "ols",
        )
    )

    summary.update(
        _fit_metrics(
            elapsed,
            offset,
            huber_slope,
            huber_intercept,
            "huber",
            )
    )

    # =========================================================
    # OLS vs Huber sensitivity
    # =========================================================

    summary["huber_vs_ols_delta_drift_ppm"] = _drift_ppm(huber_slope) - _drift_ppm(ols_slope)
    summary["huber_vs_ols_abs_delta_drift_ppm"] = abs(summary["huber_vs_ols_delta_drift_ppm"])
    summary["huber_vs_ols_delta_offset_at_reference_ms"] = (huber_intercept - ols_intercept) * 1000.0
    summary["huber_vs_ols_abs_delta_offset_at_reference_ms"] = abs(summary["huber_vs_ols_delta_offset_at_reference_ms"])

    # =========================================================
    # Stable-only fit
    # =========================================================

    if (np.sum(stable_mask) >= 3 and np.ptp(elapsed[stable_mask]) > 0):

        stable_slope, stable_intercept = _fit_offset_huber(elapsed[stable_mask], offset[stable_mask])
        stable_residual =  _model_residuals(elapsed[stable_mask], offset[stable_mask], stable_slope, stable_intercept)
        
        summary["stable_only_fit_drift_ppm"] = _drift_ppm(stable_slope)
        summary["stable_only_fit_offset_at_reference_ms"] = stable_intercept * 1000.0

        summary["stable_only_fit_rmse_ms"] =  _rmse(stable_residual) * 1000.0
        summary["stable_only_fit_mad_ms"] =  _mad(stable_residual) * 1000.0
        

        summary["stable_only_vs_huber_delta_drift_ppm"] = _drift_ppm(stable_slope) - _drift_ppm(huber_slope)
        summary["stable_only_vs_huber_delta_offset_ms"] = (stable_intercept - huber_intercept) * 1000.0

    else:
        summary["stable_only_fit_drift_ppm"] = np.nan
        summary["stable_only_fit_offset_at_reference_ms"] = np.nan

        summary["stable_only_fit_rmse_ms"] = np.nan
        summary["stable_only_fit_mad_ms"] = np.nan

        summary["stable_only_vs_huber_delta_drift_ppm"] = np.nan
        summary["stable_only_vs_huber_delta_offset_ms"] = np.nan

    # =========================================================
    # First-mark effect using current pipeline residuals
    # =========================================================

    if (np.isfinite(summary["current_rmse_ms"]) and np.isfinite(summary["current_stable_rmse_ms"])):

        summary["rmse_improvement_excluding_first_ms"] = summary["current_rmse_ms"] - summary["current_stable_rmse_ms"]
        
    else:
        summary["rmse_improvement_excluding_first_ms"] = np.nan

    if (np.isfinite(summary["current_mad_ms"]) and np.isfinite(summary["current_stable_mad_ms"])):

        summary["mad_improvement_excluding_first_ms"] = summary["current_mad_ms"] - summary["current_stable_mad_ms"]
    else:
        summary["mad_improvement_excluding_first_ms"] = np.nan

    # =========================================================
    # Leave-one-mark-out robustness
    # =========================================================

    loo_mark_summary, loo_mark_details = _leave_one_mark_analysis(work, huber_slope, huber_intercept)

    summary.update(loo_mark_summary)

    if not loo_mark_details.empty:
        loo_mark_details.insert(0, "session", session)

    # =========================================================
    # Leave-one-burst-out robustness
    # =========================================================

    
    loo_burst_summary, loo_burst_details = _leave_one_burst_analysis(work, huber_slope, huber_intercept, min_retained_bursts=(args.min_retained_bursts_for_loo_burst))
    summary.update(loo_burst_summary)

    if not loo_burst_details.empty:
        loo_burst_details.insert(0, "session", session)

    # =========================================================
    # Within-burst measurement noise
    # =========================================================

    summary.update(_within_burst_noise(work))

    # =========================================================
    # Diagnostic flags
    #
    # These are descriptive screening flags only.
    # =========================================================

    method_sensitive = (
        (np.isfinite(summary["huber_vs_ols_abs_delta_offset_at_reference_ms"]) and summary["huber_vs_ols_abs_delta_offset_at_reference_ms"] > args.large_method_offset_delta_ms)
        or (np.isfinite(summary["huber_vs_ols_abs_delta_drift_ppm"]) and summary["huber_vs_ols_abs_delta_drift_ppm"] > args.large_method_drift_delta_ppm))

    mark_sensitive = (
        (np.isfinite(summary["loo_mark_max_abs_delta_offset_ms"]) and summary["loo_mark_max_abs_delta_offset_ms"] > args.large_loo_mark_offset_delta_ms)
        or (np.isfinite(summary["loo_mark_max_abs_delta_drift_ppm"]) and summary["loo_mark_max_abs_delta_drift_ppm"] > args.large_loo_mark_drift_delta_ppm))

    burst_test_available = bool(summary["loo_burst_test_available"])
    burst_sensitive = (
        (np.isfinite(summary["loo_burst_max_abs_delta_offset_ms"]) and summary["loo_burst_max_abs_delta_offset_ms"] > args.large_loo_burst_offset_delta_ms)
        or (np.isfinite(summary["loo_burst_max_abs_delta_drift_ppm"]) and summary["loo_burst_max_abs_delta_drift_ppm"] > args.large_loo_burst_drift_delta_ppm))

    first_mark_sensitive = np.isfinite(summary["rmse_improvement_excluding_first_ms"]) and summary["rmse_improvement_excluding_first_ms"] >= args.first_mark_improvement_ms

    summary["fit_method_sensitive"] = bool(method_sensitive)
    summary["leave_one_mark_sensitive"] = bool(mark_sensitive)
    summary["leave_one_burst_sensitive"] = bool(burst_sensitive)
    summary["first_mark_sensitive"] = bool(first_mark_sensitive)
    summary["loo_burst_test_unavailable"] = not burst_test_available
    flags = []

    if method_sensitive:
        flags.append("fit_method_sensitive")

    if mark_sensitive:
        flags.append("mark_sensitive")

    if burst_sensitive:
        flags.append("burst_sensitive")

    if first_mark_sensitive:
        flags.append("first_mark_sensitive")

    if not flags:
        flags.append("fit_stable")

    summary["robustness_flags"] = ";".join(flags)

    if flags == ["fit_stable"]:
        summary["robustness_primary_category"] = "stable_but_noisy"

    elif len(flags) > 1:
        summary["robustness_primary_category"] = "mixed_sensitivity"

    elif flags[0] == "fit_method_sensitive":
        summary["robustness_primary_category"] = "fit_method_sensitive"

    elif flags[0] == "mark_sensitive":
        summary["robustness_primary_category"] = "outlier_or_mark_sensitive"

    elif flags[0] == "burst_sensitive":
        summary["robustness_primary_category"] = "burst_sensitive"

    elif flags[0] == "first_mark_sensitive":
        summary["robustness_primary_category"] = "first_mark_sensitive"

    else:
        summary["robustness_primary_category"] = "unclassified"

    return summary, loo_mark_details, loo_burst_details,
    


def main() -> None:
    args = parse_args()

    root = Path(args.root).expanduser().resolve()

    if not root.exists():
        raise FileNotFoundError(root)

    out_dir = (
        Path(args.out_dir).expanduser().resolve()
        if args.out_dir
        else root / "FitRobustnessQC"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    pattern = (
        f"**/*{DIAGNOSTIC_SUFFIX}"
        if args.recursive
        else f"*{DIAGNOSTIC_SUFFIX}"
    )

    paths = sorted(root.glob(pattern))

    if not paths:
        raise FileNotFoundError("No global-affine mark diagnostics files found.")

    selected_sessions = None

    if args.manifest_csv:
        manifest_path = Path(args.manifest_csv).expanduser().resolve()
        

        if not manifest_path.exists():
            raise FileNotFoundError(manifest_path)

        selected_sessions = _load_manifest_sessions(manifest_path)

        paths = [
            path
            for path in paths
            if _session_name(path) in selected_sessions
        ]

        if not paths:
            raise ValueError("No diagnostics files matched sessions in the supplied manifest.")

    session_rows = []
    loo_mark_frames = []
    loo_burst_frames = []
    error_rows = []

    for path in paths:
        try:
            summary, mark_details, burst_details = characterize_session(path, args)

            session_rows.append(summary)

            if not mark_details.empty:
                loo_mark_frames.append(mark_details)

            if not burst_details.empty:
                loo_burst_frames.append(burst_details)

            print(f"[ok] {summary['session']} -> {summary['robustness_primary_category']}")

        except Exception as exc:
            error_rows.append(
                {
                    "diagnostics_file": str(path),
                    "session": _session_name(path),
                    "error": str(exc),
                }
            )

            print(f"[fail] {path.name}: {exc}")

    if not session_rows:
        raise RuntimeError("No sessions could be characterized.")

    session_df = pd.DataFrame(session_rows)

    # ---------------------------------------------------------
    # Write session-level output.
    # ---------------------------------------------------------

    summary_path = out_dir / "fit_robustness_session_summary.csv"

    session_df.to_csv(summary_path, index=False)

    # ---------------------------------------------------------
    # Write leave-one-mark details.
    # ---------------------------------------------------------

    mark_path = out_dir / "fit_robustness_leave_one_mark.csv"

    if loo_mark_frames:
        pd.concat(loo_mark_frames, ignore_index=True).to_csv(mark_path, index=False)

    # ---------------------------------------------------------
    # Write leave-one-burst details.
    # ---------------------------------------------------------

    burst_path = out_dir / "fit_robustness_leave_one_burst.csv"

    if loo_burst_frames:
        pd.concat(loo_burst_frames, ignore_index=True).to_csv(burst_path, index=False)

    # ---------------------------------------------------------
    # Category manifests.
    # ---------------------------------------------------------

    category_dir = out_dir / "Categories"
    
    category_dir.mkdir(parents=True, exist_ok=True)

    for category, group in session_df.groupby("robustness_primary_category", dropna=False):
        safe_name = str(category).replace("/", "_").replace(" ", "_")

        group.to_csv(category_dir / f"{safe_name}.csv", index=False)

    category_counts = session_df["robustness_primary_category"].value_counts(dropna=False).rename_axis("robustness_primary_category").reset_index(name="n_sessions")
    category_counts.to_csv(out_dir / "fit_robustness_category_counts.csv", index=False)

    # ---------------------------------------------------------
    # Errors.
    # ---------------------------------------------------------

    if error_rows:
        pd.DataFrame(error_rows).to_csv(out_dir / "fit_robustness_errors.csv", index=False)

    print()
    print(f"[done] sessions characterized: {len(session_df)}")
    print(f"[done] summary -> {summary_path}")
    print(f"[done] output root -> {out_dir}")

    if error_rows:
        print(f"[warn] sessions with errors: {len(error_rows)}")

if __name__ == "__main__":
    main()