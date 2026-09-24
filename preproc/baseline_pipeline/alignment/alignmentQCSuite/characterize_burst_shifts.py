#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# characterize_burst_shifts.py
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Characterize burst-to-burst shifts in global-affine residuals.")
    parser.add_argument("--root", required=True, help="Directory containing *_global_affine_mark_diagnostics.csv files.")
    parser.add_argument("--out-dir", default="", help="Output directory. Defaults to <root>/BurstShiftQC.",)
    parser.add_argument("--recursive", action="store_true", help="Search recursively below --root.")

    return parser.parse_args()


def _find_residual_column(df: pd.DataFrame) -> str:
    candidates = [
        "global_alignment_residual_s",
        "alignment_residual_s",
        "global_affine_residual_s",
        "affine_residual_s",
        "residual_s",
    ]

    for column in candidates:
        if column in df.columns:
            return column

    raise KeyError(f"Could not find residual column. Checked: {candidates}. Available: {list(df.columns)}")


def _session_name(path: Path) -> str:
    suffix = "_global_affine_mark_diagnostics.csv"

    if path.name.endswith(suffix):
        return path.name[:-len(suffix)]

    return path.stem


def _finite(series: pd.Series) -> np.ndarray:
    values = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)

    return values[np.isfinite(values)]


def _median(values: np.ndarray) -> float:
    if values.size == 0:
        return np.nan
    return float(np.median(values))


def _mad(values: np.ndarray) -> float:
    if values.size == 0:
        return np.nan
    center = np.median(values)

    return float(np.median(np.abs(values - center)))


def _rmse(values: np.ndarray) -> float:
    if values.size == 0:
        return np.nan

    return float(np.sqrt(np.mean(values ** 2)))


def characterize_session(path: Path) -> tuple[dict, pd.DataFrame]:

    df = pd.read_csv(path)

    residual_col = _find_residual_column(df)

    required = ["matched_burst_id", "matched_burst_position"]

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise KeyError(f"{path.name}: missing columns {missing}")

    work = df.copy()

    work["_residual_s"] = pd.to_numeric(work[residual_col], errors="coerce")
    work["_burst_id"] = pd.to_numeric(work["matched_burst_id"], errors="coerce")
    work["_burst_position"] = pd.to_numeric(work["matched_burst_position"], errors="coerce")

    if "ml_time" in work.columns:
        work["_ml_time"] = pd.to_datetime(work["ml_time"], errors="coerce")
    else:
        work["_ml_time"] = pd.NaT

    work = work.loc[work["_residual_s"].notna() & work["_burst_id"].notna() & work["_burst_position"].notna()].copy()

    if work.empty:
        raise ValueError(f"{path.name}: no usable residual rows")

    session = _session_name(path)

    burst_rows = []

    for burst_id, group in work.groupby("_burst_id", sort=True):
        group = group.sort_values("_burst_position")
        all_values = _finite(group["_residual_s"])
        stable_group = group.loc[group["_burst_position"] > 1]
        stable_values = _finite(stable_group["_residual_s"])

        if "_ml_time" in group.columns and group["_ml_time"].notna().any():
            burst_time = group["_ml_time"].dropna().median()
    
        else:
            burst_time = pd.NaT

        burst_rows.append(
            {
                "session": session,
                "burst_id": int(burst_id),
                "n_marks": int(len(group)),
                "n_stable_marks": int(len(stable_group)),
                "burst_time": burst_time,

                "all_median_residual_s": _median(all_values),
                "all_mad_s": _mad(all_values),
                "all_rmse_s": _rmse(all_values),

                "stable_median_residual_s": _median(stable_values),
                "stable_mad_s": _mad(stable_values),
                "stable_rmse_s": _rmse(stable_values),
            }
        )

    burst_df = pd.DataFrame(burst_rows)

    stable_bursts = burst_df.loc[(burst_df["n_stable_marks"] > 0) & burst_df["stable_median_residual_s"].notna()].copy()

    if stable_bursts.empty:
        raise ValueError(f"{path.name}: no bursts with stable marks")

    stable_medians = stable_bursts["stable_median_residual_s"].to_numpy(dtype=float)

    burst_to_burst_diffs = np.diff(stable_medians)

    if len(stable_medians) >= 2:
        residual_range_s = float(np.max(stable_medians) - np.min(stable_medians))
    else:
        residual_range_s = np.nan

    if burst_to_burst_diffs.size > 0:
        median_abs_burst_shift_s = float(np.median(np.abs(burst_to_burst_diffs)))
        max_abs_burst_shift_s = float(np.max(np.abs(burst_to_burst_diffs)))
    else:
        median_abs_burst_shift_s = np.nan
        max_abs_burst_shift_s = np.nan

    # ---------------------------------------------------------
    # Optional trend across burst time.
    # ---------------------------------------------------------

    slope_s_per_min = np.nan
    trend_r2 = np.nan

    trend_df = stable_bursts.loc[stable_bursts["burst_time"].notna()].copy()

    if len(trend_df) >= 3:
        t0 = trend_df["burst_time"].iloc[0]

        x_min = (trend_df["burst_time"] - t0).dt.total_seconds().to_numpy(dtype=float) / 60.0

        y = trend_df["stable_median_residual_s"].to_numpy(dtype=float)

        if np.ptp(x_min) > 0:
            slope, intercept = np.polyfit(x_min, y, 1)
            y_hat = slope * x_min + intercept
            ss_res = float(np.sum((y - y_hat) ** 2))
            ss_tot = float(np.sum((y - np.mean(y)) ** 2))

            slope_s_per_min = float(slope)

            if ss_tot > 0:
                trend_r2 = float(1.0 - ss_res / ss_tot)

    summary = {
        "session": session,
        "diagnostics_file": str(path),
        "n_bursts_total": int(len(burst_df)),
        "n_bursts_with_stable_marks": int(len(stable_bursts)),

        "stable_burst_median_min_s": float(np.min(stable_medians)),
        "stable_burst_median_max_s": float(np.max(stable_medians)),
        "stable_burst_median_range_s": residual_range_s,

        "median_abs_burst_shift_s": median_abs_burst_shift_s,
        "max_abs_burst_shift_s": max_abs_burst_shift_s,

        "burst_trend_slope_s_per_min": slope_s_per_min,
        "burst_trend_r2": trend_r2,
    }

    return summary, burst_df


def main() -> None:
    args = parse_args()

    root = Path(args.root)
    print(root)
    if not root.exists():
        raise FileNotFoundError(root)

    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else root / "BurstShiftQC"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    pattern = (
        "**/*_global_affine_mark_diagnostics.csv"
        if args.recursive
        else "*_global_affine_mark_diagnostics.csv"
    )

    paths = sorted(root.glob(pattern))

    if not paths:
        raise FileNotFoundError("No global affine diagnostics files found.")

    session_rows = []
    burst_frames = []
    error_rows = []

    for path in paths:
        try:
            summary, burst_df = characterize_session(path)

            session_rows.append(summary)
            burst_frames.append(burst_df)

            print(f"[ok] {path.name}")

        except Exception as exc:
            error_rows.append(
                {
                    "diagnostics_file": str(path),
                    "error": str(exc),
                }
            )

            print(f"[fail] {path.name}: {exc}")

    if not session_rows:
        raise RuntimeError("No sessions could be characterized.")

    session_df = pd.DataFrame(session_rows)

    burst_df = pd.concat(burst_frames, ignore_index=True)

    session_csv = out_dir / "burst_shift_session_summary.csv"
    burst_csv = out_dir / "burst_shift_per_burst.csv"
    errors_csv = out_dir / "burst_shift_errors.csv"
    
    session_df.to_csv(session_csv, index=False)
    burst_df.to_csv(burst_csv, index=False)

    if error_rows:
        pd.DataFrame(error_rows).to_csv(errors_csv, index=False)

    print()
    print(f"[done] sessions characterized: {len(session_df)}")
    print(f"[done] wrote -> {session_csv}")
    print(f"[done] wrote -> {burst_csv}")

    if error_rows:
        print(f"[warn] files with errors: {len(error_rows)}")

if __name__ == "__main__":
    main()