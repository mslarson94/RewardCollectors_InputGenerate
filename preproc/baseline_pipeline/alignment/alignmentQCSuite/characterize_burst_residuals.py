#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# characterize_burst_residuals.py
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Characterize global-affine residuals by within-burst mark position.")
    parser.add_argument("--root", required=True, help="Directory containing *_global_affine_mark_diagnostics.csv files.")
    parser.add_argument("--out-dir", default="", help="Output directory. Defaults to <root>/BurstResidualQC.")
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

    raise KeyError(f"Could not find a final affine residual column. Checked: {candidates}. Available columns: {list(df.columns)}")


def _finite_values(series: pd.Series) -> np.ndarray:
    values = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
    return values[np.isfinite(values)]


def _rmse(values: np.ndarray) -> float:
    ''' Root mean square error'''
    if values.size == 0:
        return np.nan
    return float(np.sqrt(np.mean(values ** 2)))


def _median(values: np.ndarray) -> float:
    if values.size == 0:
        return np.nan
    return float(np.median(values))


def _median_abs(values: np.ndarray) -> float:
    if values.size == 0:
        return np.nan
    return float(np.median(np.abs(values)))


def _mad(values: np.ndarray) -> float:
    """
    Median absolute deviation around the residual median.
    """
    if values.size == 0:
        return np.nan
    center = np.median(values)
    return float(np.median(np.abs(values - center)))


def _p95_abs(values: np.ndarray) -> float:
    ''' 95th percentile '''
    if values.size == 0:
        return np.nan
    return float(np.percentile(np.abs(values), 95))


def _summarize_residuals(values: np.ndarray, prefix: str) -> dict:
    return {
        f"{prefix}_n": int(values.size),
        f"{prefix}_median_s": _median(values),
        f"{prefix}_median_abs_s": _median_abs(values),
        f"{prefix}_mad_s": _mad(values),
        f"{prefix}_rmse_s": _rmse(values),
        f"{prefix}_p95_abs_s": _p95_abs(values),
    }


def _session_name(path: Path) -> str:
    suffix = "_global_affine_mark_diagnostics.csv"

    if path.name.endswith(suffix):
        return path.name[:-len(suffix)]
    return path.stem


def characterize_session(path: Path) -> tuple[dict, pd.DataFrame, pd.DataFrame]:

    df = pd.read_csv(path)
    residual_col = _find_residual_column(df)
    required = ["matched_burst_id", "matched_burst_position"]

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise KeyError(f"{path.name}: missing burst columns {missing}")

    work = df.copy()

    work["_residual_s"] = pd.to_numeric(work[residual_col], errors="coerce")
    work["_burst_id"] = pd.to_numeric(work["matched_burst_id"], errors="coerce")
    work["_burst_position"] = pd.to_numeric(work["matched_burst_position"], errors="coerce")

    work = work.loc[
        work["_residual_s"].notna()
        & work["_burst_id"].notna()
        & work["_burst_position"].notna()
    ].copy()

    session = _session_name(path)

    if work.empty:
        raise ValueError(f"{path.name}: no rows with both residual and burst metadata")

    work["_is_first"] = work["_burst_position"] == 1
    

    all_values = _finite_values(work["_residual_s"])
    first_values = _finite_values(work.loc[work["_is_first"], "_residual_s",])
    stable_values = _finite_values(work.loc[~work["_is_first"], "_residual_s",])

    summary = {
        "session": session,
        "diagnostics_file": str(path),
        "residual_column": residual_col,
        "n_bursts": int(work["_burst_id"].nunique()),

        **_summarize_residuals(all_values, "all"),
        **_summarize_residuals(first_values, "first"),
        **_summarize_residuals(stable_values, "stable"),
    }

    summary["rmse_improvement_excluding_first_s"] = (
        summary["all_rmse_s"] - summary["stable_rmse_s"]
        if (np.isfinite(summary["all_rmse_s"]) and np.isfinite(summary["stable_rmse_s"]))
        else np.nan
    )

    summary["mad_improvement_excluding_first_s"] = (
        summary["all_mad_s"] - summary["stable_mad_s"]
        if (np.isfinite(summary["all_mad_s"]) and np.isfinite(summary["stable_mad_s"]))
        else np.nan
    )

    summary["median_abs_improvement_excluding_first_s"] = (
        summary["all_median_abs_s"] - summary["stable_median_abs_s"]
        if (np.isfinite(summary["all_median_abs_s"]) and np.isfinite(summary["stable_median_abs_s"]))
        else np.nan
    )

    summary["first_to_stable_median_abs_ratio"] = (
        summary["first_median_abs_s"] / summary["stable_median_abs_s"]
        if (np.isfinite(summary["first_median_abs_s"]) and np.isfinite(summary["stable_median_abs_s"]) and summary["stable_median_abs_s"] > 0)
        else np.nan
    )

    # ---------------------------------------------------------
    # Per-position summary for this session.
    # ---------------------------------------------------------

    position_rows = []

    for position, group in work.groupby("_burst_position", sort=True):
        values = _finite_values(group["_residual_s"])

        row = {
            "session": session,
            "burst_position": int(position)
        }

        row.update(_summarize_residuals(values, "residual"))
        position_rows.append(row)

    position_df = pd.DataFrame(position_rows)

    # ---------------------------------------------------------
    # Per-burst summary for this session.
    # ---------------------------------------------------------

    burst_rows = []

    for burst_id, group in work.groupby("_burst_id", sort=True,):
        group = group.sort_values("_burst_position")

        all_burst_values = _finite_values(group["_residual_s"])
        first_burst_values = _finite_values(group.loc[group["_burst_position"] == 1, "_residual_s"])
        stable_burst_values = _finite_values(group.loc[group["_burst_position"] > 1, "_residual_s"])
        

        burst_rows.append(
            {
                "session": session,
                "burst_id": int(burst_id),
                "n_marks": int(len(group)),

                "first_residual_s": (
                    float(first_burst_values[0])
                    if (first_burst_values.size > 0)
                    else np.nan
                ),

                "first_abs_residual_s": (
                    float(abs(first_burst_values[0]))
                    if (first_burst_values.size > 0)
                    else np.nan
                ),

                "stable_n": int(stable_burst_values.size),
                "stable_median_residual_s": _median(stable_burst_values),
                "stable_median_abs_residual_s": _median_abs(stable_burst_values),
                "stable_mad_s": _mad(stable_burst_values),
                "burst_rmse_s": _rmse(all_burst_values),
            }
        )

    burst_df = pd.DataFrame(burst_rows)

    return summary, position_df, burst_df
    


def main() -> None:
    args = parse_args()
    root = Path(args.root)

    if not root.exists():
        raise FileNotFoundError(root)

    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else root / "BurstResidualQC"
    )

    out_dir.mkdir(parents=True, exist_ok=True,)

    pattern = (
        "**/*_global_affine_mark_diagnostics.csv"
        if args.recursive
        else "*_global_affine_mark_diagnostics.csv"
    )

    paths = sorted(root.glob(pattern))

    if not paths:
        raise FileNotFoundError(f"No global affine diagnostics files found under {root}")

    session_rows = []
    position_frames = []
    burst_frames = []
    error_rows = []

    for path in paths:
        try:
            summary, position_df, burst_df = characterize_session(path)
            session_rows.append(summary)
            position_frames.append(position_df)
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
        raise RuntimeError("No diagnostics files could be characterized.")

    session_df = pd.DataFrame(session_rows)
    position_df = pd.concat(position_frames, ignore_index=True)
    burst_df = pd.concat(burst_frames, ignore_index=True)

    # ---------------------------------------------------------
    # Dataset-wide position summary.
    # ---------------------------------------------------------

    pooled_position_rows = []

    # Reload residuals so every individual mark contributes.
    pooled_marks = []

    for path in paths:
        try:
            df = pd.read_csv(path)

            residual_col = _find_residual_column(df)
            

            if "matched_burst_position" not in df.columns:
                continue

            temp = pd.DataFrame(
                {
                    "session": _session_name(path),
                    "burst_position": pd.to_numeric(df["matched_burst_position"], errors="coerce"),
                    "residual_s": pd.to_numeric(df[residual_col], errors="coerce"),
                }
            )

            temp = temp.dropna(subset=["burst_position", "residual_s"])

            pooled_marks.append(temp)

        except Exception:
            continue

    if pooled_marks:
        pooled_df = pd.concat(pooled_marks, ignore_index=True)

        pooled_df["burst_position"] = pooled_df["burst_position"].astype(int)

        for position, group in (pooled_df.groupby("burst_position", sort=True)):
            values = _finite_values(group["residual_s"])
            pooled_position_rows.append(
                {
                    "burst_position": int(position),
                    "n_marks": int(values.size),
                    "n_sessions": int(group["session"].nunique()),
                    "median_residual_s": _median(values),
                    "median_abs_residual_s": _median_abs(values),
                    "mad_s": _mad(values),
                    "rmse_s": _rmse(values),
                    "p95_abs_residual_s": _p95_abs(values),
                }
            )

    pooled_position_df = pd.DataFrame(pooled_position_rows)

    # ---------------------------------------------------------
    # Write outputs.
    # ---------------------------------------------------------

    session_csv = out_dir / "burst_residual_session_summary.csv"
    position_csv = out_dir / "burst_residual_position_by_session.csv"
    pooled_position_csv = out_dir / "burst_residual_position_pooled.csv"
    burst_csv = out_dir / "burst_residual_per_burst.csv"
    errors_csv = out_dir / "burst_residual_errors.csv"
    

    session_df.to_csv(session_csv, index=False)
    position_df.to_csv(position_csv, index=False)
    pooled_position_df.to_csv(pooled_position_csv, index=False)
    burst_df.to_csv(burst_csv, index=False)

    if error_rows:
        pd.DataFrame(error_rows).to_csv(errors_csv, index=False)

    print()
    print(f"[done] sessions characterized: {len(session_df)}")
    print(f"[done] wrote -> {session_csv}")
    print(f"[done] wrote -> {position_csv}")
    print(f"[done] wrote -> {pooled_position_csv}")
    print(f"[done] wrote -> {burst_csv}")

    if error_rows:
        print(f"[warn] files with errors: {len(error_rows)}")


if __name__ == "__main__":
    main()