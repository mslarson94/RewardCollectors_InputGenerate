#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def _read_one_row(path: Path) -> dict:
    df = pd.read_csv(path)
    if len(df) != 1:
        raise ValueError(f"Expected one-row summary CSV: {path}")
    return df.iloc[0].to_dict()


def _coalesce(row: dict, *names: str, default=np.nan):
    for name in names:
        if name in row and pd.notna(row[name]):
            return row[name]
    return default


def _load_pairs(path: Path, method: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    rename = {}
    if "ml_mark_index" in df.columns and "ml_original_ordinal" not in df.columns:
        rename["ml_mark_index"] = "ml_original_ordinal"
    if rename:
        df = df.rename(columns=rename)

    required = {
        "ml_original_ordinal",
        "ml_time",
        "matched",
        "alignment_residual_s",
        "affine_inlier",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise KeyError(f"{path.name} missing pair columns: {missing}")

    df["method"] = method
    df["ml_time"] = pd.to_datetime(df["ml_time"], errors="coerce")
    df["alignment_residual_s"] = pd.to_numeric(
        df["alignment_residual_s"], errors="coerce"
    )
    if pd.api.types.is_bool_dtype(df["affine_inlier"]):
        df["affine_inlier"] = df["affine_inlier"].fillna(False)
    else:
        df["affine_inlier"] = (
            df["affine_inlier"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"1", "true", "t", "yes", "y"})
        )
    return df


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Compare fully automatic affine alignment against "
            "manual-exclusion-filtered automatic affine alignment."
        )
    )
    ap.add_argument("--automatic_summary", required=True)
    ap.add_argument("--filtered_summary", required=True)
    ap.add_argument("--automatic_pairs", required=True)
    ap.add_argument("--filtered_pairs", required=True)
    ap.add_argument("--out_dir", default="")
    ap.add_argument("--name", default="alignment_method_comparison")
    args = ap.parse_args()

    auto_summary_path = Path(args.automatic_summary)
    filt_summary_path = Path(args.filtered_summary)
    auto_pairs_path = Path(args.automatic_pairs)
    filt_pairs_path = Path(args.filtered_pairs)

    for p in (auto_summary_path, filt_summary_path, auto_pairs_path, filt_pairs_path):
        if not p.exists():
            raise FileNotFoundError(p)

    auto = _read_one_row(auto_summary_path)
    filt = _read_one_row(filt_summary_path)

    auto_pairs = _load_pairs(auto_pairs_path, "automatic")
    filt_pairs = _load_pairs(filt_pairs_path, "manual_filtered")

    rows = []
    for method, row in (("automatic", auto), ("manual_filtered", filt)):
        n_matched = int(_coalesce(row, "n_matched_marks", default=0))
        n_inliers = int(_coalesce(row, "n_affine_inliers", default=0))
        rows.append({
            "method": method,
            "n_ml_marks_raw": _coalesce(row, "n_ml_marks_raw", "n_ml_marks"),
            "n_rpi_marks_raw": _coalesce(row, "n_rpi_marks_raw", "n_rpi_marks"),
            "n_manual_excluded_ml_marks": _coalesce(
                row, "n_manual_excluded_ml_marks", default=0
            ),
            "n_manual_excluded_rpi_marks": _coalesce(
                row, "n_manual_excluded_rpi_marks", default=0
            ),
            "n_matched_marks": n_matched,
            "n_affine_inliers": n_inliers,
            "n_affine_outliers": n_matched - n_inliers,
            "inlier_fraction_of_matched": (
                n_inliers / n_matched if n_matched else np.nan
            ),
            "clock_offset_at_reference_s": _coalesce(
                row, "clock_offset_at_reference_s"
            ),
            "clock_drift_ppm": _coalesce(row, "clock_drift_ppm"),
            "elapsed_time_s": _coalesce(row, "elapsed_time_s"),
            "elapsed_time_min": _coalesce(row, "elapsed_time_min"),
            "elapsed_time_hours": _coalesce(row, "elapsed_time_hours"),
            "drift_accumulated_over_elapsed_s": _coalesce(
                row, "drift_accumulated_over_elapsed_s"
            ),
            "drift_accumulated_over_elapsed_ms": _coalesce(
                row, "drift_accumulated_over_elapsed_ms"
            ),
            "residual_median_s": _coalesce(row, "residual_median_s"),
            "residual_mad_s": _coalesce(row, "residual_mad_s"),
            "residual_rmse_s": _coalesce(row, "residual_rmse_s"),
        })

    comparison = pd.DataFrame(rows)

    # Compute changes in the direction that is easy to interpret:
    # positive "improvement" means lower error after filtering.
    auto_row = comparison.loc[comparison["method"] == "automatic"].iloc[0]
    filt_row = comparison.loc[comparison["method"] == "manual_filtered"].iloc[0]

    comparison["rmse_improvement_vs_auto_s"] = np.nan
    comparison["mad_improvement_vs_auto_s"] = np.nan
    comparison["inlier_fraction_change_vs_auto"] = np.nan
    comparison.loc[comparison["method"] == "manual_filtered", "rmse_improvement_vs_auto_s"] = (
        auto_row["residual_rmse_s"] - filt_row["residual_rmse_s"]
    )
    comparison.loc[comparison["method"] == "manual_filtered", "mad_improvement_vs_auto_s"] = (
        auto_row["residual_mad_s"] - filt_row["residual_mad_s"]
    )
    comparison.loc[
        comparison["method"] == "manual_filtered",
        "inlier_fraction_change_vs_auto",
    ] = (
        filt_row["inlier_fraction_of_matched"]
        - auto_row["inlier_fraction_of_matched"]
    )

    # Common-mark comparison prevents an apparent improvement caused only by
    # discarding difficult marks.
    common = auto_pairs.merge(
        filt_pairs,
        on="ml_original_ordinal",
        how="inner",
        suffixes=("_automatic", "_manual_filtered"),
    )
    common = common.loc[
        common["affine_inlier_automatic"]
        & common["affine_inlier_manual_filtered"]
        & common["alignment_residual_s_automatic"].notna()
        & common["alignment_residual_s_manual_filtered"].notna()
    ].copy()

    common_stats = {
        "n_common_inlier_marks": len(common),
        "automatic_common_rmse_s": np.nan,
        "manual_filtered_common_rmse_s": np.nan,
        "common_rmse_improvement_s": np.nan,
        "automatic_common_median_abs_residual_s": np.nan,
        "manual_filtered_common_median_abs_residual_s": np.nan,
    }
    if len(common):
        auto_r = common["alignment_residual_s_automatic"].to_numpy(float)
        filt_r = common["alignment_residual_s_manual_filtered"].to_numpy(float)
        common_stats.update({
            "automatic_common_rmse_s": float(np.sqrt(np.mean(auto_r ** 2))),
            "manual_filtered_common_rmse_s": float(np.sqrt(np.mean(filt_r ** 2))),
            "common_rmse_improvement_s": float(
                np.sqrt(np.mean(auto_r ** 2)) - np.sqrt(np.mean(filt_r ** 2))
            ),
            "automatic_common_median_abs_residual_s": float(np.median(np.abs(auto_r))),
            "manual_filtered_common_median_abs_residual_s": float(np.median(np.abs(filt_r))),
        })

    out_dir = Path(args.out_dir) if args.out_dir else filt_summary_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    comparison_csv = out_dir / f"{args.name}_summary.csv"
    common_csv = out_dir / f"{args.name}_common_marks.csv"
    common_stats_csv = out_dir / f"{args.name}_common_marks_summary.csv"
    comparison.to_csv(comparison_csv, index=False)
    common.to_csv(common_csv, index=False)
    pd.DataFrame([common_stats]).to_csv(common_stats_csv, index=False)

    # Plot 1: residual distributions over elapsed time.
    fig, ax = plt.subplots(figsize=(12, 5))
    for pairs, label in (
        (auto_pairs, "automatic"),
        (filt_pairs, "manual filtered"),
    ):
        p = pairs.loc[
            pairs["affine_inlier"]
            & pairs["ml_time"].notna()
            & pairs["alignment_residual_s"].notna()
        ].copy()
        if len(p):
            elapsed_min = (
                (p["ml_time"] - p["ml_time"].min()).dt.total_seconds() / 60.0
            )
            ax.scatter(
                elapsed_min,
                p["alignment_residual_s"],
                label=label,
                alpha=0.75,
            )

    ax.axhline(0.0, linewidth=1)
    ax.set_title("Post-affine residuals: automatic vs manual-filtered")
    ax.set_xlabel("Elapsed ML time within each method (min)")
    ax.set_ylabel("Residual (s)")
    ax.grid(True, alpha=0.4)
    ax.legend()
    residual_png = out_dir / f"{args.name}_residuals.png"
    fig.savefig(residual_png, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # Plot 2: quality metrics where lower is better.
    metrics = pd.DataFrame({
        "method": ["automatic", "manual_filtered"],
        "RMSE": [
            float(auto_row["residual_rmse_s"]),
            float(filt_row["residual_rmse_s"]),
        ],
        "MAD": [
            float(auto_row["residual_mad_s"]),
            float(filt_row["residual_mad_s"]),
        ],
    })
    x = np.arange(len(metrics))
    width = 0.35

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(x - width / 2, metrics["RMSE"], width, label="RMSE")
    ax.bar(x + width / 2, metrics["MAD"], width, label="MAD")
    ax.set_xticks(x)
    ax.set_xticklabels(metrics["method"])
    ax.set_ylabel("Seconds")
    ax.set_title("Alignment error metrics")
    ax.grid(True, axis="y", alpha=0.4)
    ax.legend()
    quality_png = out_dir / f"{args.name}_quality_metrics.png"
    fig.savefig(quality_png, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # Plot 3: matched/inlier/outlier counts.
    fig, ax = plt.subplots(figsize=(8, 5))
    labels = ["automatic", "manual_filtered"]
    matched_vals = comparison["n_matched_marks"].to_numpy(float)
    inlier_vals = comparison["n_affine_inliers"].to_numpy(float)
    outlier_vals = comparison["n_affine_outliers"].to_numpy(float)
    x = np.arange(2)
    width = 0.25
    ax.bar(x - width, matched_vals, width, label="matched")
    ax.bar(x, inlier_vals, width, label="inliers")
    ax.bar(x + width, outlier_vals, width, label="affine outliers")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Mark count")
    ax.set_title("Mark retention and affine-fit acceptance")
    ax.grid(True, axis="y", alpha=0.4)
    ax.legend()
    counts_png = out_dir / f"{args.name}_mark_counts.png"
    fig.savefig(counts_png, dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"[ok] wrote comparison summary     -> {comparison_csv}")
    print(f"[ok] wrote common-mark table      -> {common_csv}")
    print(f"[ok] wrote common-mark summary    -> {common_stats_csv}")
    print(f"[ok] wrote residual plot          -> {residual_png}")
    print(f"[ok] wrote quality metric plot    -> {quality_png}")
    print(f"[ok] wrote mark count plot        -> {counts_png}")

    if len(common):
        print(
            "[common marks] "
            f"n={len(common)}, "
            f"RMSE automatic={common_stats['automatic_common_rmse_s']:.6f}s, "
            f"manual-filtered={common_stats['manual_filtered_common_rmse_s']:.6f}s"
        )


if __name__ == "__main__":
    main()
