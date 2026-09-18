#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


REQUIRED_COMPARISON_COLUMNS = {
    "method",
    "clock_drift_ppm",
    "residual_rmse_s",
    "residual_mad_s",
    "inlier_fraction_of_matched",
}

METHOD_ORDER = ["automatic", "manual_filtered"]


def _find_comparison_summaries(root: Path) -> list[Path]:
    """
    Find session-level comparison summaries produced by compare_alignment_methods*.py.

    We intentionally identify them by schema, not only by filename, so this
    remains robust if session names contain arbitrary underscores.
    """
    found: list[Path] = []
    for path in sorted(root.rglob("*_summary.csv")):
        try:
            header = pd.read_csv(path, nrows=0)
        except Exception:
            continue
        if REQUIRED_COMPARISON_COLUMNS.issubset(set(header.columns)):
            found.append(path)
    return found


def _find_common_mark_summary(session_dir: Path) -> Path | None:
    candidates = sorted(session_dir.glob("*_common_marks_summary.csv"))
    return candidates[0] if candidates else None


def _session_name_from_summary(path: Path) -> str:
    name = path.name
    suffix = "_summary.csv"
    return name[:-len(suffix)] if name.endswith(suffix) else path.stem


def _load_all(summary_paths: Iterable[Path]) -> tuple[pd.DataFrame, pd.DataFrame]:
    long_rows: list[pd.DataFrame] = []
    common_rows: list[dict] = []

    for path in summary_paths:
        df = pd.read_csv(path)
        if not REQUIRED_COMPARISON_COLUMNS.issubset(df.columns):
            continue

        session = _session_name_from_summary(path)
        df = df.copy()
        df.insert(0, "session", session)
        df.insert(1, "comparison_summary_file", str(path))
        long_rows.append(df)

        common_path = _find_common_mark_summary(path.parent)
        if common_path is not None:
            cdf = pd.read_csv(common_path)
            if len(cdf):
                row = cdf.iloc[0].to_dict()
                row["session"] = session
                row["common_summary_file"] = str(common_path)
                common_rows.append(row)

    if not long_rows:
        raise ValueError("No valid comparison summary CSV files found.")

    long_df = pd.concat(long_rows, ignore_index=True, sort=False)
    common_df = pd.DataFrame(common_rows)
    return long_df, common_df


def _safe_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    numeric_cols = [
        "clock_drift_ppm",
        "elapsed_time_s",
        "elapsed_time_min",
        "elapsed_time_hours",
        "drift_accumulated_over_elapsed_s",
        "drift_accumulated_over_elapsed_ms",
        "residual_rmse_s",
        "residual_mad_s",
        "residual_median_s",
        "inlier_fraction_of_matched",
        "n_matched_marks",
        "n_affine_inliers",
        "n_affine_outliers",
        "n_manual_excluded_ml_marks",
        "n_manual_excluded_rpi_marks",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = _safe_float(out[col])

    if "residual_rmse_s" in out.columns:
        out["residual_rmse_ms"] = out["residual_rmse_s"] * 1000.0
    if "residual_mad_s" in out.columns:
        out["residual_mad_ms"] = out["residual_mad_s"] * 1000.0

    if "clock_drift_ppm" in out.columns:
        out["abs_clock_drift_ppm"] = out["clock_drift_ppm"].abs()

    if (
        "clock_drift_ppm" in out.columns
        and "elapsed_time_s" in out.columns
        and "drift_accumulated_over_elapsed_ms" not in out.columns
    ):
        out["drift_accumulated_over_elapsed_ms"] = (
            out["clock_drift_ppm"] * 1e-6 * out["elapsed_time_s"] * 1000.0
        )

    if "drift_accumulated_over_elapsed_ms" in out.columns:
        out["abs_drift_accumulated_over_elapsed_ms"] = (
            out["drift_accumulated_over_elapsed_ms"].abs()
        )

    return out


def _make_wide(long_df: pd.DataFrame) -> pd.DataFrame:
    value_cols = [
        c for c in [
            "clock_drift_ppm",
            "abs_clock_drift_ppm",
            "elapsed_time_min",
            "elapsed_time_hours",
            "drift_accumulated_over_elapsed_ms",
            "abs_drift_accumulated_over_elapsed_ms",
            "residual_rmse_s",
            "residual_rmse_ms",
            "residual_mad_s",
            "residual_mad_ms",
            "inlier_fraction_of_matched",
            "n_matched_marks",
            "n_affine_inliers",
            "n_affine_outliers",
            "n_manual_excluded_ml_marks",
            "n_manual_excluded_rpi_marks",
        ]
        if c in long_df.columns
    ]

    wide = long_df.pivot_table(
        index="session",
        columns="method",
        values=value_cols,
        aggfunc="first",
    )
    wide.columns = [f"{metric}__{method}" for metric, method in wide.columns]
    wide = wide.reset_index()

    # Add direct manual-vs-automatic deltas where both exist.
    for metric in [
        "residual_rmse_ms",
        "residual_mad_ms",
        "inlier_fraction_of_matched",
        "clock_drift_ppm",
        "abs_clock_drift_ppm",
        "abs_drift_accumulated_over_elapsed_ms",
    ]:
        a = f"{metric}__automatic"
        m = f"{metric}__manual_filtered"
        if a in wide.columns and m in wide.columns:
            if metric in {"residual_rmse_ms", "residual_mad_ms"}:
                # Positive means manual filtering improved/lowered error.
                wide[f"{metric}__improvement_manual_vs_auto"] = wide[a] - wide[m]
            elif metric == "inlier_fraction_of_matched":
                wide[f"{metric}__change_manual_vs_auto"] = wide[m] - wide[a]
            else:
                wide[f"{metric}__manual_minus_auto"] = wide[m] - wide[a]

    return wide


def _save_ppm_elapsed_faceted(df: pd.DataFrame, out_path: Path) -> None:
    methods = [m for m in METHOD_ORDER if m in set(df["method"].dropna())]
    if not methods:
        return

    # User explicitly requested a faceted view, so this figure has one panel
    # per method with shared axis ranges for direct visual comparison.
    fig, axes = plt.subplots(
        1,
        len(methods),
        figsize=(7 * len(methods), 5),
        sharex=True,
        sharey=True,
        squeeze=False,
    )

    for ax, method in zip(axes[0], methods):
        d = df.loc[df["method"].eq(method)].copy()
        d = d.loc[
            d["elapsed_time_hours"].notna()
            & d["clock_drift_ppm"].notna()
        ]
        ax.scatter(d["elapsed_time_hours"], d["clock_drift_ppm"], alpha=0.8)
        ax.axhline(0.0, linewidth=1)
        ax.set_title(method.replace("_", " ").title())
        ax.set_xlabel("Full ML elapsed span (hours)")
        ax.grid(True, alpha=0.35)

    axes[0][0].set_ylabel("Clock drift (ppm)")
    fig.suptitle("Clock drift vs full ML recording span")
    fig.tight_layout()
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def _save_accumulated_drift_plot(df: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(9, 6))
    for method in METHOD_ORDER:
        d = df.loc[df["method"].eq(method)].copy()
        d = d.loc[
            d["elapsed_time_hours"].notna()
            & d["abs_drift_accumulated_over_elapsed_ms"].notna()
        ]
        if len(d):
            ax.scatter(
                d["elapsed_time_hours"],
                d["abs_drift_accumulated_over_elapsed_ms"],
                label=method.replace("_", " "),
                alpha=0.8,
            )
    ax.set_title("Accumulated clock-rate error over full ML span")
    ax.set_xlabel("Full ML elapsed span (hours)")
    ax.set_ylabel("Absolute accumulated drift (ms)")
    ax.grid(True, alpha=0.35)
    ax.legend()
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def _save_rmse_method_plot(wide: pd.DataFrame, out_path: Path) -> None:
    auto_col = "residual_rmse_ms__automatic"
    manual_col = "residual_rmse_ms__manual_filtered"
    if auto_col not in wide.columns or manual_col not in wide.columns:
        return

    d = wide.loc[
        wide[auto_col].notna() & wide[manual_col].notna()
    ].copy()
    if not len(d):
        return

    fig, ax = plt.subplots(figsize=(7, 7))
    ax.scatter(d[auto_col], d[manual_col], alpha=0.8)

    max_val = float(np.nanmax([d[auto_col].max(), d[manual_col].max()]))
    min_val = float(np.nanmin([d[auto_col].min(), d[manual_col].min()]))
    pad = max((max_val - min_val) * 0.05, 0.5)
    lo = max(0.0, min_val - pad)
    hi = max_val + pad
    ax.plot([lo, hi], [lo, hi], linestyle="--", linewidth=1)

    ax.set_xlim(lo, hi)
    ax.set_ylim(lo, hi)
    ax.set_title("Session RMSE: automatic vs manual-filtered")
    ax.set_xlabel("Automatic RMSE (ms)")
    ax.set_ylabel("Manual-filtered RMSE (ms)")
    ax.grid(True, alpha=0.35)
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def _save_inlier_fraction_plot(wide: pd.DataFrame, out_path: Path) -> None:
    auto_col = "inlier_fraction_of_matched__automatic"
    manual_col = "inlier_fraction_of_matched__manual_filtered"
    if auto_col not in wide.columns or manual_col not in wide.columns:
        return

    d = wide.loc[
        wide[auto_col].notna() & wide[manual_col].notna()
    ].copy()
    if not len(d):
        return

    fig, ax = plt.subplots(figsize=(7, 7))
    ax.scatter(d[auto_col], d[manual_col], alpha=0.8)
    ax.plot([0, 1], [0, 1], linestyle="--", linewidth=1)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_title("Affine inlier fraction: automatic vs manual-filtered")
    ax.set_xlabel("Automatic inlier fraction")
    ax.set_ylabel("Manual-filtered inlier fraction")
    ax.grid(True, alpha=0.35)
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def _save_common_rmse_plot(common_df: pd.DataFrame, out_path: Path) -> None:
    auto_col = "automatic_common_rmse_s"
    manual_col = "manual_filtered_common_rmse_s"
    if (
        common_df.empty
        or auto_col not in common_df.columns
        or manual_col not in common_df.columns
    ):
        return

    d = common_df.copy()
    d[auto_col] = _safe_float(d[auto_col]) * 1000.0
    d[manual_col] = _safe_float(d[manual_col]) * 1000.0
    d = d.loc[d[auto_col].notna() & d[manual_col].notna()]
    if not len(d):
        return

    fig, ax = plt.subplots(figsize=(7, 7))
    ax.scatter(d[auto_col], d[manual_col], alpha=0.8)

    max_val = float(np.nanmax([d[auto_col].max(), d[manual_col].max()]))
    min_val = float(np.nanmin([d[auto_col].min(), d[manual_col].min()]))
    pad = max((max_val - min_val) * 0.05, 0.5)
    lo = max(0.0, min_val - pad)
    hi = max_val + pad
    ax.plot([lo, hi], [lo, hi], linestyle="--", linewidth=1)

    ax.set_xlim(lo, hi)
    ax.set_ylim(lo, hi)
    ax.set_title("Common-mark RMSE: automatic vs manual-filtered")
    ax.set_xlabel("Automatic common-mark RMSE (ms)")
    ax.set_ylabel("Manual-filtered common-mark RMSE (ms)")
    ax.grid(True, alpha=0.35)
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Collate all per-session alignment comparison summaries and create "
            "cross-session diagnostic plots."
        )
    )
    ap.add_argument(
        "--comparison_root",
        required=True,
        help=(
            "Root containing per-session comparison directories, usually "
            "AlignmentComparisons."
        ),
    )
    ap.add_argument(
        "--out_dir",
        default="",
        help=(
            "Output directory. Defaults to "
            "<comparison_root>/CollatedAlignmentSummary."
        ),
    )
    args = ap.parse_args()

    root = Path(args.comparison_root).expanduser().resolve()
    if not root.is_dir():
        raise NotADirectoryError(root)

    out_dir = (
        Path(args.out_dir).expanduser().resolve()
        if args.out_dir
        else root / "CollatedAlignmentSummary"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    summaries = _find_comparison_summaries(root)
    print(f"[scan] found {len(summaries)} comparison summaries")

    long_df, common_df = _load_all(summaries)
    long_df = _add_derived_columns(long_df)
    wide_df = _make_wide(long_df)

    long_csv = out_dir / "all_sessions_alignment_comparison_long.csv"
    wide_csv = out_dir / "all_sessions_alignment_comparison_wide.csv"
    common_csv = out_dir / "all_sessions_common_mark_comparison.csv"

    long_df.to_csv(long_csv, index=False)
    wide_df.to_csv(wide_csv, index=False)
    if not common_df.empty:
        common_df.to_csv(common_csv, index=False)

    ppm_plot = out_dir / "ppm_vs_elapsed_span_faceted.png"
    accumulated_plot = out_dir / "accumulated_drift_vs_elapsed_span.png"
    rmse_plot = out_dir / "rmse_automatic_vs_manual_filtered.png"
    inlier_plot = out_dir / "inlier_fraction_automatic_vs_manual_filtered.png"
    common_rmse_plot = out_dir / "common_mark_rmse_automatic_vs_manual_filtered.png"

    _save_ppm_elapsed_faceted(long_df, ppm_plot)
    _save_accumulated_drift_plot(long_df, accumulated_plot)
    _save_rmse_method_plot(wide_df, rmse_plot)
    _save_inlier_fraction_plot(wide_df, inlier_plot)
    _save_common_rmse_plot(common_df, common_rmse_plot)

    print(f"[ok] wrote long summary        -> {long_csv}")
    print(f"[ok] wrote wide summary        -> {wide_csv}")
    if not common_df.empty:
        print(f"[ok] wrote common-mark summary -> {common_csv}")
    print(f"[ok] wrote ppm plot            -> {ppm_plot}")
    print(f"[ok] wrote accumulated plot    -> {accumulated_plot}")
    print(f"[ok] wrote RMSE plot           -> {rmse_plot}")
    print(f"[ok] wrote inlier plot         -> {inlier_plot}")
    if not common_df.empty:
        print(f"[ok] wrote common-RMSE plot    -> {common_rmse_plot}")


if __name__ == "__main__":
    main()
