# compare_drift_timecols.py

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import rankdata, wilcoxon


DEFAULT_METRICS = (
    "mean_drift_s",
    "median_drift_s",
    "max_abs_drift_s",
    "n_matched",
)

SUMMARY_SUFFIX = "_DriftSummary.csv"


@dataclass(frozen=True)
class DriftSummaryFile:
    session_key: str
    path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare matched drift summaries generated using eMLT_orig "
            "versus mLT_orig."
        )
    )
    parser.add_argument(
        "--emlt-root",
        required=True,
        help="Root directory containing eMLT_orig *_DriftSummary.csv files.",
    )
    parser.add_argument(
        "--mlt-root",
        required=True,
        help="Root directory containing mLT_orig *_DriftSummary.csv files.",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Directory for paired tables, statistics, and plots.",
    )
    parser.add_argument(
        "--metrics",
        default=",".join(DEFAULT_METRICS),
        help=(
            "Comma-separated metrics to compare. Default: "
            + ",".join(DEFAULT_METRICS)
        ),
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.05,
        help="Significance threshold. Default: 0.05.",
    )
    parser.add_argument(
        "--plot-format",
        choices=("png", "pdf", "svg"),
        default="png",
        help="Plot output format. Default: png.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=180,
        help="PNG resolution. Default: 180.",
    )
    return parser.parse_args()


def normalize_session_key(path: Path) -> str:
    name = path.name

    if not name.endswith(SUMMARY_SUFFIX):
        raise ValueError(f"Unexpected drift summary filename: {name}")

    return name[: -len(SUMMARY_SUFFIX)]


def discover_drift_summaries(root: Path) -> dict[str, DriftSummaryFile]:
    if not root.exists():
        raise FileNotFoundError(f"Directory does not exist: {root}")

    if not root.is_dir():
        raise NotADirectoryError(root)

    found: dict[str, DriftSummaryFile] = {}
    duplicates: dict[str, list[Path]] = {}

    for path in sorted(root.rglob(f"*{SUMMARY_SUFFIX}")):
        session_key = normalize_session_key(path)

        if session_key in found:
            duplicates.setdefault(
                session_key,
                [found[session_key].path],
            ).append(path)
            continue

        found[session_key] = DriftSummaryFile(
            session_key=session_key,
            path=path,
        )

    if duplicates:
        details = "\n".join(
            f"{key}:\n  " + "\n  ".join(str(path) for path in paths)
            for key, paths in sorted(duplicates.items())
        )
        raise ValueError(
            "Duplicate session names were found within one root. "
            "Filename-based matching would be ambiguous:\n"
            f"{details}"
        )

    return found


def read_summary_metrics(
    summary_file: DriftSummaryFile,
    metrics: Iterable[str],
) -> dict[str, float]:
    df = pd.read_csv(summary_file.path)

    if df.empty:
        raise ValueError(f"Empty drift summary: {summary_file.path}")

    if len(df) != 1:
        raise ValueError(
            f"Expected exactly one summary row in {summary_file.path}; "
            f"found {len(df)}."
        )

    row = df.iloc[0]
    values: dict[str, float] = {}

    for metric in metrics:
        if metric not in df.columns:
            values[metric] = math.nan
            continue

        values[metric] = pd.to_numeric(
            pd.Series([row[metric]]),
            errors="coerce",
        ).iloc[0]

    return values


def build_paired_table(
    emlt_files: dict[str, DriftSummaryFile],
    mlt_files: dict[str, DriftSummaryFile],
    metrics: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    emlt_keys = set(emlt_files)
    mlt_keys = set(mlt_files)

    paired_keys = sorted(emlt_keys & mlt_keys)
    emlt_only = sorted(emlt_keys - mlt_keys)
    mlt_only = sorted(mlt_keys - emlt_keys)

    unmatched_rows: list[dict[str, str]] = []

    for key in emlt_only:
        unmatched_rows.append(
            {
                "session": key,
                "present_in": "eMLT_orig",
                "missing_from": "mLT_orig",
                "path": str(emlt_files[key].path),
            }
        )

    for key in mlt_only:
        unmatched_rows.append(
            {
                "session": key,
                "present_in": "mLT_orig",
                "missing_from": "eMLT_orig",
                "path": str(mlt_files[key].path),
            }
        )

    paired_rows: list[dict[str, object]] = []

    for session_key in paired_keys:
        emlt = emlt_files[session_key]
        mlt = mlt_files[session_key]

        emlt_values = read_summary_metrics(emlt, metrics)
        mlt_values = read_summary_metrics(mlt, metrics)

        row: dict[str, object] = {
            "session": session_key,
            "emlt_summary_csv": str(emlt.path),
            "mlt_summary_csv": str(mlt.path),
        }

        for metric in metrics:
            emlt_col = f"eMLT_orig_{metric}"
            mlt_col = f"mLT_orig_{metric}"
            delta_col = f"delta_{metric}"

            row[emlt_col] = emlt_values[metric]
            row[mlt_col] = mlt_values[metric]

            if (
                pd.notna(emlt_values[metric])
                and pd.notna(mlt_values[metric])
            ):
                row[delta_col] = (
                    float(mlt_values[metric])
                    - float(emlt_values[metric])
                )
            else:
                row[delta_col] = math.nan

        paired_rows.append(row)

    paired_df = pd.DataFrame(paired_rows)
    unmatched_df = pd.DataFrame(
        unmatched_rows,
        columns=("session", "present_in", "missing_from", "path"),
    )

    return paired_df, unmatched_df


def rank_biserial_from_differences(
    differences: np.ndarray,
) -> float:
    differences = np.asarray(differences, dtype=float)
    differences = differences[
        np.isfinite(differences) & (differences != 0)
    ]

    if differences.size == 0:
        return 0.0

    ranks = rankdata(np.abs(differences), method="average")

    positive_rank_sum = float(ranks[differences > 0].sum())
    negative_rank_sum = float(ranks[differences < 0].sum())
    total_rank_sum = positive_rank_sum + negative_rank_sum

    if total_rank_sum == 0:
        return 0.0

    return (
        positive_rank_sum - negative_rank_sum
    ) / total_rank_sum


def benjamini_hochberg(
    p_values: pd.Series,
) -> pd.Series:
    adjusted = pd.Series(
        np.nan,
        index=p_values.index,
        dtype=float,
    )

    valid = p_values.dropna()

    if valid.empty:
        return adjusted

    order = np.argsort(valid.to_numpy())
    sorted_p = valid.to_numpy()[order]
    n_tests = len(sorted_p)

    adjusted_sorted = np.empty(n_tests, dtype=float)

    running_min = 1.0

    for reverse_index in range(n_tests - 1, -1, -1):
        rank = reverse_index + 1
        adjusted_value = (
            sorted_p[reverse_index] * n_tests / rank
        )
        running_min = min(running_min, adjusted_value)
        adjusted_sorted[reverse_index] = min(running_min, 1.0)

    original_order = np.empty(n_tests, dtype=int)
    original_order[order] = np.arange(n_tests)

    adjusted.loc[valid.index] = adjusted_sorted[original_order]

    return adjusted


def run_wilcoxon_comparisons(
    paired_df: pd.DataFrame,
    metrics: list[str],
    alpha: float,
) -> pd.DataFrame:
    results: list[dict[str, object]] = []

    for metric in metrics:
        emlt_col = f"eMLT_orig_{metric}"
        mlt_col = f"mLT_orig_{metric}"

        subset = paired_df[
            ["session", emlt_col, mlt_col]
        ].copy()

        subset[emlt_col] = pd.to_numeric(
            subset[emlt_col],
            errors="coerce",
        )
        subset[mlt_col] = pd.to_numeric(
            subset[mlt_col],
            errors="coerce",
        )

        subset = subset.dropna()

        emlt = subset[emlt_col].to_numpy(dtype=float)
        mlt = subset[mlt_col].to_numpy(dtype=float)
        differences = mlt - emlt

        n_pairs = len(differences)
        n_nonzero = int(np.count_nonzero(differences))

        result: dict[str, object] = {
            "metric": metric,
            "n_pairs": n_pairs,
            "n_nonzero_differences": n_nonzero,
            "emlt_mean": (
                float(np.mean(emlt))
                if n_pairs
                else math.nan
            ),
            "mlt_mean": (
                float(np.mean(mlt))
                if n_pairs
                else math.nan
            ),
            "mean_difference_mlt_minus_emlt": (
                float(np.mean(differences))
                if n_pairs
                else math.nan
            ),
            "median_difference_mlt_minus_emlt": (
                float(np.median(differences))
                if n_pairs
                else math.nan
            ),
            "mean_absolute_difference": (
                float(np.mean(np.abs(differences)))
                if n_pairs
                else math.nan
            ),
            "rank_biserial_effect_size": (
                rank_biserial_from_differences(differences)
                if n_pairs
                else math.nan
            ),
            "wilcoxon_statistic": math.nan,
            "p_value": math.nan,
        }

        if n_pairs > 0:
            if n_nonzero == 0:
                result["wilcoxon_statistic"] = 0.0
                result["p_value"] = 1.0
            else:
                test = wilcoxon(
                    mlt,
                    emlt,
                    alternative="two-sided",
                    zero_method="wilcox",
                    correction=False,
                    method="auto",
                )
                result["wilcoxon_statistic"] = float(
                    test.statistic
                )
                result["p_value"] = float(test.pvalue)

        results.append(result)

    results_df = pd.DataFrame(results)

    results_df["p_value_fdr_bh"] = benjamini_hochberg(
        results_df["p_value"]
    )

    results_df["significant_raw"] = (
        results_df["p_value"] < alpha
    )

    results_df["significant_fdr_bh"] = (
        results_df["p_value_fdr_bh"] < alpha
    )

    return results_df


def safe_metric_name(metric: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", metric)


def metric_display_name(metric: str) -> str:
    replacements = {
        "mean_drift_s": "Mean drift (s)",
        "median_drift_s": "Median drift (s)",
        "max_abs_drift_s": "Maximum absolute drift (s)",
        "n_matched": "Matched events",
    }

    return replacements.get(
        metric,
        metric.replace("_", " ").strip().title(),
    )


def save_identity_plot(
    paired_df: pd.DataFrame,
    metric: str,
    out_dir: Path,
    plot_format: str,
    dpi: int,
) -> None:
    emlt_col = f"eMLT_orig_{metric}"
    mlt_col = f"mLT_orig_{metric}"

    data = paired_df[[emlt_col, mlt_col]].apply(
        pd.to_numeric,
        errors="coerce",
    ).dropna()

    if data.empty:
        return

    x = data[emlt_col].to_numpy(dtype=float)
    y = data[mlt_col].to_numpy(dtype=float)

    all_values = np.concatenate([x, y])
    minimum = float(np.nanmin(all_values))
    maximum = float(np.nanmax(all_values))

    if math.isclose(minimum, maximum):
        padding = max(abs(minimum) * 0.05, 0.01)
    else:
        padding = (maximum - minimum) * 0.05

    line_min = minimum - padding
    line_max = maximum + padding

    fig, ax = plt.subplots(figsize=(7, 7))
    ax.scatter(x, y)
    ax.plot(
        [line_min, line_max],
        [line_min, line_max],
        linestyle="--",
    )

    ax.set_xlim(line_min, line_max)
    ax.set_ylim(line_min, line_max)
    ax.set_xlabel(f"eMLT_orig {metric_display_name(metric)}")
    ax.set_ylabel(f"mLT_orig {metric_display_name(metric)}")
    ax.set_title(
        f"eMLT_orig vs mLT_orig — {metric_display_name(metric)}"
    )
    ax.grid(True, alpha=0.3)

    output = (
        out_dir
        / f"{safe_metric_name(metric)}_identity.{plot_format}"
    )
    fig.tight_layout()
    fig.savefig(output, dpi=dpi)
    plt.close(fig)


def save_paired_plot(
    paired_df: pd.DataFrame,
    metric: str,
    out_dir: Path,
    plot_format: str,
    dpi: int,
) -> None:
    emlt_col = f"eMLT_orig_{metric}"
    mlt_col = f"mLT_orig_{metric}"

    data = paired_df[
        ["session", emlt_col, mlt_col]
    ].copy()

    data[emlt_col] = pd.to_numeric(
        data[emlt_col],
        errors="coerce",
    )
    data[mlt_col] = pd.to_numeric(
        data[mlt_col],
        errors="coerce",
    )

    data = data.dropna()

    if data.empty:
        return

    fig, ax = plt.subplots(figsize=(8, 6))

    for _, row in data.iterrows():
        ax.plot(
            [0, 1],
            [row[emlt_col], row[mlt_col]],
            alpha=0.35,
        )

    ax.scatter(
        np.zeros(len(data)),
        data[emlt_col],
        label="eMLT_orig",
    )
    ax.scatter(
        np.ones(len(data)),
        data[mlt_col],
        label="mLT_orig",
    )

    ax.set_xticks([0, 1])
    ax.set_xticklabels(["eMLT_orig", "mLT_orig"])
    ax.set_ylabel(metric_display_name(metric))
    ax.set_title(
        f"Paired session comparison — {metric_display_name(metric)}"
    )
    ax.grid(True, axis="y", alpha=0.3)

    output = (
        out_dir
        / f"{safe_metric_name(metric)}_paired.{plot_format}"
    )
    fig.tight_layout()
    fig.savefig(output, dpi=dpi)
    plt.close(fig)


def save_difference_plot(
    paired_df: pd.DataFrame,
    metric: str,
    out_dir: Path,
    plot_format: str,
    dpi: int,
) -> None:
    delta_col = f"delta_{metric}"

    differences = pd.to_numeric(
        paired_df[delta_col],
        errors="coerce",
    ).dropna()

    if differences.empty:
        return

    fig, ax = plt.subplots(figsize=(7, 6))

    values = differences.to_numpy(dtype=float)

    ax.violinplot(
        values,
        positions=[0],
        showmeans=False,
        showmedians=True,
        showextrema=True,
    )

    jitter = np.random.default_rng(42).normal(
        loc=0.0,
        scale=0.035,
        size=len(values),
    )

    ax.scatter(
        jitter,
        values,
        alpha=0.65,
    )

    ax.axhline(0, linestyle="--")
    ax.set_xticks([0])
    ax.set_xticklabels(["mLT_orig − eMLT_orig"])
    ax.set_ylabel(f"Difference in {metric_display_name(metric)}")
    ax.set_title(
        f"Paired difference — {metric_display_name(metric)}"
    )
    ax.grid(True, axis="y", alpha=0.3)

    output = (
        out_dir
        / f"{safe_metric_name(metric)}_differences.{plot_format}"
    )
    fig.tight_layout()
    fig.savefig(output, dpi=dpi)
    plt.close(fig)


def save_all_plots(
    paired_df: pd.DataFrame,
    metrics: list[str],
    out_dir: Path,
    plot_format: str,
    dpi: int,
) -> None:
    plot_dir = out_dir / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    for metric in metrics:
        save_identity_plot(
            paired_df,
            metric,
            plot_dir,
            plot_format,
            dpi,
        )
        save_paired_plot(
            paired_df,
            metric,
            plot_dir,
            plot_format,
            dpi,
        )
        save_difference_plot(
            paired_df,
            metric,
            plot_dir,
            plot_format,
            dpi,
        )


def print_summary(
    paired_df: pd.DataFrame,
    unmatched_df: pd.DataFrame,
    stats_df: pd.DataFrame,
) -> None:
    print("\n=== Drift time-column comparison ===")
    print(f"Matched sessions:   {len(paired_df)}")
    print(f"Unmatched sessions: {len(unmatched_df)}")

    if stats_df.empty:
        print("No valid metric comparisons were available.")
        return

    print("\nPaired Wilcoxon results:")
    display_cols = [
        "metric",
        "n_pairs",
        "median_difference_mlt_minus_emlt",
        "rank_biserial_effect_size",
        "p_value",
        "p_value_fdr_bh",
        "significant_fdr_bh",
    ]

    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        160,
    ):
        print(stats_df[display_cols].to_string(index=False))


def main() -> None:
    args = parse_args()

    emlt_root = Path(args.emlt_root).expanduser().resolve()
    mlt_root = Path(args.mlt_root).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()

    out_dir.mkdir(parents=True, exist_ok=True)

    metrics = [
        metric.strip()
        for metric in args.metrics.split(",")
        if metric.strip()
    ]

    if not metrics:
        raise ValueError("At least one metric must be supplied.")

    print(f"[input] eMLT_orig root: {emlt_root}")
    print(f"[input] mLT_orig root:  {mlt_root}")
    print(f"[output]             :  {out_dir}")

    emlt_files = discover_drift_summaries(emlt_root)
    mlt_files = discover_drift_summaries(mlt_root)

    print(f"[found] eMLT_orig summaries: {len(emlt_files)}")
    print(f"[found] mLT_orig summaries:  {len(mlt_files)}")

    paired_df, unmatched_df = build_paired_table(
        emlt_files,
        mlt_files,
        metrics,
    )

    if paired_df.empty:
        raise RuntimeError(
            "No matching DriftSummary filenames were found between "
            "the eMLT_orig and mLT_orig roots."
        )

    paired_csv = out_dir / "paired_drift_comparison.csv"
    unmatched_csv = out_dir / "unmatched_drift_summaries.csv"
    stats_csv = out_dir / "paired_wilcoxon_results.csv"

    paired_df.to_csv(paired_csv, index=False)
    unmatched_df.to_csv(unmatched_csv, index=False)

    stats_df = run_wilcoxon_comparisons(
        paired_df,
        metrics,
        args.alpha,
    )
    stats_df.to_csv(stats_csv, index=False)

    save_all_plots(
        paired_df,
        metrics,
        out_dir,
        args.plot_format,
        args.dpi,
    )

    print_summary(
        paired_df,
        unmatched_df,
        stats_df,
    )

    print(f"\n[ok] paired table -> {paired_csv}")
    print(f"[ok] statistics   -> {stats_csv}")
    print(f"[ok] unmatched    -> {unmatched_csv}")
    print(f"[ok] plots        -> {out_dir / 'plots'}")


if __name__ == "__main__":
    main()