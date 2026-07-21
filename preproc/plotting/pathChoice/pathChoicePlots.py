#!/usr/bin/env python3
"""Faceted violin and proportion plots for participant path choices."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Mapping

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import pandas as pd
import seaborn as sns

from pathChoiceHelpers import SUMMARY_COLUMNS, derive_path_order


def _format_value(value: object, *, digits: int = 3) -> str:
    """Format compact facet annotation values."""
    if pd.isna(value):
        return "NA"
    if isinstance(value, (int, float)):
        return f"{value:.{digits}g}"
    return str(value)


def _facet_title(frame: pd.DataFrame) -> str:
    """Build a compact title containing participant and round count."""
    first = frame.iloc[0]
    if "totRounds" in frame.columns and pd.notna(first["totRounds"]):
        total_rounds = _format_value(first["totRounds"])
    else:
        total_rounds = str(frame["roundID_int"].nunique())
    return f"Participant {first['participantID']} | Total Completed Rounds: {total_rounds}"


def _facet_annotation(frame: pd.DataFrame) -> str:
    """Build a compact two-line metrics annotation."""
    first = frame.iloc[0]
    metric_labels = {
        "PVSS_AvgScore": "PVSS",
        "swapRate_tot": "swapRate",
        "rounds2criterion": "R2C",
        "totScore": "score",
    }
    values = [
        f"{label}={_format_value(first[column])}"
        for column, label in metric_labels.items()
        if column in frame.columns
    ]
    if not values:
        return ""
    midpoint = math.ceil(len(values) / 2)
    return "\n".join((" | ".join(values[:midpoint]), " | ".join(values[midpoint:]))).strip()


def _path_palette(path_order: list[int]) -> dict[int, tuple[float, float, float]]:
    """Return one stable tab10 color for each path code."""
    return dict(zip(path_order, sns.color_palette("tab10", n_colors=len(path_order))))


def _add_path_legend(
    figure: plt.Figure,
    *,
    path_order: list[int],
    path_labels: Mapping[int, str],
    color_map: Mapping[int, object],
) -> None:
    """Add one figure-level legend mapping numeric codes to full path labels."""
    handles = [
        Patch(
            facecolor=color_map[code],
            edgecolor="0.25",
            label=f"{code} — {path_labels[code]}",
        )
        for code in path_order
    ]
    figure.legend(
        handles=handles,
        title="Path choice lookup",
        loc="upper center",
        bbox_to_anchor=(0.5, 0.005),
        ncol=2,
        frameon=True,
        fontsize=8,
        title_fontsize=9,
    )


def _show_numeric_x_ticks(axis: plt.Axes, path_order: list[int]) -> None:
    """Force numeric path codes to appear on every facet."""
    axis.set_xticks(range(len(path_order)))
    axis.set_xticklabels([str(code) for code in path_order], fontsize=9)
    axis.tick_params(axis="x", labelbottom=True)


def plot_faceted_path_violins(
    data: pd.DataFrame,
    *,
    output_path: str | Path | None = None,
    ncols: int = 3,
    jitter: float = 0.3,
    point_size: float = 3.2,
    point_alpha: float = 0.25,
    violin_inner: str | None = "quartile",
    panel_width: float = 5.1,
    panel_height: float = 4.2,
    annotation_fontsize: float = 7.2,
    title: str = "Path-choice proportions by participant and session for the first 50 rounds",
) -> plt.Figure:
    """Plot one participant/session per facet with fixed colored path positions."""
    required = {
        "participantID",
        "sessionID",
        "roundID_int",
        "round_plot_value",
        "path_code",
        "path_label",
    }
    missing = sorted(required.difference(data.columns))
    if missing:
        raise ValueError(f"Plot data are missing columns: {missing}")
    if ncols < 1:
        raise ValueError("ncols must be at least 1.")

    path_order, path_labels = derive_path_order(data)
    color_map = _path_palette(path_order)
    facets = list(data.groupby(["participantID", "sessionID"], sort=True, dropna=False))
    nrows = math.ceil(len(facets) / ncols)

    sns.set_theme(style="whitegrid", context="notebook")
    figure, axes = plt.subplots(
        nrows,
        ncols,
        figsize=(panel_width * ncols, panel_height * nrows + 1.5),
        sharex=False,
        sharey=False,
        squeeze=False,
    )

    for axis, (_, frame) in zip(axes.flat, facets):
        sns.violinplot(
            data=frame,
            x="path_code",
            y="round_plot_value",
            order=path_order,
            palette=color_map,
            hue="path_code",
            hue_order=path_order,
            legend=False,
            inner=violin_inner,
            cut=0,
            density_norm="width",
            common_norm=False,
            linewidth=1,
            saturation=1,
            ax=axis,
        )
        sns.stripplot(
            data=frame,
            x="path_code",
            y="round_plot_value",
            order=path_order,
            color="0.1",
            jitter=jitter,
            size=point_size,
            alpha=point_alpha,
            ax=axis,
        )

        axis.set_title(_facet_title(frame), fontsize=10.5)
        axis.set_xlabel("Path choice code")
        axis.set_ylabel(str(frame["round_axis_label"].iloc[0]))
        _show_numeric_x_ticks(axis, path_order)

        annotation = _facet_annotation(frame)
        if annotation:
            axis.text(
                0.02,
                0.5,
                annotation,
                transform=axis.transAxes,
                va="top",
                ha="left",
                fontsize=annotation_fontsize,
                linespacing=1.25,
                bbox={
                    "boxstyle": "round,pad=0.28",
                    "facecolor": "white",
                    "edgecolor": "0.65",
                    "alpha": 0.82,
                },
            )

    for axis in axes.flat[len(facets):]:
        axis.set_visible(False)

    figure.suptitle(title, fontsize=15, y=0.995)
    _add_path_legend(
        figure,
        path_order=path_order,
        path_labels=path_labels,
        color_map=color_map,
    )
    figure.tight_layout(rect=(0, 0.13, 1, 0.97))

    if output_path is not None:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        figure.savefig(output, dpi=240, bbox_inches="tight")
    return figure


def plot_faceted_path_proportions(
    data: pd.DataFrame,
    *,
    output_path: str | Path | None = None,
    ncols: int = 3,
    panel_width: float = 5.1,
    panel_height: float = 4.0,
    annotation_fontsize: float = 7.2,
    title: str = "Path-choice proportions by participant and session for the first 50 rounds",
) -> plt.Figure:
    """Plot path proportions using the same code-to-color mapping as the violins."""
    path_order, path_labels = derive_path_order(data)
    color_map = _path_palette(path_order)
    facets = list(data.groupby(["participantID", "sessionID"], sort=True, dropna=False))
    nrows = math.ceil(len(facets) / ncols)

    sns.set_theme(style="whitegrid", context="notebook")
    figure, axes = plt.subplots(
        nrows,
        ncols,
        figsize=(panel_width * ncols, panel_height * nrows + 1.5),
        sharex=False,
        sharey=True,
        squeeze=False,
    )

    for axis, (_, frame) in zip(axes.flat, facets):
        counts = frame["path_code"].value_counts().reindex(path_order, fill_value=0)
        proportions = counts / counts.sum()
        axis.bar(
            range(len(path_order)),
            proportions.to_numpy(),
            color=[color_map[code] for code in path_order],
            edgecolor="0.25",
            linewidth=0.8,
        )
        axis.set_title(_facet_title(frame), fontsize=10.5)
        axis.set_xlabel("Path choice code")
        axis.set_ylabel("Proportion of rounds")
        axis.set_ylim(0, 1)
        _show_numeric_x_ticks(axis, path_order)

        for index, (count, proportion) in enumerate(zip(counts, proportions)):
            axis.text(
                index,
                proportion,
                f"n={count}",
                ha="center",
                va="bottom",
                fontsize=7.5,
            )

        annotation = _facet_annotation(frame)
        if annotation:
            axis.text(
                0.35,
                0.98,
                annotation,
                transform=axis.transAxes,
                va="top",
                ha="left",
                fontsize=annotation_fontsize,
                linespacing=1.25,
                bbox={
                    "boxstyle": "round,pad=0.28",
                    "facecolor": "white",
                    "edgecolor": "0.65",
                    "alpha": 0.82,
                },
            )

    for axis in axes.flat[len(facets):]:
        axis.set_visible(False)

    figure.suptitle(title, fontsize=15, y=0.995)
    _add_path_legend(
        figure,
        path_order=path_order,
        path_labels=path_labels,
        color_map=color_map,
    )
    figure.tight_layout(rect=(0, 0.13, 1, 0.97))

    if output_path is not None:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        figure.savefig(output, dpi=240, bbox_inches="tight")
    return figure
