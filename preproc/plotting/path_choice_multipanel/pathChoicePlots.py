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

from pathChoiceHelpers import RepresentativePair, SUMMARY_COLUMNS, derive_path_order

#IBM color blind safe palette for plotting
PLOT_COLORS = [
    "#648FFF",
    "#785EF0",
    "#DC267F",
    "#FE6100",
    "#FFB000",
]


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
        bbox_to_anchor=(0.5, 0.01),
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


def _comparison_facets(
    data: pd.DataFrame,
    *,
    pair: RepresentativePair,
    figure_type: str,
    session_column: str = "sessionID",
    cohort_column: str = "main_RR",
    main_cohort_value: str = "main",
    rr_cohort_value: str = "RR",
) -> list[tuple[str, pd.DataFrame, bool]]:
    """Build comparison facets as (title, frame, is_representative)."""
    session_ids = data[session_column].astype("string").str.strip()
    main_rep = data.loc[session_ids.eq(pair.main_session_id)].copy()
    rr_rep = data.loc[session_ids.eq(pair.rr_session_id)].copy()

    if main_rep.empty or rr_rep.empty:
        raise ValueError(
            f"Representative data are empty for {pair.mode}: "
            f"Main={len(main_rep)}, RR={len(rr_rep)}."
        )

    if figure_type == "representatives":
        return [
            ("All participants", data.copy(), False),
            (f"Main representative\n{pair.main_session_id}", main_rep, True),
            (f"RR representative\n{pair.rr_session_id}", rr_rep, True),
        ]

    if figure_type != "cohorts":
        raise ValueError("figure_type must be 'representatives' or 'cohorts'.")

    if cohort_column not in data.columns:
        raise ValueError(f"Plot data do not contain cohort column '{cohort_column}'.")

    cohort_values = data[cohort_column].astype("string").str.strip().str.lower()
    main = data.loc[cohort_values.eq(str(main_cohort_value).strip().lower())].copy()
    rr = data.loc[cohort_values.eq(str(rr_cohort_value).strip().lower())].copy()
    if main.empty or rr.empty:
        raise ValueError(
            f"Cohort data are empty: Main={len(main)}, RR={len(rr)}. "
            f"Check --cohort-column/--main-cohort-value/--rr-cohort-value."
        )

    return [
        (f"Main cohort\nN = {main[['participantID', session_column]].drop_duplicates().shape[0]} sessions", main, False),
        (f"RR cohort\nN = {rr[['participantID', session_column]].drop_duplicates().shape[0]} sessions", rr, False),
        (f"Main representative\n{pair.main_session_id}", main_rep, True),
        (f"RR representative\n{pair.rr_session_id}", rr_rep, True),
    ]


def _comparison_axes(
    *,
    figure_type: str,
    panel_width: float,
    panel_height: float,
    sharey: bool,
) -> tuple[plt.Figure, list[plt.Axes]]:
    """Create fixed 1×3 representative or 2×2 cohort comparison axes."""
    if figure_type == "representatives":
        figure, axes = plt.subplots(
            1,
            3,
            figsize=(panel_width * 3, panel_height + 1.6),
            sharex=False,
            sharey=sharey,
            squeeze=False,
        )
        return figure, list(axes.flat)

    figure, axes = plt.subplots(
        2,
        2,
        figsize=(panel_width * 2, panel_height * 2 + 1.8),
        sharex=False,
        sharey=sharey,
        squeeze=False,
    )
    return figure, list(axes.flat)


def _comparison_title(
    *,
    base: str,
    pair: RepresentativePair,
    figure_type: str,
) -> str:
    scope = (
        "All participants and representatives"
        if figure_type == "representatives"
        else "Cohorts and matched representatives"
    )
    return f"{base} — {scope} — {pair.mode.replace('_', ' ').title()}"


def plot_path_comparison_violins(
    data: pd.DataFrame,
    *,
    pair: RepresentativePair,
    figure_type: str,
    output_path: str | Path | None = None,
    session_column: str = "sessionID",
    cohort_column: str = "main_RR",
    main_cohort_value: str = "main",
    rr_cohort_value: str = "RR",
    jitter: float = 0.3,
    point_size: float = 3.2,
    point_alpha: float = 0.20,
    violin_inner: str | None = "quartile",
    panel_width: float = 5.1,
    panel_height: float = 4.2,
    annotation_fontsize: float = 7.2,
) -> plt.Figure:
    """Plot path-choice round distributions for representative/cohort comparisons."""
    path_order, path_labels = derive_path_order(data)
    color_map = _path_palette(path_order)
    facets = _comparison_facets(
        data,
        pair=pair,
        figure_type=figure_type,
        session_column=session_column,
        cohort_column=cohort_column,
        main_cohort_value=main_cohort_value,
        rr_cohort_value=rr_cohort_value,
    )

    sns.set_theme(style="whitegrid", context="notebook")
    figure, axes = _comparison_axes(
        figure_type=figure_type,
        panel_width=panel_width,
        panel_height=panel_height,
        sharey=True,
    )

    for axis, (facet_title, frame, is_representative) in zip(axes, facets):
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

        rounds = frame["roundID_int"].nunique()
        axis.set_title(f"{facet_title}\n{len(frame):,} path-choice rounds", fontsize=10.5)
        axis.set_xlabel("Path choice code")
        axis.set_ylabel(str(frame["round_axis_label"].iloc[0]))
        _show_numeric_x_ticks(axis, path_order)

        if is_representative:
            annotation = _facet_annotation(frame)
            if annotation:
                axis.text(
                    0.02, 0.98, annotation,
                    transform=axis.transAxes,
                    va="top", ha="left",
                    fontsize=annotation_fontsize,
                    linespacing=1.25,
                    bbox={
                        "boxstyle": "round,pad=0.28",
                        "facecolor": "white",
                        "edgecolor": "0.65",
                        "alpha": 0.82,
                    },
                )

    figure.suptitle(
        _comparison_title(
            base="Path-choice round distributions",
            pair=pair,
            figure_type=figure_type,
        ),
        fontsize=15,
        y=0.995,
    )
    _add_path_legend(
        figure,
        path_order=path_order,
        path_labels=path_labels,
        color_map=color_map,
    )
    figure.tight_layout(rect=(0, 0.13, 1, 0.96))
    if output_path is not None:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        figure.savefig(output, dpi=240, bbox_inches="tight")
    return figure


def plot_path_comparison_proportions(
    data: pd.DataFrame,
    *,
    pair: RepresentativePair,
    figure_type: str,
    output_path: str | Path | None = None,
    session_column: str = "sessionID",
    cohort_column: str = "main_RR",
    main_cohort_value: str = "main",
    rr_cohort_value: str = "RR",
    panel_width: float = 5.1,
    panel_height: float = 4.0,
    annotation_fontsize: float = 7.2,
) -> plt.Figure:
    """Plot aggregate path proportions for representative/cohort comparisons."""
    path_order, path_labels = derive_path_order(data)
    color_map = _path_palette(path_order)
    facets = _comparison_facets(
        data,
        pair=pair,
        figure_type=figure_type,
        session_column=session_column,
        cohort_column=cohort_column,
        main_cohort_value=main_cohort_value,
        rr_cohort_value=rr_cohort_value,
    )

    sns.set_theme(style="whitegrid", context="notebook")
    figure, axes = _comparison_axes(
        figure_type=figure_type,
        panel_width=panel_width,
        panel_height=panel_height,
        sharey=True,
    )

    for axis, (facet_title, frame, is_representative) in zip(axes, facets):
        counts = frame["path_code"].value_counts().reindex(path_order, fill_value=0)
        total = int(counts.sum())
        proportions = counts / total if total else counts.astype(float)

        axis.bar(
            range(len(path_order)),
            proportions.to_numpy(),
            color=[color_map[code] for code in path_order],
            edgecolor="0.25",
            linewidth=0.8,
        )
        axis.set_title(f"{facet_title}\nN = {total:,} rounds", fontsize=10.5)
        axis.set_xlabel("Path choice code")
        axis.set_ylabel("Proportion of rounds")
        axis.set_ylim(0, 1)
        _show_numeric_x_ticks(axis, path_order)

        for index, (count, proportion) in enumerate(zip(counts, proportions)):
            axis.text(
                index,
                float(proportion),
                f"n={int(count)}",
                ha="center",
                va="bottom",
                fontsize=7.5,
            )

        if is_representative:
            annotation = _facet_annotation(frame)
            if annotation:
                axis.text(
                    0.35, 0.98, annotation,
                    transform=axis.transAxes,
                    va="top", ha="left",
                    fontsize=annotation_fontsize,
                    linespacing=1.25,
                    bbox={
                        "boxstyle": "round,pad=0.28",
                        "facecolor": "white",
                        "edgecolor": "0.65",
                        "alpha": 0.82,
                    },
                )

    figure.suptitle(
        _comparison_title(
            base="Path-choice proportions",
            pair=pair,
            figure_type=figure_type,
        ),
        fontsize=15,
        y=0.995,
    )
    _add_path_legend(
        figure,
        path_order=path_order,
        path_labels=path_labels,
        color_map=color_map,
    )

    figure.tight_layout(rect=(0, 0.01, 1, .97))
    if output_path is not None:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        figure.savefig(output, dpi=240, bbox_inches="tight")
    return figure
