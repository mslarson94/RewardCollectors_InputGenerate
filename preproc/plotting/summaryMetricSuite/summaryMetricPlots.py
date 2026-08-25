#!/usr/bin/env python3
"""Pin-drop-style histogram/KDE and violin plots for participant summary metrics."""

from __future__ import annotations

import math

import matplotlib.pyplot as plt
from matplotlib.figure import Figure
import numpy as np
import pandas as pd
import seaborn as sns

from summaryMetricHelpers import metric_axis_label


# IBM color blind safe palette
PLOT_COLORS = [
    "#648FFF",
    "#785EF0",
    "#DC267F",
    "#FE6100",
    "#FFB000",

]


def _plot_color(color_index: int = 0) -> str:
    """Return a predefined plot color, cycling when needed."""
    return PLOT_COLORS[color_index % len(PLOT_COLORS)]


def _numeric_values(data: pd.DataFrame, variable: str) -> pd.Series:
    """Return finite numeric values for plotting."""
    values = pd.to_numeric(data[variable], errors="coerce")
    return values[np.isfinite(values)].copy()


def _stats_text(values: pd.Series) -> str:
    """Build the compact statistics label used across plots."""
    if values.empty:
        return "n=0"

    if len(values) > 1:
        return (
            f"n={len(values)}   "
            f"mean={values.mean():.3g}\n"
            f"median={values.median():.3g}  "
            f"SD={values.std(ddof=1):.3g}"
        )

    return (
        f"n=1   "
        f"mean={values.mean():.3g}   "
        f"median={values.median():.3g}"
    )


def _hist_bins(values: pd.Series, bin_width: float | None) -> str | np.ndarray:
    """Use explicit-width bins when requested, otherwise Freedman-Diaconis bins."""
    if values.empty or bin_width is None:
        return "fd"

    if not math.isfinite(bin_width) or bin_width <= 0:
        raise ValueError("bin_width must be a positive finite number.")

    low = float(values.min())
    high = float(values.max())

    if math.isclose(low, high):
        return np.array([low - bin_width / 2, high + bin_width / 2])

    start = math.floor(low / bin_width) * bin_width
    stop = math.ceil(high / bin_width) * bin_width + bin_width
    return np.arange(start, stop, bin_width)


def _shared_bin_edges(
    data: pd.DataFrame,
    *,
    variable: str,
    bin_width: float | None,
) -> np.ndarray:
    """Compute one shared set of histogram bin edges for all facets."""
    values = _numeric_values(data, variable)
    if values.empty:
        raise ValueError(f"No finite numeric values remain for '{variable}'.")

    requested_bins = _hist_bins(values, bin_width)

    if isinstance(requested_bins, str):
        return np.histogram_bin_edges(
            values.to_numpy(dtype=float),
            bins=requested_bins,
        )

    return np.asarray(requested_bins, dtype=float)


def _plot_kde_as_counts(
    axis: plt.Axes,
    values: pd.Series,
    bin_edges: np.ndarray,
    *,
    color: str,
    label: str = "KDE",
) -> None:
    """Draw a Gaussian KDE scaled to histogram counts without SciPy."""
    if len(values) < 3 or values.nunique() < 2:
        return

    array = values.to_numpy(dtype=float)
    std = float(np.std(array, ddof=1))

    if not math.isfinite(std) or std <= 0:
        return

    bandwidth = 1.06 * std * (len(array) ** (-1 / 5))

    if not math.isfinite(bandwidth) or bandwidth <= 0:
        return

    x_grid = np.linspace(float(array.min()), float(array.max()), 300)
    z = (x_grid[:, None] - array[None, :]) / bandwidth
    density = (
        np.exp(-0.5 * z**2).mean(axis=1)
        / (bandwidth * math.sqrt(2 * math.pi))
    )

    typical_bin_width = float(np.median(np.diff(bin_edges)))
    count_scale = len(array) * typical_bin_width

    axis.plot(
        x_grid,
        density * count_scale,
        linewidth=2.6,
        color=color,
        label=label,
    )


def plot_histogram(
    data: pd.DataFrame,
    *,
    variable: str,
    variable_label: str,
    unit: str = "",
    title_prefix: str = "",
    bin_width: float | None = None,
    xlim: tuple[float, float] | None = None,
    dot_mode: str = "panel",
    color_index: int = 0,
    show_stats: bool = True,
) -> Figure:
    """Plot a white-grid histogram with KDE and optional participant dots."""
    values = _numeric_values(data, variable)
    if values.empty:
        raise ValueError(f"No finite numeric values remain for '{variable}'.")

    plot_color = _plot_color(color_index)

    sns.set_theme(style="whitegrid", context="notebook")
    axis_label = metric_axis_label(variable_label, unit)
    title = f"{title_prefix} — {variable_label}" if title_prefix else variable_label

    if dot_mode == "panel":
        figure = plt.figure(figsize=(8.0, 5.8))
        grid = figure.add_gridspec(2, 1, height_ratios=(5.0, 0.75), hspace=0.05)
        axis = figure.add_subplot(grid[0])
        dot_axis = figure.add_subplot(grid[1], sharex=axis)
    else:
        figure, axis = plt.subplots(figsize=(8.0, 5.2))
        dot_axis = None

    requested_bins = _hist_bins(values, bin_width)

    if isinstance(requested_bins, str):
        bin_edges = np.histogram_bin_edges(
            values.to_numpy(dtype=float),
            bins=requested_bins,
        )
    else:
        bin_edges = np.asarray(requested_bins, dtype=float)

    actual_bin_width = float(np.median(np.diff(bin_edges)))

    axis.hist(
        values.to_numpy(dtype=float),
        bins=bin_edges,
        color=plot_color,
        edgecolor="0.25",
        linewidth=0.8,
        alpha=0.65,
    )

    _plot_kde_as_counts(
        axis,
        values,
        bin_edges,
        color=plot_color,
    )

    axis.axvline(
        values.mean(),
        linestyle="--",
        linewidth=2,
        color="k",
        label="Mean",
    )
    axis.axvline(
        values.median(),
        linestyle=":",
        linewidth=2,
        color="k",
        label="Median",
    )

    axis.set_title(title)
    axis.set_xlabel("" if dot_axis is not None else axis_label)
    axis.set_ylabel("Participants")

    if show_stats:
        stats_label = (
            f"{_stats_text(values)}\n"
            f"bin width={actual_bin_width:.3g}"
        )

        axis.text(
            0.99,
            0.97,
            stats_label,
            transform=axis.transAxes,
            ha="right",
            va="top",
            fontsize=9,
            bbox={
                "boxstyle": "round,pad=0.3",
                "facecolor": "white",
                "alpha": 0.85,
            },
        )

    axis.legend(frameon=True)

    if xlim is not None:
        axis.set_xlim(*xlim)

    if dot_axis is not None:
        strip_data = pd.DataFrame(
            {
                "value": values,
                "row": [""] * len(values),
            }
        )

        sns.stripplot(
            data=strip_data,
            x="value",
            y="row",
            jitter=0.22,
            size=5,
            alpha=0.42,
            color="k",
            ax=dot_axis,
        )

        dot_axis.set_xlabel(axis_label)
        dot_axis.set_ylabel("")
        dot_axis.set_yticks([])
        dot_axis.grid(axis="y", visible=False)
        dot_axis.spines["left"].set_visible(False)
        dot_axis.spines["right"].set_visible(False)
        dot_axis.spines["top"].set_visible(False)

    elif dot_mode == "baseline":
        baseline_y = np.zeros(len(values))
        axis.scatter(
            values,
            baseline_y,
            s=18,
            alpha=0.42,
            color=plot_color,
            zorder=5,
        )

    figure.tight_layout()
    return figure


def plot_violin(
    data: pd.DataFrame,
    *,
    variable: str,
    variable_label: str,
    unit: str = "",
    title_prefix: str = "",
    xlim: tuple[float, float] | None = None,
    color_index: int = 0,
    show_stats: bool = True,
) -> Figure:
    """Plot a horizontal violin with quartiles and participant points."""
    values = _numeric_values(data, variable)
    if values.empty:
        raise ValueError(f"No finite numeric values remain for '{variable}'.")

    plot_color = _plot_color(color_index)

    sns.set_theme(style="whitegrid", context="notebook")
    figure, axis = plt.subplots(figsize=(8.0, 3.8))

    plot_data = pd.DataFrame(
        {
            "value": values,
            "distribution": ["Participants"] * len(values),
        }
    )

    sns.violinplot(
        data=plot_data,
        x="value",
        y="distribution",
        inner="quartile",
        cut=0,
        density_norm="width",
        linewidth=1.5,
        saturation=1,
        color=plot_color,
        ax=axis,
    )

    sns.stripplot(
        data=plot_data,
        x="value",
        y="distribution",
        jitter=0.18,
        size=5,
        alpha=0.42,
        color="k",
        ax=axis,
    )

    axis_label = metric_axis_label(variable_label, unit)
    title = f"{title_prefix} — {variable_label}" if title_prefix else variable_label

    axis.set_title(title)
    axis.set_xlabel(axis_label)
    axis.set_ylabel("")

    if show_stats:
        axis.text(
            0.99,
            0.95,
            _stats_text(values),
            transform=axis.transAxes,
            ha="right",
            va="top",
            fontsize=9,
            bbox={
                "boxstyle": "round,pad=0.3",
                "facecolor": "white",
                "alpha": 0.85,
            },
        )

    if xlim is not None:
        axis.set_xlim(*xlim)

    figure.tight_layout()
    return figure


def plot_faceted_histogram_overlay(
    data: pd.DataFrame,
    *,
    variable: str,
    facet_by: str,
    variable_label: str,
    unit: str = "",
    title_prefix: str = "",
    bin_width: float | None = None,
    xlim: tuple[float, float] | None = None,
    show_stats: bool = True,
) -> Figure:
    """Overlay all facet histograms on one axis using shared bins."""
    if facet_by not in data.columns:
        raise ValueError(f"Facet column '{facet_by}' is not present in the input.")

    bin_edges = _shared_bin_edges(
        data,
        variable=variable,
        bin_width=bin_width,
    )
    actual_bin_width = float(np.median(np.diff(bin_edges)))

    sns.set_theme(style="whitegrid", context="notebook")
    figure, axis = plt.subplots(figsize=(8.6, 5.6))

    for color_index, (facet_value, facet_data) in enumerate(
        data.groupby(facet_by, dropna=False, sort=True)
    ):
        values = _numeric_values(facet_data, variable)
        if values.empty:
            continue

        color = _plot_color(color_index)
        label = str(facet_value)

        axis.hist(
            values.to_numpy(dtype=float),
            bins=bin_edges,
            color=color,
            edgecolor="0.25",
            linewidth=0.7,
            alpha=0.42,
            label=f"{label} (n={len(values)})",
        )

        _plot_kde_as_counts(
            axis,
            values,
            bin_edges,
            color=color,
            label=f"{label} KDE",
        )

    axis_label = metric_axis_label(variable_label, unit)
    title = (
        f"{title_prefix} — {variable_label} by {facet_by}"
        if title_prefix
        else f"{variable_label} by {facet_by}"
    )

    axis.set_title(title)
    axis.set_xlabel(axis_label)
    axis.set_ylabel("Participants")
    if show_stats:
        axis.text(
            0.99,
            0.97,
            f"shared bin width={actual_bin_width:.3g}",
            transform=axis.transAxes,
            ha="right",
            va="top",
            fontsize=9,
            bbox={
                "boxstyle": "round,pad=0.3",
                "facecolor": "white",
                "alpha": 0.85,
            },
        )
    axis.legend(frameon=True)

    if xlim is not None:
        axis.set_xlim(*xlim)

    figure.tight_layout()
    return figure


def plot_faceted_histogram_panels(
    data: pd.DataFrame,
    *,
    variable: str,
    facet_by: str,
    variable_label: str,
    unit: str = "",
    title_prefix: str = "",
    bin_width: float | None = None,
    xlim: tuple[float, float] | None = None,
    vertical_threshold: int = 4,
    show_stats: bool = True,
) -> Figure:
    """Plot facet histograms in shared-axis panels, stacking when facets are many."""
    if facet_by not in data.columns:
        raise ValueError(f"Facet column '{facet_by}' is not present in the input.")

    grouped = list(data.groupby(facet_by, dropna=False, sort=True))
    grouped = [
        (facet_value, facet_data)
        for facet_value, facet_data in grouped
        if not _numeric_values(facet_data, variable).empty
    ]

    if not grouped:
        raise ValueError(f"No finite numeric values remain for '{variable}'.")

    bin_edges = _shared_bin_edges(
        data,
        variable=variable,
        bin_width=bin_width,
    )
    actual_bin_width = float(np.median(np.diff(bin_edges)))
    n_facets = len(grouped)

    sns.set_theme(style="whitegrid", context="notebook")

    if n_facets >= vertical_threshold:
        figure, axes = plt.subplots(
            n_facets,
            1,
            figsize=(8.4, max(3.0 * n_facets, 4.8)),
            sharex=True,
            sharey=True,
            squeeze=False,
        )
        axes_flat = axes.ravel()
    else:
        figure, axes = plt.subplots(
            1,
            n_facets,
            figsize=(max(4.2 * n_facets, 7.0), 4.8),
            sharex=True,
            sharey=True,
            squeeze=False,
        )
        axes_flat = axes.ravel()

    for color_index, ((facet_value, facet_data), axis) in enumerate(
        zip(grouped, axes_flat)
    ):
        values = _numeric_values(facet_data, variable)
        color = _plot_color(color_index)

        axis.hist(
            values.to_numpy(dtype=float),
            bins=bin_edges,
            color=color,
            edgecolor="0.25",
            linewidth=0.8,
            alpha=0.65,
        )

        _plot_kde_as_counts(
            axis,
            values,
            bin_edges,
            color=color,
        )

        axis.set_title(f"{facet_by}: {facet_value}")
        if show_stats:
            axis.text(
                0.98,
                0.94,
                f"n={len(values)}",
                transform=axis.transAxes,
                ha="right",
                va="top",
                fontsize=9,
                bbox={
                    "boxstyle": "round,pad=0.25",
                    "facecolor": "white",
                    "alpha": 0.8,
                },
            )

    axis_label = metric_axis_label(variable_label, unit)

    if n_facets >= vertical_threshold:
        for axis in axes_flat:
            axis.set_ylabel("Participants")
        axes_flat[-1].set_xlabel(axis_label)
    else:
        axes_flat[0].set_ylabel("Participants")
        for axis in axes_flat:
            axis.set_xlabel(axis_label)

    if xlim is not None:
        for axis in axes_flat:
            axis.set_xlim(*xlim)

    title = (
        f"{title_prefix} — {variable_label} by {facet_by}"
        if title_prefix
        else f"{variable_label} by {facet_by}"
    )

    panel_title = (
        f"{title}\nshared bin width={actual_bin_width:.3g}"
        if show_stats
        else title
    )
    figure.suptitle(
        panel_title,
        y=1.02,
    )
    figure.tight_layout()
    return figure


def plot_faceted_violin(
    data: pd.DataFrame,
    *,
    variable: str,
    facet_by: str,
    variable_label: str,
    unit: str = "",
    title_prefix: str = "",
    xlim: tuple[float, float] | None = None,
) -> Figure:
    """Plot all facet distributions together as horizontal violins."""
    if facet_by not in data.columns:
        raise ValueError(f"Facet column '{facet_by}' is not present in the input.")

    plot_data = data[[facet_by, variable]].copy()
    plot_data["value"] = pd.to_numeric(plot_data[variable], errors="coerce")
    plot_data = plot_data[np.isfinite(plot_data["value"])].copy()

    if plot_data.empty:
        raise ValueError(f"No finite numeric values remain for '{variable}'.")

    facet_order = list(
        plot_data[facet_by]
        .drop_duplicates()
        .sort_values(key=lambda series: series.astype(str))
    )

    palette = {
        facet_value: _plot_color(index)
        for index, facet_value in enumerate(facet_order)
    }

    sns.set_theme(style="whitegrid", context="notebook")
    figure_height = max(3.8, 0.9 * len(facet_order) + 1.8)
    figure, axis = plt.subplots(figsize=(8.6, figure_height))

    sns.violinplot(
        data=plot_data,
        x="value",
        y=facet_by,
        order=facet_order,
        inner="quartile",
        cut=0,
        density_norm="width",
        linewidth=1,
        saturation=1,
        palette=palette,
        hue=facet_by,
        hue_order=facet_order,
        legend=False,
        ax=axis,
    )

    sns.stripplot(
        data=plot_data,
        x="value",
        y=facet_by,
        order=facet_order,
        color="k",
        dodge=False,
        jitter=0.16,
        size=5.0,
        alpha=0.42,
        legend=False,
        ax=axis,
    )

    axis_label = metric_axis_label(variable_label, unit)
    title = (
        f"{title_prefix} — {variable_label} by {facet_by}"
        if title_prefix
        else f"{variable_label} by {facet_by}"
    )

    axis.set_title(title)
    axis.set_xlabel(axis_label)
    axis.set_ylabel(facet_by)

    if xlim is not None:
        axis.set_xlim(*xlim)

    figure.tight_layout()
    return figure
