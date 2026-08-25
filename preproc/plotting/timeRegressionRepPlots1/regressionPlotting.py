# =========================
# file: regressionPlotting.py
# =========================
from __future__ import annotations

from collections.abc import Sequence

from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from regressionHelpers import (
    COLOR_BY_CORRECTNESS,
    DEFAULT_POINT_ALPHA,
    DEFAULT_POINT_SIZE,
    FILLED_BY_COIN,
    MARKER_BY_COIN,
    RepresentativePair,
)
from regressionStats import FacetFit


def scatter_styled_point(
    ax: plt.Axes,
    *,
    x: float,
    y: float,
    coin: str,
    correctness: str,
    alpha: float = DEFAULT_POINT_ALPHA,
    size: float = DEFAULT_POINT_SIZE,
) -> None:
    """Draw one raw observation using the suite's coin/correctness encoding."""

    marker = MARKER_BY_COIN.get(coin, "o")
    filled = FILLED_BY_COIN.get(coin, True)
    color = COLOR_BY_CORRECTNESS.get(correctness, "gray")

    if filled:
        ax.scatter(
            x,
            y,
            marker=marker,
            s=size,
            alpha=alpha,
            facecolors=color,
            edgecolors=color,
            linewidth=0.9,
        )
        return

    ax.scatter(
        x,
        y,
        marker=marker,
        s=size,
        alpha=alpha,
        facecolors="none",
        edgecolors=color,
        linewidth=1.0,
    )


def add_coin_and_correctness_legends(
    ax: plt.Axes,
    *,
    correctness_values: set[str],
) -> None:
    """Add the raw-point legends to one designated facet."""

    coin_handles = [
        Line2D(
            [0],
            [0],
            marker="*",
            linestyle="None",
            markersize=10,
            markerfacecolor="black",
            markeredgecolor="black",
            label="HV",
        ),
        Line2D(
            [0],
            [0],
            marker="o",
            linestyle="None",
            markersize=9,
            markerfacecolor="black",
            markeredgecolor="black",
            label="LV",
        ),
        Line2D(
            [0],
            [0],
            marker="o",
            linestyle="None",
            markersize=9,
            markerfacecolor="none",
            markeredgecolor="black",
            label="NV",
        ),
    ]
    coin_legend = ax.legend(
        handles=coin_handles,
        title="Coin Type",
        loc="upper left",
        fontsize=8,
        title_fontsize=9,
    )
    ax.add_artist(coin_legend)

    correctness_handles: list[Line2D] = []
    for value in ("correct", "incorrect"):
        if value in correctness_values:
            correctness_handles.append(
                Line2D(
                    [0],
                    [0],
                    marker="s",
                    linestyle="None",
                    markersize=8,
                    markerfacecolor=COLOR_BY_CORRECTNESS[value],
                    markeredgecolor=COLOR_BY_CORRECTNESS[value],
                    label=value,
                )
            )

    if correctness_handles:
        ax.legend(
            handles=correctness_handles,
            title="Drop Correctness",
            loc="upper right",
            fontsize=8,
            title_fontsize=9,
        )


def _finite_values(*arrays: Sequence[float] | pd.Series | np.ndarray) -> np.ndarray:
    values: list[np.ndarray] = []
    for array in arrays:
        numeric = pd.to_numeric(pd.Series(array), errors="coerce").to_numpy(dtype=float)
        numeric = numeric[np.isfinite(numeric)]
        if numeric.size:
            values.append(numeric)
    if not values:
        return np.array([], dtype=float)
    return np.concatenate(values)


def _padded_limits(
    values: np.ndarray,
    *,
    padding_fraction: float = 0.05,
) -> tuple[float, float]:
    if values.size == 0:
        return 0.0, 1.0

    lower = float(np.min(values))
    upper = float(np.max(values))
    span = upper - lower

    if not np.isfinite(span) or span <= 0:
        pad = max(abs(lower) * padding_fraction, 1.0)
    else:
        pad = span * padding_fraction

    return lower - pad, upper + pad


def derive_shared_limits(
    fits: Sequence[FacetFit],
    *,
    outcome_column: str,
    task_progression_column: str,
    xlim: tuple[float, float] | None,
    ylim: tuple[float, float] | None,
) -> tuple[tuple[float, float], tuple[float, float]]:
    """Derive shared figure-level limits from raw data and fitted CIs."""

    x_values: list[np.ndarray] = []
    y_values: list[np.ndarray] = []

    for fit in fits:
        data = fit.facet.data
        if not data.empty:
            x_values.append(
                _finite_values(data[task_progression_column])
            )
            y_values.append(
                _finite_values(data[outcome_column])
            )

        if fit.predictions is not None:
            pred = fit.predictions
            x_values.append(
                _finite_values(pred[task_progression_column])
            )
            y_values.extend(
                [
                    _finite_values(pred["mean"]),
                    _finite_values(pred["mean_ci_lower"]),
                    _finite_values(pred["mean_ci_upper"]),
                ]
            )

    combined_x = (
        np.concatenate([values for values in x_values if values.size])
        if any(values.size for values in x_values)
        else np.array([], dtype=float)
    )
    combined_y = (
        np.concatenate([values for values in y_values if values.size])
        if any(values.size for values in y_values)
        else np.array([], dtype=float)
    )

    shared_x = xlim if xlim is not None else _padded_limits(combined_x, padding_fraction=0.02)
    shared_y = ylim if ylim is not None else _padded_limits(combined_y, padding_fraction=0.05)
    return shared_x, shared_y


def _draw_regression_panel(
    ax: plt.Axes,
    *,
    fit: FacetFit,
    outcome_column: str,
    task_progression_column: str,
    outcome_label: str,
    coin_column: str,
    ci_style: str,
    show_raw: bool,
    show_legends: bool,
    shared_xlim: tuple[float, float],
    shared_ylim: tuple[float, float],
) -> None:
    """Draw one regression facet or a clear skipped-data state."""

    data = fit.facet.data

    if show_raw and not data.empty:
        for row in data.itertuples(index=False):
            row_dict = row._asdict()
            scatter_styled_point(
                ax,
                x=float(row_dict[task_progression_column]),
                y=float(row_dict[outcome_column]),
                coin=str(row_dict[coin_column]),
                correctness=str(row_dict["dropCorrectness"]),
            )

    if fit.predictions is not None:
        pred = fit.predictions
        line = ax.plot(
            pred[task_progression_column],
            pred["mean"],
            linewidth=2.1,
            label="OLS fit",
        )[0]
        line_color = line.get_color()

        if ci_style == "ribbon":
            ax.fill_between(
                pred[task_progression_column],
                pred["mean_ci_lower"],
                pred["mean_ci_upper"],
                alpha=0.18,
                color=line_color,
                label="95% mean CI",
            )
        else:
            ax.plot(
                pred[task_progression_column],
                pred["mean_ci_lower"],
                linestyle=(0, (3, 3)),
                linewidth=1.3,
                color=line_color,
            )
            ax.plot(
                pred[task_progression_column],
                pred["mean_ci_upper"],
                linestyle=(0, (3, 3)),
                linewidth=1.3,
                color=line_color,
            )
    else:
        ax.text(
            0.5,
            0.5,
            fit.status.replace("_", " "),
            transform=ax.transAxes,
            ha="center",
            va="center",
            fontsize=10,
        )

    ax.set_xlim(shared_xlim)
    ax.set_ylim(shared_ylim)
    ax.grid(True, alpha=0.25)
    ax.set_title(
        f"{fit.facet.title}\nN = {len(data):,} Pin Drops",
        fontsize=12,
    )
    ax.set_xlabel("Task Progression")
    ax.set_ylabel(outcome_label)

    if show_raw and show_legends and not data.empty:
        add_coin_and_correctness_legends(
            ax,
            correctness_values=set(
                data["dropCorrectness"].dropna().astype(str).unique().tolist()
            ),
        )


def _figure_footer(
    *,
    pair: RepresentativePair,
    include_tp1: bool,
    correct_only: bool,
) -> str:
    tp_text = "TP1 + TP2" if include_tp1 else "TP2 Only"
    correctness_text = "Correct Only" if correct_only else "All Drops"
    return (
        f"Role: {pair.role} | Representative mode: "
        f"{pair.mode.replace('_', ' ').title()} | {tp_text} | {correctness_text}"
    )


def create_representative_regression_figure(
    *,
    fits: Sequence[FacetFit],
    pair: RepresentativePair,
    outcome_column: str,
    task_progression_column: str,
    outcome_label: str,
    coin_column: str,
    include_tp1: bool,
    correct_only: bool,
    ci_style: str,
    show_raw: bool,
    xlim: tuple[float, float] | None,
    ylim: tuple[float, float] | None,
) -> plt.Figure:
    """Create All Participants + Main Rep + RR Rep regression facets."""

    if len(fits) != 3:
        raise ValueError("Representative figure requires exactly three facets.")

    shared_xlim, shared_ylim = derive_shared_limits(
        fits,
        outcome_column=outcome_column,
        task_progression_column=task_progression_column,
        xlim=xlim,
        ylim=ylim,
    )

    figure, axes = plt.subplots(
        1,
        3,
        figsize=(18, 6.5),
        sharex=True,
        sharey=True,
    )

    for index, (axis, fit) in enumerate(zip(axes, fits)):
        _draw_regression_panel(
            axis,
            fit=fit,
            outcome_column=outcome_column,
            task_progression_column=task_progression_column,
            outcome_label=outcome_label,
            coin_column=coin_column,
            ci_style=ci_style,
            show_raw=show_raw,
            show_legends=index == 0,
            shared_xlim=shared_xlim,
            shared_ylim=shared_ylim,
        )
        if index > 0:
            axis.set_ylabel("")

    figure.suptitle(
        f"{outcome_label} × Task Progression — Representative Comparison",
        fontsize=16,
        y=0.99,
    )
    figure.text(
        0.5,
        0.01,
        _figure_footer(
            pair=pair,
            include_tp1=include_tp1,
            correct_only=correct_only,
        ),
        ha="center",
        fontsize=9,
    )
    figure.tight_layout(rect=(0, 0.04, 1, 0.95))
    return figure


def create_cohort_regression_figure(
    *,
    fits: Sequence[FacetFit],
    pair: RepresentativePair,
    outcome_column: str,
    task_progression_column: str,
    outcome_label: str,
    coin_column: str,
    include_tp1: bool,
    correct_only: bool,
    ci_style: str,
    show_raw: bool,
    xlim: tuple[float, float] | None,
    ylim: tuple[float, float] | None,
) -> plt.Figure:
    """Create Main/RR cohort and representative regressions in a 2×2 grid."""

    if len(fits) != 4:
        raise ValueError("Cohort figure requires exactly four facets.")

    shared_xlim, shared_ylim = derive_shared_limits(
        fits,
        outcome_column=outcome_column,
        task_progression_column=task_progression_column,
        xlim=xlim,
        ylim=ylim,
    )

    figure, axes = plt.subplots(
        2,
        2,
        figsize=(13, 10),
        sharex=True,
        sharey=True,
    )
    flat_axes = axes.ravel()

    for index, (axis, fit) in enumerate(zip(flat_axes, fits)):
        _draw_regression_panel(
            axis,
            fit=fit,
            outcome_column=outcome_column,
            task_progression_column=task_progression_column,
            outcome_label=outcome_label,
            coin_column=coin_column,
            ci_style=ci_style,
            show_raw=show_raw,
            show_legends=index == 0,
            shared_xlim=shared_xlim,
            shared_ylim=shared_ylim,
        )

        if index % 2 == 1:
            axis.set_ylabel("")
        if index < 2:
            axis.set_xlabel("")

    figure.suptitle(
        f"{outcome_label} × Task Progression — Cohort vs Representative",
        fontsize=16,
        y=0.995,
    )
    figure.text(
        0.5,
        0.01,
        _figure_footer(
            pair=pair,
            include_tp1=include_tp1,
            correct_only=correct_only,
        ),
        ha="center",
        fontsize=9,
    )
    figure.tight_layout(rect=(0, 0.035, 1, 0.96))
    return figure
