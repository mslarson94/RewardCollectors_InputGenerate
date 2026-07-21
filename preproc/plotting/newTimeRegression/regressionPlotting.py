# =========================
# file: regressionPlotting.py
# =========================
from __future__ import annotations

from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import pandas as pd

from regressionHelpers import (
    COLOR_BY_CORRECTNESS,
    DEFAULT_POINT_ALPHA,
    DEFAULT_POINT_SIZE,
    FILLED_BY_COIN,
    MARKER_BY_COIN,
)
from regressionStats import PlotSpec


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
    first = ax.legend(handles=coin_handles, title="Coin Type", loc="upper left")
    ax.add_artist(first)

    correctness_handles: list[Line2D] = []
    if "correct" in correctness_values:
        correctness_handles.append(
            Line2D(
                [0],
                [0],
                marker="s",
                linestyle="None",
                markersize=9,
                markerfacecolor=COLOR_BY_CORRECTNESS["correct"],
                markeredgecolor=COLOR_BY_CORRECTNESS["correct"],
                label="correct",
            )
        )
    if "incorrect" in correctness_values:
        correctness_handles.append(
            Line2D(
                [0],
                [0],
                marker="s",
                linestyle="None",
                markersize=9,
                markerfacecolor=COLOR_BY_CORRECTNESS["incorrect"],
                markeredgecolor=COLOR_BY_CORRECTNESS["incorrect"],
                label="incorrect",
            )
        )

    if correctness_handles:
        ax.legend(handles=correctness_handles, title="Drop Correctness", loc="upper right")


def make_description_text(
    *,
    outcome_label: str,
    spec: PlotSpec,
    include_tp1: bool,
    correct_only: bool,
) -> str:
    tp_text = "TP1 + TP2" if include_tp1 else "TP2 Only"
    correctness_text = "Correct Only" if correct_only else "All Drops"
    return (
        f"{outcome_label} × Task Progression\n"
        f"{spec.subject_label}\n"
        f"{spec.coin_label}\n"
        f"{tp_text} | {correctness_text}"
    )


def create_regression_summary_plot(
    *,
    df: pd.DataFrame,
    pred_df: pd.DataFrame,
    outcome_column: str,
    outcome_label: str,
    spec: PlotSpec,
    include_tp1: bool,
    correct_only: bool,
    ci_style: str,
    show_raw: bool,
    width: float,
    height: float,
) -> plt.Figure:
    fig = plt.figure(figsize=(width, height))
    ax = fig.add_axes([0.12, 0.36, 0.78, 0.55])

    if show_raw:
        for _, row in df.iterrows():
            scatter_styled_point(
                ax,
                x=float(row["taskProgression"]),
                y=float(row[outcome_column]),
                coin=str(row["coinLabel"]),
                correctness=str(row["dropCorrectness"]),
            )

    ax.plot(
        pred_df["taskProgression"],
        pred_df["mean"],
        linewidth=2.1,
    )

    if ci_style == "ribbon":
        ax.fill_between(
            pred_df["taskProgression"],
            pred_df["mean_ci_lower"],
            pred_df["mean_ci_upper"],
            alpha=0.18,
        )
    else:
        ax.plot(
            pred_df["taskProgression"],
            pred_df["mean_ci_lower"],
            linestyle=(0, (3, 3)),
            linewidth=1.3,
        )
        ax.plot(
            pred_df["taskProgression"],
            pred_df["mean_ci_upper"],
            linestyle=(0, (3, 3)),
            linewidth=1.3,
        )

    ax.set_xlabel("Task Progression")
    ax.set_ylabel(outcome_label)
    ax.grid(True, alpha=0.25)

    if show_raw:
        add_coin_and_correctness_legends(
            ax,
            correctness_values=set(df["dropCorrectness"].dropna().astype(str).unique().tolist()),
        )

    description = make_description_text(
        outcome_label=outcome_label,
        spec=spec,
        include_tp1=include_tp1,
        correct_only=correct_only,
    )

    fig.text(
        0.12,
        0.14,
        description,
        ha="left",
        va="bottom",
        fontsize=10,
    )
    return fig


