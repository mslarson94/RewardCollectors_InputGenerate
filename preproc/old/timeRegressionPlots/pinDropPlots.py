# =========================
# file: pinDropPlots.py
# =========================
from __future__ import annotations

import math

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd
import seaborn as sns

from pinDropHelpers import (
    ALPHA,
    COLOR_BY_QUAL,
    FILLED_BY_COIN,
    MARKER_BY_COIN,
    SIZE,
    prepare_block3_round_num_data,
    prepare_block3_round_time_data,
    prepare_blocks_gt3_blocknum_data,
    prepare_blocks_gt3_time_data,
    prepare_hist_data,
)


def scatter_point(
    ax: plt.Axes,
    x: float,
    y: float,
    coin: str,
    qual: str,
    *,
    alpha: float = ALPHA,
    size: float = SIZE,
) -> None:
    marker = MARKER_BY_COIN.get(coin, "o")
    filled = FILLED_BY_COIN.get(coin, True)
    color = COLOR_BY_QUAL.get(qual, "gray")

    if filled:
        ax.scatter(
            x,
            y,
            marker=marker,
            s=size,
            alpha=alpha,
            facecolors=color,
            edgecolors=color,
            linewidth=1,
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
        linewidth=1,
    )


def add_legends(ax: plt.Axes) -> None:
    coin_handles = [
        Line2D([0], [0], marker="*", linestyle="None", markersize=10, markerfacecolor="none", markeredgecolor="black", label="HV"),
        Line2D([0], [0], marker="o", linestyle="None", markersize=10, markerfacecolor="black", markeredgecolor="black", label="LV"),
        Line2D([0], [0], marker="o", linestyle="None", markersize=10, markerfacecolor="none", markeredgecolor="black", label="NV"),
    ]
    first = ax.legend(handles=coin_handles, title="Coin Type", loc="upper left")
    ax.add_artist(first)

    qual_handles = [
        Line2D([0], [0], marker="s", linestyle="None", markersize=10, markerfacecolor=COLOR_BY_QUAL["good"], markeredgecolor=COLOR_BY_QUAL["good"], label="good"),
        Line2D([0], [0], marker="s", linestyle="None", markersize=10, markerfacecolor=COLOR_BY_QUAL["bad"], markeredgecolor=COLOR_BY_QUAL["bad"], label="bad"),
    ]
    ax.legend(handles=qual_handles, title="Drop Quality", loc="upper right")


def plot_block3_round_time(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    y_label: str,
) -> plt.Figure:
    dat = prepare_block3_round_time_data(df, variable_of_interest=variable_of_interest)

    fig, ax = plt.subplots(figsize=(10, 6))
    for _, row in dat.iterrows():
        scatter_point(
            ax,
            float(row["trueSession_elapsed_s"]),
            float(row[variable_of_interest]),
            str(row["coinLabel"]),
            str(row["dropQual"]),
        )

    ax.set_title(f"{variable_of_interest} in Block 3 (Session Elapsed Time)")
    ax.set_xlabel("Session Elapsed Time (s)")
    ax.set_ylabel(y_label)
    add_legends(ax)
    fig.tight_layout()
    return fig


def plot_block3_round_num(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    y_label: str,
    round_max: int = 100,
) -> plt.Figure:
    dat = prepare_block3_round_num_data(
        df,
        variable_of_interest=variable_of_interest,
        round_max=round_max,
    )

    fig, ax = plt.subplots(figsize=(10, 6))
    for _, row in dat.iterrows():
        scatter_point(
            ax,
            float(row["RoundNum"]),
            float(row[variable_of_interest]),
            str(row["coinLabel"]),
            str(row["dropQual"]),
        )

    ax.set_title(f"{variable_of_interest} in Block 3 (By Rounds)")
    ax.set_xlabel("Round Number")
    ax.set_ylabel(y_label)
    add_legends(ax)
    fig.tight_layout()
    return fig


def plot_blocks_gt3_vs_time(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    y_label: str,
    blocks_min: int = 3,
) -> plt.Figure:
    dat = prepare_blocks_gt3_time_data(
        df,
        variable_of_interest=variable_of_interest,
        blocks_min=blocks_min,
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    for _, row in dat.iterrows():
        scatter_point(
            ax,
            float(row["trueSession_elapsed_s"]),
            float(row[variable_of_interest]),
            str(row["coinLabel"]),
            str(row["dropQual"]),
        )

    ax.set_title(f"{variable_of_interest} in Blocks > {blocks_min}")
    ax.set_xlabel("Session Elapsed Time (s)")
    ax.set_ylabel(y_label)
    add_legends(ax)
    fig.tight_layout()
    return fig


def plot_blocks_gt3_vs_block_num(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    y_label: str,
    blocks_min: int = 3,
) -> plt.Figure:
    dat = prepare_blocks_gt3_blocknum_data(
        df,
        variable_of_interest=variable_of_interest,
        blocks_min=blocks_min,
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    for _, row in dat.iterrows():
        scatter_point(
            ax,
            float(row["BlockNum"]),
            float(row[variable_of_interest]),
            str(row["coinLabel"]),
            str(row["dropQual"]),
        )

    ax.set_title(f"{variable_of_interest} in Blocks > {blocks_min}")
    ax.set_xlabel("Block Number")
    ax.set_ylabel(y_label)
    add_legends(ax)
    fig.tight_layout()
    return fig


def plot_blocks_gt3_block_facets(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    y_label: str,
    blocks_min: int = 3,
    blocks_per_facet: int = 10,
) -> list[plt.Figure]:
    dat = prepare_blocks_gt3_time_data(
        df,
        variable_of_interest=variable_of_interest,
        blocks_min=blocks_min,
    )
    dat = dat.copy()
    dat["BlockNum"] = pd.to_numeric(dat["BlockNum"], errors="coerce").astype(int)
    dat["facet_idx"] = (dat["BlockNum"] - 1) // blocks_per_facet

    figures: list[plt.Figure] = []
    for facet_idx, frame in dat.groupby("facet_idx", sort=True):
        start = facet_idx * blocks_per_facet + 1
        end = (facet_idx + 1) * blocks_per_facet

        fig, ax = plt.subplots(figsize=(12, 6))
        for _, row in frame.iterrows():
            scatter_point(
                ax,
                float(row["trueSession_elapsed_s"]),
                float(row[variable_of_interest]),
                str(row["coinLabel"]),
                str(row["dropQual"]),
            )

        ax.set_title(f"{variable_of_interest} — Blocks {start}–{end}")
        ax.set_xlabel("Session Elapsed Time (s)")
        ax.set_ylabel(y_label)
        add_legends(ax)
        fig.tight_layout()
        figures.append(fig)

    return figures


def plot_hist_kde_by_coin(
    df: pd.DataFrame,
    *,
    variable_of_interest: str = "truecontent_elapsed_s",
    blocks_min: int = 3,
    bins: int | str = "auto",
    stat: str = "density",
    common_norm: bool = False,
    palette: str | dict = "tab10",
    dot_mode: str = "panel",
    dot_alpha: float = 0.6,
    dot_size: float = 12,
    dot_jitter: float = 0.15,
    max_points_per_group: int | None = 4000,
) -> plt.Figure:
    dat = prepare_hist_data(
        df,
        variable_of_interest=variable_of_interest,
        blocks_min=blocks_min,
    )
    dat[variable_of_interest] = pd.to_numeric(dat[variable_of_interest], errors="coerce")
    dat = dat.dropna(subset=[variable_of_interest, "coinLabel"]).reset_index(drop=True)

    if dat.empty:
        raise ValueError(f"No data remain for histogram/KDE of {variable_of_interest}.")

    sns.set_theme(style="whitegrid")

    if dot_mode == "panel":
        fig, (ax_main, ax_dots) = plt.subplots(
            2,
            1,
            figsize=(12, 8),
            gridspec_kw={"height_ratios": [4, 1], "hspace": 0.05},
            sharex=True,
        )
    else:
        fig, ax_main = plt.subplots(figsize=(12, 6))
        ax_dots = None

    sns.histplot(
        data=dat,
        x=variable_of_interest,
        hue="coinLabel",
        bins=bins,
        stat=stat,
        common_norm=common_norm,
        kde=True,
        palette=palette,
        element="step",
        fill=False,
        ax=ax_main,
    )

    ax_main.set_title(f"{variable_of_interest} distribution by coin type")
    ax_main.set_xlabel(variable_of_interest)
    ax_main.set_ylabel(stat.capitalize())

    groups = list(dat.groupby("coinLabel", sort=True))
    if max_points_per_group is not None:
        trimmed = []
        for coin, frame in groups:
            if len(frame) > max_points_per_group:
                frame = frame.sample(max_points_per_group, random_state=7)
            trimmed.append((coin, frame))
        groups = trimmed

    if dot_mode == "panel":
        for row_index, (coin, frame) in enumerate(groups):
            y = np.full(len(frame), row_index, dtype=float)
            jitter = np.random.default_rng(7).uniform(-dot_jitter, dot_jitter, size=len(frame))
            ax_dots.scatter(
                frame[variable_of_interest].to_numpy(dtype=float),
                y + jitter,
                s=dot_size,
                alpha=dot_alpha,
                label=str(coin),
            )

        ax_dots.set_yticks(range(len(groups)))
        ax_dots.set_yticklabels([str(coin) for coin, _ in groups])
        ax_dots.set_xlabel(variable_of_interest)
        ax_dots.set_ylabel("coinLabel")
    else:
        ymin, ymax = ax_main.get_ylim()
        band_height = (ymax - ymin) * max(dot_jitter, 0.02)
        baseline = ymin + band_height
        rng = np.random.default_rng(7)

        for coin, frame in groups:
            jitter = rng.uniform(0, band_height, size=len(frame))
            ax_main.scatter(
                frame[variable_of_interest].to_numpy(dtype=float),
                baseline + jitter,
                s=dot_size,
                alpha=dot_alpha,
                label=f"{coin} dots",
            )

    fig.tight_layout()
    return fig


