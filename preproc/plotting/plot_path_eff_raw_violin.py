# file: plot_path_eff_raw_violin_with_pathValue.py

from __future__ import annotations

from itertools import combinations
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as st
import seaborn as sns
from matplotlib.patches import Patch
from statsmodels.stats.multitest import multipletests


PATH_METADATA = pd.DataFrame(
    [
        {"path_order_round": "HV -> LV -> NV", "path_order_round_num": 1, "orderedCollect": 1, "pathValue": 30},
        {"path_order_round": "LV -> HV -> NV", "path_order_round_num": 2, "orderedCollect": 0, "pathValue": 30},
        {"path_order_round": "NV -> HV -> LV", "path_order_round_num": 3, "orderedCollect": 0, "pathValue": 25},
        {"path_order_round": "HV -> NV -> LV", "path_order_round_num": 4, "orderedCollect": 1, "pathValue": 25},
        {"path_order_round": "NV -> LV -> HV", "path_order_round_num": 5, "orderedCollect": 0, "pathValue": 20},
        {"path_order_round": "LV -> NV -> HV", "path_order_round_num": 6, "orderedCollect": 1, "pathValue": 20},
    ]
)


def p_to_stars(p_value: float) -> str:
    if p_value < 0.001:
        return "***"
    if p_value < 0.01:
        return "**"
    if p_value < 0.05:
        return "*"
    return "ns"


def add_path_metadata(df: pd.DataFrame, path_col: str = "path_order_round") -> pd.DataFrame:
    out = df.merge(
        PATH_METADATA,
        how="left",
        left_on=path_col,
        right_on="path_order_round",
        suffixes=("", "_meta"),
    )
    out["plot_group"] = (
        "PV="
        + out["pathValue"].astype("Int64").astype(str)
        + " | OC="
        + out["orderedCollect"].astype("Int64").astype(str)
        + "\n"
        + out[path_col].astype(str)
    )
    return out


def summarize_to_round_level(
    df: pd.DataFrame,
    value_col: str = "path_eff_raw",
    round_col: str = "roundID",
    group_col: str = "plot_group",
) -> pd.DataFrame:
    keep_cols = [
        round_col,
        value_col,
        group_col,
        "path_order_round",
        "path_order_round_num",
        "orderedCollect",
        "pathValue",
    ]
    return (
        df[keep_cols]
        .dropna(subset=[round_col, value_col, group_col, "orderedCollect", "pathValue"])
        .groupby(
            [
                round_col,
                group_col,
                "path_order_round",
                "path_order_round_num",
                "orderedCollect",
                "pathValue",
            ],
            as_index=False,
        )[value_col]
        .mean()
    )


def build_group_order(plot_df: pd.DataFrame, group_col: str = "plot_group") -> list[str]:
    group_table = (
        plot_df[[group_col, "pathValue", "orderedCollect", "path_order_round_num"]]
        .drop_duplicates()
        .sort_values(
            ["pathValue", "orderedCollect", "path_order_round_num"],
            ascending=[False, False, True],
        )
    )
    return group_table[group_col].tolist()


def compute_pairwise_stats(
    plot_df: pd.DataFrame,
    order: list[str],
    value_col: str = "path_eff_raw",
    group_col: str = "plot_group",
) -> tuple[float, pd.DataFrame]:
    if len(order) < 2:
        return np.nan, pd.DataFrame(columns=["group1", "group2", "p_raw", "p_holm", "significant"])

    groups = {
        group_name: plot_df.loc[plot_df[group_col] == group_name, value_col].to_numpy()
        for group_name in order
    }

    _, overall_p = st.kruskal(*(groups[group_name] for group_name in order))

    rows: list[tuple[str, str, float]] = []
    for group1, group2 in combinations(order, 2):
        _, p_raw = st.mannwhitneyu(groups[group1], groups[group2], alternative="two-sided")
        rows.append((group1, group2, p_raw))

    pairwise_df = pd.DataFrame(rows, columns=["group1", "group2", "p_raw"])
    pairwise_df["p_holm"] = multipletests(pairwise_df["p_raw"], method="holm")[1]
    pairwise_df["significant"] = pairwise_df["p_holm"] < 0.05

    return overall_p, pairwise_df.sort_values(
        ["significant", "p_holm"],
        ascending=[False, True],
    ).reset_index(drop=True)


def add_significance_bars(
    ax: plt.Axes,
    order: list[str],
    pairwise_df: pd.DataFrame,
    y_data: np.ndarray,
    max_bars: int | None = None,
) -> None:
    if pairwise_df.empty:
        return

    sig_df = pairwise_df.loc[pairwise_df["significant"]].copy()
    if max_bars is not None:
        sig_df = sig_df.nsmallest(max_bars, "p_holm")
    if sig_df.empty:
        return

    y_min = float(np.nanmin(y_data))
    y_max = float(np.nanmax(y_data))
    y_range = y_max - y_min if y_max > y_min else 1.0

    base_y = y_max + y_range * 0.05
    step = y_range * 0.08
    bar_height = y_range * 0.03
    x_positions = {label: idx for idx, label in enumerate(order)}

    for idx, row in sig_df.reset_index(drop=True).iterrows():
        x1 = x_positions[row["group1"]]
        x2 = x_positions[row["group2"]]
        if x1 > x2:
            x1, x2 = x2, x1

        y = base_y + idx * step
        ax.plot([x1, x1, x2, x2], [y, y + bar_height, y + bar_height, y], lw=1.2, c="black")
        ax.text(
            (x1 + x2) / 2,
            y + bar_height,
            p_to_stars(float(row["p_holm"])),
            ha="center",
            va="bottom",
            fontsize=10,
            color="black",
        )

    ax.set_ylim(y_min - y_range * 0.02, base_y + len(sig_df) * step + y_range * 0.08)


def add_pathvalue_headers(ax: plt.Axes, plot_df: pd.DataFrame, order: list[str]) -> None:
    group_positions = {label: idx for idx, label in enumerate(order)}
    group_table = plot_df[["plot_group", "pathValue"]].drop_duplicates()

    pathvalue_to_labels: dict[int, list[str]] = {}
    for _, row in group_table.iterrows():
        pathvalue_to_labels.setdefault(int(row["pathValue"]), []).append(row["plot_group"])

    for path_value in sorted(pathvalue_to_labels, reverse=True):
        labels = [label for label in order if label in pathvalue_to_labels[path_value]]
        xs = [group_positions[label] for label in labels]
        center = float(np.mean(xs))
        ax.text(
            center,
            1.02,
            f"pathValue = {path_value}",
            transform=ax.get_xaxis_transform(),
            ha="center",
            va="bottom",
            fontsize=11,
        )

    ordered_pathvalues = sorted(pathvalue_to_labels, reverse=True)
    for left_value, right_value in zip(ordered_pathvalues[:-1], ordered_pathvalues[1:]):
        left_labels = [label for label in order if label in pathvalue_to_labels[left_value]]
        right_labels = [label for label in order if label in pathvalue_to_labels[right_value]]
        boundary = (group_positions[left_labels[-1]] + group_positions[right_labels[0]]) / 2
        #ax.axvline(boundary, color="black", linestyle="--", linewidth=1, alpha=0.6)


def build_paired_palette_map() -> dict[tuple[int, int], tuple[float, float, float]]:
    paired = sns.color_palette("Paired", 12)
    return {
        (30, 0): paired[0],
        (30, 1): paired[1],
        (25, 0): paired[6],
        (25, 1): paired[7],
        (20, 0): paired[8],
        (20, 1): paired[9],
    }


def plot_violin_with_jitter(
    df: pd.DataFrame,
    value_col: str = "path_eff_raw",
    round_col: str = "roundID",
    path_col: str = "path_order_round",
    point_alpha: float = 0.5,
    point_color: str = "black",
    jitter: float = 0.25,
    point_size: float = 3.8,
    figsize: tuple[float, float] = (14, 8),
    output_path: str | Path | None = None,
    max_sig_bars: int | None = 10,
) -> tuple[plt.Figure, plt.Axes, pd.DataFrame, pd.DataFrame]:
    working_df = add_path_metadata(df, path_col=path_col)
    working_df = working_df.dropna(subset=["orderedCollect", "pathValue", "path_order_round_num"]).copy()

    plot_df = summarize_to_round_level(
        df=working_df,
        value_col=value_col,
        round_col=round_col,
        group_col="plot_group",
    )

    order = build_group_order(plot_df, group_col="plot_group")
    overall_p, pairwise_df = compute_pairwise_stats(
        plot_df=plot_df,
        order=order,
        value_col=value_col,
        group_col="plot_group",
    )

    group_meta = (
        plot_df[["plot_group", "pathValue", "orderedCollect"]]
        .drop_duplicates()
        .set_index("plot_group")
    )

    pv_oc_palette = build_paired_palette_map()
    palette_list = [
        pv_oc_palette[(int(group_meta.loc[label, "pathValue"]), int(group_meta.loc[label, "orderedCollect"]))]
        for label in order
    ]

    fig, ax = plt.subplots(figsize=figsize)

    sns.violinplot(
        data=plot_df,
        x="plot_group",
        y=value_col,
        order=order,
        palette=palette_list,
        inner=None,
        cut=0,
        ax=ax,
    )

    sns.stripplot(
        data=plot_df,
        x="plot_group",
        y=value_col,
        order=order,
        color=point_color,
        alpha=point_alpha,
        jitter=jitter,
        dodge=False,
        size=point_size,
        ax=ax,
    )

    sns.pointplot(
        data=plot_df,
        x="plot_group",
        y=value_col,
        order=order,
        estimator=np.median,
        errorbar=lambda x: np.percentile(x, [25, 75]),
        join=False,
        color="black",
        markers="_",
        scale=1.2,
        errwidth=1.5,
        capsize=0.15,
        ax=ax,
    )

    add_significance_bars(
        ax=ax,
        order=order,
        pairwise_df=pairwise_df,
        y_data=plot_df[value_col].to_numpy(),
        max_bars=max_sig_bars,
    )
    add_pathvalue_headers(ax=ax, plot_df=plot_df, order=order)

    legend_handles = [
        Patch(facecolor=pv_oc_palette[(30, 0)], edgecolor="black", label="PV=30, OC=0"),
        Patch(facecolor=pv_oc_palette[(30, 1)], edgecolor="black", label="PV=30, OC=1"),
        Patch(facecolor=pv_oc_palette[(25, 0)], edgecolor="black", label="PV=25, OC=0"),
        Patch(facecolor=pv_oc_palette[(25, 1)], edgecolor="black", label="PV=25, OC=1"),
        Patch(facecolor=pv_oc_palette[(20, 0)], edgecolor="black", label="PV=20, OC=0"),
        Patch(facecolor=pv_oc_palette[(20, 1)], edgecolor="black", label="PV=20, OC=1"),
    ]
    ax.legend(handles=legend_handles, title="", loc="upper left", bbox_to_anchor=(1.01, 1.0))

    ax.set_xlabel("Grouped by pathValue, ordered by orderedCollect")
    ax.set_ylabel(value_col)
    ax.set_title(
        f"{value_col} by pathValue / orderedCollect\n"
        f"one point per unique {round_col}; Kruskal-Wallis p = {overall_p:.2e}"
    )

    ax.set_xticklabels(order, rotation=0, ha="center")
    fig.tight_layout()

    if output_path is not None:
        output_path = Path(output_path)
        fig.savefig(output_path, dpi=200, bbox_inches="tight")

    return fig, ax, plot_df, pairwise_df


if __name__ == "__main__":
    input_csv = "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain_PO/EventSegmentation/megaFiles/allIntervalData_AN_with_rejectWalkDist.csv"
    output_png = "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain_PO/Plotting/PathEff/path_eff_raw_violin_jitter_black_stats.png"
    output_stats_csv = "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain_PO/Plotting/PathEff/path_eff_raw_pairwise_stats.csv"
    output_round_csv = "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain_PO/Plotting/PathEff/path_eff_raw_roundlevel_pathValue_orderedCollect.csv"

    data_raw = pd.read_csv(input_csv)
    data_raw = pd.read_csv(input_csv)
    print(data_raw.columns.tolist())
    data = data_raw[data_raw["rejectWalkDist"] != 1].copy()
    _, _, round_level_df, pairwise_stats = plot_violin_with_jitter(
        df=data,
        value_col="path_eff_raw",
        round_col="roundID",
        path_col="path_order_round",
        point_alpha=0.5,
        point_color="black",
        output_path=output_png,
    )


    round_level_df.to_csv(output_round_csv, index=False)
    pairwise_stats.to_csv(output_stats_csv, index=False)