from __future__ import annotations
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from histoHelpers import (
    scatter_point, add_legends, _subtitle_from,
    make_axes_with_dots, draw_dots_strip, style_x_for_main_and_dots,
)

def plot_block3_roundNum(df: pd.DataFrame, variableOfInterest: str, yLabel: str, voi_str: str = "Round Elapsed Time",
    voi_UnitStr: str = "(s)"):
    req = ["BlockNum", variableOfInterest, "trueSession_elapsed_s", "RoundNum", "coinLabel", "dropQual", "BlockStatus"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Block 3 plot: {missing}")
    block3 = df[
        (df["BlockNum"] == 3)
        & (df["BlockStatus"] == "complete")
        & df[variableOfInterest].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good", "bad"])
        & (pd.to_numeric(df["RoundNum"], errors="coerce") < 100)
    ][["RoundNum", variableOfInterest, "coinLabel", "dropQual"]].copy()
    fig, ax = plt.subplots(figsize=(10, 6))
    for _, r in block3.iterrows():
        scatter_point(ax, r["RoundNum"], r[variableOfInterest], r["coinLabel"], r["dropQual"])
    title_string = f"{voi_str} in Block 3 (By Rounds)"
    subtitle_string = _subtitle_from(dat) if "_subtitle_from" in globals() else ""
    ax.set_title(title_string, fontsize=14, y=1.055)
    if subtitle_string:
        ax.text(0.5, 1.02, subtitle_string, ha="center", fontsize=11, transform=ax.transAxes)

    #ax.set_title(f"{variableOfInterest} in Block 3 (By Rounds)")
    ax.set_xlabel("Rounds")
    ax.set_ylabel(yLabel)
    add_legends(ax)
    fig.tight_layout()
    plt.show()

def plot_block3_roundTime(df: pd.DataFrame, variableOfInterest: str, yLabel: str, voi_str: str = "Round Elapsed Time",
    voi_UnitStr: str = "(s)"):
    req = ["BlockNum", variableOfInterest, "trueSession_elapsed_s", "coinLabel", "dropQual", "BlockStatus"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Block 3 plot: {missing}")
    block3 = df[
        (df["BlockNum"] == 3)
        & (df["BlockStatus"] == "complete")
        & df[variableOfInterest].notna()
        & df["trueSession_elapsed_s"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good", "bad"])
    ][["trueSession_elapsed_s", variableOfInterest, "coinLabel", "dropQual"]].copy()
    fig, ax = plt.subplots(figsize=(10, 6))
    for _, r in block3.iterrows():
        scatter_point(ax, r["trueSession_elapsed_s"], r[variableOfInterest], r["coinLabel"], r["dropQual"])
    title_string = f"{voi_str} in Block 3 by Coin Type in total Session Elapsed Time"
    subtitle_string = _subtitle_from(dat) if "_subtitle_from" in globals() else ""
    ax.set_title(title_string, fontsize=14, y=1.055)
    if subtitle_string:
        ax.text(0.5, 1.02, subtitle_string, ha="center", fontsize=11, transform=ax.transAxes)

    #ax.set_title(f"{variableOfInterest} in Block 3 (Round Elapsed Time)")
    ax.set_xlabel("Session Elapsed Time (s)")
    ax.set_ylabel(yLabel)
    add_legends(ax)
    fig.tight_layout()
    plt.show()

def plot_blocks_gt3_overall_vs_TP2Blocks_Time(df: pd.DataFrame, variableOfInterest: str, yLabel: str, voi_str: str = "Round Elapsed Time",
    voi_UnitStr: str = "(s)"):
    req = ["BlockNum", variableOfInterest, "trueSession_elapsed_s", "coinLabel", "dropQual", "BlockStatus"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Blocks>3 plot: {missing}")
    gt3 = df[
        (pd.to_numeric(df["BlockNum"], errors="coerce") > 3)
        & (df["BlockStatus"] == "complete")
        & df[variableOfInterest].notna()
        & df["trueSession_elapsed_s"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good", "bad"])
    ][["trueSession_elapsed_s", variableOfInterest, "coinLabel", "dropQual"]].copy()
    fig, ax = plt.subplots(figsize=(12, 6))
    for _, r in gt3.iterrows():
        scatter_point(ax, r["trueSession_elapsed_s"], r[variableOfInterest], r["coinLabel"], r["dropQual"])
    title_string = f"{voi_str} in Block 3 by Coin Type in total Session Elapsed Time"
    subtitle_string = _subtitle_from(dat) if "_subtitle_from" in globals() else ""
    ax.set_title(title_string, fontsize=14, y=1.055)
    if subtitle_string:
        ax.text(0.5, 1.02, subtitle_string, ha="center", fontsize=11, transform=ax.transAxes)

    #ax.set_title(f"{variableOfInterest} in Blocks > 3 by Coin Type and Drop Quality")
    ax.set_xlabel("Session Elapsed Time (s)")
    ax.set_ylabel(yLabel)
    add_legends(ax)
    fig.tight_layout()
    plt.show()

def plot_blocks_gt3_overall_vs_TP2Blocks_BlockNum(df: pd.DataFrame, variableOfInterest: str, yLabel: str, voi_str: str = "Round Elapsed Time",
    voi_UnitStr: str = "(s)"):
    req = ["BlockNum", variableOfInterest, "coinLabel", "dropQual", "BlockStatus"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Blocks>3 plot: {missing}")
    gt3 = df[
        (pd.to_numeric(df["BlockNum"], errors="coerce") > 3)
        & (df["BlockStatus"] == "complete")
        & df[variableOfInterest].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good", "bad"])
    ][["BlockNum", variableOfInterest, "coinLabel", "dropQual"]].copy()
    fig, ax = plt.subplots(figsize=(12, 6))
    for _, r in gt3.iterrows():
        scatter_point(ax, r["BlockNum"], r[variableOfInterest], r["coinLabel"], r["dropQual"])
    #ax.set_title(f"{variableOfInterest} by Coin Type Across All Test Phase 2 Blocks")
    title_string = f"{voi_str} by Coin Type Across All Test Phase 2 Blocks"
    subtitle_string = _subtitle_from(dat) if "_subtitle_from" in globals() else ""
    ax.set_title(title_string, fontsize=14, y=1.055)
    if subtitle_string:
        ax.text(0.5, 1.02, subtitle_string, ha="center", fontsize=11, transform=ax.transAxes)

    ax.set_xlabel("Test Phase 2 Blocks")
    ax.set_ylabel(yLabel)
    add_legends(ax)
    fig.tight_layout()
    plt.show()

def plot_blocks_gt3_overall_vs_block_facetV1(df: pd.DataFrame, variableOfInterest: str, yLabel: str, blocks_per_facet: int = 10, voi_str: str = "Round Elapsed Time",
    voi_UnitStr: str = "(s)"):
    req = ["BlockNum", "trueSession_elapsed_s", variableOfInterest, "coinLabel", "dropQual", "BlockStatus"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Blocks>3 plot: {missing}")

    sub = df[
        (pd.to_numeric(df["BlockNum"], errors="coerce").notna())
        & (df["BlockNum"] > 3)
        & (df["BlockStatus"] == "complete")
        & df[variableOfInterest].notna()
        & df["trueSession_elapsed_s"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good","bad"])
    ][["BlockNum", variableOfInterest, "trueSession_elapsed_s", "coinLabel", "dropQual"]].copy()

    sub["BlockNum"] = pd.to_numeric(sub["BlockNum"], errors="coerce").astype(int)
    sub["facet_idx"] = (sub["BlockNum"] - 1) // blocks_per_facet

    for idx, g in sub.groupby("facet_idx", sort=True):
        start = idx * blocks_per_facet + 1
        end = (idx + 1) * blocks_per_facet

        fig, ax = plt.subplots(figsize=(12, 6))
        for _, r in g.iterrows():
            scatter_point(ax, r["trueSession_elapsed_s"], r[variableOfInterest], r["coinLabel"], r["dropQual"])
        title_string = f"{voi_str} in Block 3 (By Rounds)"
        subtitle_string = _subtitle_from(dat) if "_subtitle_from" in globals() else ""
        ax.set_title(title_string, fontsize=14, y=1.055)
        if subtitle_string:
            ax.text(0.5, 1.02, subtitle_string, ha="center", fontsize=11, transform=ax.transAxes)

        #ax.set_title(f"{variableOfInterest} — Blocks {start}–{end} by Coin Type and Drop Quality")
        ax.set_xlabel(f"Facet {(sub["BlockNum"] - 1) // blocks_per_facet} | Overall Session Elapsed Time (s)")
        ax.set_ylabel(yLabel)
        add_legends(ax)
        fig.tight_layout()
        plt.show()

def plot_blocks_gt3_overall_vs_block_facet(
    df: pd.DataFrame,
    variableOfInterest: str,
    yLabel: str,
    blocks_per_facet: int = 10,
    voi_str: str = "Round Elapsed Time",
    voi_UnitStr: str = "(s)",
):
    req = ["BlockNum", "trueSession_elapsed_s", variableOfInterest, "coinLabel", "dropQual", "BlockStatus"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Blocks>3 plot: {missing}")

    # keep optional ID columns for subtitle if present
    opt_cols = [c for c in ("participantID", "pairID", "coinSet") if c in df.columns]

    sub = df[
        (pd.to_numeric(df["BlockNum"], errors="coerce").notna())
        & (df["BlockNum"] > 3)
        & (df["BlockStatus"] == "complete")
        & df[variableOfInterest].notna()
        & df["trueSession_elapsed_s"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good","bad"])
    ][["BlockNum", variableOfInterest, "trueSession_elapsed_s", "coinLabel", "dropQual", *opt_cols]].copy()

    # integer block numbers for binning
    sub["BlockNum"] = pd.to_numeric(sub["BlockNum"], errors="coerce").astype(int)
    sub["facet_idx"] = (sub["BlockNum"] - 1) // blocks_per_facet

    # one figure per facet (Blocks 1–N, N+1–2N, …)
    for idx, g in sub.groupby("facet_idx", sort=True):
        start = idx * blocks_per_facet + 1
        end   = (idx + 1) * blocks_per_facet

        fig, ax = plt.subplots(figsize=(12, 6))
        for _, r in g.iterrows():
            scatter_point(ax, r["trueSession_elapsed_s"], r[variableOfInterest], r["coinLabel"], r["dropQual"])

        # Title & subtitle (use the current facet’s data for subtitle)
        title_string = f"{voi_str} — Test Phase 2 Blocks {start}–{end} by Coin Type"
        subtitle_string = _subtitle_from(g) if "_subtitle_from" in globals() else ""
        ax.set_title(title_string, fontsize=14, y=1.055)
        if subtitle_string:
            ax.text(0.5, 1.02, subtitle_string, ha="center", fontsize=11, transform=ax.transAxes)

        # Axes labels
        ax.set_xlabel(f"Overall Session Elapsed Time {voi_UnitStr}")
        ax.set_ylabel(yLabel)
        ax.set_xlim(left=0)

        add_legends(ax)
        fig.tight_layout()
        plt.show()


def plot_hist_kde_by_coin(
    df: pd.DataFrame,
    *,
    variableOfInterest: str = "truecontent_elapsed_s",
    voi_str: str = "Round Elapsed Time",
    voi_UnitStr: str = "(s)",
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
    hspace: float = 0.40,
    height_ratios: tuple[int, int] = (6, 1),
    main_labelpad: float = 6.0,
    dots_top_pad: float = 4.0,
):
    req = ["BlockNum", "BlockStatus", variableOfInterest, "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for {variableOfInterest} plot: {missing}")

    sdf = df.copy()
    sdf[variableOfInterest] = pd.to_numeric(sdf[variableOfInterest], errors="coerce")
    filt = (
        (sdf["BlockNum"] > blocks_min)
        & (sdf["BlockStatus"] == "complete")
        & sdf["coinLabel"].notna()
        & sdf["dropQual"].isin(["good", "bad"])
        & sdf[variableOfInterest].notna()
    )

    opt_cols = [c for c in ("participantID", "pairID", "coinSet") if c in sdf.columns]
    dat = sdf.loc[filt, [variableOfInterest, "coinLabel", *opt_cols]].reset_index(drop=True)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    hue_order = sorted(dat["coinLabel"].unique().tolist())
    if isinstance(palette, dict):
        color_map = {k: palette.get(k, "#333333") for k in hue_order}
    else:
        color_map = dict(zip(hue_order, sns.color_palette(palette, n_colors=len(hue_order))))

    sns.set_theme(style="whitegrid")

    fig, ax, ax_dots = make_axes_with_dots(
        dot_mode=dot_mode,
        height_ratios=height_ratios,
        hspace=hspace,
    )

    sns.histplot(
        data=dat, x=variableOfInterest, hue="coinLabel", hue_order=hue_order,
        bins=bins, stat=stat, common_norm=common_norm, element="step",
        alpha=0.35, multiple="layer", ax=ax, palette=color_map, legend=True,
    )
    sns.kdeplot(
        data=dat, x=variableOfInterest, hue="coinLabel", hue_order=hue_order,
        common_norm=common_norm, ax=ax, palette=color_map, lw=2, legend=False,
    )

    if dot_mode == "panel" and ax_dots is not None:
        draw_dots_strip(
            ax_dots, dat,
            x_col=variableOfInterest, group_col="coinLabel", color_map=color_map,
            dot_size=dot_size, dot_alpha=dot_alpha, dot_jitter=dot_jitter,
            max_points_per_group=max_points_per_group,
        )

    title_string = f"{voi_str} Distribution (Blocks > {blocks_min}) by Coin Type"
    subtitle_string = _subtitle_from(dat) if "_subtitle_from" in globals() else ""
    ax.set_title(title_string, fontsize=14, y=1.055)
    if subtitle_string:
        ax.text(0.5, 1.02, subtitle_string, ha="center", fontsize=11, transform=ax.transAxes)

    style_x_for_main_and_dots(
        ax, ax_dots,
        main_xlabel=variableOfInterest,
        dots_label_top=f"{voi_str} {voi_UnitStr}\n\nObserved points (jittered)",
        main_labelpad=main_labelpad,
        dots_top_pad=dots_top_pad,
        dots_frame=False,
        despine=True,
    )
    if ax_dots is not None:
        ax.xaxis.set_label_coords(0.5, -0.08)

    ax.set_ylabel("Density" if stat == "density" else stat.capitalize())
    ax.set_xlim(left=0)

    plt.tight_layout()
    plt.show()
