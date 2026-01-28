from __future__ import annotations
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from histoHelpers import (
    _slugify, make_axes_with_dots, draw_dots_strip, style_x_for_main_and_dots, annotate_hist_bins, compute_fixed_bin_edges, coerce_xlim,
)

def plot_histkde_allsubjects(
    df: pd.DataFrame,
    *,
    variableOfInterest: str,
    voi_str: str = "Measure",
    voi_unit: str = "",
    bins: int | str = "auto",
    bin_width: float | None = None,
    stat: str = "density",
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
    title_prefix: str = "",
    show_bin_stats: bool = True,
    fix_xlim: bool = False,
    xlim: tuple[float, float] | None = None,
):
    """Histogram + KDE of VOI across all subjects, hue=coinLabel, optional bottom dot strip."""
    req = [variableOfInterest, "coinLabel"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for histKDE: {missing}")

    sdf = df.copy()
    sdf[variableOfInterest] = pd.to_numeric(sdf[variableOfInterest], errors="coerce")
    # be permissive: if dropQual is present, keep only good/bad; otherwise ignore
    mask = sdf[variableOfInterest].notna() & sdf["coinLabel"].notna()
    if "dropQual" in sdf.columns:
        mask &= sdf["dropQual"].astype(str).str.lower().isin(["good", "bad"])
    dat = sdf.loc[mask, [variableOfInterest, "coinLabel"]].reset_index(drop=True)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    # Decide on global edges (if requested) and x-limits
    edges = None
    if bin_width is not None or fix_xlim or xlim is not None:
        edges = compute_fixed_bin_edges(dat, x=variableOfInterest, bin_width=bin_width)
        bins_arg = edges
    else:
        bins_arg = bins

    #fig, (ax, axdots) = make_axes_with_dots(hspace=hspace, height_ratios=height_ratios)
    hue_order = sorted(dat["coinLabel"].unique().tolist())
    if isinstance(palette, dict):
        color_map = {k: palette.get(k, "#333333") for k in hue_order}
    else:
        color_map = dict(zip(hue_order, sns.color_palette(palette, n_colors=len(hue_order))))

    sns.set_theme(style="whitegrid")

    fig, ax, ax_dots = make_axes_with_dots(dot_mode=dot_mode, height_ratios=height_ratios, hspace=hspace)

    sns.histplot(
        data=dat, x=variableOfInterest, hue="coinLabel", hue_order=hue_order,
        bins=bins_arg,  # use the computed edges/setting
        stat=stat, common_norm=False, element="step",
        alpha=0.35, multiple="layer", ax=ax, palette=color_map, legend=True,
    )

    # --- NEW: annotate bins & stats ---
    if show_bin_stats:
        annotate_hist_bins(ax, dat, x=variableOfInterest, bins=bins_arg, hue="coinLabel", stat=stat)

    sns.kdeplot(
        data=dat, x=variableOfInterest, hue="coinLabel", hue_order=hue_order,
        common_norm=False, ax=ax, palette=color_map, lw=2, legend=False,
    )

    # Fix x-limits if requested
    xl = coerce_xlim(xlim, edges) if fix_xlim or xlim is not None else None
    if xl:
        ax.set_xlim(xl)

    if dot_mode == "panel" and ax_dots is not None:
        draw_dots_strip(
            ax_dots, dat,
            x_col=variableOfInterest, group_col="coinLabel", color_map=color_map,
            dot_size=dot_size, dot_alpha=dot_alpha, dot_jitter=dot_jitter,
            max_points_per_group=max_points_per_group,
        )

    title_bits = [t for t in [title_prefix.strip(), f"{voi_str} Distribution by Coin Type"] if t]
    ax.set_title(" — ".join(title_bits), fontsize=14, y=1.055)

    style_x_for_main_and_dots(
        ax, ax_dots,
        main_xlabel=variableOfInterest,  # keep the raw column label here
        dots_label_top=f"{voi_str} {voi_unit}".strip(),
        main_labelpad=main_labelpad,
        dots_top_pad=dots_top_pad,
        dots_frame=False,
        despine=True,
    )
    if ax_dots is not None:
        ax.xaxis.set_label_coords(0.5, -0.08)

    ax.set_ylabel("Density" if stat == "density" else stat.capitalize())
    ax.set_xlim(left=0)

    fig.tight_layout()  # use fig-level to avoid the warning
    plt.show()

def plot_violin_allsubjects(
    df: pd.DataFrame,
    *,
    variableOfInterest: str,
    voi_str: str = "Measure",
    voi_unit: str = "",
    title_prefix: str = "",
):
    """Violin plot of VOI across all subjects, split by coinLabel."""
    req = [variableOfInterest, "coinLabel"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for violin: {missing}")

    sdf = df.copy()
    sdf[variableOfInterest] = pd.to_numeric(sdf[variableOfInterest], errors="coerce")

    # gate by dropQual if present, same as histKDE
    mask = sdf[variableOfInterest].notna() & sdf["coinLabel"].notna()
    if "dropQual" in sdf.columns:
        mask &= sdf["dropQual"].astype(str).str.lower().isin(["good", "bad"])

    dat = sdf.loc[mask, [variableOfInterest, "coinLabel"]].reset_index(drop=True)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    hue_order = sorted(dat["coinLabel"].unique().tolist())
    # re-use palette logic from histKDE
    color_map = dict(zip(hue_order, sns.color_palette("tab10", n_colors=len(hue_order))))

    sns.set_theme(style="whitegrid")

    fig, ax = plt.subplots(figsize=(10, 6))

    # main violins
    sns.violinplot(
        data=dat,
        x="coinLabel",
        y=variableOfInterest,
        order=hue_order,
        palette=color_map,
        #cut=0,
        inner="quartile",
        ax=ax,
    )

    # optional jittered points on top (helps show N / spread)
    sns.stripplot(
        data=dat,
        x="coinLabel",
        y=variableOfInterest,
        order=hue_order,
        color="0.1",
        jitter=0.3,
        # size=2,
        alpha=0.3,
        dodge=True,
        ax=ax,
    )

    title_bits = [t for t in [title_prefix.strip(), f"{voi_str} by Coin Type (violin)"] if t]
    ax.set_title(" — ".join(title_bits), fontsize=14, y=1.03)

    ax.set_xlabel("Coin Type")
    ax.set_ylabel(f"{voi_str} {voi_unit}".strip())

    fig.tight_layout()
    plt.show()

def plot_tp2_scatter_allsubjects(
    df: pd.DataFrame,
    *,
    variableOfInterest: str,
    voi_str: str = "Measure",
    voi_unit: str = "",
    title_prefix: str = "",
):
    """Scatter of VOI vs trueSession_elapsed_s across all subjects (hue=coinLabel)."""
    req = [variableOfInterest, "coinLabel", "trueSession_elapsed_s"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for TP2 scatter: {missing}")

    sdf = df.copy()
    sdf[variableOfInterest] = pd.to_numeric(sdf[variableOfInterest], errors="coerce")
    sdf["trueSession_elapsed_s"] = pd.to_numeric(sdf["trueSession_elapsed_s"], errors="coerce")
    # gate by dropQual if present
    mask = sdf[variableOfInterest].notna() & sdf["trueSession_elapsed_s"].notna() & sdf["coinLabel"].notna()
    if "dropQual" in sdf.columns:
        mask &= sdf["dropQual"].astype(str).str.lower().isin(["good", "bad"])
    dat = sdf.loc[mask, ["trueSession_elapsed_s", variableOfInterest, "coinLabel"]].reset_index(drop=True)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    hue_order = sorted(dat["coinLabel"].unique().tolist())
    palette = dict(zip(hue_order, sns.color_palette("tab10", n_colors=len(hue_order))))
    sns.set_theme(style="whitegrid")

    fig, ax = plt.subplots(figsize=(12, 6))
    for k, sub in dat.groupby("coinLabel", sort=True):
        ax.scatter(sub["trueSession_elapsed_s"], sub[variableOfInterest], s=18, alpha=0.5, label=k)

    title_bits = [t for t in [title_prefix.strip(), f"{voi_str} vs Overall Session Elapsed Time"] if t]
    ax.set_title(" — ".join(title_bits), fontsize=14)
    ax.set_xlabel(f"Overall Session Elapsed Time (s)")
    ax.set_ylabel(f"{voi_str} {voi_unit}".strip())
    ax.legend(title="Coin Type")
    fig.tight_layout()
    plt.show()



def plot_pinDrop_block3_lines_by_round(
    df: pd.DataFrame,
    variableOfInterest: str = "dropDist",
    yLabel: str = "Pin Drop Distance (m)",
    title: str = "Pin Drop Distance (Block 3, by Round)",
    ylim: float | None = None,
    exclude_outliers: bool = False,
    outlier_z: float = 3.0,
):
    """
    Block 3 only: mean ±1 SD of pin-drop distance by RoundNum,
    one line per coinLabel (HV/LV/NV).
    """
    req = ["BlockNum", "RoundNum", "coinLabel", "dropQual", "BlockStatus", variableOfInterest]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Block 3 line plot: {missing}")

    sdf = df.copy()
    sdf["BlockNum"] = pd.to_numeric(sdf["BlockNum"], errors="coerce")
    sdf["RoundNum"] = pd.to_numeric(sdf["RoundNum"], errors="coerce")
    sdf[variableOfInterest] = pd.to_numeric(sdf[variableOfInterest], errors="coerce")

    mask = (
        (sdf["BlockNum"] == 3)
        & (sdf["BlockStatus"] == "complete")
        & sdf["RoundNum"].notna()
        & sdf[variableOfInterest].notna()
        & sdf["coinLabel"].notna()
        & sdf["dropQual"].isin(["good", "bad"])
    )
    sub = sdf.loc[mask, ["RoundNum", "coinLabel", variableOfInterest]].copy()
    if sub.empty:
        raise ValueError("No data left after filtering for Block 3.")

    if exclude_outliers:
        def _drop_outliers(group: pd.DataFrame) -> pd.DataFrame:
            s = group[variableOfInterest]
            mu = s.mean()
            sigma = s.std(ddof=0)
            if sigma == 0 or np.isnan(sigma):
                return group
            z = (s - mu) / sigma
            return group[np.abs(z) <= outlier_z]

        sub = sub.groupby("coinLabel", group_keys=False).apply(_drop_outliers)
        if sub.empty:
            raise ValueError("All data removed as outliers for Block 3.")

    stats = (
        sub
        .groupby(["RoundNum", "coinLabel"], as_index=False)[variableOfInterest]
        .agg(["mean", "std", "count"])
        .reset_index()
    )

    coin_order = sorted(stats["coinLabel"].unique().tolist())
    palette = dict(zip(coin_order, sns.color_palette("tab10", n_colors=len(coin_order))))

    fig, ax = plt.subplots(figsize=(10, 6))

    for coin in coin_order:
        g = stats[stats["coinLabel"] == coin].sort_values("RoundNum")
        x = g["RoundNum"].to_numpy()
        m = g["mean"].to_numpy()
        s = g["std"].to_numpy()

        ax.plot(x, m, label=coin, linewidth=2, color=palette[coin])

        lo = m - s
        hi = m + s
        ax.fill_between(x, lo, hi, color=palette[coin], alpha=0.2)

    ax.set_title(title, fontsize=14)
    ax.set_xlabel("Round #")
    ax.set_ylabel(yLabel)
    ax.grid(True, alpha=0.3)
    ax.legend(title="Coin Type")
    if ylim is not None:
        ax.set_ylim(0, ylim)
    fig.tight_layout()
    plt.show()


def plot_pinDrop_blocks_lines_by_block(
    df: pd.DataFrame,
    variableOfInterest: str = "dropDist",
    yLabel: str = "Pin Drop Distance (m)",
    block_min: int = 4,
    block_max: int = 24,
    title: str | None = None,
    ylim: float | None = None,
    exclude_outliers: bool = False,
    outlier_z: float = 3.0,
):
    """
    Blocks [block_min, block_max]: mean ±1 SD of pin-drop distance by BlockNum,
    one line per coinLabel (HV/LV/NV).
    """
    if title is None:
        title = f"Pin Drop Distance (Blocks {block_min}–{block_max}, by Block)"

    req = ["BlockNum", "coinLabel", "dropQual", "BlockStatus", variableOfInterest]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for TP2 line plot: {missing}")

    sdf = df.copy()
    sdf["BlockNum"] = pd.to_numeric(sdf["BlockNum"], errors="coerce")
    sdf[variableOfInterest] = pd.to_numeric(sdf[variableOfInterest], errors="coerce")

    mask = (
        sdf["BlockNum"].between(block_min, block_max, inclusive="both")
        & (sdf["BlockStatus"] == "complete")
        & sdf[variableOfInterest].notna()
        & sdf["coinLabel"].notna()
        & sdf["dropQual"].isin(["good", "bad"])
    )
    sub = sdf.loc[mask, ["BlockNum", "coinLabel", variableOfInterest]].copy()
    if sub.empty:
        raise ValueError(f"No data left after filtering for blocks {block_min}–{block_max}.")

    sub["BlockNum"] = sub["BlockNum"].astype(int)

    if exclude_outliers:
        def _drop_outliers(group: pd.DataFrame) -> pd.DataFrame:
            s = group[variableOfInterest]
            mu = s.mean()
            sigma = s.std(ddof=0)
            if sigma == 0 or np.isnan(sigma):
                return group
            z = (s - mu) / sigma
            return group[np.abs(z) <= outlier_z]

        sub = sub.groupby("coinLabel", group_keys=False).apply(_drop_outliers)
        if sub.empty:
            raise ValueError(f"All data removed as outliers for blocks {block_min}–{block_max}.")

    stats = (
        sub
        .groupby(["BlockNum", "coinLabel"], as_index=False)[variableOfInterest]
        .agg(["mean", "std", "count"])
        .reset_index()
    )

    coin_order = sorted(stats["coinLabel"].unique().tolist())
    palette = dict(zip(coin_order, sns.color_palette("tab10", n_colors=len(coin_order))))

    fig, ax = plt.subplots(figsize=(10, 6))

    for coin in coin_order:
        g = stats[stats["coinLabel"] == coin].sort_values("BlockNum")
        x = g["BlockNum"].to_numpy()
        m = g["mean"].to_numpy()
        s = g["std"].to_numpy()

        ax.plot(x, m, label=coin, linewidth=2, color=palette[coin])

        lo = m - s
        hi = m + s
        ax.fill_between(x, lo, hi, color=palette[coin], alpha=0.2)

    ax.set_title(title, fontsize=14)
    ax.set_xlabel("Block #")
    ax.set_ylabel(yLabel)
    ax.grid(True, alpha=0.3)
    ax.legend(title="Coin Type")
    if ylim is not None:
        ax.set_ylim(0, ylim)
    fig.tight_layout()
    plt.show()
