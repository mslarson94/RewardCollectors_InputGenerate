# ============================================================
# End-to-end plotting script (matplotlib-only, no seaborn)
# - Computes corrected elapsed times if TrueSessionElapsedTime exists
# - Reproduces the two key plots with your styling choices
#     1) Block 3: X=BlockElapsedTime, Y=RoundElapsedTime (≤200s), color=dropQual, shape=coinLabel
#     2) Blocks > 3: X=mLTimestamp, Y=BlockElapsedTime, color=dropQual, shape=coinLabel
# ============================================================

# === Add this block to your script (wrappers + small fixes) ===
from __future__ import annotations
from pathlib import Path
from typing import Iterable, Sequence, Callable, Any
import re, glob
import contextlib

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


from itertools import combinations
from collections import defaultdict

from scipy.stats import (
    anderson_ksamp,       # k-sample Anderson–Darling (omnibus, distributional)
    kruskal,              # Kruskal–Wallis (omnibus, location shift)
    ks_2samp,             # Kolmogorov–Smirnov (pairwise, distributional)
    mannwhitneyu,         # for effect size via A12 / Cliff's delta
)

# If you use seaborn in plot_hist_kde_by_coin
import seaborn as sns  # noqa

from matplotlib.lines import Line2D

# -------------------------------
# Configuration
# -------------------------------
CSV_PATH = Path("/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/Merged_PtRoleCoinSet_Flat_csv/R037_03_17_2025_AN_B_ML2G_main_R037_AN_B_events.csv")
#CSV_PATH = Path("/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/PinDrops_All/PinDrops_ALL.csv")
# Marker/Color conventions (explicitly requested)
MARKER_BY_COIN = {"HV": "*", "LV": "o", "NV": "o"}       # NV will be hollow
FILLED_BY_COIN = {"HV": True, "LV": True, "NV": False}
COLOR_BY_QUAL  = {"good": "blue", "bad": "red"}          # else -> gray
ALPHA = 0.5
SIZE  = 100

# -------------------------------
# Helpers
# -------------------------------
def read_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Normalize coin label & drop quality text
    if "coinLabel" in df.columns:
        df["coinLabel"] = df["coinLabel"].astype(str).str.strip()
    if "dropQual" in df.columns:
        df["dropQual"] = df["dropQual"].astype(str).str.strip().str.lower()
    # Parse session timestamp for overall-time plots
    if "mLTimestamp" in df.columns:
        df["mLTimestamp"] = pd.to_datetime(df["mLTimestamp"], errors="coerce")
    # Numeric-ize block/round
    for c in ("BlockNum", "RoundNum"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def scatter_point(ax, x, y, coin, qual, alpha=ALPHA, size=SIZE):
    marker = MARKER_BY_COIN.get(coin, "o")
    filled = FILLED_BY_COIN.get(coin, True)
    color  = COLOR_BY_QUAL.get(qual, "gray")
    if filled:
        ax.scatter(x, y, marker=marker, s=size, alpha=alpha,
                   facecolors=color, edgecolors=color, linewidth=1)
    else:
        ax.scatter(x, y, marker=marker, s=size, alpha=alpha,
                   facecolors="none", edgecolors=color, linewidth=1)

def add_legends(ax):
    # Coin type legend (shapes)
    coin_handles = [
        Line2D([0], [0], marker="*", linestyle="None", markersize=10,
               markerfacecolor="none", markeredgecolor="black", label="HV (star)"),
        Line2D([0], [0], marker="o", linestyle="None", markersize=10,
               markerfacecolor="black", markeredgecolor="black", label="LV (filled circle)"),
        Line2D([0], [0], marker="o", linestyle="None", markersize=10,
               markerfacecolor="none", markeredgecolor="black", label="NV (hollow circle)"),
    ]
    leg1 = ax.legend(handles=coin_handles, title="Coin Type", loc="upper left")
    ax.add_artist(leg1)

    # Drop quality legend (colors)
    qual_handles = [
        Line2D([0], [0], marker="s", linestyle="None", markersize=10,
               markerfacecolor=COLOR_BY_QUAL["good"], markeredgecolor=COLOR_BY_QUAL["good"], label="good"),
        Line2D([0], [0], marker="s", linestyle="None", markersize=10,
               markerfacecolor=COLOR_BY_QUAL["bad"], markeredgecolor=COLOR_BY_QUAL["bad"], label="bad"),
    ]
    ax.legend(handles=qual_handles, title="Drop Quality", loc="upper right")

def _subtitle_from(frame: pd.DataFrame) -> str:
    parts = []
    for label, col in (("Pair", "pairID"),
                       ("Participant", "participantID"),
                       ("Coin Set", "coinSet")):
        if col in frame.columns:
            vals = pd.unique(frame[col].dropna())
            if len(vals) == 1:
                parts.append(f"{label} {vals[0]}")
            elif len(vals) > 1:
                parts.append(f"{label} {len(vals)} values")
    return " | ".join(parts)


# === PATCH: add just below your imports ===
def _opt_mask(df: pd.DataFrame, col: str, *, values: Iterable[str] | None = None) -> pd.Series:
    """If `col` exists, return a mask for (values or non-NaN); else all True."""
    if col not in df.columns:
        return pd.Series(True, index=df.index)
    s = df[col]
    if values is None:
        return s.notna()
    s_str = s.astype(str).str.lower()
    vals = [str(v).lower() for v in values]
    return s_str.isin(vals)

def _safe_count(df: pd.DataFrame, mask: pd.Series) -> int:
    try:
        return int(mask.sum())
    except Exception:
        return 0


# -------------------------------
# Plot 2: Blocks > 3 — X=variableOfInterest, Y=BlockElapsed
# -------------------------------

def plot_blocks_gt3_overall_vs_block_facet(df: pd.DataFrame, variableOfInterest: str, yLabel: str, blocks_per_facet: int = 10):
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

    # integer block numbers for binning
    sub["BlockNum"] = pd.to_numeric(sub["BlockNum"], errors="coerce").astype(int)

    # facet index: 1–blocks_per_facet => 0, (blocks_per_facet+1)–2*blocks_per_facet => 1, etc.
    sub["facet_idx"] = (sub["BlockNum"] - 1) // blocks_per_facet

    # one figure per facet (Blocks 1–N, N+1–2N, …)
    for idx, g in sub.groupby("facet_idx", sort=True):
        start = idx * blocks_per_facet + 1
        end = (idx + 1) * blocks_per_facet

        fig, ax = plt.subplots(figsize=(12, 6))
        for _, r in g.iterrows():
            scatter_point(ax, r["trueSession_elapsed_s"], r[variableOfInterest], r["coinLabel"], r["dropQual"])

        ax.set_title(f"{variableOfInterest} — Blocks {start}–{end} by Coin Type and Drop Quality")
        ax.set_xlabel("Overall Time (trueSession_elapsed_s)")
        ax.set_ylabel(yLabel)
        add_legends(ax)
        fig.tight_layout()
        plt.show()



def plot_hist_kde_by_coin_v1(
    df: pd.DataFrame,
    *,
    variableOfInterest: str = "truecontent_elapsed_s",
    blocks_min: int = 3,
    bins: int | str = "auto",
    stat: str = "density",
    common_norm: bool = False,
    palette: str | dict = "tab10",
    # dot options
    dot_mode: str = "panel",        # "panel" (separate axis) or "baseline" (thin band on main axis)
    dot_alpha: float = 0.6,
    dot_size: float = 12,
    dot_jitter: float = 0.15,       # vertical jitter for "panel" (in axis units) or fraction of y-range for "baseline"
    max_points_per_group: int | None = 4000,
):
    """
    dot_mode:
      - "panel": draw a small strip of dots on a second axis below the density plot (most readable).
      - "baseline": draw dots in a very thin band near y=0 on the main axis.
    """
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

    if dot_mode == "panel":
        # Two stacked axes, shared x
        fig, (ax, ax_dots) = plt.subplots(
            2, 1, figsize=(12, 7),
            gridspec_kw={"height_ratios": [6, 1], "hspace": 0.05},
            sharex=True
        )
    else:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax_dots = None

    # Histogram
    sns.histplot(
        data=dat,
        x=variableOfInterest,
        hue="coinLabel",
        hue_order=hue_order,
        bins=bins,
        stat=stat,              # "count", "frequency", "probability", or "density"
        common_norm=common_norm,
        element="step",
        alpha=0.35,
        multiple="layer",
        ax=ax,
        palette=color_map,
        legend=True,
    )

    # KDE overlays
    sns.kdeplot(
        data=dat,
        x=variableOfInterest,
        hue="coinLabel",
        hue_order=hue_order,
        common_norm=common_norm,
        ax=ax,
        palette=color_map,
        lw=2,
        legend=False,
    )

    # # === Real data points ===
    # rng = np.random.default_rng(0)
    # if dot_mode == "panel":
    #     ax_dots.set_ylim(-0.5, 0.5)
    #     ax_dots.axis("off")
    #     # one horizontal strip; jitter vertically for overplot reduction
    #     for coin, sub in dat.groupby("coinLabel", sort=False):
    #         x = sub[variableOfInterest].to_numpy()
    #         if max_points_per_group is not None and len(x) > max_points_per_group:
    #             x = rng.choice(x, size=max_points_per_group, replace=False)
    #         y = rng.uniform(-dot_jitter, dot_jitter, size=len(x))
    #         ax_dots.scatter(
    #             x, y,
    #             s=dot_size,
    #             alpha=dot_alpha,
    #             color=color_map[coin],
    #             edgecolors="white",
    #             linewidths=0.4,
    #         )
    # === Real data points ===
    rng = np.random.default_rng(0)
    if dot_mode == "panel":
        ax_dots.set_ylim(-0.5, 0.5)

        # KEEP the bottom axis visible for ticks/label; hide only y stuff
        ax_dots.set_yticks([])
        ax_dots.set_ylabel("")
        ax_dots.grid(False)
        for s in ("top", "left", "right"):
            ax_dots.spines[s].set_visible(False)
        ax_dots.tick_params(axis="x", which="both", labelbottom=True)
        ax.tick_params(axis="x", which="both", labelbottom=False)

        # one horizontal strip; jitter vertically for overplot reduction
        for coin, sub in dat.groupby("coinLabel", sort=False):
            x = sub[variableOfInterest].to_numpy()
            if max_points_per_group is not None and len(x) > max_points_per_group:
                x = rng.choice(x, size=max_points_per_group, replace=False)
            y = rng.uniform(-dot_jitter, dot_jitter, size=len(x))
            ax_dots.scatter(
                x, y,
                s=dot_size,
                alpha=dot_alpha,
                color=color_map[coin],
                edgecolors="white",
                linewidths=0.4,
            )
    elif dot_mode == "baseline":
        # dots in a thin band near y=0 on the main axis
        y0, y1 = ax.get_ylim()
        band = (y1 - y0) * (dot_jitter if dot_jitter > 0 else 0.04)
        for coin, sub in dat.groupby("coinLabel", sort=False):
            x = sub[variableOfInterest].to_numpy()
            if max_points_per_group is not None and len(x) > max_points_per_group:
                x = rng.choice(x, size=max_points_per_group, replace=False)
            y = rng.uniform(y0, y0 + band, size=len(x))
            ax.scatter(
                x, y,
                s=dot_size,
                alpha=dot_alpha,
                color=color_map[coin],
                edgecolors="white",
                linewidths=0.4,
                zorder=4,
            )
        ax.set_ylim(y0, y1)

    #ax.set_title(f"{variableOfInterest} Distribution (Blocks > {blocks_min}) by Coin Type")
    title_string = (f"{variableOfInterest} Distribution (Blocks > {blocks_min}) by Coin Type")
    subtitle_string = _subtitle_from(dat)
    ax.set_title(title_string, fontsize=12, y=1.055)
    if subtitle_string:
        ax.text(0.5, 1.02, subtitle_string, ha="center", fontsize=9, transform=ax.transAxes)
    ax.set_title(title_string, fontsize=12, y=1.055)
    
    ax.set_xlabel(f"{variableOfInterest}")
    ax.set_ylabel("Density" if stat == "density" else stat.capitalize())
    ax.set_xlim(left=0)
    sns.despine(ax=ax)
    if ax_dots is not None:
        sns.despine(ax=ax_dots, left=True, bottom=True)
    fig.tight_layout()
    plt.show()


def plot_hist_kde_by_coin_v2(
    df: pd.DataFrame,
    *,
    variableOfInterest: str = "truecontent_elapsed_s",
    blocks_min: int = 3,
    bins: int | str = "auto",
    stat: str = "density",
    common_norm: bool = False,
    palette: str | dict = "tab10",
    # dot options
    dot_mode: str = "panel",        # "panel" (separate axis) or "baseline" (thin band on main axis)
    dot_alpha: float = 0.6,
    dot_size: float = 12,
    dot_jitter: float = 0.15,       # vertical jitter for "panel" (axis units) or fraction of y-range for "baseline"
    max_points_per_group: int | None = 4000,
):
    """
    dot_mode:
      - "panel": draw a small strip of dots on a second axis below the density plot (most readable).
      - "baseline": draw dots in a very thin band near y=0 on the main axis.
    """
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

    # keep optional id columns if present
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

    if dot_mode == "panel":
        # Two stacked axes, shared x
        fig, (ax, ax_dots) = plt.subplots(
            2, 1, figsize=(12, 7),
            gridspec_kw={"height_ratios": [6, 1], "hspace": 0.05},
            sharex=True
        )
    else:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax_dots = None

    # Histogram
    sns.histplot(
        data=dat,
        x=variableOfInterest,
        hue="coinLabel",
        hue_order=hue_order,
        bins=bins,
        stat=stat,              # "count", "frequency", "probability", or "density"
        common_norm=common_norm,
        element="step",
        alpha=0.35,
        multiple="layer",
        ax=ax,
        palette=color_map,
        legend=True,
    )

    # KDE overlays
    sns.kdeplot(
        data=dat,
        x=variableOfInterest,
        hue="coinLabel",
        hue_order=hue_order,
        common_norm=common_norm,
        ax=ax,
        palette=color_map,
        lw=2,
        legend=False,
    )

    # === Real data points ===
    rng = np.random.default_rng(0)
    if dot_mode == "panel":
        ax_dots.set_ylim(-0.5, 0.5)
        # Keep bottom axis visible for ticks/label; hide only y stuff
        ax_dots.set_yticks([])
        ax_dots.set_ylabel("")
        ax_dots.grid(False)
        for s in ("top", "left", "right"):
            ax_dots.spines[s].set_visible(False)
        ax.tick_params(axis="x", which="both", labelbottom=False)
        ax_dots.tick_params(axis="x", which="both", labelbottom=True)

        # one horizontal strip; jitter vertically for overplot reduction
        for coin, sub in dat.groupby("coinLabel", sort=False):
            x = sub[variableOfInterest].to_numpy()
            if max_points_per_group is not None and len(x) > max_points_per_group:
                x = rng.choice(x, size=max_points_per_group, replace=False)
            y = rng.uniform(-dot_jitter, dot_jitter, size=len(x))
            ax_dots.scatter(
                x, y,
                s=dot_size,
                alpha=dot_alpha,
                color=color_map[coin],
                edgecolors="white",
                linewidths=0.4,
            )

    elif dot_mode == "baseline":
        # dots in a thin band near y=0 on the main axis
        y0, y1 = ax.get_ylim()
        band = (y1 - y0) * (dot_jitter if dot_jitter > 0 else 0.04)
        for coin, sub in dat.groupby("coinLabel", sort=False):
            x = sub[variableOfInterest].to_numpy()
            if max_points_per_group is not None and len(x) > max_points_per_group:
                x = rng.choice(x, size=max_points_per_group, replace=False)
            y = rng.uniform(y0, y0 + band, size=len(x))
            ax.scatter(
                x, y,
                s=dot_size,
                alpha=dot_alpha,
                color=color_map[coin],
                edgecolors="white",
                linewidths=0.4,
                zorder=4,
            )
        ax.set_ylim(y0, y1)

    # Titles/subtitle
    title_string = f"{variableOfInterest} Distribution (Blocks > {blocks_min}) by Coin Type"
    subtitle_string = _subtitle_from(dat) if "_subtitle_from" in globals() else ""
    ax.set_title(title_string, fontsize=12, y=1.055)
    if subtitle_string:
        ax.text(0.5, 1.02, subtitle_string, ha="center", fontsize=9, transform=ax.transAxes)

    # X label & ticks: put them on the bottom (dots) axis if panel mode
    if dot_mode == "panel":
        ax.set_xlabel("")
        ax_dots.set_xlabel(variableOfInterest)          # uses the column name directly
        # keep bottom spine so ticks render
        sns.despine(ax=ax_dots, left=True)              # NOTE: do NOT pass bottom=True
    else:
        ax.set_xlabel(variableOfInterest)

    ax.set_ylabel("Density" if stat == "density" else stat.capitalize())
    ax.set_xlim(left=0)
    sns.despine(ax=ax)
    fig.tight_layout()
    plt.show()


def plot_hist_kde_by_coin_v3(
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
    # dot options
    dot_mode: str = "panel",        # "panel" or "baseline"
    dot_alpha: float = 0.6,
    dot_size: float = 12,
    dot_jitter: float = 0.15,
    max_points_per_group: int | None = 4000,
):
    """
    Request: x-axis ticks+label attached to MAIN density axis,
             and a separate label for the lower dots panel.

    TUNE SPACING HERE:
      - HSPACE controls the vertical gap between the main axis and the dots panel.
      - MAIN_X_PAD moves the main x label down/up.
      - DOTS_TOP_PAD moves the dots panel's top label up/down.
    """
    HSPACE       = 0.5   # <--- increase if labels touch/overlap
    MAIN_X_PAD   = 2      # <--- pad for main axis x-label (points)
    DOTS_TOP_PAD = 4     # <--- pad for dots panel top label (points)

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

    # keep optional id columns if present (for subtitle)
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

    if dot_mode == "panel":
        # Two stacked axes, shared x
        fig, (ax, ax_dots) = plt.subplots(
            2, 1, figsize=(12, 7),
            gridspec_kw={"height_ratios": [6, 1], "hspace": HSPACE},  # <--- adjust HSPACE above
            sharex=True
        )
    else:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax_dots = None

    # Histogram
    sns.histplot(
        data=dat,
        x=variableOfInterest,
        hue="coinLabel",
        hue_order=hue_order,
        bins=bins,
        stat=stat,
        common_norm=common_norm,
        element="step",
        alpha=0.35,
        multiple="layer",
        ax=ax,
        palette=color_map,
        legend=True,
    )

    # KDE overlays
    sns.kdeplot(
        data=dat,
        x=variableOfInterest,
        hue="coinLabel",
        hue_order=hue_order,
        common_norm=common_norm,
        ax=ax,
        palette=color_map,
        lw=2,
        legend=False,
    )

    # === Real data points ===
    rng = np.random.default_rng(0)
    if dot_mode == "panel":
        ax_dots.set_ylim(-0.5, 0.5)
        # minimalist dots strip
        ax_dots.set_yticks([]); ax_dots.set_ylabel(""); ax_dots.grid(False)
        for s in ("top", "left", "right"):
            ax_dots.spines[s].set_visible(False)

        for coin, sub in dat.groupby("coinLabel", sort=False):
            x = sub[variableOfInterest].to_numpy()
            if max_points_per_group is not None and len(x) > max_points_per_group:
                x = rng.choice(x, size=max_points_per_group, replace=False)
            y = rng.uniform(-dot_jitter, dot_jitter, size=len(x))
            ax_dots.scatter(
                x, y, s=dot_size, alpha=dot_alpha,
                color=color_map[coin], edgecolors="white", linewidths=0.4,
            )

        # ---- AXIS LABEL/TICKS LAYOUT ----
        # Main axis: show ticks+label *here* (attached to the density plot)
        ax.tick_params(axis="x", which="both", labelbottom=True)
        ax.set_xlabel(f"{voi_str} {voi_UnitStr}", labelpad=MAIN_X_PAD)  # <--- adjust MAIN_X_PAD above

        # Dots panel: its own label at the top (no tick labels to avoid crowding)
        ax_dots.tick_params(axis="x", which="both", labelbottom=False)
        ax_dots.xaxis.set_label_position("top")
        ax_dots.set_xlabel("Observed points (jittered)", labelpad=DOTS_TOP_PAD)  # <--- adjust DOTS_TOP_PAD above

        # Keep bottom spine for dots panel so it reads as a strip; remove left
        sns.despine(ax=ax)
        sns.despine(ax=ax_dots, left=True)

    elif dot_mode == "baseline":
        # dots in a thin band near y=0 on the main axis
        y0, y1 = ax.get_ylim()
        band = (y1 - y0) * (dot_jitter if dot_jitter > 0 else 0.04)
        for coin, sub in dat.groupby("coinLabel", sort=False):
            x = sub[variableOfInterest].to_numpy()
            if max_points_per_group is not None and len(x) > max_points_per_group:
                x = rng.choice(x, size=max_points_per_group, replace=False)
            y = rng.uniform(y0, y0 + band, size=len(x))
            ax.scatter(
                x, y, s=dot_size, alpha=dot_alpha, color=color_map[coin],
                edgecolors="white", linewidths=0.4, zorder=4,
            )
        ax.set_ylim(y0, y1)
        ax.set_xlabel((f"{voi_str} {voi_UnitStr}"), labelpad=MAIN_X_PAD)
        sns.despine(ax=ax)

    # Titles/subtitle
    title_string = f"{voi_str} Distribution (Blocks > {blocks_min}) by Coin Type"
    subtitle_string = _subtitle_from(dat) if "_subtitle_from" in globals() else ""
    ax.set_title(title_string, fontsize=12, y=1.055)
    if subtitle_string:
        ax.text(0.5, 1.02, subtitle_string, ha="center", fontsize=9, transform=ax.transAxes)

    ax.set_ylabel("Density" if stat == "density" else stat.capitalize())
    ax.set_xlim(left=0)

    fig.tight_layout()
    plt.show()


# ===== Common helpers for a reusable “bottom dots” panel =====

def make_axes_with_dots(
    *,
    dot_mode: str = "panel",
    figsize: tuple[int, int] = (12, 7),
    height_ratios: tuple[int, int] = (6, 1),
    hspace: float = 0.18,
):
    """Create axes for a main plot and (optionally) a bottom dots strip that shares x."""
    if dot_mode == "panel":
        fig, (ax, ax_dots) = plt.subplots(
            2, 1, figsize=figsize,
            gridspec_kw={"height_ratios": list(height_ratios), "hspace": hspace},
            sharex=True,
        )
    else:
        fig, ax = plt.subplots(figsize=(figsize[0], figsize[1] - 1))
        ax_dots = None
    return fig, ax, ax_dots


def draw_dots_strip(
    ax_dots: plt.Axes,
    dat: pd.DataFrame,
    *,
    x_col: str,
    group_col: str = "coinLabel",
    color_map: dict[str, str],
    dot_size: float = 12,
    dot_alpha: float = 0.6,
    dot_jitter: float = 0.15,
    max_points_per_group: int | None = 4000,
    rng: np.random.Generator | None = None,
):
    """Render a thin jittered strip of points by group on ax_dots."""
    if ax_dots is None:
        return
    if rng is None:
        rng = np.random.default_rng(0)

    ax_dots.set_ylim(-0.5, 0.5)
    ax_dots.set_yticks([]); ax_dots.set_ylabel(""); ax_dots.grid(False)
    for s in ("top", "left", "right"):
        ax_dots.spines[s].set_visible(False)

    for grp, sub in dat.groupby(group_col, sort=False):
        x = pd.to_numeric(sub[x_col], errors="coerce").dropna().to_numpy()
        if max_points_per_group is not None and x.size > max_points_per_group:
            x = rng.choice(x, size=max_points_per_group, replace=False)
        y = rng.uniform(-dot_jitter, dot_jitter, size=x.size)
        ax_dots.scatter(
            x, y,
            s=dot_size, alpha=dot_alpha,
            color=color_map.get(grp, "0.3"),
            edgecolors="white", linewidths=0.4,
        )


def style_x_for_main_and_dots_v1(
    ax_main: plt.Axes,
    ax_dots: plt.Axes | None,
    *,
    main_xlabel: str,
    dots_label_top: str = "Observed points (jittered)",
    main_labelpad: float = 6.0,
    dots_top_pad: float = 8.0,
    despine: bool = True,
):
    """Attach x ticks/label to MAIN axis; give dots panel its own top label."""
    if ax_dots is None:
        ax_main.set_xlabel(main_xlabel, labelpad=main_labelpad)
        if despine:
            sns.despine(ax=ax_main)
        return

    # main axis x: visible and labeled
    ax_main.tick_params(axis="x", which="both", labelbottom=True)
    ax_main.set_xlabel(main_xlabel, labelpad=main_labelpad)

    # dots axis: no bottom tick labels; top label only
    ax_dots.tick_params(axis="x", which="both", labelbottom=False)
    ax_dots.xaxis.set_label_position("top")
    ax_dots.set_xlabel(dots_label_top, labelpad=dots_top_pad)

    if despine:
        sns.despine(ax=ax_main)           # remove top/right on main
        sns.despine(ax=ax_dots, left=True)  # keep bottom spine for the strip



def style_x_for_main_and_dots(
    ax_main: plt.Axes,
    ax_dots: plt.Axes | None,
    *,
    main_xlabel: str,
    dots_label_top: str = "Observed points (jittered)",
    main_labelpad: float = 6.0,
    dots_top_pad: float = 8.0,
    despine: bool = True,
    dots_frame: bool = False,           # <--- NEW
    frame_lw: float = 1.2,
    frame_color: str = "0.2",
):
    if ax_dots is None:
        ax_main.set_xlabel(main_xlabel, labelpad=main_labelpad)
        if despine:
            sns.despine(ax=ax_main)
        return

    # main axis x on bottom
    ax_main.tick_params(axis="x", which="both", labelbottom=True)
    ax_main.set_xlabel(main_xlabel, labelpad=main_labelpad)

    # dots axis: top label, no bottom tick labels
    ax_dots.tick_params(axis="x", which="both", labelbottom=False)
    ax_dots.xaxis.set_label_position("top")
    ax_dots.set_xlabel(dots_label_top, labelpad=dots_top_pad)

    # spines
    if despine:
        sns.despine(ax=ax_main)  # keep main clean

    if dots_frame:
        # full outline around the dots panel
        for side in ("top", "right", "bottom", "left"):
            sp = ax_dots.spines[side]
            sp.set_visible(True)
            sp.set_linewidth(frame_lw)
            sp.set_edgecolor(frame_color)
    else:
        # minimalist strip (old behavior)
        sns.despine(ax=ax_dots, left=True)  # keep bottom spine only

# ===== Example: refactor your hist+kde to use the helpers above =====

def plot_hist_kde_by_coin_v4(
    df: pd.DataFrame,
    *,
    variableOfInterest: str = "truecontent_elapsed_s",
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
    # layout knobs
    hspace: float = 0.50,
    height_ratios: tuple[int, int] = (6, 1),
    main_labelpad: float = 6.0,
    dots_top_pad: float = 1.0,
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

    # --- axes
    fig, ax, ax_dots = make_axes_with_dots(dot_mode=dot_mode,
                                           height_ratios=height_ratios,
                                           hspace=hspace)

    # --- hist + kde
    sns.histplot(
        data=dat, x=variableOfInterest, hue="coinLabel", hue_order=hue_order,
        bins=bins, stat=stat, common_norm=common_norm, element="step",
        alpha=0.35, multiple="layer", ax=ax, palette=color_map, legend=True,
    )
    sns.kdeplot(
        data=dat, x=variableOfInterest, hue="coinLabel", hue_order=hue_order,
        common_norm=common_norm, ax=ax, palette=color_map, lw=2, legend=False,
    )

    # --- dots strip
    if dot_mode == "panel":
        draw_dots_strip(
            ax_dots, dat,
            x_col=variableOfInterest, group_col="coinLabel", color_map=color_map,
            dot_size=dot_size, dot_alpha=dot_alpha, dot_jitter=dot_jitter,
            max_points_per_group=max_points_per_group,
        )

    # --- titles
    title_string = f"{variableOfInterest} Distribution (Blocks > {blocks_min}) by Coin Type"
    subtitle_string = _subtitle_from(dat) if "_subtitle_from" in globals() else ""
    ax.set_title(title_string, fontsize=12, y=1.055)
    if subtitle_string:
        ax.text(0.5, 1.02, subtitle_string, ha="center", fontsize=9, transform=ax.transAxes)

    # --- labels (FIX: pass the variable, not the literal string)
    style_x_for_main_and_dots(
        ax, ax_dots,
        main_xlabel=variableOfInterest,
        dots_label_top="Observed points (jittered)",
        main_labelpad=main_labelpad,
        dots_top_pad=dots_top_pad,
        despine=True,
    )
    # Ensure main x-label isn’t occluded by the strip
    if ax_dots is not None:
        ax.xaxis.set_label_coords(0.5, -0.08)  # adjust if needed

    ax.set_ylabel("Density" if stat == "density" else stat.capitalize())
    ax.set_xlim(left=0)

    # Use ONE layout manager: keep tight_layout, do NOT use constrained_layout simultaneously
    #fig.tight_layout()
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
    # layout knobs
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

    # --- axes
    fig, ax, ax_dots = make_axes_with_dots(dot_mode=dot_mode,
                                           height_ratios=height_ratios,
                                           hspace=hspace)

    # --- hist + kde
    sns.histplot(
        data=dat, x=variableOfInterest, hue="coinLabel", hue_order=hue_order,
        bins=bins, stat=stat, common_norm=common_norm, element="step",
        alpha=0.35, multiple="layer", ax=ax, palette=color_map, legend=True,
    )
    sns.kdeplot(
        data=dat, x=variableOfInterest, hue="coinLabel", hue_order=hue_order,
        common_norm=common_norm, ax=ax, palette=color_map, lw=2, legend=False,
    )

    # --- dots strip
    if dot_mode == "panel":
        draw_dots_strip(
            ax_dots, dat,
            x_col=variableOfInterest, group_col="coinLabel", color_map=color_map,
            dot_size=dot_size, dot_alpha=dot_alpha, dot_jitter=dot_jitter,
            max_points_per_group=max_points_per_group,
        )

    # --- titles
    title_string = f"{voi_str} Distribution (Blocks > {blocks_min}) by Coin Type"
    subtitle_string = _subtitle_from(dat) if "_subtitle_from" in globals() else ""
    ax.set_title(title_string, fontsize=14, y=1.055)
    if subtitle_string:
        ax.text(0.5, 1.02, subtitle_string, ha="center", fontsize=11, transform=ax.transAxes)

    # --- labels (FIX: pass the variable, not the literal string)
    style_x_for_main_and_dots(
        ax, ax_dots,
        main_xlabel=variableOfInterest,
        dots_label_top=(f"{voi_str} {voi_UnitStr}\n\nObserved points (jittered)"),
        main_labelpad=main_labelpad,
        dots_top_pad=dots_top_pad,
        dots_frame=False,
        despine=True,
    )
    # Ensure main x-label isn’t occluded by the strip
    if ax_dots is not None:
        ax.xaxis.set_label_coords(0.5, -0.08)  # adjust if needed

    ax.set_ylabel("Density" if stat == "density" else stat.capitalize())
    ax.set_xlim(left=0)

    # Use ONE layout manager: keep tight_layout, do NOT use constrained_layout simultaneously
    fig.tight_layout()
    plt.show()



# Omnibus + pairwise tests for "are the latency distributions different by coin type?"



try:
    from scipy.stats import epps_singleton_2samp  # pairwise, distributional
    _HAS_ES = True
except Exception:
    _HAS_ES = False


def _fdr_bh(pvals):
    """Benjamini–Hochberg FDR correction (returns array of q-values in original order)."""
    pvals = np.asarray(pvals, float)
    n = pvals.size
    order = np.argsort(pvals)
    ranked = pvals[order]
    q = np.empty_like(ranked)
    prev = 1.0
    for i in range(n - 1, -1, -1):
        rank = i + 1
        val = ranked[i] * n / rank
        prev = min(prev, val)
        q[i] = prev
    out = np.empty_like(q)
    out[order] = q
    return out


def _cliffs_delta(x, y):
    """
    Cliff's delta via Mann–Whitney U:
      delta = 2*A12 - 1, where A12 = U_greater / (n*m).
    Positive -> x tends to be larger than y.
    """
    x = np.asarray(x); y = np.asarray(y)
    n, m = len(x), len(y)
    if n == 0 or m == 0:
        return np.nan
    U_greater = mannwhitneyu(x, y, alternative="greater", method="asymptotic").statistic
    A12 = U_greater / (n * m)
    return 2 * A12 - 1


def test_coin_distributions(
    df: pd.DataFrame,
    *,
    variableOfInterest: str = "truecontent_elapsed_s",
    blocks_min: int = 3,
    min_n_per_group: int = 10,
    alpha: float = 0.05,
    verbose: bool = True,
):
    """
    Filters like your plots, then runs:
      - Omnibus (distributional): Anderson–Darling k-sample
      - Omnibus (location): Kruskal–Wallis
      - Pairwise (distributional): KS 2-sample (+ Epps–Singleton if available)
      - Effect size: Cliff's delta for each pair
    Returns a dict of results; prints a compact summary if verbose=True.
    """
    req = ["BlockNum", "BlockStatus", variableOfInterest, "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    sdf = df.copy()
    sdf[variableOfInterest] = pd.to_numeric(sdf[variableOfInterest], errors="coerce")
    filt = (
        (sdf["BlockNum"] > blocks_min)
        & (sdf["BlockStatus"] == "complete")
        & sdf["coinLabel"].notna()
        & sdf["dropQual"].isin(["good", "bad"])
        & sdf[variableOfInterest].notna()
    )
    dat = sdf.loc[filt, [variableOfInterest, "coinLabel"]].reset_index(drop=True)
    if dat.empty:
        raise ValueError("No data after filtering.")

    groups = {k: v[variableOfInterest].to_numpy()
              for k, v in dat.groupby("coinLabel")}
    # enforce min size
    groups = {k: x for k, x in groups.items() if len(x) >= min_n_per_group}
    if len(groups) < 2:
        raise ValueError("Need at least two coin types with sufficient data.")

    labels = sorted(groups.keys())
    samples = [groups[k] for k in labels]

    # Omnibus tests
    ad_res = anderson_ksamp(samples)  # statistic, critical_values, significance_level
    kw_res = kruskal(*samples)        # H, pvalue

    # Pairwise tests + effect sizes
    pair_rows = []
    for a, b in combinations(labels, 2):
        xa, xb = groups[a], groups[b]
        ks = ks_2samp(xa, xb, alternative="two-sided", method="auto")
        es_stat, es_p = (np.nan, np.nan)
        if _HAS_ES:
            try:
                es = epps_singleton_2samp(xa, xb)
                es_stat, es_p = es.statistic, es.pvalue
            except Exception:
                pass
        delta = _cliffs_delta(xa, xb)
        pair_rows.append({
            "A": a, "B": b,
            "n_A": len(xa), "n_B": len(xb),
            "KS_D": ks.statistic, "KS_p": ks.pvalue,
            "ES_stat": es_stat, "ES_p": es_p,
            "Cliffs_delta": delta,
        })

    pair_df = pd.DataFrame(pair_rows)
    # FDR for KS (and ES if present)
    pair_df["KS_q"] = _fdr_bh(pair_df["KS_p"].values)
    if _HAS_ES and pair_df["ES_p"].notna().any():
        es_mask = pair_df["ES_p"].notna()
        qs = np.full(len(pair_df), np.nan)
        qs[es_mask] = _fdr_bh(pair_df.loc[es_mask, "ES_p"].values)
        pair_df["ES_q"] = qs
    else:
        pair_df["ES_q"] = np.nan

    results = {
        "labels": labels,
        "sizes": {k: len(v) for k, v in groups.items()},
        "anderson_ksamp": {
            "statistic": float(ad_res.statistic),
            "significance_level": float(ad_res.significance_level),  # ≈ p-value (%)
        },
        "kruskal": {
            "H": float(kw_res.statistic),
            "pvalue": float(kw_res.pvalue),
        },
        "pairwise": pair_df.sort_values(["KS_q", "KS_p"], ignore_index=True),
        "alpha": alpha,
    }

    if verbose:
        print(f"{variableOfInterest} & Coin Type")
        print("== Omnibus tests ==")
        print(f"Anderson–Darling k-sample: A² = {ad_res.statistic:.3f}, approx p ≈ {ad_res.significance_level/100:.4f}")
        print(f"Kruskal–Wallis: H = {kw_res.statistic:.3f}, p = {kw_res.pvalue:.4g}")
        print("\n== Pairwise (Benjamini–Hochberg FDR on KS) ==")
        show_cols = ["A","B","n_A","n_B","KS_D","KS_p","KS_q","Cliffs_delta"]
        if _HAS_ES:
            show_cols += ["ES_stat","ES_p","ES_q"]
        print(pair_df[show_cols].to_string(index=False, float_format=lambda x: f"{x:.4g}"))
        print("\nCliff's δ thresholds (|δ|): small≈0.147, medium≈0.33, large≈0.474")

        # quick yes/no “consistent pattern” heuristic
        ad_p = ad_res.significance_level / 100.0
        kw_p = kw_res.pvalue
        sig_pairs = (pair_df["KS_q"] <= alpha).sum()
        total_pairs = len(pair_df)
        print(f"\nHeuristic summary:")
        if ad_p <= alpha or kw_p <= alpha:
            print(f"- Omnibus difference detected (AD p≈{ad_p:.4g} or KW p={kw_p:.4g}).")
        else:
            print(f"- No omnibus difference detected (AD p≈{ad_p:.4g}, KW p={kw_p:.4g}).")
        print(f"- Pairwise KS: {sig_pairs}/{total_pairs} significant at FDR q≤{alpha}.")
        strong = (pair_df["KS_q"] <= alpha) & (pair_df["Cliffs_delta"].abs() >= 0.33)
        if strong.any():
            print(f"- {strong.sum()} pair(s) show ≥medium effect (|δ|≥0.33).")

    return results

def _enough_for_stats(df: pd.DataFrame,
                      variableOfInterest: str,
                      *,
                      blocks_min: int = 3,
                      min_n_per_group: int = 10,
                      allowed_status: Iterable[str] | None = ("complete",)) -> tuple[bool, str]:
    req = ["BlockNum", variableOfInterest, "coinLabel"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        return False, f"missing columns: {missing}"

    sdf = df.copy()
    bn  = pd.to_numeric(sdf["BlockNum"], errors="coerce")
    x   = pd.to_numeric(sdf[variableOfInterest], errors="coerce")

    mask = (bn > blocks_min) & x.notna() & sdf["coinLabel"].notna()
    if allowed_status:
        mask &= _opt_mask(sdf, "BlockStatus", values=allowed_status)
    mask &= _opt_mask(sdf, "dropQual", values=["good", "bad"])  # ignored if col missing

    dat = sdf.loc[mask, [variableOfInterest, "coinLabel"]]
    if dat.empty:
        return False, "no rows after filtering"
    counts = dat.groupby("coinLabel").size()
    counts = counts[counts >= min_n_per_group]
    if len(counts) < 2:
        return False, "need ≥2 coin groups with sufficient rows"
    return True, ""

# ---------- utilities ----------
PlotFn = Callable[..., Any]

def _slugify(x: Any, maxlen: int = 64) -> str:
    s = re.sub(r"\W+", "_", str(x)).strip("_")
    return (s[:maxlen] or "NA")

def _ensure_dirs(*paths: Path) -> None:
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)

def _run_and_collect_figsv1(fn: PlotFn, *args, **kwargs) -> list[plt.Figure]:
    before = set(plt.get_fignums())
    _orig_show = plt.show
    try:
        plt.show = lambda *a, **k: None  # suppress interactive window
        fn(*args, **kwargs)
    finally:
        plt.show = _orig_show
    after = set(plt.get_fignums())
    new_nums = sorted(after - before)
    return [plt.figure(n) for n in new_nums]

def _run_and_collect_figs(fn: PlotFn, *args, **kwargs) -> list[plt.Figure]:
    before = set(plt.get_fignums())
    _orig_show = plt.show
    try:
        plt.show = lambda *a, **k: None
        fn(*args, **kwargs)
    except Exception as e:
        print(f"[plot] {fn.__name__} skipped: {e}", flush=True)
        return []
    finally:
        plt.show = _orig_show
    after = set(plt.get_fignums())
    new_nums = sorted(after - before)
    return [plt.figure(n) for n in new_nums]


def _save_figs(
    figs: Sequence[plt.Figure],
    *,
    common_dir: Path,
    per_file_dir: Path,
    stem: str,
    tag: str,
    formats: Iterable[str] = ("png", "pdf"),
    dpi: int = 220,
) -> None:
    _ensure_dirs(common_dir, per_file_dir)
    multi = len(figs) > 1
    for i, fig in enumerate(figs, 1):
        suffix = f"_p{i:02d}" if multi else ""
        base_common = f"{stem}__{tag}{suffix}"
        base_specific = f"{tag}{suffix}"
        with contextlib.suppress(Exception):
            fig.tight_layout()
        for ext in formats:
            fig.savefig(Path(common_dir) / f"{base_common}.{ext}", dpi=dpi, bbox_inches="tight")
            fig.savefig(Path(per_file_dir) / f"{base_specific}.{ext}", dpi=dpi, bbox_inches="tight")
    plt.close("all")


# ---------- (re)provide elapsed-time helpers so wrapper can prep data ----------
def add_true_session_elapsed(df: pd.DataFrame,
                             source_col: str = "truecontent_elapsed_s",
                             out_col: str = "trueSession_elapsed_s") -> pd.DataFrame:
    t = pd.to_numeric(df.get(source_col), errors="coerce")
    prev = t.shift(1)
    inside = t.notna()
    continuing = inside & prev.notna() & (t >= prev)
    starting   = inside & ~continuing
    delta = pd.Series(0.0, index=df.index)
    delta.loc[continuing] = (t - prev).loc[continuing]
    delta.loc[starting]   = t.loc[starting].fillna(0.0)
    df[out_col] = delta.cumsum()
    return df

def add_true_session_elapsed_by_block_events(
    df: pd.DataFrame,
    source_col: str = "truecontent_elapsed_s",
    event_col: str = "lo_eventType",
    start_token: str = "BlockStart",
    end_token: str = "BlockEnd",
    out_col: str = "trueSession_block_elapsed_s",
    include_end_row: bool = False,
) -> pd.DataFrame:
    if event_col not in df.columns:
        return df  # be permissive
    t = pd.to_numeric(df[source_col], errors="coerce")
    ev = df[event_col].astype(str)
    is_start, is_end = ev.eq(start_token), ev.eq(end_token)
    starts_cum, ends_cum = is_start.cumsum(), is_end.cumsum()
    in_block = starts_cum.gt(ends_cum) if not include_end_row else starts_cum.ge(ends_cum)
    block_id = starts_cum.where(in_block)
    prev = t.groupby(block_id).shift(1)
    inside = in_block & t.notna()
    continuing = inside & prev.notna() & (t >= prev)
    starting   = inside & ~continuing
    delta = pd.Series(0.0, index=df.index, dtype="float64")
    delta.loc[continuing] = (t - prev).loc[continuing]
    delta.loc[starting]   = t.loc[starting].fillna(0.0)
    df[out_col] = delta.groupby(block_id).cumsum()
    return df

# ---------- simple outlier helper (per earlier design) ----------
def exclude_outliers(
    df: pd.DataFrame,
    column: str,
    *,
    method: str = "median",
    sigma: float = 2.0,
    ddof: int = 1,
    groupby: str | list[str] | None = None,
    keep_na: bool = False,
) -> pd.DataFrame:
    if column not in df.columns:
        return df
    x = pd.to_numeric(df[column], errors="coerce")
    if groupby is None:
        center = pd.Series((x.mean() if method == "mean" else x.median()), index=df.index)
        scale  = pd.Series(x.std(ddof=ddof), index=df.index)
    else:
        agg = (pd.core.groupby.SeriesGroupBy.mean if method == "mean"
               else pd.core.groupby.SeriesGroupBy.median)
        center = df.groupby(groupby)[column].transform(lambda s: pd.to_numeric(s, errors="coerce").agg(method))
        scale  = df.groupby(groupby)[column].transform(lambda s: pd.to_numeric(s, errors="coerce").std(ddof=ddof))
    scale = scale.replace(0, np.nan)
    z = (x - center).abs() / scale
    mask = (z <= sigma) | z.isna()
    if not keep_na:
        mask &= x.notna()
    return df.loc[mask].copy()

def _prep_df_for_file(df: pd.DataFrame,
                      *,
                      use_outlier_filter: bool = False,
                      filter_columns: Iterable[str] = ("truecontent_elapsed_s", "dropDist"),
                      outlier_groupby: str | list[str] | None = "coinLabel",
                      outlier_method: str = "median",
                      outlier_sigma: float = 2.0,
                      outlier_ddof: int = 1,
                      outlier_keep_na: bool = False) -> pd.DataFrame:
    df = df.copy()
    if "coinLabel" in df.columns:
        df["coinLabel"] = df["coinLabel"].astype(str)
    if use_outlier_filter:
        for col in filter_columns:
            if col in df.columns:
                df = exclude_outliers(
                    df, col,
                    method=outlier_method,
                    sigma=outlier_sigma,
                    ddof=outlier_ddof,
                    groupby=outlier_groupby,
                    keep_na=outlier_keep_na,
                )
    # ensure computed times exist
    df = add_true_session_elapsed(df)
    df = add_true_session_elapsed_by_block_events(df)
    return df

# ---------- FIXES to your functions so wrapper calls succeed ----------
def plot_block3_roundNum(df: pd.DataFrame, variableOfInterest: str, yLabel: str):
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
    ax.set_title(f"{variableOfInterest} in Block 3 (By Rounds)")
    ax.set_xlabel("Rounds")
    ax.set_ylabel(yLabel)
    add_legends(ax)
    fig.tight_layout()
    plt.show()

def plot_block3_roundTime(df: pd.DataFrame, variableOfInterest: str, yLabel: str):
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
    ax.set_title(f"{variableOfInterest} in Block 3 (Round Elapsed Time)")
    ax.set_xlabel("Session Elapsed Time")
    ax.set_ylabel(yLabel)
    add_legends(ax)
    fig.tight_layout()
    plt.show()

def plot_blocks_gt3_overall_vs_TP2Blocks_Time(df: pd.DataFrame, variableOfInterest: str, yLabel: str):
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
    ax.set_title(f"{variableOfInterest} in Blocks > 3 by Coin Type and Drop Quality")
    ax.set_xlabel("Overall Time (trueSession_elapsed_s)")
    ax.set_ylabel(yLabel)
    add_legends(ax)
    fig.tight_layout()
    plt.show()

def plot_blocks_gt3_overall_vs_TP2Blocks_BlockNum(df: pd.DataFrame, variableOfInterest: str, yLabel: str):
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
    ax.set_title(f"{variableOfInterest} in Blocks > 3 by Coin Type and Drop Quality")
    ax.set_xlabel("BlockNum")
    ax.set_ylabel(yLabel)
    add_legends(ax)
    fig.tight_layout()
    plt.show()

# ---------- WRAPPER 1: run suite for a single file (with optional grouping & outliers) ----------
def run_suite_for_file(
    csv_path: Path | str,
    *,
    out_root: Path | str = "plots_out",
    use_outlier_filter: bool = False,
    outlier_groupby: str | list[str] | None = "coinLabel",
    outlier_method: str = "median",
    outlier_sigma: float = 2.0,
    outlier_ddof: int = 1,
    outlier_keep_na: bool = False,
    filter_columns: Iterable[str] = ("truecontent_elapsed_s", "dropDist"),
    groupby: str | list[str] | None = None,
    group_subdirs: bool = True,
    formats: Iterable[str] = ("png", "pdf"),
    dpi: int = 220,
    blocks_per_facet: int = 20,
    variableOfInterest: str = "truecontent_elapsed_s",
    yLabel_map: dict[str, str] | None = None,
    allowed_status: Iterable[str] | None = ("complete",),
) -> dict:

    """
    Executes:
      test_coin_distributions
      plot_hist_kde_by_coin
      plot_blocks_gt3_overall_vs_TP2Blocks_Time
      plot_blocks_gt3_overall_vs_TP2Blocks_BlockNum
      plot_blocks_gt3_overall_vs_block_facet
      plot_block3_roundNum
      plot_block3_roundTime
    """
    csv_path = Path(csv_path)
    out_root = Path(out_root)
    common_dir = out_root / "_ALL"
    per_file_dir = out_root / csv_path.stem
    _ensure_dirs(common_dir, per_file_dir)

    # read
    df = read_data(csv_path)
    # prep (outliers + computed times)
    df = _prep_df_for_file(
        df,
        use_outlier_filter=use_outlier_filter,
        filter_columns=filter_columns,
        outlier_groupby=outlier_groupby,
        outlier_method=outlier_method,
        outlier_sigma=outlier_sigma,
        outlier_ddof=outlier_ddof,
        outlier_keep_na=outlier_keep_na,
    )

    # grouping
    groups: list[tuple[Any, pd.DataFrame]]
    if groupby is None:
        groups = [(None, df)]
    else:
        groups = list(df.groupby(groupby, dropna=False, sort=True))

    # labels
    default_labels = {
        "truecontent_elapsed_s": "Pin Drop Latency Within Round (s)",
        "dropDist": "Pin Drop Distance to Closest Coin Not Yet Collected (m)",
    }
    if yLabel_map:
        default_labels.update(yLabel_map)
    yLabel = default_labels.get(variableOfInterest, variableOfInterest)

    manifest: dict = {"file": str(csv_path), "outputs": []}

    for gkey, gdf in groups:
        group_tag = None if gkey is None else _slugify(gkey)
        target_dir = per_file_dir / group_tag if (group_tag and group_subdirs) else per_file_dir
        _ensure_dirs(target_dir)

        def _tag(base: str) -> str:
            return f"{base}__grp-{group_tag}" if group_tag else base

        # # 1) test_coin_distributions (save CSV)
        # res = test_coin_distributions(
        #     gdf,
        #     variableOfInterest=variableOfInterest,
        #     blocks_min=3,
        #     min_n_per_group=10,
        #     alpha=0.05,
        #     verbose=False,
        # )
        # pair_df = res["pairwise"]
        # stats_name = _tag(f"stats_pairwise__{variableOfInterest}")
        # pair_df.to_csv(target_dir / f"{stats_name}.csv", index=False)
        # manifest["outputs"].append({"type": "stats", "tag": stats_name, "rows": int(pair_df.shape[0])})

        # 1) STATS — only run if dataset is meaningful; otherwise skip
        ok, why_not = _enough_for_stats(
            gdf,
            variableOfInterest,
            blocks_min=3,
            min_n_per_group=10,
            allowed_status=allowed_status,
        )
        group_desc = f" [{gkey}]" if gkey is not None else ""
        if ok:
            log(f"[stats] {csv_path.name}{group_desc}: running")
            res = test_coin_distributions(
                gdf,
                variableOfInterest=variableOfInterest,
                blocks_min=3,
                min_n_per_group=10,
                alpha=0.05,
                verbose=False,
            )


            pair_df = res["pairwise"]
            stats_name = _tag(f"stats_pairwise__{variableOfInterest}")
            pair_df.to_csv(target_dir / f"{stats_name}.csv", index=False)
            manifest["outputs"].append({"type": "stats", "tag": stats_name, "rows": int(pair_df.shape[0])})
        else:
            log(f"[stats] {csv_path.name}{group_desc}: SKIPPED — {why_not}")
            manifest["outputs"].append({
                "type": "stats",
                "tag": _tag(f"stats_pairwise__{variableOfInterest}"),
                "skipped": True,
                "reason": why_not,
            })


        # 2) plot_hist_kde_by_coin
        figs = _run_and_collect_figs(
            plot_hist_kde_by_coin,
            gdf,
            variableOfInterest=variableOfInterest,
        )
        _save_figs(figs, common_dir=common_dir, per_file_dir=target_dir,
                   stem=csv_path.stem, tag=_tag(f"hist_kde_{variableOfInterest}"),
                   formats=formats, dpi=dpi)
        manifest["outputs"].append({"type": "plot", "tag": _tag(f"hist_kde_{variableOfInterest}"), "count": len(figs)})

        # 3) plot_blocks_gt3_overall_vs_TP2Blocks_Time
        figs = _run_and_collect_figs(
            plot_blocks_gt3_overall_vs_TP2Blocks_Time,
            gdf, variableOfInterest=variableOfInterest, yLabel=yLabel
        )
        _save_figs(figs, common_dir=common_dir, per_file_dir=target_dir,
                   stem=csv_path.stem, tag=_tag(f"tp2_time__{variableOfInterest}"),
                   formats=formats, dpi=dpi)
        manifest["outputs"].append({"type": "plot", "tag": _tag(f"tp2_time__{variableOfInterest}"), "count": len(figs)})

        # 4) plot_blocks_gt3_overall_vs_TP2Blocks_BlockNum
        figs = _run_and_collect_figs(
            plot_blocks_gt3_overall_vs_TP2Blocks_BlockNum,
            gdf, variableOfInterest=variableOfInterest, yLabel=yLabel
        )
        _save_figs(figs, common_dir=common_dir, per_file_dir=target_dir,
                   stem=csv_path.stem, tag=_tag(f"tp2_blocknum__{variableOfInterest}"),
                   formats=formats, dpi=dpi)
        manifest["outputs"].append({"type": "plot", "tag": _tag(f"tp2_blocknum__{variableOfInterest}"), "count": len(figs)})

        # 5) plot_blocks_gt3_overall_vs_block_facet
        figs = _run_and_collect_figs(
            plot_blocks_gt3_overall_vs_block_facet,
            gdf, variableOfInterest=variableOfInterest, yLabel=yLabel, blocks_per_facet=blocks_per_facet
        )
        _save_figs(figs, common_dir=common_dir, per_file_dir=target_dir,
                   stem=csv_path.stem, tag=_tag(f"tp2_facet{blocks_per_facet}__{variableOfInterest}"),
                   formats=formats, dpi=dpi)
        manifest["outputs"].append({"type": "plot", "tag": _tag(f"tp2_facet{blocks_per_facet}__{variableOfInterest}"), "count": len(figs)})

        # 6) plot_block3_roundNum
        figs = _run_and_collect_figs(
            plot_block3_roundNum,
            gdf, variableOfInterest=variableOfInterest, yLabel=yLabel
        )
        _save_figs(figs, common_dir=common_dir, per_file_dir=target_dir,
                   stem=csv_path.stem, tag=_tag(f"block3_roundNum__{variableOfInterest}"),
                   formats=formats, dpi=dpi)
        manifest["outputs"].append({"type": "plot", "tag": _tag(f"block3_roundNum__{variableOfInterest}"), "count": len(figs)})

        # 7) plot_block3_roundTime
        figs = _run_and_collect_figs(
            plot_block3_roundTime,
            gdf, variableOfInterest=variableOfInterest, yLabel=yLabel
        )
        _save_figs(figs, common_dir=common_dir, per_file_dir=target_dir,
                   stem=csv_path.stem, tag=_tag(f"block3_roundTime__{variableOfInterest}"),
                   formats=formats, dpi=dpi)
        manifest["outputs"].append({"type": "plot", "tag": _tag(f"block3_roundTime__{variableOfInterest}"), "count": len(figs)})

    return manifest
def log(msg: str) -> None:
    print(msg, flush=True)

# ---------- WRAPPER 2: iterate over all files in a directory ----------
def run_suite_for_directory(
    input_dir: Path | str,
    pattern: str = "*.csv",
    *,
    recursive: bool = False,
    **kwargs,   # forwarded to run_suite_for_file
) -> list[dict]:
    root = Path(input_dir)
    paths = sorted(root.rglob(pattern) if recursive else root.glob(pattern))
    results = []
    for p in paths:
        if p.is_file():
            print(f"[run] {p}")
            results.append(run_suite_for_file(p, **kwargs))
    print(f"Done. Processed {len(results)} files.")
    return results

# ---------- Optional: convenience CLI main ----------
# 

# --- Replace your current main() with this CLI that supports a directory run ---
def main():
    import argparse, json
    from pathlib import Path

    ap = argparse.ArgumentParser(description="Run plotting/stat suite for a CSV file or a directory of CSVs.")
    ap.add_argument("--input", help="Path to a CSV file or a directory containing CSVs")
    ap.add_argument("-p", "--pattern", default="*.csv", help="Glob pattern when input is a directory (default: *.csv)")
    ap.add_argument("-r", "--recursive", action="store_true", help="Recurse into subdirectories when input is a directory")
    ap.add_argument("-o", "--out-root", default="plots_out", help="Output root directory (default: plots_out)")

    # Suite options
    ap.add_argument("--variable-of-interest", dest="voi", default="truecontent_elapsed_s",
                    help="Column to analyze/plot (e.g., truecontent_elapsed_s or dropDist)")
    ap.add_argument("--blocks-per-facet", type=int, default=20, help="Facet size for block facet plots (default: 20)")
    ap.add_argument("--formats", default="png,pdf", help="Comma-separated output formats (default: png,pdf)")

    # Grouping (for output stratification)
    ap.add_argument("--groupby", nargs="*", default=None, help="Columns to group results by (space-separated)")
    ap.add_argument("--no-group-subdirs", dest="group_subdirs", action="store_false",
                    help="Do not create per-group subdirectories")

    # Outlier filtering (applied once per file/group)
    ap.add_argument("--use-outlier-filter", action="store_true", help="Enable outlier removal")
    ap.add_argument("--outlier-groupby", nargs="*", default=["coinLabel"],
                    help="Columns to compute outliers within (default: coinLabel)")
    ap.add_argument("--outlier-method", choices=["mean", "median"], default="median")
    ap.add_argument("--outlier-sigma", type=float, default=2.0)
    ap.add_argument("--outlier-ddof", type=int, default=1)
    ap.add_argument("--outlier-keep-na", action="store_true", help="Keep NaNs in outlier column(s)")
    ap.add_argument("--filter-columns", nargs="*", default=["truecontent_elapsed_s", "dropDist"],
                    help="Columns to apply outlier filtering to")

    args = ap.parse_args()

    formats = tuple(s.strip() for s in args.formats.split(",") if s.strip())
    gby = args.groupby if args.groupby else None
    ogby = args.outlier_groupby if args.outlier_groupby else None

    p = Path(args.input)
    if p.is_file():
        manifest = run_suite_for_file(
            p,
            out_root=args.out_root,
            variableOfInterest=args.voi,
            blocks_per_facet=args.blocks_per_facet,
            formats=formats,
            groupby=gby,
            group_subdirs=args.group_subdirs,
            use_outlier_filter=args.use_outlier_filter,
            outlier_groupby=ogby,
            outlier_method=args.outlier_method,
            outlier_sigma=args.outlier_sigma,
            outlier_ddof=args.outlier_ddof,
            outlier_keep_na=args.outlier_keep_na,
            filter_columns=args.filter_columns,
        )
        print(json.dumps(manifest, indent=2))
    elif p.is_dir():
        results = run_suite_for_directory(
            p,
            pattern=args.pattern,
            recursive=args.recursive,
            out_root=args.out_root,
            variableOfInterest=args.voi,
            blocks_per_facet=args.blocks_per_facet,
            formats=formats,
            groupby=gby,
            group_subdirs=args.group_subdirs,
            use_outlier_filter=args.use_outlier_filter,
            outlier_groupby=ogby,
            outlier_method=args.outlier_method,
            outlier_sigma=args.outlier_sigma,
            outlier_ddof=args.outlier_ddof,
            outlier_keep_na=args.outlier_keep_na,
            filter_columns=args.filter_columns,
        )
        print(json.dumps(results, indent=2))
    else:
        raise SystemExit(f"Input path not found: {p}")

if __name__ == "__main__":
    main()
