# ============================================================
# End-to-end plotting script (matplotlib-only, no seaborn)
# - Computes corrected elapsed times if TrueSessionElapsedTime exists
# - Reproduces the two key plots with your styling choices
#     1) Block 3: X=BlockElapsedTime, Y=RoundElapsedTime (≤200s), color=dropQual, shape=coinLabel
#     2) Blocks > 3: X=mLTimestamp, Y=BlockElapsedTime, color=dropQual, shape=coinLabel
# ============================================================

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

# -------------------------------
# Configuration
# -------------------------------
#CSV_PATH = Path("/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/MergedEvents_noWalks/Merged_ParticipantDayRoleCoinSet/MergedEvents/R037_AN_B_main_events.csv")
CSV_PATH = Path("/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/PinDrops_All/PinDrops_ALL.csv")
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


# -------------------------------
# Plot 1: Block 3 — X=BlockElapsed, Y=RoundElapsed (≤200s)
# -------------------------------
def plot_block3_roundTime(df: pd.DataFrame, variableOfInterest: str, yLabel: str):

    req = ["BlockNum", variableOfInterest, "trueSession_elapsed_s", "coinLabel", "dropQual", "participantID", "pairID", "coinSet"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Block 3 plot: {missing}")

    block3 = df[
        (df["BlockNum"] == 3)
        & (df["BlockStatus"] == "complete")
        & df[variableOfInterest].notna()
        & df["trueSession_elapsed_s"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good","bad"])
        #& (df["truecontent_elapsed_s"])
    ][["trueSession_elapsed_s", variableOfInterest, "coinLabel", "dropQual", "participantID", "pairID", "coinSet"]].copy()

    fig, ax = plt.subplots(figsize=(10, 6))
    for _, r in block3.iterrows():
        scatter_point(ax, r["trueSession_elapsed_s"], r[variableOfInterest], r["coinLabel"], r["dropQual"])

    title_string = f"{variableOfInterest} in Block 3 (Round Elapsed Time)"
    subtitle_string = f"Pair {pairID} | Participant {participantID} | Coin Set {coinSet}"
    
    ax.set_title(title_string, fontsize=12, y=1.055)
    ax.text(0.5, 1.02, subtitle_string, ha='center', fontsize = 9, transform=ax.transAxes)
    ax.set_xlabel("Session Elapsed Time")
    ax.set_ylabel(yLabel)
    add_legends(ax)
    fig.tight_layout()
    plt.show()


def plot_block3_roundNum(df: pd.DataFrame, variableOfInterest: str, yLabel: str):
    req = ["BlockNum", variableOfInterest, "trueSession_elapsed_s", "RoundNum","coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Block 3 plot: {missing}")

    block3 = df[
        (df["BlockNum"] == 3)
        & (df["BlockStatus"] == "complete")
        & df[variableOfInterest].notna()
        & df["trueSession_elapsed_s"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good","bad"]
        & df["RoundNum"] < 100)
        #& (df["truecontent_elapsed_s"])
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


# -------------------------------
# Plot 2a: Blocks > 3 — X=mLTimestamp, Y=BlockElapsed
# -------------------------------
def plot_blocks_gt3_overall_vs_TP2Blocks_BlockNum(df: pd.DataFrame, variableOfInterest: str , yLabel: str):
    req = ["BlockNum", variableOfInterest, "trueSession_elapsed_s", "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Blocks>3 plot: {missing}")

    gt3 = df[
        (df["BlockNum"] > 3)
        & (df["BlockStatus"] == "complete")
        & df[variableOfInterest].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good","bad"])
    ][[variableOfInterest, "BlockNum", "coinLabel", "dropQual"]].copy()

    fig, ax = plt.subplots(figsize=(12, 6))
    for _, r in gt3.iterrows():
        scatter_point(ax, r["BlockNum"], r[variableOfInterest], r["coinLabel"], r["dropQual"])
    ax.set_title(f"{variableOfInterest} in Blocks > 3 by Coin Type and Drop Quality")
    ax.set_xlabel("BlockNum")
    ax.set_ylabel(yLabel)
    add_legends(ax)
    fig.tight_layout()
    plt.show()

def plot_blocks_gt3_overall_vs_TP2Blocks_Time(df: pd.DataFrame, variableOfInterest: str , yLabel: str):
    req = ["BlockNum", variableOfInterest, "trueSession_elapsed_s", "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Blocks>3 plot: {missing}")

    gt3 = df[
        (df["BlockNum"] > 3)
        & (df["BlockStatus"] == "complete")
        & df[variableOfInterest].notna()
        & df["trueSession_elapsed_s"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good","bad"])
    ][[groupingVariable, "BlockNum", "coinLabel", "dropQual"]].copy()

    fig, ax = plt.subplots(figsize=(12, 6))
    for _, r in gt3.iterrows():
        scatter_point(ax, r["BlockNum"], r[variableOfInterest], r["coinLabel"], r["dropQual"])
    ax.set_title(f"{variableOfInterest} in Blocks > 3 by Coin Type and Drop Quality")
    ax.set_xlabel("BlockNum")
    ax.set_ylabel(yLabel)
    add_legends(ax)
    fig.tight_layout()
    plt.show()

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns



import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def plot_hist_kde_by_coin(
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
    dat = sdf.loc[filt, [variableOfInterest, "coinLabel"]].reset_index(drop=True)
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
        ax_dots.axis("off")
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

    ax.set_title(f"{variableOfInterest} Distribution (Blocks > {blocks_min}) by Coin Type")
    ax.set_xlabel(f"{variableOfInterest}")
    ax.set_ylabel("Density" if stat == "density" else stat.capitalize())
    ax.set_xlim(left=0)
    sns.despine(ax=ax)
    if ax_dots is not None:
        sns.despine(ax=ax_dots, left=True, bottom=True)
    fig.tight_layout()
    plt.show()

# Omnibus + pairwise tests for "are the latency distributions different by coin type?"

import numpy as np
import pandas as pd
from itertools import combinations
from collections import defaultdict

from scipy.stats import (
    anderson_ksamp,       # k-sample Anderson–Darling (omnibus, distributional)
    kruskal,              # Kruskal–Wallis (omnibus, location shift)
    ks_2samp,             # Kolmogorov–Smirnov (pairwise, distributional)
    mannwhitneyu,         # for effect size via A12 / Cliff's delta
)

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
# -------------------------------
# Main
# -------------------------------
def main():
    df = read_data(CSV_PATH)
    # keep only rows with non-empty coin labels
    if "coinLabel" in df.columns:
        df = df[df["coinLabel"] != ""]
    dropLatency_ylabel = "Pin Drop Latency Within Round (Round Elapsed Time)"
    dropDist_ylabel = "Pin Drop Distance to Closest Coin Not Yet Collected (m)"

    # Generate both plots
    plot_block3_round_vs_block(df)
    plot_blocks_gt3_overall_vs_block(df)
    plot_blocks_gt3_overall_vs_session(df)
    plot_blocks_gt3_overall_vs_block_facet(df, 20)
    plot_blocks_gt3_overall_vs_BlockNum(df)

    plot_hist_kde_by_coin(df, variableOfInterest="dropDist")
    plot_hist_kde_by_coin(df, variableOfInterest="truecontent_elapsed_s")


    #results = test_coin_latency_distributions(df_elapsed, blocks_min=3, min_n_per_group=10, alpha=0.05)
    results = test_coin_distributions(df, blocks_min=3, min_n_per_group=10, alpha=0.05, variableOfInterest="truecontent_elapsed_s")
    #print('Stats for PinDrop Latency Distributions by Coin Type')
    print(results["pairwise"])  # pandas DataFrame with per-pair stats (KS, ES if available, and Cliff's delta)

    results2 = test_coin_distributions(df, blocks_min=3, min_n_per_group=10, alpha=0.05, variableOfInterest="dropDist")
    #print('Stats for PinDrop Distance Distributions by Coin Type')
    print(results2["pairwise"])  # pandas DataFrame with per-pair stats (KS, ES if available, and Cliff's delta)

if __name__ == "__main__":
    main()




# === Add this block to your script (wrappers + small fixes) ===
from __future__ import annotations
from pathlib import Path
from typing import Iterable, Sequence, Callable, Any
import re, glob
import contextlib

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# If you use seaborn in plot_hist_kde_by_coin
import seaborn as sns  # noqa

# ---------- utilities ----------
PlotFn = Callable[..., Any]

def _slugify(x: Any, maxlen: int = 64) -> str:
    s = re.sub(r"\W+", "_", str(x)).strip("_")
    return (s[:maxlen] or "NA")

def _ensure_dirs(*paths: Path) -> None:
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)

def _run_and_collect_figs(fn: PlotFn, *args, **kwargs) -> list[plt.Figure]:
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
    # data prep
    use_outlier_filter: bool = False,
    outlier_groupby: str | list[str] | None = "coinLabel",
    outlier_method: str = "median",
    outlier_sigma: float = 2.0,
    outlier_ddof: int = 1,
    outlier_keep_na: bool = False,
    filter_columns: Iterable[str] = ("truecontent_elapsed_s", "dropDist"),
    # grouping of results
    groupby: str | list[str] | None = None,
    group_subdirs: bool = True,
    # plot/figure saving
    formats: Iterable[str] = ("png", "pdf"),
    dpi: int = 220,
    blocks_per_facet: int = 20,
    # variable and label used across the suite
    variableOfInterest: str = "truecontent_elapsed_s",
    yLabel_map: dict[str, str] | None = None,
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

        # 1) test_coin_distributions (save CSV)
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

        # 2) plot_hist_kde_by_coin
        figs = _run_and_collect_figs(
            plot_hist_kde_by_coin,
            gdf,
            variableOfInterest=variableOfInterest,
        )
        _save_figs(figs, common_dir=common_dir, per_file_dir=target_dir,
                   stem=csv_path.stem, tag=_tag(f"hist_kde__{variableOfInterest}"),
                   formats=formats, dpi=dpi)
        manifest["outputs"].append({"type": "plot", "tag": _tag(f"hist_kde__{variableOfInterest}"), "count": len(figs)})

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
if __name__ == "__main__":
    # Example single-file run (edit CSV_PATH if desired)
    manifest = run_suite_for_file(
        CSV_PATH,
        out_root="plots_out",
        use_outlier_filter=False,
        groupby=None,
        variableOfInterest="truecontent_elapsed_s",  # or "dropDist"
        blocks_per_facet=20,
    )
    print(manifest)
