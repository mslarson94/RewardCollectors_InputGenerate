#!/usr/bin/env python3
"""
plot_idealDistByCoinLayout.py
Describing Ideal Distances Distributions between Coin Layouts 

1) plot ideal distance distribution by coin layout 
2) plot ideal distance distribution per start position 
3) plot ideal distance distribution per start position by coin layout
4) plot value-distance correlation 
5) how often HV is distance dominant vs. distance disadvantaged by Coin Layout (Normalized Ideal Distance >= 0.5 vs. < 0.5 ?)

plot_idealDistByCoinLayout.py

Reads ideal_routes_*.csv files (one per CoinSet) and produces plots + summary stats for
ideal_distance distributions across CoinSet, start_pos_key, and path types.

Expected columns (minimum):
  - CoinSet
  - start_pos_key
  - path_order_round   (e.g., "HV->LV->NV")
  - ideal_distance

Usage:
  python plot_idealDistByCoinLayout.py \
    --input_glob "ideal_routes_*.csv" \
    --out_dir "plots_idealDist"

If you want to explicitly pass files:
  python plot_idealDistByCoinLayout.py --files ideal_routes_A.csv ideal_routes_B.csv

Outputs:
  - PNG plots into out_dir/
  - summary CSV tables into out_dir/summary/
"""

from __future__ import annotations

import argparse
import glob
import os
from dataclasses import dataclass

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import itertools
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from scipy.stats import friedmanchisquare, wilcoxon

# -----------------------------
# Configuration / mappings
# -----------------------------

PATH_POINTS: Dict[str, int] = {
    "HV->LV->NV": 30,  # type 1
    "LV->HV->NV": 30,  # type 2
    "HV->NV->LV": 25,  # type 3
    "NV->HV->LV": 25,  # type 4
    "LV->NV->HV": 20,  # type 5
    "NV->LV->HV": 20,  # type 6
}

TYPE_LABELS: Dict[str, str] = {
    "HV->LV->NV": "Type1 HV->LV->NV (30)",
    "LV->HV->NV": "Type2 LV->HV->NV (30)",
    "HV->NV->LV": "Type3 HV->NV->LV (25)",
    "NV->HV->LV": "Type4 NV->HV->LV (25)",
    "LV->NV->HV": "Type5 LV->NV->HV (20)",
    "NV->LV->HV": "Type6 NV->LV->HV (20)",
}


# -----------------------------
# Helpers
# -----------------------------

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _tight_save(fig: plt.Figure, out_path: str, dpi: int = 160) -> None:
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi)
    plt.close(fig)


def _jitter(n: int, scale: float = 0.08) -> np.ndarray:
    rng = np.random.default_rng(0)
    return rng.normal(0.0, scale, size=n)


def _basic_box_with_points(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    out_path: str,
    order: List[str] | None = None,
    rotate_xticks: bool = True,
) -> None:
    """Matplotlib-only boxplot + jittered points."""
    fig, ax = plt.subplots(figsize=(10, 5))

    if order is None:
        cats = list(pd.unique(df[x_col]))
    else:
        cats = [c for c in order if c in set(df[x_col])]

    data = [df.loc[df[x_col] == c, y_col].to_numpy() for c in cats]
    ax.boxplot(
        data,
        tick_labels=cats,
        showfliers=False,
        medianprops=dict(linewidth=2),
    )

    # overlay points
    for i, c in enumerate(cats, start=1):
        y = df.loc[df[x_col] == c, y_col].to_numpy()
        x = np.full_like(y, i, dtype=float) + _jitter(len(y))
        ax.scatter(x, y, s=14, alpha=0.6)

    ax.set_title(title)
    ax.set_xlabel(x_col)
    ax.set_ylabel(y_col)
    if rotate_xticks:
        ax.tick_params(axis="x", rotation=35)

    _tight_save(fig, out_path)


def _hist_overlay_by_category(
    df: pd.DataFrame,
    cat_col: str,
    y_col: str,
    title: str,
    out_path: str,
    bins: int = 20,
    order: List[str] | None = None,
) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))

    if order is None:
        cats = list(pd.unique(df[cat_col]))
    else:
        cats = [c for c in order if c in set(df[cat_col])]

    for c in cats:
        y = df.loc[df[cat_col] == c, y_col].to_numpy()
        ax.hist(y, bins=bins, alpha=0.35, label=str(c))

    ax.set_title(title)
    ax.set_xlabel(y_col)
    ax.set_ylabel("Count")
    ax.legend(loc="best", fontsize=9, frameon=False)

    _tight_save(fig, out_path)


def _linear_fit_line(x: np.ndarray, y: np.ndarray) -> Tuple[float, float]:
    """Return slope, intercept for y = m x + b."""
    if len(x) < 2:
        return (np.nan, np.nan)
    m, b = np.polyfit(x, y, 1)
    return m, b


def _pearsonr(x: np.ndarray, y: np.ndarray) -> float:
    if len(x) < 2:
        return np.nan
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    if np.std(x) == 0 or np.std(y) == 0:
        return np.nan
    return float(np.corrcoef(x, y)[0, 1])


def _spearmanr(x: np.ndarray, y: np.ndarray) -> float:
    if len(x) < 2:
        return np.nan
    rx = pd.Series(x).rank(method="average").to_numpy()
    ry = pd.Series(y).rank(method="average").to_numpy()
    return _pearsonr(rx, ry)


def _minmax_advantage_score(dist: pd.Series) -> pd.Series:
    """
    Normalize within a group so:
      1.0 = best (shortest distance)
      0.0 = worst (longest distance)

    score = (max - d) / (max - min)
    If max == min -> 0.5 for all (neutral).
    """
    mx = float(dist.max())
    mn = float(dist.min())
    if np.isclose(mx, mn):
        return pd.Series(np.full(len(dist), 0.5), index=dist.index)
    return (mx - dist) / (mx - mn)


def _dominance_flags(dist: pd.Series, tol: float = 1e-9) -> Tuple[pd.Series, pd.Series]:
    """
    For a group, flag:
      dominant: distance == min (within tol)
      disadvantaged: distance == max (within tol)
    """
    mn = float(dist.min())
    mx = float(dist.max())
    dominant = (dist - mn).abs() <= tol
    disadvantaged = (dist - mx).abs() <= tol
    return dominant, disadvantaged


# -----------------------------
# Load + validate
# -----------------------------

REQUIRED_COLS = {"CoinSet", "start_pos_key", "path_order_round", "ideal_distance"}


def load_files(files: List[str]) -> pd.DataFrame:
    frames = []
    for f in files:
        df = pd.read_csv(f)
        missing = REQUIRED_COLS - set(df.columns)
        if missing:
            raise ValueError(f"{f} missing required columns: {sorted(missing)}")
        frames.append(df)

    out = pd.concat(frames, ignore_index=True)

    # Attach points/value
    out["path_value"] = out["path_order_round"].map(PATH_POINTS).astype("Int64")
    if out["path_value"].isna().any():
        unknown = sorted(set(out.loc[out["path_value"].isna(), "path_order_round"]))
        raise ValueError(
            "Unknown path_order_round values found (not in PATH_POINTS): "
            + ", ".join(map(str, unknown))
        )

    # A friendly label, useful for ordering
    out["path_label"] = out["path_order_round"].map(TYPE_LABELS)

    return out


# -----------------------------
# Plotting tasks (1)-(7)
# -----------------------------

@dataclass
class OutputPaths:
    out_dir: str
    plot_dir: str
    summary_dir: str


def make_output_paths(out_dir: str) -> OutputPaths:
    plot_dir = os.path.join(out_dir, "plots")
    summary_dir = os.path.join(out_dir, "summary")
    _ensure_dir(plot_dir)
    _ensure_dir(summary_dir)
    return OutputPaths(out_dir=out_dir, plot_dir=plot_dir, summary_dir=summary_dir)


def plot_1_distribution_by_coinset(df: pd.DataFrame, out: OutputPaths) -> None:
    # 1) plot ideal_distance distribution by CoinSet
    coin_order = sorted(pd.unique(df["CoinSet"]))
    _basic_box_with_points(
        df=df,
        x_col="CoinSet",
        y_col="ideal_distance",
        title="Ideal Distance Distribution by CoinSet",
        out_path=os.path.join(out.plot_dir, "01_ideal_distance_by_CoinSet_box.png"),
        order=coin_order,
        rotate_xticks=False,
    )
    _hist_overlay_by_category(
        df=df,
        cat_col="CoinSet",
        y_col="ideal_distance",
        title="Ideal Distance Histogram Overlay by CoinSet",
        out_path=os.path.join(out.plot_dir, "01_ideal_distance_by_CoinSet_hist.png"),
        order=coin_order,
    )


def plot_2_by_path_per_startpos(df: pd.DataFrame, out: OutputPaths) -> None:
    # 2) per path_order_round by start_pos_key (across all CoinSets)
    path_order = list(PATH_POINTS.keys())
    for sp in sorted(pd.unique(df["start_pos_key"])):
        sub = df[df["start_pos_key"] == sp].copy()
        _basic_box_with_points(
            df=sub,
            x_col="path_order_round",
            y_col="ideal_distance",
            title=f"Ideal Distance by Path Type (all CoinSets) — start_pos_key={sp}",
            out_path=os.path.join(out.plot_dir, f"02_by_path_allCoinSets_startpos_{sp}_box.png"),
            order=path_order,
            rotate_xticks=True,
        )


def plot_3_by_path_per_coinset(df: pd.DataFrame, out: OutputPaths) -> None:
    # 3) per path_order_round by CoinSet
    path_order = list(PATH_POINTS.keys())
    for cs in sorted(pd.unique(df["CoinSet"])):
        sub = df[df["CoinSet"] == cs].copy()
        _basic_box_with_points(
            df=sub,
            x_col="path_order_round",
            y_col="ideal_distance",
            title=f"Ideal Distance by Path Type — CoinSet={cs}",
            out_path=os.path.join(out.plot_dir, f"03_by_path_CoinSet_{cs}_box.png"),
            order=path_order,
            rotate_xticks=True,
        )


def plot_4_by_value_per_startpos(df: pd.DataFrame, out: OutputPaths) -> None:
    # 4) per path value by start_pos_key (across all CoinSets)
    value_order = [20, 25, 30]
    for sp in sorted(pd.unique(df["start_pos_key"])):
        sub = df[df["start_pos_key"] == sp].copy()
        sub["path_value_str"] = sub["path_value"].astype(int).astype(str)
        _basic_box_with_points(
            df=sub,
            x_col="path_value_str",
            y_col="ideal_distance",
            title=f"Ideal Distance by Path Value (all CoinSets) — start_pos_key={sp}",
            out_path=os.path.join(out.plot_dir, f"04_by_value_allCoinSets_startpos_{sp}_box.png"),
            order=[str(v) for v in value_order],
            rotate_xticks=False,
        )


def plot_5_by_value_per_coinset(df: pd.DataFrame, out: OutputPaths) -> None:
    # 5) per path value by CoinSet
    value_order = [20, 25, 30]
    for cs in sorted(pd.unique(df["CoinSet"])):
        sub = df[df["CoinSet"] == cs].copy()
        sub["path_value_str"] = sub["path_value"].astype(int).astype(str)
        _basic_box_with_points(
            df=sub,
            x_col="path_value_str",
            y_col="ideal_distance",
            title=f"Ideal Distance by Path Value — CoinSet={cs}",
            out_path=os.path.join(out.plot_dir, f"05_by_value_CoinSet_{cs}_box.png"),
            order=[str(v) for v in value_order],
            rotate_xticks=False,
        )


def plot_6_value_vs_distance_corr(df: pd.DataFrame, out: OutputPaths) -> pd.DataFrame:
    # 6) value vs. ideal_distance correlation by CoinSet
    rows = []
    for cs in sorted(pd.unique(df["CoinSet"])):
        sub = df[df["CoinSet"] == cs].copy()
        x = sub["path_value"].astype(float).to_numpy()
        y = sub["ideal_distance"].astype(float).to_numpy()

        pear = _pearsonr(x, y)
        spear = _spearmanr(x, y)
        m, b = _linear_fit_line(x, y)

        rows.append(
            dict(
                CoinSet=cs,
                pearson_r=pear,
                spearman_r=spear,
                n=len(sub),
                slope=m,
                intercept=b,
            )
        )

        fig, ax = plt.subplots(figsize=(7, 5))
        ax.scatter(x, y, s=22, alpha=0.75)

        # regression line (only meaningful because x has 3 levels, but still useful visually)
        if np.isfinite(m) and np.isfinite(b):
            xline = np.linspace(np.min(x), np.max(x), 50)
            yline = m * xline + b
            ax.plot(xline, yline, linewidth=2)

        ax.set_title(f"Path Value vs Ideal Distance — CoinSet={cs}\n"
                     f"Pearson r={pear:.3f} | Spearman r={spear:.3f} | n={len(sub)}")
        ax.set_xlabel("path_value (points)")
        ax.set_ylabel("ideal_distance")

        _tight_save(fig, os.path.join(out.plot_dir, f"06_value_vs_distance_CoinSet_{cs}.png"))

    corr_df = pd.DataFrame(rows)
    corr_df.to_csv(os.path.join(out.summary_dir, "06_value_vs_distance_correlation_by_CoinSet.csv"), index=False)
    return corr_df


def analysis_7_path12_advantage_disadvantage(df: pd.DataFrame, out: OutputPaths) -> pd.DataFrame:
    """
    7) How often path type 1 & 2 are each distance-dominant vs distance-disadvantaged by CoinSet.

    To avoid the “logical fallacy” concern, we report TWO views:
      A) Dominant/Disadvantaged by min/max distance within each (CoinSet, start_pos_key)
      B) Advantage score >= 0.5 vs < 0.5 where advantage score is min-max normalized (best=1, worst=0)

    Note: Since lower distance is better, the min-max normalization is computed as:
        score = (max - dist) / (max - min)
    """
    focus_paths = ["HV->LV->NV", "LV->HV->NV"]  # types 1 & 2

    work = df.copy()

    # groupwise normalization + dominance flags per (CoinSet, start_pos_key)
    grp_cols = ["CoinSet", "start_pos_key"]
    work["adv_score"] = work.groupby(grp_cols)["ideal_distance"].transform(_minmax_advantage_score)

    dom, disadv = [], []
    for _, g in work.groupby(grp_cols, sort=False):
        d, a = _dominance_flags(g["ideal_distance"])
        dom.append(d)
        disadv.append(a)
    work["is_dominant"] = pd.concat(dom).sort_index()
    work["is_disadvantaged"] = pd.concat(disadv).sort_index()

    # filter for path types 1 & 2
    w = work[work["path_order_round"].isin(focus_paths)].copy()
    w["path_type"] = w["path_order_round"].map(TYPE_LABELS)

    # summarize by CoinSet + type
    def _count_bool(s: pd.Series) -> int:
        return int(np.sum(s.astype(bool)))

    summary = (
        w.groupby(["CoinSet", "path_type"], as_index=False)
        .agg(
            n=("ideal_distance", "size"),
            dominant_count=("is_dominant", _count_bool),
            disadvantaged_count=("is_disadvantaged", _count_bool),
            adv_ge_0_5=("adv_score", lambda s: int(np.sum(s >= 0.5))),
            adv_lt_0_5=("adv_score", lambda s: int(np.sum(s < 0.5))),
            adv_score_mean=("adv_score", "mean"),
            ideal_distance_mean=("ideal_distance", "mean"),
        )
    )

    summary.to_csv(os.path.join(out.summary_dir, "07_path12_advantage_disadvantage_by_CoinSet.csv"), index=False)

    # Bar chart: dominant vs disadvantaged counts by CoinSet for type1+type2
    for cs in sorted(pd.unique(summary["CoinSet"])):
        sub = summary[summary["CoinSet"] == cs].copy()

        labels = sub["path_type"].tolist()
        dom_counts = sub["dominant_count"].to_numpy()
        disadv_counts = sub["disadvantaged_count"].to_numpy()

        x = np.arange(len(labels))
        width = 0.38

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar(x - width / 2, dom_counts, width, label="Dominant (min distance)")
        ax.bar(x + width / 2, disadv_counts, width, label="Disadvantaged (max distance)")
        ax.set_title(f"Path Types 1 & 2: Dominant vs Disadvantaged — CoinSet={cs}")
        ax.set_ylabel("Count across start_pos_key")
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=15)
        ax.legend(frameon=False)

        _tight_save(fig, os.path.join(out.plot_dir, f"07_path12_dom_vs_disadv_CoinSet_{cs}.png"))

        # Bar chart: advantage threshold counts
        adv_ge = sub["adv_ge_0_5"].to_numpy()
        adv_lt = sub["adv_lt_0_5"].to_numpy()

        fig2, ax2 = plt.subplots(figsize=(10, 5))
        ax2.bar(x - width / 2, adv_ge, width, label="adv_score >= 0.5 (advantage)")
        ax2.bar(x + width / 2, adv_lt, width, label="adv_score < 0.5 (disadvantage)")
        ax2.set_title(f"Path Types 1 & 2: Advantage Threshold — CoinSet={cs}")
        ax2.set_ylabel("Count across start_pos_key")
        ax2.set_xticks(x)
        ax2.set_xticklabels(labels, rotation=15)
        ax2.legend(frameon=False)

        _tight_save(fig2, os.path.join(out.plot_dir, f"07_path12_advThreshold_CoinSet_{cs}.png"))

    return summary


def write_general_stats(df: pd.DataFrame, out: OutputPaths) -> None:
    # Helpful summary tables
    stats_coinset = (
        df.groupby("CoinSet")["ideal_distance"]
        .agg(["count", "mean", "std", "min", "median", "max"])
        .reset_index()
    )
    stats_coinset.to_csv(os.path.join(out.summary_dir, "00_stats_by_CoinSet.csv"), index=False)

    stats_coinset_path = (
        df.groupby(["CoinSet", "path_order_round"])["ideal_distance"]
        .agg(["count", "mean", "std", "min", "median", "max"])
        .reset_index()
    )
    stats_coinset_path.to_csv(os.path.join(out.summary_dir, "00_stats_by_CoinSet_and_path.csv"), index=False)

    stats_startpos_path = (
        df.groupby(["start_pos_key", "path_order_round"])["ideal_distance"]
        .agg(["count", "mean", "std", "min", "median", "max"])
        .reset_index()
    )
    stats_startpos_path.to_csv(os.path.join(out.summary_dir, "00_stats_by_startpos_and_path_allCoinSets.csv"), index=False)

# Add these imports (if not already present)
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from statsmodels.stats.anova import anova_lm
from statsmodels.stats.multicomp import pairwise_tukeyhsd


def partial_eta_squared(anova_table: pd.DataFrame, effect: str) -> float:
    """
    Partial eta-squared for a single effect:
        ηp² = SS_effect / (SS_effect + SS_error)
    where SS_error is the residual sum of squares.
    """
    ss_effect = float(anova_table.loc[effect, "sum_sq"])
    ss_error = float(anova_table.loc["Residual", "sum_sq"])
    return ss_effect / (ss_effect + ss_error)


def omega_squared(anova_table: pd.DataFrame, effect: str) -> float:
    """
    Omega-squared (less biased than eta-squared) for an effect in an OLS ANOVA:
        ω² = (SS_effect - df_effect * MS_error) / (SS_total + MS_error)
    Clipped at 0 to avoid negative values in small/noisy effects.
    """
    ss_effect = float(anova_table.loc[effect, "sum_sq"])
    df_effect = float(anova_table.loc[effect, "df"])

    ss_error = float(anova_table.loc["Residual", "sum_sq"])
    df_error = float(anova_table.loc["Residual", "df"])
    ms_error = ss_error / df_error

    ss_total = float(anova_table["sum_sq"].sum())

    w2 = (ss_effect - df_effect * ms_error) / (ss_total + ms_error)
    return float(max(0.0, w2))


def compute_effect_sizes_and_tukey(df: pd.DataFrame) -> dict:
    """
    Fits the OLS model:
        ideal_distance ~ C(CoinSet) + C(path_order_round) + C(CoinSet):C(start_pos_key)

    Returns:
      - fitted model
      - ANOVA table (Type II)
      - effect sizes table (partial eta^2 and omega^2)
      - Tukey HSD results for CoinSet and path_order_round (if desired)
    """
    # Ensure clean dtypes for statsmodels/patsy
    df = df.copy()
    df["CoinSet"] = df["CoinSet"].astype(str)
    df["path_order_round"] = df["path_order_round"].astype(str)
    df["start_pos_key"] = df["start_pos_key"].astype(str)

    # Fit OLS
    formula = "ideal_distance ~ C(CoinSet) + C(path_order_round) + C(CoinSet):C(start_pos_key)"
    model = smf.ols(formula, data=df).fit()

    # ANOVA (Type II)
    aov = anova_lm(model, typ=2)

    # Effect sizes for each tested term (exclude residual)
    effects = [ix for ix in aov.index if ix != "Residual"]
    es_rows = []
    for eff in effects:
        es_rows.append({
            "effect": eff,
            "partial_eta_sq": partial_eta_squared(aov, eff),
            "omega_sq": omega_squared(aov, eff),
            "df": aov.loc[eff, "df"],
            "F": aov.loc[eff, "F"],
            "p": aov.loc[eff, "PR(>F)"],
        })
    es = pd.DataFrame(es_rows).sort_values("partial_eta_sq", ascending=False)

    # Tukey HSD (post-hoc) for main effects
    # Note: Tukey is typically used after a significant omnibus test,
    # and it assumes independent errors; with the blocking term present,
    # treat this as descriptive unless you do a proper marginal-means approach.
    tukey_coinset = pairwise_tukeyhsd(
        endog=df["ideal_distance"].values,
        groups=df["CoinSet"].values,
        alpha=0.05
    )
    tukey_path = pairwise_tukeyhsd(
        endog=df["ideal_distance"].values,
        groups=df["path_order_round"].values,
        alpha=0.05
    )

    return {
        "model": model,
        "anova": aov,
        "effect_sizes": es,
        "tukey_coinset": tukey_coinset,
        "tukey_path": tukey_path,
    }
# ============================
# Nonparametric within-CoinSet analysis (Friedman + Kendall's W + planned & full pairwise)
#   - Friedman omnibus per CoinSet (blocked by start_pos_key)
#   - Kendall's W effect size per CoinSet
#   - Pairwise comparisons within CoinSet:
#       * Wilcoxon signed-rank (exact-ish via SciPy when possible)
#       * Sign-flip permutation p-values (paired, distribution-free)
#       * Bootstrap CIs for paired mean/median differences
#       * Holm correction (within CoinSet) for both Wilcoxon & permutation p-values
#   - Optionally restrict to PLANNED comparisons to preserve power
# ============================




# ----------------------------
# Utilities: corrections, effect sizes, resampling
# ----------------------------

def holm_adjust(pvals: Sequence[float]) -> List[float]:
    """Holm step-down adjusted p-values (FWER)."""
    pvals = np.asarray(pvals, dtype=float)
    m = len(pvals)
    order = np.argsort(pvals)
    adjusted = np.empty(m, dtype=float)
    running_max = 0.0
    for k, idx in enumerate(order):
        adj = (m - k) * pvals[idx]
        running_max = max(running_max, adj)
        adjusted[idx] = min(1.0, running_max)
    return adjusted.tolist()


def kendalls_w_from_friedman(stat: float, n_blocks: int, k_conditions: int) -> float:
    """
    Kendall's W effect size from Friedman chi-square:
      W = chi2 / (n*(k-1))
    where n = number of blocks (subjects), k = number of conditions.
    """
    if n_blocks <= 0 or k_conditions <= 1:
        return np.nan
    return float(stat / (n_blocks * (k_conditions - 1)))


def sign_flip_permutation_pvalue(diffs: np.ndarray, n_perm: int = 20000, seed: int = 0) -> float:
    """
    Paired sign-flip permutation test (aka randomization test):
      H0: differences are symmetric around 0
    Test statistic: mean(diffs)
    Two-sided p-value computed by random sign flips.

    Works well for n=8 and is very robust.
    """
    diffs = np.asarray(diffs, dtype=float)
    diffs = diffs[~np.isnan(diffs)]
    n = diffs.size
    if n == 0:
        return np.nan
    if np.allclose(diffs, 0.0):
        return 1.0

    rng = np.random.default_rng(seed)
    obs = float(np.mean(diffs))

    # random sign flips
    signs = rng.choice([-1.0, 1.0], size=(n_perm, n), replace=True)
    perm_stats = np.mean(signs * diffs[None, :], axis=1)

    p = (np.sum(np.abs(perm_stats) >= abs(obs)) + 1.0) / (n_perm + 1.0)
    return float(p)


def bootstrap_ci_paired(
    diffs: np.ndarray,
    stat: str = "mean",
    n_boot: int = 20000,
    ci: float = 0.95,
    seed: int = 0,
) -> Tuple[float, float]:
    """
    Bootstrap CI for paired differences using resampling of blocks.
    diffs: vector of paired differences (A - B) across start_pos_key

    stat: "mean" or "median"
    Returns: (lower, upper) percentile CI
    """
    diffs = np.asarray(diffs, dtype=float)
    diffs = diffs[~np.isnan(diffs)]
    n = diffs.size
    if n == 0:
        return (np.nan, np.nan)

    rng = np.random.default_rng(seed)
    idx = rng.integers(0, n, size=(n_boot, n))
    samples = diffs[idx]

    if stat == "mean":
        vals = samples.mean(axis=1)
    elif stat == "median":
        vals = np.median(samples, axis=1)
    else:
        raise ValueError("stat must be 'mean' or 'median'")

    alpha = (1.0 - ci) / 2.0
    lo = np.quantile(vals, alpha)
    hi = np.quantile(vals, 1.0 - alpha)
    return (float(lo), float(hi))


def rank_biserial_from_wilcoxon(diffs: np.ndarray) -> float:
    """
    Rank-biserial correlation effect size for paired data.
    Here we approximate via:
      r_rb = (W_pos - W_neg) / (W_pos + W_neg)
    where W_pos = sum ranks of positive diffs, W_neg = sum ranks of negative diffs.
    This matches common definitions for Wilcoxon signed-rank.

    Returns NaN if all diffs are zero or not enough data.
    """
    diffs = np.asarray(diffs, dtype=float)
    diffs = diffs[~np.isnan(diffs)]
    diffs = diffs[diffs != 0]
    n = diffs.size
    if n == 0:
        return np.nan

    abs_d = np.abs(diffs)
    ranks = pd.Series(abs_d).rank(method="average").to_numpy()

    w_pos = float(np.sum(ranks[diffs > 0]))
    w_neg = float(np.sum(ranks[diffs < 0]))
    denom = w_pos + w_neg
    if denom == 0:
        return np.nan
    return float((w_pos - w_neg) / denom)

def winner_counts(df):
    idx = df.groupby(["CoinSet", "start_pos_key"])["ideal_distance"].idxmin()
    winners = df.loc[idx, ["CoinSet", "path_order_round"]]
    return (winners.groupby(["CoinSet", "path_order_round"])
            .size()
            .reset_index(name="wins_out_of_8")
            .sort_values(["CoinSet", "wins_out_of_8"], ascending=[True, False]))
# ----------------------------
# Core analysis
# ----------------------------

@dataclass(frozen=True)
class NonparamConfig:
    n_perm: int = 20000
    n_boot: int = 20000
    ci: float = 0.95
    seed: int = 0
    alpha: float = 0.05
    # If True: only compute pairwise within CoinSet if Friedman p <= alpha
    gate_pairwise_by_friedman: bool = False


def _pivot_coinset(g: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot to start_pos_key x path_order_round with ideal_distance as values.
    Drops start positions that don't have all path types.
    """
    pivot = g.pivot_table(
        index="start_pos_key",
        columns="path_order_round",
        values="ideal_distance",
        aggfunc="mean",
    ).dropna(axis=0, how="any")
    return pivot


def analyze_within_coinset_nonparam(
    df: pd.DataFrame,
    config: NonparamConfig = NonparamConfig(),
    planned_pairs: Optional[Sequence[Tuple[str, str]]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Per CoinSet:
      - Friedman omnibus test across path_order_round (blocked by start_pos_key)
      - Kendall's W effect size
      - Pairwise (within CoinSet) comparisons between path_order_round levels:
          Wilcoxon p, sign-flip permutation p
          Bootstrap CI for mean and median difference (A - B)
          Rank-biserial effect size

    planned_pairs:
      - If provided: only test these pairs (preserves power; recommended).
      - Else: test all 15 pairs among 6 paths.

    Returns:
      - friedman_summary: one row per CoinSet
      - pairwise_summary: many rows per CoinSet, with Holm corrections within CoinSet
    """
    work = df.copy()
    work["CoinSet"] = work["CoinSet"].astype(str)
    work["start_pos_key"] = work["start_pos_key"].astype(str)
    work["path_order_round"] = work["path_order_round"].astype(str)

    friedman_rows = []
    pairwise_rows = []

    for cs, g in work.groupby("CoinSet"):
        pivot = _pivot_coinset(g)
        n_blocks = pivot.shape[0]
        paths = list(pivot.columns)
        k = len(paths)

        if n_blocks < 3 or k < 3:
            friedman_rows.append(
                dict(CoinSet=cs, n_blocks=n_blocks, k_paths=k, friedman_chi2=np.nan, friedman_p=np.nan, kendalls_W=np.nan)
            )
            continue

        # Omnibus Friedman
        samples = [pivot[p].to_numpy() for p in paths]
        chi2, p_f = friedmanchisquare(*samples)
        W = kendalls_w_from_friedman(float(chi2), n_blocks=n_blocks, k_conditions=k)

        friedman_rows.append(
            dict(CoinSet=cs, n_blocks=n_blocks, k_paths=k, friedman_chi2=float(chi2), friedman_p=float(p_f), kendalls_W=W)
        )

        if config.gate_pairwise_by_friedman and not (p_f <= config.alpha):
            continue

        # Determine which pairs to test
        if planned_pairs is not None:
            pairs = [(a, b) for (a, b) in planned_pairs if a in paths and b in paths]
        else:
            pairs = list(itertools.combinations(paths, 2))

        # Pairwise tests
        wilcoxon_pvals = []
        perm_pvals = []

        # Keep all details, then Holm-adjust within CoinSet
        tmp_rows = []

        for a, b in pairs:
            diffs = (pivot[a] - pivot[b]).to_numpy()

            # Wilcoxon signed-rank (two-sided). With n=8 it may be conservative/discrete.
            try:
                w_stat, p_w = wilcoxon(pivot[a].to_numpy(), pivot[b].to_numpy(), zero_method="wilcox", alternative="two-sided")
            except ValueError:
                # e.g., all differences zero
                w_stat, p_w = (np.nan, 1.0)

            # Sign-flip permutation p-value (paired)
            p_perm = sign_flip_permutation_pvalue(diffs, n_perm=config.n_perm, seed=config.seed)

            # Effect sizes and CIs
            mean_diff = float(np.mean(diffs))
            med_diff = float(np.median(diffs))
            ci_mean = bootstrap_ci_paired(diffs, stat="mean", n_boot=config.n_boot, ci=config.ci, seed=config.seed)
            ci_med = bootstrap_ci_paired(diffs, stat="median", n_boot=config.n_boot, ci=config.ci, seed=config.seed)
            r_rb = rank_biserial_from_wilcoxon(diffs)

            tmp_rows.append(
                dict(
                    CoinSet=cs,
                    path_A=a,
                    path_B=b,
                    n_blocks=n_blocks,
                    mean_diff_A_minus_B=mean_diff,
                    mean_ci_low=ci_mean[0],
                    mean_ci_high=ci_mean[1],
                    median_diff_A_minus_B=med_diff,
                    median_ci_low=ci_med[0],
                    median_ci_high=ci_med[1],
                    rank_biserial=r_rb,
                    wilcoxon_stat=w_stat,
                    p_wilcoxon=float(p_w),
                    p_perm=float(p_perm),
                )
            )
            wilcoxon_pvals.append(float(p_w))
            perm_pvals.append(float(p_perm))

        # Holm adjustments (within this CoinSet)
        p_w_holm = holm_adjust(wilcoxon_pvals) if wilcoxon_pvals else []
        p_perm_holm = holm_adjust(perm_pvals) if perm_pvals else []

        for row, adj_w, adj_p in zip(tmp_rows, p_w_holm, p_perm_holm):
            row["p_wilcoxon_holm"] = adj_w
            row["p_perm_holm"] = adj_p
            row["reject_wilcoxon_holm_0_05"] = bool(adj_w < config.alpha)
            row["reject_perm_holm_0_05"] = bool(adj_p < config.alpha)
            pairwise_rows.append(row)

    friedman_summary = pd.DataFrame(friedman_rows).sort_values(["friedman_p", "CoinSet"])
    pairwise_summary = pd.DataFrame(pairwise_rows).sort_values(["CoinSet", "p_perm_holm", "p_wilcoxon_holm"])

    return friedman_summary, pairwise_summary


# ----------------------------
# Suggested planned comparisons (optional, improves power)
# ----------------------------

def default_planned_pairs() -> List[Tuple[str, str]]:
    """
    Example planned comparisons that often make sense in your context:
      - Compare same-value permutations (Type1 vs Type2; Type3 vs Type4; Type5 vs Type6)
      - Compare best-value bucket (30) against worst-value bucket (20) using a representative type,
        OR just do all within-value comparisons only (safer).
    Adjust this list to match your hypotheses.
    """
    return [
        ("HV->LV->NV", "LV->HV->NV"),  # both 30
        ("HV->NV->LV", "NV->HV->LV"),  # both 25
        ("LV->NV->HV", "NV->LV->HV"),  # both 20
    ]


# ----------------------------
# Example integration in your script's main():
# ----------------------------
# config = NonparamConfig(n_perm=50000, n_boot=20000, seed=1, gate_pairwise_by_friedman=False)
#
# # Option 1: test ALL pairs (15 per CoinSet)
# friedman_tbl, pairwise_tbl = analyze_within_coinset_nonparam(df, config=config, planned_pairs=None)
#
# # Option 2 (recommended for power): test only planned pairs
# planned = default_planned_pairs()
# friedman_tbl_plan, pairwise_tbl_plan = analyze_within_coinset_nonparam(df, config=config, planned_pairs=planned)
#
# # Save results:
# friedman_tbl.to_csv(os.path.join(out.summary_dir, "NP_withinCoinSet_friedman_kendallsW.csv"), index=False)
# pairwise_tbl.to_csv(os.path.join(out.summary_dir, "NP_withinCoinSet_pairwise_allPairs.csv"), index=False)
# friedman_tbl_plan.to_csv(os.path.join(out.summary_dir, "NP_withinCoinSet_friedman_kendallsW_planned.csv"), index=False)
# pairwise_tbl_plan.to_csv(os.path.join(out.summary_dir, "NP_withinCoinSet_pairwise_plannedPairs.csv"), index=False)
#
# # Print a concise view of significant results (permutation-Holm):
# sig = pairwise_tbl[pairwise_tbl["reject_perm_holm_0_05"]].copy()
# print(sig[["CoinSet", "path_A", "path_B", "mean_diff_A_minus_B", "mean_ci_low", "mean_ci_high", "p_perm_holm"]])
def within_start_spread_summary(df: pd.DataFrame) -> pd.DataFrame:
    g = (
        df.groupby(["CoinSet", "start_pos_key"])["ideal_distance"]
        .agg(start_min="min", start_max="max")
        .reset_index()
    )
    g["spread"] = g["start_max"] - g["start_min"]

    out = (
        g.groupby("CoinSet")["spread"]
        .agg(
            n_starts="count",
            spread_mean="mean",
            spread_median="median",
            spread_min="min",
            spread_max="max",
        )
        .reset_index()
        .sort_values("spread_mean", ascending=False)
    )
    return out

import itertools
import numpy as np
import pandas as pd
from scipy.stats import friedmanchisquare, wilcoxon


# Reuse holm_adjust, sign_flip_permutation_pvalue, bootstrap_ci_paired from your earlier section.

def kendalls_w_from_friedman(stat: float, n_blocks: int, k_conditions: int) -> float:
    if n_blocks <= 0 or k_conditions <= 1:
        return np.nan
    return float(stat / (n_blocks * (k_conditions - 1)))


def collapse_to_value_distance(
    df: pd.DataFrame,
    agg: str = "min",  # "min", "mean", "max"
) -> pd.DataFrame:
    """
    Collapse the two path types per value into a single distance per (CoinSet, start_pos_key, path_value).

    Returns a dataframe with columns:
      CoinSet, start_pos_key, path_value, value_distance
    """
    work = df.copy()
    work["CoinSet"] = work["CoinSet"].astype(str)
    work["start_pos_key"] = work["start_pos_key"].astype(str)
    # ensure path_value is plain int
    work["path_value"] = work["path_value"].astype(int)

    if agg == "min":
        fun = "min"
    elif agg == "mean":
        fun = "mean"
    elif agg == "max":
        fun = "max"
    else:
        raise ValueError("agg must be one of: 'min', 'mean', 'max'")

    collapsed = (
        work.groupby(["CoinSet", "start_pos_key", "path_value"], as_index=False)["ideal_distance"]
        .agg(value_distance=fun)
    )
    return collapsed


def analyze_within_coinset_by_value_nonparam(
    df: pd.DataFrame,
    agg_within_value: str = "min",
    n_perm: int = 50000,
    n_boot: int = 20000,
    ci: float = 0.95,
    seed: int = 0,
    alpha: float = 0.05,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Per CoinSet:
      - Friedman omnibus across path_value (20/25/30), blocking by start_pos_key
      - Kendall's W
      - Pairwise comparisons between values with:
          * Wilcoxon p + Holm
          * Sign-flip permutation p + Holm
          * Bootstrap CI for paired mean/median difference

    agg_within_value: how to collapse the two paths within each value
      - "min" (best-case), "mean" (typical), "max" (worst-case)

    Returns:
      - friedman_summary_value (one row per CoinSet)
      - pairwise_summary_value (three pairs per CoinSet)
    """
    collapsed = collapse_to_value_distance(df, agg=agg_within_value)

    friedman_rows = []
    pairwise_rows = []

    for cs, g in collapsed.groupby("CoinSet"):
        pivot = g.pivot_table(
            index="start_pos_key",
            columns="path_value",
            values="value_distance",
            aggfunc="mean",
        ).dropna(axis=0, how="any")

        # Expect columns 20,25,30; proceed with whatever is present
        values = sorted(pivot.columns.tolist())
        n_blocks = pivot.shape[0]
        k = len(values)

        if n_blocks < 3 or k < 3:
            friedman_rows.append(
                dict(CoinSet=cs, n_blocks=n_blocks, k_values=k, agg_within_value=agg_within_value,
                     friedman_chi2=np.nan, friedman_p=np.nan, kendalls_W=np.nan)
            )
            continue

        # Friedman omnibus
        samples = [pivot[v].to_numpy() for v in values]
        chi2, p_f = friedmanchisquare(*samples)
        W = kendalls_w_from_friedman(float(chi2), n_blocks=n_blocks, k_conditions=k)

        friedman_rows.append(
            dict(CoinSet=cs, n_blocks=n_blocks, k_values=k, agg_within_value=agg_within_value,
                 friedman_chi2=float(chi2), friedman_p=float(p_f), kendalls_W=W)
        )

        # Pairwise (3 pairs if values are [20,25,30])
        pairs = list(itertools.combinations(values, 2))
        p_w_list, p_perm_list = [], []
        tmp = []

        for a, b in pairs:
            diffs = (pivot[a] - pivot[b]).to_numpy()  # A - B

            # Wilcoxon
            try:
                w_stat, p_w = wilcoxon(pivot[a].to_numpy(), pivot[b].to_numpy(), zero_method="wilcox")
            except ValueError:
                w_stat, p_w = (np.nan, 1.0)

            # Permutation sign-flip
            p_perm = sign_flip_permutation_pvalue(diffs, n_perm=n_perm, seed=seed)

            mean_diff = float(np.mean(diffs))
            med_diff = float(np.median(diffs))
            ci_mean = bootstrap_ci_paired(diffs, stat="mean", n_boot=n_boot, ci=ci, seed=seed)
            ci_med = bootstrap_ci_paired(diffs, stat="median", n_boot=n_boot, ci=ci, seed=seed)

            tmp.append(dict(
                CoinSet=cs,
                agg_within_value=agg_within_value,
                value_A=int(a),
                value_B=int(b),
                n_blocks=n_blocks,
                mean_diff_A_minus_B=mean_diff,
                mean_ci_low=ci_mean[0],
                mean_ci_high=ci_mean[1],
                median_diff_A_minus_B=med_diff,
                median_ci_low=ci_med[0],
                median_ci_high=ci_med[1],
                wilcoxon_stat=w_stat,
                p_wilcoxon=float(p_w),
                p_perm=float(p_perm),
            ))
            p_w_list.append(float(p_w))
            p_perm_list.append(float(p_perm))

        # Holm within CoinSet
        p_w_holm = holm_adjust(p_w_list)
        p_perm_holm = holm_adjust(p_perm_list)

        for row, pwH, ppH in zip(tmp, p_w_holm, p_perm_holm):
            row["p_wilcoxon_holm"] = pwH
            row["p_perm_holm"] = ppH
            row["reject_wilcoxon_holm_0_05"] = bool(pwH < alpha)
            row["reject_perm_holm_0_05"] = bool(ppH < alpha)
            pairwise_rows.append(row)

    friedman_summary = pd.DataFrame(friedman_rows).sort_values(["friedman_p", "CoinSet"])
    pairwise_summary = pd.DataFrame(pairwise_rows).sort_values(["CoinSet", "p_perm_holm", "p_wilcoxon_holm"])
    return friedman_summary, pairwise_summary

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Plot ideal_distance distributions across CoinSets / start positions / path types.")
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--files", nargs="+", default=None, help="Explicit list of CSV files to load.")
    g.add_argument("--input_glob", default="ideal_routes_*.csv", help='Glob for CSV files (default: "ideal_routes_*.csv").')

    p.add_argument("--out_dir", default="plots_idealDist", help='Output directory (default: "plots_idealDist").')

    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.files is not None and len(args.files) > 0:
        files = args.files
    else:
        files = sorted(glob.glob(args.input_glob))

    if not files:
        raise SystemExit(f"No CSV files found. Tried: {args.files if args.files else args.input_glob}")

    out = make_output_paths(args.out_dir)

    df = load_files(files)

    # sanity checks: 8 start positions x 6 paths per CoinSet is common
    # but we won't hard-require it; we just warn in stdout-like style via print
    for cs, g in df.groupby("CoinSet"):
        n_start = g["start_pos_key"].nunique()
        n_path = g["path_order_round"].nunique()
        if n_start != 8 or n_path != 6:
            print(f"[WARN] CoinSet={cs}: start_pos_key={n_start} unique, path_order_round={n_path} unique (expected 8 and 6).")

    write_general_stats(df, out)

    # (1)-(5) distributions
    plot_1_distribution_by_coinset(df, out)
    plot_2_by_path_per_startpos(df, out)
    plot_3_by_path_per_coinset(df, out)
    plot_4_by_value_per_startpos(df, out)
    plot_5_by_value_per_coinset(df, out)

    # (6) correlation
    _ = plot_6_value_vs_distance_corr(df, out)

    # (7) path type 1 & 2 dominance/advantage analysis
    _ = analysis_7_path12_advantage_disadvantage(df, out)

    import statsmodels.formula.api as smf

    df1 = df.copy()
    df1["path_value"] = df1["path_value"].astype(int)          # or .astype("int64")
    df1["CoinSet"] = df1["CoinSet"].astype(str)
    df1["start_pos_key"] = df1["start_pos_key"].astype(str)
    df1["coinset_start"] = df1["CoinSet"] + ":" + df["start_pos_key"]
    #df["coinset_start"] = df["CoinSet"].astype(str) + ":" + df["start_pos_key"].astype(str)

    print('\n'*5)
    print("ideal_distance ~ C(CoinSet) * C(path_value)")
    m = smf.mixedlm(
        "ideal_distance ~ C(CoinSet) * C(path_value)",
        data=df1,
        groups=df1["coinset_start"],  # random intercept per CoinSet:start_pos_key
    ).fit(reml=True)

    print(m.summary())

    print('\n'*5)
    print("ideal_distance ~ C(CoinSet) + C(path_value)")
    m0 = smf.mixedlm(
        "ideal_distance ~ C(CoinSet) + C(path_value)",
        data=df1,
        groups=df1["coinset_start"],
    ).fit(reml=True, method="lbfgs")
    print(m0.summary())

    print('\n'*5)
    print("ideal_distance ~ C(CoinSet) + C(path_order_round)")
    df2 = df1.copy()
    df2["CoinSet"] = df2["CoinSet"].astype(str)
    df2["start_pos_key"] = df2["start_pos_key"].astype(str)
    df2["path_order_round"] = df2["path_order_round"].astype(str)

    m1 = smf.mixedlm(
        "ideal_distance ~ C(CoinSet) + C(path_order_round)",
        data=df2,
        groups=df2["start_pos_key"],   # <-- key change
    ).fit(reml=True, method="lbfgs")
    print(m1.summary())

    print('\n'*5)
    print("ideal_distance ~ C(CoinSet) + C(path_order_round) re_formula=1")
    m2 = smf.mixedlm(
        "ideal_distance ~ C(CoinSet) + C(path_order_round)",
        data=df2,
        groups=df2["start_pos_key"],
        re_formula="1"   # random intercept per start_pos_key
    ).fit(reml=True, method="lbfgs")
    print(m2.summary())

    import statsmodels.formula.api as smf
    from statsmodels.stats.anova import anova_lm

    print('\n'*5)
    print('OLS: "ideal_distance ~ C(CoinSet) + C(path_order_round) + C(CoinSet):C(start_pos_key)"')
    df1["CoinSet"] = df1["CoinSet"].astype(str)
    df1["start_pos_key"] = df1["start_pos_key"].astype(str)
    df1["path_order_round"] = df1["path_order_round"].astype(str)

    ols = smf.ols(
        "ideal_distance ~ C(CoinSet) + C(path_order_round) + C(CoinSet):C(start_pos_key)",
        data=df1
    ).fit()

    print(anova_lm(ols, typ=2))

    print('\n'*5)
    results = compute_effect_sizes_and_tukey(df1)
    print(results["anova"])
    print(results["effect_sizes"])
    print(results["tukey_coinset"].summary())
    print(results["tukey_path"].summary())

    print('\n'*5)
    print("Nonparametric within-CoinSet analysis (Friedman + Kendall's W + planned & full pairwise)")
    config = NonparamConfig(n_perm=50000, n_boot=20000, seed=1, gate_pairwise_by_friedman=False)
    friedman_tbl, pairwise_tbl = analyze_within_coinset_nonparam(df1, config=config, planned_pairs=None)
    friedman_tbl.to_csv(os.path.join(out.summary_dir, "NP_withinCoinSet_friedman_kendallsW.csv"), index=False)
    pairwise_tbl.to_csv(os.path.join(out.summary_dir, "NP_withinCoinSet_pairwise_allPairs.csv"), index=False)
    sig = pairwise_tbl[pairwise_tbl["reject_perm_holm_0_05"]].copy()
    print(sig[["CoinSet", "path_A", "path_B", "mean_diff_A_minus_B", "mean_ci_low", "mean_ci_high", "p_perm_holm"]])
    print(within_start_spread_summary(df1))
    print(winner_counts(df1))

    print('\n'*5)
    print('planned friedmans')
    planned = default_planned_pairs()
    friedman_tbl_plan, pairwise_tbl_plan = analyze_within_coinset_nonparam(df1, config=config, planned_pairs=planned)

    # Save results:
    friedman_tbl_plan.to_csv(os.path.join(out.summary_dir, "NP_withinCoinSet_friedman_kendallsW_planned.csv"), index=False)
    pairwise_tbl_plan.to_csv(os.path.join(out.summary_dir, "NP_withinCoinSet_pairwise_plannedPairs.csv"), index=False)

    # Print a concise view of significant results (permutation-Holm):
    sig_plan = pairwise_tbl_plan[pairwise_tbl_plan["reject_perm_holm_0_05"]].copy()
    print(sig_plan[["CoinSet", "path_A", "path_B", "mean_diff_A_minus_B", "mean_ci_low", "mean_ci_high", "p_perm_holm"]])

    print('\n'*5)
    print('analysis within coinset by value nonparam')
    # Best-case per value (min over the two paths in that value)
    fried_val_min, pair_val_min = analyze_within_coinset_by_value_nonparam(df1, agg_within_value="min")

    # Typical-case per value (mean over the two paths)
    fried_val_mean, pair_val_mean = analyze_within_coinset_by_value_nonparam(df1, agg_within_value="mean")

    fried_val_min.to_csv(os.path.join(out.summary_dir, "NP_withinCoinSet_value_friedman_min.csv"), index=False)
    pair_val_min.to_csv(os.path.join(out.summary_dir, "NP_withinCoinSet_value_pairwise_min.csv"), index=False)

    fried_val_mean.to_csv(os.path.join(out.summary_dir, "NP_withinCoinSet_value_friedman_mean.csv"), index=False)
    pair_val_mean.to_csv(os.path.join(out.summary_dir, "NP_withinCoinSet_value_pairwise_mean.csv"), index=False)


if __name__ == "__main__":
    main()