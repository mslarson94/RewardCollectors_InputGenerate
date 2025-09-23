from __future__ import annotations
from itertools import combinations
import numpy as np
import pandas as pd
from scipy.stats import (
    anderson_ksamp, kruskal, ks_2samp, mannwhitneyu
)

from histoHelpers import _opt_mask  # for _enough_for_stats

try:
    from scipy.stats import epps_singleton_2samp  # pairwise, distributional
    _HAS_ES = True
except Exception:
    _HAS_ES = False

def _fdr_bh(pvals):
    """Benjamini–Hochberg FDR correction (returns q-values in original order)."""
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
    voi_str: str = "Round Elapsed Time",
    blocks_min: int = 3,
    min_n_per_group: int = 10,
    alpha: float = 0.05,
    verbose: bool = True,
):
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
    groups = {k: x for k, x in groups.items() if len(x) >= min_n_per_group}
    if len(groups) < 2:
        raise ValueError("Need at least two coin types with sufficient data.")

    labels = sorted(groups.keys())
    samples = [groups[k] for k in labels]

    # Omnibus
    ad_res = anderson_ksamp(samples)  # statistic, critical_values, significance_level
    kw_res = kruskal(*samples)        # H, pvalue

    # Pairwise + effect sizes
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
            "significance_level": float(ad_res.significance_level),
        },
        "kruskal": {"H": float(kw_res.statistic), "pvalue": float(kw_res.pvalue)},
        "pairwise": pair_df.sort_values(["KS_q", "KS_p"], ignore_index=True),
        "alpha": alpha,
    }

    if verbose:
        print(f"{voi_str} & Coin Type")
        print("== Omnibus tests ==")
        print(f"Anderson–Darling k-sample: A² = {ad_res.statistic:.3f}, approx p ≈ {ad_res.significance_level/100:.4f}")
        print(f"Kruskal–Wallis: H = {kw_res.statistic:.3f}, p = {kw_res.pvalue:.4g}")
        print("\n== Pairwise (Benjamini–Hochberg FDR on KS) ==")
        show_cols = ["A","B","n_A","n_B","KS_D","KS_p","KS_q","Cliffs_delta"]
        if _HAS_ES:
            show_cols += ["ES_stat","ES_p","ES_q"]
        print(pair_df[show_cols].to_string(index=False, float_format=lambda x: f"{x:.4g}"))
        print("\nCliff's δ thresholds (|δ|): small≈0.147, medium≈0.33, large≈0.474")

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
    mask &= _opt_mask(sdf, "dropQual", values=["good", "bad"])

    dat = sdf.loc[mask, [variableOfInterest, "coinLabel"]]
    if dat.empty:
        return False, "no rows after filtering"
    counts = dat.groupby("coinLabel").size()
    counts = counts[counts >= min_n_per_group]
    if len(counts) < 2:
        return False, "need ≥2 coin groups with sufficient rows"
    return True, ""
