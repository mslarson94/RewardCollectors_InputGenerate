# layoutFacetStats.py
from __future__ import annotations

from itertools import combinations
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import anderson_ksamp, kruskal, ks_2samp, mannwhitneyu

try:
    from scipy.stats import epps_singleton_2samp
    _HAS_ES = True
except Exception:
    _HAS_ES = False


def _fdr_bh(pvals: np.ndarray) -> np.ndarray:
    pvals = np.asarray(pvals, dtype=float)
    n = pvals.size
    if n == 0:
        return pvals
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


def _cliffs_delta(x: np.ndarray, y: np.ndarray) -> float:
    x = np.asarray(x)
    y = np.asarray(y)
    n, m = len(x), len(y)
    if n == 0 or m == 0:
        return np.nan
    u_greater = mannwhitneyu(x, y, alternative="greater", method="asymptotic").statistic
    a12 = u_greater / (n * m)
    return 2 * a12 - 1


def _coerce_analysis_df(df: pd.DataFrame, *, variable_of_interest: str) -> pd.DataFrame:
    req = [variable_of_interest, "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    out = df.copy()
    out[variable_of_interest] = pd.to_numeric(out[variable_of_interest], errors="coerce")
    out["coinLabel"] = out["coinLabel"].astype("string").str.strip()
    out["dropQual"] = out["dropQual"].astype("string").str.strip().str.lower()

    mask = (
        out[variable_of_interest].notna()
        & out["coinLabel"].notna()
        & out["dropQual"].isin(["good", "bad"])
    )
    return out.loc[mask].copy()


def _distribution_tests_for_groups(
    dat: pd.DataFrame,
    *,
    variable_of_interest: str,
    group_col: str,
    min_n_per_group: int,
    alpha: float,
    context_name: str,
    context_value: str,
) -> tuple[dict[str, Any], pd.DataFrame]:
    groups = {
        str(k): v[variable_of_interest].to_numpy()
        for k, v in dat.groupby(group_col, dropna=False)
    }
    groups = {k: x for k, x in groups.items() if len(x) >= min_n_per_group}
    if len(groups) < 2:
        raise ValueError(f"Need at least two {group_col} groups with n >= {min_n_per_group}.")

    labels = sorted(groups.keys())
    samples = [groups[k] for k in labels]

    ad_res = anderson_ksamp(samples)
    kw_res = kruskal(*samples)

    pair_rows: list[dict[str, Any]] = []
    for a, b in combinations(labels, 2):
        xa, xb = groups[a], groups[b]
        ks = ks_2samp(xa, xb, alternative="two-sided", method="auto")
        mwu = mannwhitneyu(xa, xb, alternative="two-sided", method="asymptotic")
        delta = _cliffs_delta(xa, xb)

        es_stat, es_p = np.nan, np.nan
        if _HAS_ES:
            try:
                es = epps_singleton_2samp(xa, xb)
                es_stat = float(es.statistic)
                es_p = float(es.pvalue)
            except Exception:
                pass

        pair_rows.append(
            {
                "context_name": context_name,
                "context_value": context_value,
                "comparison_group": group_col,
                "A": a,
                "B": b,
                "n_A": len(xa),
                "n_B": len(xb),
                "KS_D": float(ks.statistic),
                "KS_p": float(ks.pvalue),
                "MWU_U": float(mwu.statistic),
                "MWU_p": float(mwu.pvalue),
                "Cliffs_delta": float(delta),
                "ES_stat": es_stat,
                "ES_p": es_p,
            }
        )

    pair_df = pd.DataFrame(pair_rows)
    pair_df["KS_q"] = _fdr_bh(pair_df["KS_p"].to_numpy())
    pair_df["MWU_q"] = _fdr_bh(pair_df["MWU_p"].to_numpy())

    if _HAS_ES and pair_df["ES_p"].notna().any():
        mask = pair_df["ES_p"].notna()
        q = np.full(len(pair_df), np.nan)
        q[mask] = _fdr_bh(pair_df.loc[mask, "ES_p"].to_numpy())
        pair_df["ES_q"] = q
    else:
        pair_df["ES_q"] = np.nan

    omnibus = {
        "context_name": context_name,
        "context_value": context_value,
        "comparison_group": group_col,
        "labels": labels,
        "sizes": {k: len(v) for k, v in groups.items()},
        "anderson_ksamp": {
            "statistic": float(ad_res.statistic),
            "significance_level": float(ad_res.significance_level),
        },
        "kruskal": {
            "H": float(kw_res.statistic),
            "pvalue": float(kw_res.pvalue),
        },
        "alpha": float(alpha),
    }
    return omnibus, pair_df.sort_values(["KS_q", "KS_p"], ignore_index=True)


def _summarize_panel(
    dat: pd.DataFrame,
    *,
    variable_of_interest: str,
    panel_type: str,
    panel_value: str,
    group_col: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for group_value, sub in dat.groupby(group_col, dropna=False):
        x = pd.to_numeric(sub[variable_of_interest], errors="coerce").dropna()
        rows.append(
            {
                "panel_type": panel_type,
                "panel_value": panel_value,
                "group_col": group_col,
                "group_value": str(group_value),
                "n": int(len(x)),
                "mean": float(x.mean()) if len(x) else np.nan,
                "sd": float(x.std(ddof=1)) if len(x) > 1 else np.nan,
                "median": float(x.median()) if len(x) else np.nan,
                "q1": float(x.quantile(0.25)) if len(x) else np.nan,
                "q3": float(x.quantile(0.75)) if len(x) else np.nan,
                "iqr": float(x.quantile(0.75) - x.quantile(0.25)) if len(x) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def run_layout_first_stats(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    layout_col: str,
    min_n_per_group: int = 10,
    alpha: float = 0.05,
) -> dict[str, Any]:
    if layout_col not in df.columns:
        raise ValueError(f"Missing layout column: {layout_col}")

    dat = _coerce_analysis_df(df, variable_of_interest=variable_of_interest)

    omnibus_rows: list[dict[str, Any]] = []
    pairwise_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    verbose_chunks: list[str] = []

    panels: list[tuple[str, pd.DataFrame]] = [("ALL", dat)]
    levels = sorted([x for x in dat[layout_col].dropna().unique().tolist()], key=lambda x: str(x))
    panels.extend((str(v), dat.loc[dat[layout_col] == v].copy()) for v in levels)

    for panel_value, subdf in panels:
        summary_frames.append(
            _summarize_panel(
                subdf,
                variable_of_interest=variable_of_interest,
                panel_type="layout_first",
                panel_value=panel_value,
                group_col="coinLabel",
            )
        )

        try:
            omnibus, pair_df = _distribution_tests_for_groups(
                subdf,
                variable_of_interest=variable_of_interest,
                group_col="coinLabel",
                min_n_per_group=min_n_per_group,
                alpha=alpha,
                context_name=layout_col,
                context_value=panel_value,
            )
        except Exception as exc:
            omnibus_rows.append(
                {
                    "panel_type": "layout_first",
                    "context_name": layout_col,
                    "context_value": panel_value,
                    "status": "skipped",
                    "reason": str(exc),
                }
            )
            verbose_chunks.append(f"[layout-first: {layout_col}={panel_value}] skipped: {exc}")
            continue

        omnibus_rows.append(
            {
                "panel_type": "layout_first",
                "context_name": layout_col,
                "context_value": panel_value,
                "status": "ok",
                "ad_statistic": omnibus["anderson_ksamp"]["statistic"],
                "ad_p_approx": omnibus["anderson_ksamp"]["significance_level"] / 100.0,
                "kw_H": omnibus["kruskal"]["H"],
                "kw_p": omnibus["kruskal"]["pvalue"],
                "group_sizes": omnibus["sizes"],
            }
        )
        pairwise_frames.append(pair_df)

        ad_p = omnibus["anderson_ksamp"]["significance_level"] / 100.0
        kw_p = omnibus["kruskal"]["pvalue"]
        sig_pairs = int((pair_df["KS_q"] <= alpha).sum())
        strong_pairs = int(((pair_df["KS_q"] <= alpha) & (pair_df["Cliffs_delta"].abs() >= 0.33)).sum())

        verbose_chunks.append(
            "\n".join(
                [
                    f"{variable_of_interest} & Coin Type [{layout_col}={panel_value}]",
                    "== Omnibus tests ==",
                    f"Anderson–Darling k-sample: A² = {omnibus['anderson_ksamp']['statistic']:.3f}, approx p ≈ {ad_p:.4g}",
                    f"Kruskal–Wallis: H = {omnibus['kruskal']['H']:.3f}, p = {kw_p:.4g}",
                    "",
                    "== Pairwise (BH FDR on KS and MWU) ==",
                    pair_df[
                        ["A", "B", "n_A", "n_B", "KS_D", "KS_p", "KS_q", "MWU_U", "MWU_p", "MWU_q", "Cliffs_delta"]
                    ].to_string(index=False, float_format=lambda x: f"{x:.4g}"),
                    "",
                    "Cliff's δ thresholds (|δ|): small≈0.147, medium≈0.33, large≈0.474",
                    "",
                    "Heuristic summary:",
                    f"- Omnibus difference detected ({'yes' if (ad_p <= alpha or kw_p <= alpha) else 'no'}).",
                    f"- Pairwise KS: {sig_pairs}/{len(pair_df)} significant at FDR q≤{alpha}.",
                    f"- {strong_pairs} pair(s) show ≥medium effect (|δ|≥0.33).",
                    "",
                ]
            )
        )

    return {
        "omnibus": pd.DataFrame(omnibus_rows),
        "pairwise": pd.concat(pairwise_frames, ignore_index=True) if pairwise_frames else pd.DataFrame(),
        "summary": pd.concat(summary_frames, ignore_index=True) if summary_frames else pd.DataFrame(),
        "verbose_text": "\n".join(verbose_chunks).strip() + ("\n" if verbose_chunks else ""),
    }


def run_coin_type_first_stats(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    layout_col: str,
    min_n_per_group: int = 10,
    alpha: float = 0.05,
) -> dict[str, Any]:
    if layout_col not in df.columns:
        raise ValueError(f"Missing layout column: {layout_col}")

    dat = _coerce_analysis_df(df, variable_of_interest=variable_of_interest)

    omnibus_rows: list[dict[str, Any]] = []
    pairwise_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    verbose_chunks: list[str] = []

    coin_levels = sorted([x for x in dat["coinLabel"].dropna().unique().tolist()], key=lambda x: str(x))

    for coin_label in coin_levels:
        subdf = dat.loc[dat["coinLabel"] == coin_label].copy()

        summary_frames.append(
            _summarize_panel(
                subdf,
                variable_of_interest=variable_of_interest,
                panel_type="coin_type_first",
                panel_value=str(coin_label),
                group_col=layout_col,
            )
        )

        try:
            omnibus, pair_df = _distribution_tests_for_groups(
                subdf,
                variable_of_interest=variable_of_interest,
                group_col=layout_col,
                min_n_per_group=min_n_per_group,
                alpha=alpha,
                context_name="coinLabel",
                context_value=str(coin_label),
            )
        except Exception as exc:
            omnibus_rows.append(
                {
                    "panel_type": "coin_type_first",
                    "context_name": "coinLabel",
                    "context_value": str(coin_label),
                    "status": "skipped",
                    "reason": str(exc),
                }
            )
            verbose_chunks.append(f"[coin-type-first: coinLabel={coin_label}] skipped: {exc}")
            continue

        omnibus_rows.append(
            {
                "panel_type": "coin_type_first",
                "context_name": "coinLabel",
                "context_value": str(coin_label),
                "status": "ok",
                "ad_statistic": omnibus["anderson_ksamp"]["statistic"],
                "ad_p_approx": omnibus["anderson_ksamp"]["significance_level"] / 100.0,
                "kw_H": omnibus["kruskal"]["H"],
                "kw_p": omnibus["kruskal"]["pvalue"],
                "group_sizes": omnibus["sizes"],
            }
        )
        pairwise_frames.append(pair_df)

        ad_p = omnibus["anderson_ksamp"]["significance_level"] / 100.0
        kw_p = omnibus["kruskal"]["pvalue"]
        sig_pairs = int((pair_df["KS_q"] <= alpha).sum())
        strong_pairs = int(((pair_df["KS_q"] <= alpha) & (pair_df["Cliffs_delta"].abs() >= 0.33)).sum())

        verbose_chunks.append(
            "\n".join(
                [
                    f"{variable_of_interest} & {layout_col} [coinLabel={coin_label}]",
                    "== Omnibus tests ==",
                    f"Anderson–Darling k-sample: A² = {omnibus['anderson_ksamp']['statistic']:.3f}, approx p ≈ {ad_p:.4g}",
                    f"Kruskal–Wallis: H = {omnibus['kruskal']['H']:.3f}, p = {kw_p:.4g}",
                    "",
                    "== Pairwise (BH FDR on KS and MWU) ==",
                    pair_df[
                        ["A", "B", "n_A", "n_B", "KS_D", "KS_p", "KS_q", "MWU_U", "MWU_p", "MWU_q", "Cliffs_delta"]
                    ].to_string(index=False, float_format=lambda x: f"{x:.4g}"),
                    "",
                    "Cliff's δ thresholds (|δ|): small≈0.147, medium≈0.33, large≈0.474",
                    "",
                    "Heuristic summary:",
                    f"- Omnibus difference detected ({'yes' if (ad_p <= alpha or kw_p <= alpha) else 'no'}).",
                    f"- Pairwise KS: {sig_pairs}/{len(pair_df)} significant at FDR q≤{alpha}.",
                    f"- {strong_pairs} pair(s) show ≥medium effect (|δ|≥0.33).",
                    "",
                ]
            )
        )

    return {
        "omnibus": pd.DataFrame(omnibus_rows),
        "pairwise": pd.concat(pairwise_frames, ignore_index=True) if pairwise_frames else pd.DataFrame(),
        "summary": pd.concat(summary_frames, ignore_index=True) if summary_frames else pd.DataFrame(),
        "verbose_text": "\n".join(verbose_chunks).strip() + ("\n" if verbose_chunks else ""),
    }