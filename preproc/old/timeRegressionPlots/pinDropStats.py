# =========================
# file: pinDropStats.py
# =========================
from __future__ import annotations

from itertools import combinations

import numpy as np
import pandas as pd
from scipy.stats import anderson_ksamp, kruskal, ks_2samp

try:
    from scipy.stats import epps_singleton_2samp
    HAS_EPPS_SINGLETON = True
except Exception:
    HAS_EPPS_SINGLETON = False

from pinDropHelpers import prepare_hist_data, require_columns


def _fdr_bh(pvals: np.ndarray) -> np.ndarray:
    pvals = np.asarray(pvals, dtype=float)
    n = len(pvals)

    if n == 0:
        return pvals

    order = np.argsort(pvals)
    ranked = pvals[order]
    adjusted = ranked * n / np.arange(1, n + 1)
    adjusted = np.minimum.accumulate(adjusted[::-1])[::-1]
    adjusted = np.clip(adjusted, 0.0, 1.0)

    result = np.empty_like(adjusted)
    result[order] = adjusted
    return result


def _cliffs_delta(x: np.ndarray, y: np.ndarray) -> float:
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)

    if len(x) == 0 or len(y) == 0:
        return np.nan

    greater = 0
    lower = 0
    for value in x:
        greater += np.sum(value > y)
        lower += np.sum(value < y)

    return float((greater - lower) / (len(x) * len(y)))


def test_coin_distributions(
    df: pd.DataFrame,
    *,
    variable_of_interest: str = "truecontent_elapsed_s",
    blocks_min: int = 3,
    min_n_per_group: int = 10,
    alpha: float = 0.05,
) -> dict[str, object]:
    require_columns(
        df,
        ["BlockNum", "BlockStatus", variable_of_interest, "coinLabel", "dropQual"],
        label="Stats data",
    )

    dat = prepare_hist_data(
        df,
        variable_of_interest=variable_of_interest,
        blocks_min=blocks_min,
    )
    dat[variable_of_interest] = pd.to_numeric(dat[variable_of_interest], errors="coerce")
    dat = dat.dropna(subset=[variable_of_interest, "coinLabel"]).reset_index(drop=True)

    if dat.empty:
        raise ValueError("No data remain after filtering.")

    groups = {
        coin: frame[variable_of_interest].to_numpy(dtype=float)
        for coin, frame in dat.groupby("coinLabel", dropna=False)
    }
    groups = {coin: values for coin, values in groups.items() if len(values) >= min_n_per_group}

    if len(groups) < 2:
        raise ValueError("Need at least two coin groups with sufficient data.")

    labels = sorted(groups)
    samples = [groups[label] for label in labels]

    ad_res = anderson_ksamp(samples)
    kw_res = kruskal(*samples)

    pair_rows: list[dict[str, float | str | int]] = []
    for left, right in combinations(labels, 2):
        x = groups[left]
        y = groups[right]

        ks = ks_2samp(x, y, alternative="two-sided", method="auto")

        es_stat = np.nan
        es_p = np.nan
        if HAS_EPPS_SINGLETON:
            try:
                es = epps_singleton_2samp(x, y)
                es_stat = float(es.statistic)
                es_p = float(es.pvalue)
            except Exception:
                pass

        pair_rows.append(
            {
                "A": left,
                "B": right,
                "n_A": int(len(x)),
                "n_B": int(len(y)),
                "KS_D": float(ks.statistic),
                "KS_p": float(ks.pvalue),
                "ES_stat": es_stat,
                "ES_p": es_p,
                "Cliffs_delta": float(_cliffs_delta(x, y)),
            }
        )

    pairwise = pd.DataFrame(pair_rows)
    pairwise["KS_q"] = _fdr_bh(pairwise["KS_p"].to_numpy(dtype=float))

    if HAS_EPPS_SINGLETON and pairwise["ES_p"].notna().any():
        mask = pairwise["ES_p"].notna()
        qs = np.full(len(pairwise), np.nan, dtype=float)
        qs[mask.to_numpy()] = _fdr_bh(pairwise.loc[mask, "ES_p"].to_numpy(dtype=float))
        pairwise["ES_q"] = qs
    else:
        pairwise["ES_q"] = np.nan

    pairwise = pairwise.sort_values(["KS_q", "KS_p"], ignore_index=True)

    return {
        "labels": labels,
        "sizes": {label: int(len(groups[label])) for label in labels},
        "anderson_ksamp": {
            "statistic": float(ad_res.statistic),
            "significance_level": float(ad_res.significance_level),
        },
        "kruskal": {
            "H": float(kw_res.statistic),
            "pvalue": float(kw_res.pvalue),
        },
        "pairwise": pairwise,
        "alpha": float(alpha),
    }


