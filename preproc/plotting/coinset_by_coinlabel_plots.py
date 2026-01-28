#!/usr/bin/env python3
from __future__ import annotations

import argparse
from itertools import combinations
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from histoHelpers import (
    read_data,
    _slugify,
    _ensure_dirs,
    exclude_outliers,
)

# Optional seaborn: your suite already uses it, so this should be fine
import seaborn as sns
from scipy.stats import kruskal, ks_2samp, mannwhitneyu


# -----------------------------
# Outlier filtering (reuses your helper)
# -----------------------------
def maybe_filter_outliers(
    df: pd.DataFrame,
    *,
    use_outlier_filter: bool,
    filter_columns: list[str],
    outlier_groupby: list[str] | None,
    outlier_method: str,
    outlier_sigma: float,
    outlier_ddof: int,
    outlier_keep_na: bool = False,
) -> pd.DataFrame:
    if not use_outlier_filter:
        return df
    out = df.copy()
    groupby = outlier_groupby if outlier_groupby else None
    for col in filter_columns:
        if col in out.columns:
            out = exclude_outliers(
                out,
                col,
                method=outlier_method,
                sigma=outlier_sigma,
                ddof=outlier_ddof,
                groupby=groupby,
                keep_na=outlier_keep_na,
            )
    return out


# -----------------------------
# Stats helpers
# -----------------------------
def cliffs_delta(x: np.ndarray, y: np.ndarray) -> float:
    x = np.asarray(x)
    y = np.asarray(y)
    if len(x) == 0 or len(y) == 0:
        return float("nan")
    U_greater = mannwhitneyu(x, y, alternative="greater", method="asymptotic").statistic
    A12 = U_greater / (len(x) * len(y))
    return float(2 * A12 - 1)


def fdr_bh(pvals: np.ndarray) -> np.ndarray:
    pvals = np.asarray(pvals, float)
    n = pvals.size
    if n == 0:
        return pvals
    order = np.argsort(pvals)
    ranked = pvals[order]
    q = np.empty_like(ranked)
    prev = 1.0
    for i in range(n - 1, -1, -1):
        rank = i + 1
        prev = min(prev, ranked[i] * n / rank)
        q[i] = prev
    out = np.empty_like(q)
    out[order] = q
    return out


# -----------------------------
# Core analysis: per coinLabel, compare CoinSetID
# -----------------------------
def plot_and_stats_for_coinlabel(
    df: pd.DataFrame,
    *,
    coinlabel: str,
    voi: str,
    out_dir: Path,
    coinlabel_col: str,
    coinset_col: str,
    min_n: int,
    formats: list[str],
) -> dict[str, Any]:
    sub = df[df[coinlabel_col].astype("string") == str(coinlabel)].copy()
    if sub.empty:
        raise ValueError(f"No rows for coinLabel={coinlabel!r}")

    # ensure numeric
    sub[voi] = pd.to_numeric(sub[voi], errors="coerce")
    sub = sub[sub[voi].notna() & sub[coinset_col].notna()].copy()
    if sub.empty:
        raise ValueError(f"No numeric data for {voi} at coinLabel={coinlabel!r}")

    # order coinsets
    order = sorted(sub[coinset_col].astype("string").unique().tolist(), key=lambda s: (s == "Other", s))

    # ---- plot: violin + points + hist overlay
    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    sns.violinplot(data=sub, x=coinset_col, y=voi, order=order, inner="quartile", ax=axes[0])
    sns.stripplot(data=sub, x=coinset_col, y=voi, order=order, color="0.1", jitter=0.25, alpha=0.25, ax=axes[0])
    axes[0].set_xlabel(coinset_col)
    axes[0].set_ylabel(voi)
    axes[0].set_title("Violin + points")

    sns.histplot(
        data=sub, x=voi, hue=coinset_col, hue_order=order,
        stat="density", common_norm=False, element="step", alpha=0.35, ax=axes[1]
    )
    try:
        sns.kdeplot(data=sub, x=voi, hue=coinset_col, hue_order=order, common_norm=False, ax=axes[1], lw=2, legend=False)
    except Exception:
        pass
    axes[1].set_xlabel(voi)
    axes[1].set_ylabel("Density")
    axes[1].set_title("Hist + KDE by CoinSetID")

    fig.suptitle(f"coinLabel={coinlabel} — {voi} by CoinSetID", fontsize=14, y=1.03)
    fig.tight_layout()

    stem = f"coinLabel-{_slugify(coinlabel)}__{_slugify(voi)}__by_CoinSetID"
    for ext in formats:
        fig.savefig(out_dir / f"{stem}.{ext}", dpi=220, bbox_inches="tight")
    plt.close(fig)

    # ---- stats
    groups = {}
    for k, g in sub.groupby(coinset_col, dropna=False, sort=True):
        x = pd.to_numeric(g[voi], errors="coerce").dropna().to_numpy()
        if x.size >= min_n:
            groups[str(k)] = x

    omnibus = {"coinLabel": coinlabel, "voi": voi, "kruskal_H": np.nan, "kruskal_p": np.nan, "sizes": {}}
    pairwise = pd.DataFrame()

    if len(groups) >= 2:
        labels = sorted(groups.keys(), key=lambda s: (s == "Other", s))
        samples = [groups[k] for k in labels]
        kw = kruskal(*samples)
        omnibus["kruskal_H"] = float(kw.statistic)
        omnibus["kruskal_p"] = float(kw.pvalue)
        omnibus["sizes"] = {k: int(len(v)) for k, v in groups.items()}

        rows = []
        for a, b in combinations(labels, 2):
            xa, xb = groups[a], groups[b]
            ks = ks_2samp(xa, xb, alternative="two-sided", method="auto")
            rows.append({
                "coinLabel": coinlabel,
                "voi": voi,
                "A": a, "B": b,
                "n_A": int(len(xa)), "n_B": int(len(xb)),
                "KS_D": float(ks.statistic),
                "KS_p": float(ks.pvalue),
                "Cliffs_delta": float(cliffs_delta(xa, xb)),
            })
        pairwise = pd.DataFrame(rows)
        pairwise["KS_q"] = fdr_bh(pairwise["KS_p"].to_numpy())
        pairwise = pairwise.sort_values(["KS_q", "KS_p"], ignore_index=True)

    # write stats
    pd.DataFrame([omnibus]).to_csv(out_dir / f"{stem}__stats_omnibus.csv", index=False)
    if not pairwise.empty:
        pairwise.to_csv(out_dir / f"{stem}__stats_pairwise.csv", index=False)

    return {
        "coinLabel": coinlabel,
        "voi": voi,
        "out_dir": str(out_dir),
        "kruskal_p": omnibus["kruskal_p"],
        "sizes": omnibus["sizes"],
    }


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Facet by coinLabel and compare CoinSetID distributions (dropDist / truecontent_elapsed_s).")
    ap.add_argument("--input", required=True, help="Input CSV (PinDrops table)")
    ap.add_argument("--out-root", required=True, help="Output directory")
    ap.add_argument("--formats", default="png,pdf", help="Comma-separated formats (png,pdf)")
    ap.add_argument("--coinlabel-col", default="coinLabel")
    ap.add_argument("--coinset-col", default="CoinSetID")
    ap.add_argument("--voi", nargs="*", default=["dropDist", "truecontent_elapsed_s"])
    ap.add_argument("--min-n", type=int, default=10, help="Min N per CoinSetID group for stats")

    # basic filters (optional)
    ap.add_argument("--blockstatus-col", default="BlockStatus")
    ap.add_argument("--allowed-status", nargs="*", default=["complete"])
    ap.add_argument("--dropqual-col", default="dropQual")
    ap.add_argument("--keep-dropqual", nargs="*", default=["good", "bad"])
    ap.add_argument("--blocknum-col", default="BlockNum")
    ap.add_argument("--blocks-min", type=int, default=3, help="Keep BlockNum > this (set -1 to disable)")

    # outlier filtering (matches your suite style)
    ap.add_argument("--use-outlier-filter", action="store_true", help="Enable outlier removal")
    ap.add_argument("--filter-columns", nargs="*", default=None, help="Columns to apply outlier filtering to (default: VOIs)")
    ap.add_argument("--outlier-groupby", nargs="*", default=["coinLabel", "CoinSetID"], help="Columns for outlier grouping")
    ap.add_argument("--outlier-method", choices=["mean", "median"], default="median")
    ap.add_argument("--outlier-sigma", type=float, default=2.0)
    ap.add_argument("--outlier-ddof", type=int, default=1)

    args = ap.parse_args()

    in_path = Path(args.input)
    out_root = Path(args.out_root)
    _ensure_dirs(out_root)

    formats = [s.strip() for s in args.formats.split(",") if s.strip()]
    blocks_min = None if args.blocks_min < 0 else args.blocks_min

    df = read_data(in_path)

    # optional filters if columns exist
    if args.blockstatus_col in df.columns and args.allowed_status:
        allowed = {str(x).strip().lower() for x in args.allowed_status}
        df[args.blockstatus_col] = df[args.blockstatus_col].astype("string").str.strip().str.lower()
        df = df[df[args.blockstatus_col].isin(allowed)].copy()

    if blocks_min is not None and args.blocknum_col in df.columns:
        df[args.blocknum_col] = pd.to_numeric(df[args.blocknum_col], errors="coerce")
        df = df[df[args.blocknum_col].notna() & (df[args.blocknum_col] > float(blocks_min))].copy()

    if args.dropqual_col in df.columns and args.keep_dropqual:
        keep = {str(x).strip().lower() for x in args.keep_dropqual}
        df[args.dropqual_col] = df[args.dropqual_col].astype("string").str.strip().str.lower()
        df = df[df[args.dropqual_col].isin(keep)].copy()

    # outlier filtering (reuse helper)
    filter_cols = args.filter_columns if args.filter_columns is not None else list(args.voi)
    groupby = [c for c in args.outlier_groupby if c in df.columns]
    df = maybe_filter_outliers(
        df,
        use_outlier_filter=args.use_outlier_filter,
        filter_columns=filter_cols,
        outlier_groupby=groupby if groupby else None,
        outlier_method=args.outlier_method,
        outlier_sigma=args.outlier_sigma,
        outlier_ddof=args.outlier_ddof,
    )

    # iterate coinLabels from filtered df
    if args.coinlabel_col not in df.columns:
        raise SystemExit(f"Missing {args.coinlabel_col} in input.")
    coin_labels = sorted(df[args.coinlabel_col].dropna().astype("string").str.strip().unique().tolist())

    manifest_rows = []
    for voi in args.voi:
        voi_dir = out_root / f"VOI_{_slugify(voi)}"
        _ensure_dirs(voi_dir)

        if voi not in df.columns:
            continue

        for cl in coin_labels:
            cl_dir = voi_dir / f"coinLabel_{_slugify(cl)}"
            _ensure_dirs(cl_dir)
            try:
                row = plot_and_stats_for_coinlabel(
                    df,
                    coinlabel=cl,
                    voi=voi,
                    out_dir=cl_dir,
                    coinlabel_col=args.coinlabel_col,
                    coinset_col=args.coinset_col,
                    min_n=args.min_n,
                    formats=formats,
                )
                manifest_rows.append(row)
            except Exception as e:
                manifest_rows.append({"coinLabel": cl, "voi": voi, "error": str(e), "out_dir": str(cl_dir)})

    manifest = pd.DataFrame(manifest_rows)
    manifest.to_csv(out_root / "manifest_coinset_by_coinlabel.csv", index=False)
    print(f"[ok] wrote: {out_root / 'manifest_coinset_by_coinlabel.csv'}")


if __name__ == "__main__":
    main()
