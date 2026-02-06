#!/usr/bin/env python3
"""
Make per-start_pos bar plots (utility & distance facets) for BOTH:
- best-order row (max utility) per start_pos per version
- worst-order row (min utility) per start_pos per version

Adds:
- bestWorst_order_per_start_pos_per_version.csv
- plots per start_pos where utility facet shows best vs worst side-by-side,
  and distance facet shows the corresponding distances for best vs worst.
"""

from __future__ import annotations

import argparse
import glob
import os
import re
from typing import Dict, List, Tuple

import pandas as pd
import matplotlib.pyplot as plt


REQUIRED_CANONICAL = ["start_pos", "order", "utility", "distance"]


def parse_version_from_filename(path: str) -> str:
    base = os.path.basename(path)
    m = re.search(r"_([^_]+)\.csv$", base)
    if not m:
        raise ValueError(
            f"Could not parse version from filename '{base}'. "
            "Expected something like '..._<version>.csv'."
        )
    return m.group(1)


def natural_sort_key(s: str) -> Tuple:
    parts = re.split(r"(\d+)", str(s))
    key: List[object] = []
    for p in parts:
        if p.isdigit():
            key.append(int(p))
        else:
            key.append(p.lower())
    return tuple(key)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    norm_map: Dict[str, str] = {}
    for c in df.columns:
        norm = str(c).strip().lower()
        norm_map[norm] = c

    missing = [c for c in REQUIRED_CANONICAL if c not in norm_map]
    if missing:
        raise ValueError(
            "Missing required columns. "
            f"Need {REQUIRED_CANONICAL}. "
            f"Found columns: {list(df.columns)}. "
            f"Missing: {missing}"
        )

    renames = {norm_map[k]: k for k in REQUIRED_CANONICAL}
    return df.rename(columns=renames)


def pick_best_worst_rows_per_start_pos(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each start_pos, select two rows:
      - best: maximum utility
      - worst: minimum utility

    Output columns:
      start_pos, which(best|worst), order, utility, distance
    """
    df = df.copy()

    df["utility"] = pd.to_numeric(df["utility"], errors="coerce")
    df["distance"] = pd.to_numeric(df["distance"], errors="coerce")
    df = df.dropna(subset=["start_pos", "utility"])

    # idxmax/idxmin will return first occurrence on ties
    idx_best = df.groupby("start_pos")["utility"].idxmax()
    idx_worst = df.groupby("start_pos")["utility"].idxmin()

    best = df.loc[idx_best, ["start_pos", "order", "utility", "distance"]].copy()
    best["which"] = "best"

    worst = df.loc[idx_worst, ["start_pos", "order", "utility", "distance"]].copy()
    worst["which"] = "worst"

    out = pd.concat([best, worst], ignore_index=True)
    return out


def safe_filename(s: str) -> str:
    s = str(s)
    s = re.sub(r"[^\w\-.]+", "_", s.strip())
    return s[:200] if len(s) > 200 else s


def plot_start_pos(sub: pd.DataFrame, start_pos_value: object, out_path: str, title_prefix: str = "") -> None:
    """
    sub columns: version, which(best|worst), order, utility, distance

    2 facets:
      - utility: best vs worst bars side-by-side per version
      - distance: best vs worst bars side-by-side per version

    Label behavior (per your request):
      - utility facet: put ONLY the BEST order labels inside the best bars
      - distance facet: put BOTH best and worst order labels inside their bars
    """
    sub = sub.copy()
    sub["version"] = sub["version"].astype(str)
    sub["which"] = pd.Categorical(sub["which"], categories=["best", "worst"], ordered=True)

    versions_sorted = sorted(sub["version"].unique().tolist(), key=natural_sort_key)
    sub["version"] = pd.Categorical(sub["version"], categories=versions_sorted, ordered=True)
    sub = sub.sort_values(["version", "which"])

    util_p = sub.pivot_table(index="version", columns="which", values="utility", aggfunc="first")
    dist_p = sub.pivot_table(index="version", columns="which", values="distance", aggfunc="first")
    order_p = sub.pivot_table(index="version", columns="which", values="order", aggfunc="first")

    for col in ["best", "worst"]:
        if col not in util_p.columns:
            util_p[col] = float("nan")
        if col not in dist_p.columns:
            dist_p[col] = float("nan")
        if col not in order_p.columns:
            order_p[col] = ""

    util_p = util_p.loc[versions_sorted]
    dist_p = dist_p.loc[versions_sorted]
    order_p = order_p.loc[versions_sorted]

    x = list(range(len(versions_sorted)))
    width = 0.38
    x_best = [i - width / 2 for i in x]
    x_worst = [i + width / 2 for i in x]

    # Slightly taller figure so titles/axes breathe, labels are inside bars anyway
    fig_w = max(9, 1.4 * len(versions_sorted))
    fig_h = 6.2
    fig, axes = plt.subplots(1, 2, figsize=(fig_w, fig_h), dpi=150)
    ax_u, ax_d = axes

    # Utility facet
    bars_ub = ax_u.bar(x_best, util_p["best"].tolist(), width=width, label="best")
    bars_uw = ax_u.bar(x_worst, util_p["worst"].tolist(), width=width, label="worst")
    ax_u.set_title("utility")
    ax_u.set_xticks(x, [str(v) for v in versions_sorted], rotation=45, ha="right")
    ax_u.set_ylabel("value")
    ax_u.grid(axis="y", linestyle="--", alpha=0.35)
    ax_u.legend()

    # Distance facet
    bars_db = ax_d.bar(x_best, dist_p["best"].tolist(), width=width, label="best")
    bars_dw = ax_d.bar(x_worst, dist_p["worst"].tolist(), width=width, label="worst")
    ax_d.set_title("distance")
    ax_d.set_xticks(x, [str(v) for v in versions_sorted], rotation=45, ha="right")
    ax_d.set_ylabel("value")
    ax_d.grid(axis="y", linestyle="--", alpha=0.35)
    ax_d.legend()

    def _clean_label(lab: object) -> str:
        if lab is None or (isinstance(lab, float) and pd.isna(lab)):
            return ""
        s = str(lab)
        return s if len(s) <= 28 else (s[:25] + "…")

    # Replace annotate_inside(...) with these two helpers:

    def annotate_inside(ax, bars, labels, rotation=90, color="white"):
        """Place labels inside bars (centered vertically)."""
        for b, lab in zip(bars, labels):
            lab2 = _clean_label(lab)
            if not lab2:
                continue

            h = b.get_height()
            x0 = b.get_x() + b.get_width() / 2

            if h >= 0:
                y0 = h * 0.5 if h >= 1e-6 else 0.02 * (ax.get_ylim()[1] - ax.get_ylim()[0])
                va = "center"
            else:
                y0 = h * 0.5
                va = "center"

            ax.text(
                x0, y0, lab2,
                ha="center", va=va,
                fontsize=8, rotation=rotation,
                color=color,
                clip_on=True,
            )


    def annotate_end(ax, bars, labels, rotation=90, color="black"):
        """Place labels at the end of bars (like before)."""
        # ensure limits are up-to-date for a reasonable offset
        ax.relim()
        ax.autoscale_view()
        yr = ax.get_ylim()[1] - ax.get_ylim()[0]
        offset = 0.01 * yr if yr else 0.1

        for b, lab in zip(bars, labels):
            lab2 = _clean_label(lab)
            if not lab2:
                continue

            h = b.get_height()
            x0 = b.get_x() + b.get_width() / 2

            if h >= 0:
                y0 = h + offset
                va = "bottom"
            else:
                y0 = h - offset
                va = "top"

            ax.text(
                x0, y0, lab2,
                ha="center", va=va,
                fontsize=8, rotation=rotation,
                color=color,
                clip_on=False,
            )

            

    # Make sure y-limits are computed before placing inside labels
    ax_u.relim(); ax_u.autoscale_view()
    ax_d.relim(); ax_d.autoscale_view()

    # Make sure y-limits are computed before labeling
    ax_u.relim(); ax_u.autoscale_view()
    ax_d.relim(); ax_d.autoscale_view()

    # utility facet:
    # - best labels INSIDE best bars
    annotate_inside(ax_u, bars_ub, order_p["best"].tolist(), rotation=90, color="white")
    # - worst labels at END of worst bars (like before)
    annotate_end(ax_u, bars_uw, order_p["worst"].tolist(), rotation=90, color="black")

    # distance facet (keep both inside, as you wanted earlier)
    annotate_inside(ax_d, bars_db, order_p["best"].tolist(), rotation=90, color="white")
    annotate_inside(ax_d, bars_dw, order_p["worst"].tolist(), rotation=90, color="white")



    # - distance: BOTH best and worst labels inside their bars
    annotate_inside(ax_d, bars_db, order_p["best"].tolist(), rotation=90)
    annotate_inside(ax_d, bars_dw, order_p["worst"].tolist(), rotation=90)

    sp = str(start_pos_value)
    supt = f"{title_prefix}start_pos = {sp}" if title_prefix else f"start_pos = {sp}"
    fig.suptitle(supt)
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)



def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        required=True,
        help="Input directory containing CSVs, or a glob pattern like '/path/*.csv'.",
    )
    ap.add_argument(
        "--output",
        required=True,
        help="Output directory for plots and summary CSV.",
    )
    ap.add_argument(
        "--pattern",
        default="*.csv",
        help="If --input is a directory, glob pattern inside it (default: *.csv).",
    )
    ap.add_argument(
        "--title-prefix",
        default="",
        help="Optional prefix added to plot titles (e.g., 'experiment 1 | ').",
    )
    args = ap.parse_args()

    os.makedirs(args.output, exist_ok=True)

    # Resolve files
    if os.path.isdir(args.input):
        files = sorted(glob.glob(os.path.join(args.input, args.pattern)))
    else:
        files = sorted(glob.glob(args.input))

    if not files:
        raise SystemExit(f"No CSV files found for input '{args.input}' (pattern '{args.pattern}').")

    rows_all: List[pd.DataFrame] = []

    for f in files:
        version = parse_version_from_filename(f)
        df = pd.read_csv(f)
        df = normalize_columns(df)
        bw = pick_best_worst_rows_per_start_pos(df)  # includes "which"
        bw["version"] = version
        rows_all.append(bw)

    combined = pd.concat(rows_all, ignore_index=True)

    # Save summary table
    summary_path = os.path.join(args.output, "bestWorst_order_per_start_pos_per_version.csv")
    combined.to_csv(summary_path, index=False)

    # Plot per start_pos
    for start_pos_val, sub in combined.groupby("start_pos", sort=False):
        out_file = os.path.join(args.output, f"start_pos_{safe_filename(start_pos_val)}.png")
        plot_start_pos(
            sub=sub[["version", "which", "order", "utility", "distance"]],
            start_pos_value=start_pos_val,
            out_path=out_file,
            title_prefix=args.title_prefix,
        )

    print(f"Saved summary: {summary_path}")
    print(f"Saved plots to: {args.output}")


if __name__ == "__main__":
    main()
