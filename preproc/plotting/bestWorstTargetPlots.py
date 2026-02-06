#!/usr/bin/env python3
"""
Make per-start_pos bar plots (utility & distance facets) for THREE series per version:
- best-order row (max utility) per start_pos per version
- FIXED order row for order == 'HV->LV->NV' per start_pos per version (explicit middle bar)
- worst-order row (min utility) per start_pos per version

Adds:
- bestHVWorst_order_per_start_pos_per_version.csv   (best/target/worst in one file; column `which`)
- HV_LV_NV_order_per_start_pos_per_version.csv      (just the target order rows)

Plots:
- Separate plot per start_pos
- 2 facets: utility & distance
- per version: three bars in order (best, HV->LV->NV, worst)

Annotation scheme:
- utility facet: BEST + HV->LV->NV labels inside bars; WORST labels at end of bars
- distance facet: all labels inside bars (best + HV->LV->NV + worst)
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
TARGET_ORDER = "HV->LV->NV"


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
        key.append(int(p) if p.isdigit() else p.lower())
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


def pick_best_target_worst_rows_per_start_pos(df: pd.DataFrame, target_order: str) -> pd.DataFrame:
    """
    For each start_pos, select three rows:
      - best: maximum utility
      - target: order == target_order (if multiple, take max utility among those)
      - worst: minimum utility

    Output columns:
      start_pos, which(best|target|worst), order, utility, distance
    """
    df = df.copy()
    df["utility"] = pd.to_numeric(df["utility"], errors="coerce")
    df["distance"] = pd.to_numeric(df["distance"], errors="coerce")
    df = df.dropna(subset=["start_pos", "utility"])

    # best/worst from full df
    idx_best = df.groupby("start_pos")["utility"].idxmax()
    idx_worst = df.groupby("start_pos")["utility"].idxmin()

    best = df.loc[idx_best, ["start_pos", "order", "utility", "distance"]].copy()
    best["which"] = "best"

    worst = df.loc[idx_worst, ["start_pos", "order", "utility", "distance"]].copy()
    worst["which"] = "worst"

    # target from filtered df; if missing for some start_pos, we'll still create placeholder rows later in plotting
    df_t = df[df["order"].astype(str) == target_order].copy()
    if len(df_t) > 0:
        # if duplicates, pick the one with max utility per start_pos
        idx_t = df_t.groupby("start_pos")["utility"].idxmax()
        target = df_t.loc[idx_t, ["start_pos", "order", "utility", "distance"]].copy()
        target["which"] = "target"
    else:
        target = df.iloc[0:0][["start_pos", "order", "utility", "distance"]].copy()
        target["which"] = pd.Series(dtype=str)

    out = pd.concat([best, target, worst], ignore_index=True)
    return out


def safe_filename(s: str) -> str:
    s = str(s)
    s = re.sub(r"[^\w\-.]+", "_", s.strip())
    return s[:200] if len(s) > 200 else s


def plot_start_pos(sub: pd.DataFrame, start_pos_value: object, out_path: str, title_prefix: str = "") -> None:
    """
    Horizontal version of the plot:
    - versions are on the Y axis in both facets
    - three bars per version (best, TARGET_ORDER, worst) are grouped horizontally
    """
    sub = sub.copy()
    sub["version"] = sub["version"].astype(str)
    sub["which"] = pd.Categorical(sub["which"], categories=["best", "target", "worst"], ordered=True)

    versions_sorted = sorted(sub["version"].unique().tolist(), key=natural_sort_key)
    sub["version"] = pd.Categorical(sub["version"], categories=versions_sorted, ordered=True)
    sub = sub.sort_values(["version", "which"])

    util_p = sub.pivot_table(index="version", columns="which", values="utility", aggfunc="first")
    dist_p = sub.pivot_table(index="version", columns="which", values="distance", aggfunc="first")
    order_p = sub.pivot_table(index="version", columns="which", values="order", aggfunc="first")

    for col in ["best", "target", "worst"]:
        if col not in util_p.columns:
            util_p[col] = float("nan")
        if col not in dist_p.columns:
            dist_p[col] = float("nan")
        if col not in order_p.columns:
            order_p[col] = ""

    util_p = util_p.loc[versions_sorted]
    dist_p = dist_p.loc[versions_sorted]
    order_p = order_p.loc[versions_sorted]

    # y positions for versions
    y = list(range(len(versions_sorted)))

    # three bars per version stacked in a group on the y-axis
    height = 0.26
    y_best = [i - height for i in y]
    y_target = [i for i in y]
    y_worst = [i + height for i in y]

    fig_w = 10.5
    fig_h = max(6.5, 0.6 * len(versions_sorted) + 2.5)
    fig, axes = plt.subplots(1, 2, figsize=(fig_w, fig_h), dpi=150)
    ax_u, ax_d = axes

    # --- Utility facet (horizontal bars) ---
    bars_ub = ax_u.barh(y_best, util_p["best"].tolist(), height=height, label="best")
    bars_ut = ax_u.barh(y_target, util_p["target"].tolist(), height=height, label=TARGET_ORDER)
    bars_uw = ax_u.barh(y_worst, util_p["worst"].tolist(), height=height, label="worst")
    ax_u.set_title("utility")
    ax_u.set_yticks(y, [str(v) for v in versions_sorted])
    ax_u.set_xlabel("value")
    ax_u.grid(axis="x", linestyle="--", alpha=0.35)
    ax_u.legend()

    # --- Distance facet (horizontal bars) ---
    bars_db = ax_d.barh(y_best, dist_p["best"].tolist(), height=height, label="best")
    bars_dt = ax_d.barh(y_target, dist_p["target"].tolist(), height=height, label=TARGET_ORDER)
    bars_dw = ax_d.barh(y_worst, dist_p["worst"].tolist(), height=height, label="worst")
    ax_d.set_title("distance")
    ax_d.set_yticks(y, [str(v) for v in versions_sorted])
    ax_d.set_xlabel("value")
    ax_d.grid(axis="x", linestyle="--", alpha=0.35)
    ax_d.legend()

    def _clean_label(lab: object) -> str:
        if lab is None or (isinstance(lab, float) and pd.isna(lab)):
            return ""
        s = str(lab)
        return s if len(s) <= 28 else (s[:25] + "…")

    def annotate_inside_h(ax, bars, labels, color="white"):
        """Place labels inside horizontal bars (centered)."""
        for b, lab in zip(bars, labels):
            lab2 = _clean_label(lab)
            if not lab2:
                continue
            w = b.get_width()
            y0 = b.get_y() + b.get_height() / 2

            # x at middle of bar; if tiny bar, nudge slightly from origin
            if w >= 0:
                x0 = w * 0.5 if w >= 1e-6 else 0.02 * (ax.get_xlim()[1] - ax.get_xlim()[0])
                ha = "center"
            else:
                x0 = w * 0.5
                ha = "center"

            ax.text(
                x0, y0, lab2,
                ha=ha, va="center",
                fontsize=8,
                rotation=0,          # horizontal text is usually easier to read here
                color=color,
                clip_on=True,
            )

    def annotate_end_h(ax, bars, labels, color="black"):
        """Place labels at the end of horizontal bars."""
        ax.relim()
        ax.autoscale_view()
        xr = ax.get_xlim()[1] - ax.get_xlim()[0]
        offset = 0.01 * xr if xr else 0.1

        for b, lab in zip(bars, labels):
            lab2 = _clean_label(lab)
            if not lab2:
                continue
            w = b.get_width()
            y0 = b.get_y() + b.get_height() / 2

            if w >= 0:
                x0 = w + offset
                ha = "left"
            else:
                x0 = w - offset
                ha = "right"

            ax.text(
                x0, y0, lab2,
                ha=ha, va="center",
                fontsize=8,
                rotation=0,
                color=color,
                clip_on=False,
            )

    # Ensure limits computed before inside labels
    ax_u.relim(); ax_u.autoscale_view()
    ax_d.relim(); ax_d.autoscale_view()
    # after you create the bars (and after relim/autoscale if you keep those)
    ax_u.set_xlim(0.0, 20.0)
    ax_d.set_xlim(0.0, 20.0)


    # Annotation rules (same intent as before):
    # utility: best+target inside; worst at end
    annotate_inside_h(ax_u, bars_ub, order_p["best"].tolist(), color="white")
    annotate_inside_h(ax_u, bars_ut, order_p["target"].tolist(), color="white")
    annotate_end_h(ax_u, bars_uw, order_p["worst"].tolist(), color="black")

    # distance: all inside
    annotate_inside_h(ax_d, bars_db, order_p["best"].tolist(), color="white")
    annotate_inside_h(ax_d, bars_dt, order_p["target"].tolist(), color="white")
    annotate_inside_h(ax_d, bars_dw, order_p["worst"].tolist(), color="white")

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
    ap.add_argument(
        "--target-order",
        default=TARGET_ORDER,
        help=f"Order string to plot as the explicit middle bar (default: {TARGET_ORDER!r}).",
    )
    args = ap.parse_args()

    os.makedirs(args.output, exist_ok=True)

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
        btvw = pick_best_target_worst_rows_per_start_pos(df, target_order=args.target_order)
        btvw["version"] = version
        rows_all.append(btvw)

    combined = pd.concat(rows_all, ignore_index=True)

    # Save combined best/target/worst summary
    summary_path = os.path.join(args.output, "bestHVWorst_order_per_start_pos_per_version.csv")
    combined.to_csv(summary_path, index=False)

    # Save just the target-order rows
    target_only = combined[combined["which"] == "target"].copy()
    target_only_path = os.path.join(args.output, "HV_LV_NV_order_per_start_pos_per_version.csv")
    target_only.to_csv(target_only_path, index=False)

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
    print(f"Saved target-only: {target_only_path}")
    print(f"Saved plots to: {args.output}")


if __name__ == "__main__":
    main()
