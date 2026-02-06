#!/usr/bin/env python3
"""
Compare two fixed orders across versions (x-axis), faceted by utility & distance,
with best/worst (from precomputed bestHVWorst file) shown as small red horizontal
markers spanning BOTH bars for each version group.

Inputs:
  1) bestHVWorst_order_per_start_pos_per_version.csv   (already generated)
     - must include columns: start_pos, version, which (best|target|worst or best|worst at least), utility, distance
  2) HV_LV_NV_order_per_start_pos_per_version.csv      (already generated; target rows for HV->LV->NV)
     - must include columns: start_pos, version, utility, distance (order optional)
  3) Raw per-version CSVs (only needed to grab LV->HV->NV rows for the first time)
     - version parsed from filename: text between last "_" and ".csv"

Outputs:
  - comparison_twoOrders_by_start_pos.csv
  - one PNG per start_pos: start_pos_<...>_two_orders.png
"""

from __future__ import annotations

import argparse
import glob
import os
import re
from typing import Dict, List, Tuple

import pandas as pd
import matplotlib.pyplot as plt


ORDER_A_DEFAULT = "HV->LV->NV"
ORDER_B_DEFAULT = "LV->HV->NV"


def natural_sort_key(s: str) -> Tuple:
    parts = re.split(r"(\d+)", str(s))
    key: List[object] = []
    for p in parts:
        key.append(int(p) if p.isdigit() else p.lower())
    return tuple(key)


def parse_version_from_filename(path: str) -> str:
    base = os.path.basename(path)
    m = re.search(r"_([^_]+)\.csv$", base)
    if not m:
        raise ValueError(
            f"Could not parse version from filename '{base}'. "
            "Expected something like '..._<version>.csv'."
        )
    return m.group(1)


def safe_filename(s: object) -> str:
    s = str(s)
    s = re.sub(r"[^\w\-.]+", "_", s.strip())
    return s[:200] if len(s) > 200 else s


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Expect at least these for raw CSVs:
    required = ["start_pos", "order", "utility", "distance"]
    norm_map: Dict[str, str] = {str(c).strip().lower(): c for c in df.columns}
    missing = [c for c in required if c not in norm_map]
    if missing:
        raise ValueError(f"Missing columns {missing}. Found: {list(df.columns)}")
    renames = {norm_map[k]: k for k in required}
    return df.rename(columns=renames)


def load_hv_target(hv_csv_path: str, order_a: str) -> pd.DataFrame:
    """
    Load the already-generated HV target file.
    We treat it as the source for ORDER_A values.
    """
    hv = pd.read_csv(hv_csv_path)
    hv_cols = {c.lower(): c for c in hv.columns}

    for need in ["start_pos", "version", "utility", "distance"]:
        if need not in hv_cols:
            raise ValueError(f"{hv_csv_path} missing required column '{need}'. Found {list(hv.columns)}")

    hv = hv.rename(columns={hv_cols["start_pos"]: "start_pos",
                            hv_cols["version"]: "version",
                            hv_cols["utility"]: "utility",
                            hv_cols["distance"]: "distance"})

    # if "order" exists, keep; else create
    if "order" in hv_cols:
        hv = hv.rename(columns={hv_cols["order"]: "order"})
    else:
        hv["order"] = order_a

    hv["order"] = hv["order"].astype(str)
    hv["version"] = hv["version"].astype(str)
    hv["utility"] = pd.to_numeric(hv["utility"], errors="coerce")
    hv["distance"] = pd.to_numeric(hv["distance"], errors="coerce")

    # Force label
    hv["order_kind"] = order_a
    return hv[["start_pos", "version", "order_kind", "utility", "distance"]]

def load_metric_extrema_from_raw_csvs(input_spec: str, pattern: str) -> pd.DataFrame:
    """
    Compute true per-(start_pos, version) extrema for BOTH metrics from raw CSVs:
      utility_max, utility_min, distance_max, distance_min
    """
    if os.path.isdir(input_spec):
        files = sorted(glob.glob(os.path.join(input_spec, pattern)))
    else:
        files = sorted(glob.glob(input_spec))

    if not files:
        raise SystemExit(f"No raw CSVs found for input '{input_spec}' (pattern '{pattern}').")

    out_rows: List[pd.DataFrame] = []

    for f in files:
        version = parse_version_from_filename(f)
        df = pd.read_csv(f)
        df = normalize_columns(df)

        df["utility"] = pd.to_numeric(df["utility"], errors="coerce")
        df["distance"] = pd.to_numeric(df["distance"], errors="coerce")
        df = df.dropna(subset=["start_pos", "utility", "distance"])

        agg = (
            df.groupby("start_pos", as_index=False)
              .agg(
                  utility_max=("utility", "max"),
                  utility_min=("utility", "min"),
                  distance_max=("distance", "max"),
                  distance_min=("distance", "min"),
              )
        )
        agg["version"] = str(version)
        out_rows.append(agg)

    extrema = pd.concat(out_rows, ignore_index=True)
    extrema["version"] = extrema["version"].astype(str)
    return extrema[["start_pos", "version", "utility_max", "utility_min", "distance_min", "distance_max"]]

def load_best_worst(besthvworst_csv_path: str) -> pd.DataFrame:
    """
    Load best/worst per (start_pos, version) for marker lines.

    Expects columns: start_pos, version, which, utility, distance
    where which includes at least: best, worst
    """
    bw = pd.read_csv(besthvworst_csv_path)
    bw_cols = {c.lower(): c for c in bw.columns}

    for need in ["start_pos", "version", "which", "utility", "distance"]:
        if need not in bw_cols:
            raise ValueError(
                f"{besthvworst_csv_path} missing required column '{need}'. Found {list(bw.columns)}"
            )

    bw = bw.rename(
        columns={
            bw_cols["start_pos"]: "start_pos",
            bw_cols["version"]: "version",
            bw_cols["which"]: "which",
            bw_cols["utility"]: "utility",
            bw_cols["distance"]: "distance",
        }
    )

    bw["version"] = bw["version"].astype(str)
    bw["which"] = bw["which"].astype(str).str.lower()
    bw["utility"] = pd.to_numeric(bw["utility"], errors="coerce")
    bw["distance"] = pd.to_numeric(bw["distance"], errors="coerce")

    # Keep only best & worst rows
    bw = bw[bw["which"].isin(["best", "worst"])].copy()

    # Pivot: columns become a 2-level MultiIndex: (metric, which)
    piv = bw.pivot_table(
        index=["start_pos", "version"],
        columns="which",
        values=["utility", "distance"],
        aggfunc="first",
    )

    # Flatten to: best_utility, worst_utility, best_distance, worst_distance
    piv.columns = [f"{which}_{metric}" for metric, which in piv.columns]
    out = piv.reset_index()

    # Ensure columns exist even if some missing
    for col in ["best_utility", "worst_utility", "best_distance", "worst_distance"]:
        if col not in out.columns:
            out[col] = float("nan")

    return out[["start_pos", "version", "best_utility", "worst_utility", "best_distance", "worst_distance"]]



def load_order_from_raw_csvs(input_spec: str, pattern: str, target_order: str) -> pd.DataFrame:
    """
    Read raw CSVs to extract (start_pos, version) values for target_order.
    If duplicates exist for same (start_pos, version), pick max utility among them.
    """
    if os.path.isdir(input_spec):
        files = sorted(glob.glob(os.path.join(input_spec, pattern)))
    else:
        files = sorted(glob.glob(input_spec))

    if not files:
        raise SystemExit(f"No raw CSVs found for input '{input_spec}' (pattern '{pattern}').")

    rows: List[pd.DataFrame] = []

    for f in files:
        version = parse_version_from_filename(f)
        df = pd.read_csv(f)
        df = normalize_columns(df)
        df["version"] = version

        df["utility"] = pd.to_numeric(df["utility"], errors="coerce")
        df["distance"] = pd.to_numeric(df["distance"], errors="coerce")
        df = df.dropna(subset=["start_pos", "utility"])

        df = df[df["order"].astype(str) == target_order].copy()
        if df.empty:
            continue

        # if multiple, take max utility for that start_pos
        idx = df.groupby("start_pos")["utility"].idxmax()
        pick = df.loc[idx, ["start_pos", "version", "utility", "distance"]].copy()
        pick["order_kind"] = target_order
        rows.append(pick)

    if rows:
        out = pd.concat(rows, ignore_index=True)
    else:
        out = pd.DataFrame(columns=["start_pos", "version", "utility", "distance", "order_kind"])

    out["version"] = out["version"].astype(str)
    return out[["start_pos", "version", "order_kind", "utility", "distance"]]


def plot_start_pos_two_orders(
    data_long: pd.DataFrame,
    extrema: pd.DataFrame,
    start_pos_value: object,
    out_path: str,
    order_a: str,
    order_b: str,
) -> None:
    """
    data_long: columns [start_pos, version, order_kind, utility, distance] for exactly two order_kinds
    bestworst: columns [start_pos, version, best_utility, worst_utility, best_distance, worst_distance]
    """
    sub = data_long[data_long["start_pos"] == start_pos_value].copy()
    if sub.empty:
        return

    versions = sorted(sub["version"].unique().tolist(), key=natural_sort_key)
    sub["version"] = pd.Categorical(sub["version"], categories=versions, ordered=True)

    # Build wide for plotting
    util_w = sub.pivot_table(index="version", columns="order_kind", values="utility", aggfunc="first").reindex(versions)
    dist_w = sub.pivot_table(index="version", columns="order_kind", values="distance", aggfunc="first").reindex(versions)

    for col in [order_a, order_b]:
        if col not in util_w.columns:
            util_w[col] = float("nan")
        if col not in dist_w.columns:
            dist_w[col] = float("nan")

    ex = extrema[extrema["start_pos"] == start_pos_value].copy()
    ex["version"] = pd.Categorical(ex["version"].astype(str), categories=versions, ordered=True)
    ex = ex.set_index("version").reindex(versions).reset_index()


    x = list(range(len(versions)))
    width = 0.38
    x_a = [i - width / 2 for i in x]
    x_b = [i + width / 2 for i in x]

    fig_w = max(10, 1.35 * len(versions))
    fig_h = 6.2
    fig, axes = plt.subplots(1, 2, figsize=(fig_w, fig_h), dpi=150)
    ax_u, ax_d = axes

    # Utility bars
    bars_ua = ax_u.bar(x_a, util_w[order_a].tolist(), width=width, label=order_a)
    bars_ub = ax_u.bar(x_b, util_w[order_b].tolist(), width=width, label=order_b)
    ax_u.set_title("utility")
    ax_u.set_xticks(x, [str(v) for v in versions], rotation=45, ha="right")
    ax_u.set_ylabel("value")
    ax_u.grid(axis="y", linestyle="--", alpha=0.35)
    ax_u.legend()

    # Distance bars
    bars_da = ax_d.bar(x_a, dist_w[order_a].tolist(), width=width, label=order_a)
    bars_db = ax_d.bar(x_b, dist_w[order_b].tolist(), width=width, label=order_b)
    ax_d.set_title("distance")
    ax_d.set_xticks(x, [str(v) for v in versions], rotation=45, ha="right")
    ax_d.set_ylabel("value")
    ax_d.grid(axis="y", linestyle="--", alpha=0.35)
    ax_d.legend()

    # --- Red best/worst horizontal markers spanning BOTH bars per version group ---
    # Marker spans from left edge of first bar to right edge of second bar.
    # Using the known group extents: (i - width) to (i + width)
    def draw_markers_utility(ax):
        for i, (u_max, u_min) in enumerate(zip(ex["utility_max"].tolist(), ex["utility_min"].tolist())):
            if pd.notna(u_max):
                ax.hlines(u_max, i - width, i + width, colors="black", linewidth=2.0)  # max utility
            if pd.notna(u_min):
                ax.hlines(u_min, i - width, i + width, colors="black", linewidth=2.0, linestyles="dashed")  # min utility

    def draw_markers_distance(ax):
        for i, (d_min, d_max) in enumerate(zip(ex["distance_min"].tolist(), ex["distance_max"].tolist())):
            if pd.notna(d_min):
                ax.hlines(d_min, i - width, i + width, colors="black", linewidth=2.0)  # min distance (best)
            if pd.notna(d_max):
                ax.hlines(d_max, i - width, i + width, colors="black", linewidth=2.0, linestyles="dashed")  # max distance (worst)

    draw_markers_utility(ax_u)
    draw_markers_distance(ax_d)
    ax_u.set_ylim(0.0, 20.0)
    ax_d.set_ylim(10.0, 21.0)

    sp = str(start_pos_value)
    fig.suptitle(f"start_pos = {sp} | {order_a} vs {order_b} (black=best/worst)")
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hv-csv", required=True, help="Path to HV_LV_NV_order_per_start_pos_per_version.csv")
    ap.add_argument("--raw-input", required=True, help="Directory or glob for raw CSVs (only used to fetch LV->HV->NV)")
    ap.add_argument("--pattern", default="*.csv", help="If raw-input is a directory, glob pattern (default: *.csv)")
    ap.add_argument("--output", required=True, help="Output directory for plots and merged CSV")
    ap.add_argument("--order-a", default=ORDER_A_DEFAULT, help="First order to compare (default: HV->LV->NV)")
    ap.add_argument("--order-b", default=ORDER_B_DEFAULT, help="Second order to compare (default: LV->HV->NV)")
    args = ap.parse_args()

    os.makedirs(args.output, exist_ok=True)

    # Load extrema
    extrema = load_metric_extrema_from_raw_csvs(args.raw_input, args.pattern)

    # Load ORDER_A values from the already-generated HV target file
    a_df = load_hv_target(args.hv_csv, order_a=args.order_a)

    # Load ORDER_B values by scanning raw CSVs (first time)
    b_df = load_order_from_raw_csvs(args.raw_input, args.pattern, target_order=args.order_b)

    # Combine
    combined = pd.concat([a_df, b_df], ignore_index=True)

    # Save merged comparison table
    out_csv = os.path.join(args.output, "comparison_twoOrders_by_start_pos.csv")
    combined.to_csv(out_csv, index=False)

    # Plot per start_pos
    start_positions = sorted(combined["start_pos"].dropna().unique().tolist(), key=natural_sort_key)
    for sp in start_positions:
        out_png = os.path.join(args.output, f"start_pos_{safe_filename(sp)}_two_orders.png")
        plot_start_pos_two_orders(
            data_long=combined,
            extrema=extrema,
            start_pos_value=sp,
            out_path=out_png,
            order_a=args.order_a,
            order_b=args.order_b,
        )


    print(f"Saved merged comparison CSV: {out_csv}")
    print(f"Saved plots to: {args.output}")


if __name__ == "__main__":
    main()
