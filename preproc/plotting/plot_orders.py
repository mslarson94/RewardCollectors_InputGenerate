#!/usr/bin/env python3
"""
Faceted *horizontal* bar plots (Seaborn) for utility by order, faceted by start_pos,
with colorblind-safe colors per order AND a robust legend centered below the grid.

Example:
  python plot_orders.py /mnt/data/all_orders__layout_B.csv --outdir plots
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.patches import Patch


def _set_seaborn_style() -> None:
    sns.set_theme(style="whitegrid", context="talk")


def _safe_catplot_barh(
    *,
    data: pd.DataFrame,
    x: str,
    y: str,
    col: str,
    col_wrap: int,
    order: list[str],
    palette: dict[str, tuple[float, float, float]],
):
    common = dict(
        data=data,
        kind="bar",
        x=x,
        y=y,
        col=col,
        col_wrap=col_wrap,
        order=order,         # categorical order for y
        hue=y,               # color each bar by order
        hue_order=order,     # consistent ordering
        palette=palette,     # explicit mapping
        dodge=False,
        sharex=True,
        height=3.2,
        aspect=1.25,
        legend=False,        # we will add our own legend
    )
    # seaborn>=0.12 uses errorbar; seaborn<=0.11 uses ci
    try:
        return sns.catplot(**common, errorbar=None)
    except TypeError:
        return sns.catplot(**common, ci=None)


def _add_palette_legend_below(
    g: sns.axisgrid.FacetGrid,
    *,
    order_levels: list[str],
    palette: dict[str, tuple[float, float, float]],
    title: str = "Order",
    ncol: int | None = None,
    y_anchor: float = -0.02,
    bottom: float = 0.22,
) -> None:
    """
    Robust legend below the grid, built from the palette mapping (never empty).
    """
    handles = [Patch(facecolor=palette[o], edgecolor="black", label=o) for o in order_levels]

    if ncol is None:
        ncol = min(len(handles), 6) if handles else 1

    g.fig.legend(
        handles=handles,
        labels=[h.get_label() for h in handles],
        title=title,
        loc="lower center",
        bbox_to_anchor=(0.5, y_anchor),
        ncol=ncol,
        frameon=True,
        handlelength=1.4,
        columnspacing=1.0,
    )

    # Make room for legend at bottom
    g.fig.subplots_adjust(bottom=bottom)


def plot_faceted_utility_horizontal(
    df: pd.DataFrame,
    *,
    outpath: Path,
    col_wrap: int = 4,
    sort_orders_by: str = "utility",
) -> None:
    required = {"start_pos", "order", "utility"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df = df.copy()
    df["order"] = df["order"].astype(str)
    df["start_pos"] = df["start_pos"].astype(str)

    # Sort orders by global mean utility so facets are comparable
    order_levels = (
        df.groupby("order", as_index=False)[sort_orders_by]
        .mean()
        .sort_values(sort_orders_by, ascending=False)["order"]
        .tolist()
    )

    # Colorblind-safe palette mapped to each order
    colors = sns.color_palette("colorblind", n_colors=len(order_levels))
    palette = {ord_name: colors[i] for i, ord_name in enumerate(order_levels)}

    g = _safe_catplot_barh(
        data=df,
        x="utility",
        y="order",
        col="start_pos",
        col_wrap=col_wrap,
        order=order_levels,
        palette=palette,
    )

    g.set_titles(col_template="{col_name}")
    g.set_axis_labels("Utility", "Order")

    _add_palette_legend_below(
        g,
        order_levels=order_levels,
        palette=palette,
        title="Order",
        ncol=min(len(order_levels), 6),
        y_anchor=-0.02,
        bottom=0.25,
    )

    g.fig.subplots_adjust(top=0.90)
    g.fig.suptitle("Utility by Order (faceted by start_pos)")

    outpath.parent.mkdir(parents=True, exist_ok=True)
    g.fig.savefig(outpath, dpi=200, bbox_inches="tight")
    plt.close(g.fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", type=Path, help="Input CSV (must contain start_pos, order, utility)")
    ap.add_argument("--outdir", type=Path, default=Path("plots"), help="Output directory")
    ap.add_argument("--col-wrap", type=int, default=4, help="Facet wrap columns")
    ap.add_argument(
        "--outfile",
        type=str,
        default="utility_by_order_faceted_horizontal_colorblind_legend_below.png",
        help="Output filename",
    )
    args = ap.parse_args()

    _set_seaborn_style()
    df = pd.read_csv(args.csv)

    outpath = args.outdir / args.outfile
    plot_faceted_utility_horizontal(df, outpath=outpath, col_wrap=args.col_wrap)
    print(f"Wrote: {outpath.resolve()}")


if __name__ == "__main__":
    main()
