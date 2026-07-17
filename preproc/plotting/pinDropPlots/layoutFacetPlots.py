# layoutFacetPlots.py
from __future__ import annotations

import math

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def _prep_plot_data(df: pd.DataFrame, variable_of_interest: str) -> pd.DataFrame:
    req = [variable_of_interest, "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for plotting: {missing}")

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


def plot_histkde_layout_first(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    layout_col: str,
    voi_str: str = "Measure",
    voi_unit: str = "",
    bins: int | str = "auto",
    palette: str | dict = "tab10",
    ncols: int = 3,
) -> None:
    if layout_col not in df.columns:
        raise ValueError(f"Missing layout column: {layout_col}")

    dat = _prep_plot_data(df, variable_of_interest)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    levels = sorted([x for x in dat[layout_col].dropna().unique().tolist()], key=lambda x: str(x))
    panels = [("All data", dat)] + [(f"{layout_col}={v}", dat.loc[dat[layout_col] == v].copy()) for v in levels]

    hue_order = sorted(dat["coinLabel"].astype(str).unique().tolist())
    if isinstance(palette, dict):
        color_map = {k: palette.get(k, "#333333") for k in hue_order}
    else:
        color_map = dict(zip(hue_order, sns.color_palette(palette, n_colors=len(hue_order))))

    n_panels = len(panels)
    nrows = math.ceil(n_panels / ncols)

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(5.3 * ncols, 4.2 * nrows), squeeze=False)
    axes_flat = axes.ravel()

    for ax, (title, sub) in zip(axes_flat, panels):
        sns.histplot(
            data=sub,
            x=variable_of_interest,
            hue="coinLabel",
            hue_order=hue_order,
            bins=bins,
            stat="density",
            common_norm=False,
            element="step",
            multiple="layer",
            alpha=0.35,
            palette=color_map,
            ax=ax,
            legend=(title == "All data"),
        )
        sns.kdeplot(
            data=sub,
            x=variable_of_interest,
            hue="coinLabel",
            hue_order=hue_order,
            common_norm=False,
            palette=color_map,
            lw=2,
            legend=False,
            ax=ax,
        )
        ax.set_title(title, fontsize=12)
        ax.set_xlabel(f"{voi_str} {voi_unit}".strip())
        ax.set_ylabel("Density")

    for ax in axes_flat[n_panels:]:
        ax.set_visible(False)

    fig.suptitle(f"{voi_str} distribution by coin type within layout", fontsize=15, y=0.995)
    fig.tight_layout()
    plt.show()


def plot_violin_layout_first(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    layout_col: str,
    voi_str: str = "Measure",
    voi_unit: str = "",
    palette: str | dict = "tab10",
    ncols: int = 3,
) -> None:
    if layout_col not in df.columns:
        raise ValueError(f"Missing layout column: {layout_col}")

    dat = _prep_plot_data(df, variable_of_interest)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    levels = sorted([x for x in dat[layout_col].dropna().unique().tolist()], key=lambda x: str(x))
    panels = [("All data", dat)] + [(f"{layout_col}={v}", dat.loc[dat[layout_col] == v].copy()) for v in levels]

    hue_order = sorted(dat["coinLabel"].astype(str).unique().tolist())
    if isinstance(palette, dict):
        color_map = {k: palette.get(k, "#333333") for k in hue_order}
    else:
        color_map = dict(zip(hue_order, sns.color_palette(palette, n_colors=len(hue_order))))

    n_panels = len(panels)
    nrows = math.ceil(n_panels / ncols)

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(5.1 * ncols, 4.2 * nrows), squeeze=False)
    axes_flat = axes.ravel()

    for ax, (title, sub) in zip(axes_flat, panels):
        sns.violinplot(
            data=sub,
            x="coinLabel",
            y=variable_of_interest,
            order=hue_order,
            palette=color_map,
            inner="quartile",
            ax=ax,
        )
        sns.stripplot(
            data=sub,
            x="coinLabel",
            y=variable_of_interest,
            order=hue_order,
            color="0.1",
            jitter=0.3,
            alpha=0.25,
            ax=ax,
        )
        ax.set_title(title, fontsize=12)
        ax.set_xlabel("Coin Type")
        ax.set_ylabel(f"{voi_str} {voi_unit}".strip())

    for ax in axes_flat[n_panels:]:
        ax.set_visible(False)

    fig.suptitle(f"{voi_str} by coin type within layout (violin)", fontsize=15, y=0.995)
    fig.tight_layout()
    plt.show()


def plot_histkde_coin_type_first(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    layout_col: str,
    voi_str: str = "Measure",
    voi_unit: str = "",
    bins: int | str = "auto",
    palette: str | dict = "tab10",
) -> None:
    if layout_col not in df.columns:
        raise ValueError(f"Missing layout column: {layout_col}")

    dat = _prep_plot_data(df, variable_of_interest)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    coin_levels = sorted(dat["coinLabel"].dropna().astype(str).unique().tolist())
    layout_levels = sorted([x for x in dat[layout_col].dropna().unique().tolist()], key=lambda x: str(x))

    if isinstance(palette, dict):
        color_map = {str(k): palette.get(k, "#333333") for k in layout_levels}
    else:
        color_map = dict(zip([str(x) for x in layout_levels], sns.color_palette(palette, n_colors=len(layout_levels))))

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(nrows=1, ncols=len(coin_levels), figsize=(5.6 * len(coin_levels), 4.6), squeeze=False)
    axes_flat = axes.ravel()

    for ax, coin_label in zip(axes_flat, coin_levels):
        sub = dat.loc[dat["coinLabel"] == coin_label].copy()
        sub["_layout_str"] = sub[layout_col].astype("string")

        sns.histplot(
            data=sub,
            x=variable_of_interest,
            hue="_layout_str",
            bins=bins,
            stat="density",
            common_norm=False,
            element="step",
            multiple="layer",
            alpha=0.30,
            palette=color_map,
            ax=ax,
            legend=(coin_label == coin_levels[0]),
        )
        sns.kdeplot(
            data=sub,
            x=variable_of_interest,
            hue="_layout_str",
            common_norm=False,
            palette=color_map,
            lw=2,
            legend=False,
            ax=ax,
        )
        ax.set_title(f"coinLabel={coin_label}", fontsize=12)
        ax.set_xlabel(f"{voi_str} {voi_unit}".strip())
        ax.set_ylabel("Density")

    fig.suptitle(f"{voi_str} distribution by layout within coin type", fontsize=15, y=0.995)
    fig.tight_layout()
    plt.show()


def plot_violin_coin_type_first(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    layout_col: str,
    voi_str: str = "Measure",
    voi_unit: str = "",
    palette: str | dict = "tab10",
) -> None:
    if layout_col not in df.columns:
        raise ValueError(f"Missing layout column: {layout_col}")

    dat = _prep_plot_data(df, variable_of_interest)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    coin_levels = sorted(dat["coinLabel"].dropna().astype(str).unique().tolist())
    layout_levels = sorted([x for x in dat[layout_col].dropna().unique().tolist()], key=lambda x: str(x))

    if isinstance(palette, dict):
        color_map = {str(k): palette.get(k, "#333333") for k in layout_levels}
    else:
        color_map = dict(zip([str(x) for x in layout_levels], sns.color_palette(palette, n_colors=len(layout_levels))))

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(nrows=1, ncols=len(coin_levels), figsize=(5.4 * len(coin_levels), 4.8), squeeze=False)
    axes_flat = axes.ravel()

    for ax, coin_label in zip(axes_flat, coin_levels):
        sub = dat.loc[dat["coinLabel"] == coin_label].copy()
        sub["_layout_str"] = sub[layout_col].astype("string")

        sns.violinplot(
            data=sub,
            x="_layout_str",
            y=variable_of_interest,
            order=[str(x) for x in layout_levels],
            palette=color_map,
            inner="quartile",
            ax=ax,
        )
        sns.stripplot(
            data=sub,
            x="_layout_str",
            y=variable_of_interest,
            order=[str(x) for x in layout_levels],
            color="0.1",
            jitter=0.25,
            alpha=0.22,
            ax=ax,
        )
        ax.set_title(f"coinLabel={coin_label}", fontsize=12)
        ax.set_xlabel(layout_col)
        ax.set_ylabel(f"{voi_str} {voi_unit}".strip())
        ax.tick_params(axis="x", rotation=45)

    fig.suptitle(f"{voi_str} by layout within coin type (violin)", fontsize=15, y=0.995)
    fig.tight_layout()
    plt.show()