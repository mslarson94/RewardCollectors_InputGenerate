from __future__ import annotations
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from histoHelpers import (
    _slugify, make_axes_with_dots, draw_dots_strip, style_x_for_main_and_dots,
)

def plot_histkde_allsubjects(
    df: pd.DataFrame,
    *,
    variableOfInterest: str,
    voi_str: str = "Measure",
    voi_unit: str = "",
    bins: int | str = "auto",
    stat: str = "density",
    palette: str | dict = "tab10",
    dot_mode: str = "panel",
    dot_alpha: float = 0.6,
    dot_size: float = 12,
    dot_jitter: float = 0.15,
    max_points_per_group: int | None = 4000,
    hspace: float = 0.40,
    height_ratios: tuple[int, int] = (6, 1),
    main_labelpad: float = 6.0,
    dots_top_pad: float = 4.0,
    title_prefix: str = "",
):
    """Histogram + KDE of VOI across all subjects, hue=coinLabel, optional bottom dot strip."""
    req = [variableOfInterest, "coinLabel"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for histKDE: {missing}")

    sdf = df.copy()
    sdf[variableOfInterest] = pd.to_numeric(sdf[variableOfInterest], errors="coerce")
    # be permissive: if dropQual is present, keep only good/bad; otherwise ignore
    mask = sdf[variableOfInterest].notna() & sdf["coinLabel"].notna()
    if "dropQual" in sdf.columns:
        mask &= sdf["dropQual"].astype(str).str.lower().isin(["good", "bad"])
    dat = sdf.loc[mask, [variableOfInterest, "coinLabel"]].reset_index(drop=True)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    hue_order = sorted(dat["coinLabel"].unique().tolist())
    if isinstance(palette, dict):
        color_map = {k: palette.get(k, "#333333") for k in hue_order}
    else:
        color_map = dict(zip(hue_order, sns.color_palette(palette, n_colors=len(hue_order))))

    sns.set_theme(style="whitegrid")

    fig, ax, ax_dots = make_axes_with_dots(dot_mode=dot_mode, height_ratios=height_ratios, hspace=hspace)

    sns.histplot(
        data=dat, x=variableOfInterest, hue="coinLabel", hue_order=hue_order,
        bins=bins, stat=stat, common_norm=False, element="step",
        alpha=0.35, multiple="layer", ax=ax, palette=color_map, legend=True,
    )
    sns.kdeplot(
        data=dat, x=variableOfInterest, hue="coinLabel", hue_order=hue_order,
        common_norm=False, ax=ax, palette=color_map, lw=2, legend=False,
    )

    if dot_mode == "panel" and ax_dots is not None:
        draw_dots_strip(
            ax_dots, dat,
            x_col=variableOfInterest, group_col="coinLabel", color_map=color_map,
            dot_size=dot_size, dot_alpha=dot_alpha, dot_jitter=dot_jitter,
            max_points_per_group=max_points_per_group,
        )

    title_bits = [t for t in [title_prefix.strip(), f"{voi_str} Distribution by Coin Type"] if t]
    ax.set_title(" — ".join(title_bits), fontsize=14, y=1.055)

    style_x_for_main_and_dots(
        ax, ax_dots,
        main_xlabel=variableOfInterest,  # keep the raw column label here
        dots_label_top=f"{voi_str} {voi_unit}".strip(),
        main_labelpad=main_labelpad,
        dots_top_pad=dots_top_pad,
        dots_frame=False,
        despine=True,
    )
    if ax_dots is not None:
        ax.xaxis.set_label_coords(0.5, -0.08)

    ax.set_ylabel("Density" if stat == "density" else stat.capitalize())
    ax.set_xlim(left=0)

    fig.tight_layout()  # use fig-level to avoid the warning
    plt.show()


def plot_tp2_scatter_allsubjects(
    df: pd.DataFrame,
    *,
    variableOfInterest: str,
    voi_str: str = "Measure",
    voi_unit: str = "",
    title_prefix: str = "",
):
    """Scatter of VOI vs trueSession_elapsed_s across all subjects (hue=coinLabel)."""
    req = [variableOfInterest, "coinLabel", "trueSession_elapsed_s"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for TP2 scatter: {missing}")

    sdf = df.copy()
    sdf[variableOfInterest] = pd.to_numeric(sdf[variableOfInterest], errors="coerce")
    sdf["trueSession_elapsed_s"] = pd.to_numeric(sdf["trueSession_elapsed_s"], errors="coerce")
    # gate by dropQual if present
    mask = sdf[variableOfInterest].notna() & sdf["trueSession_elapsed_s"].notna() & sdf["coinLabel"].notna()
    if "dropQual" in sdf.columns:
        mask &= sdf["dropQual"].astype(str).str.lower().isin(["good", "bad"])
    dat = sdf.loc[mask, ["trueSession_elapsed_s", variableOfInterest, "coinLabel"]].reset_index(drop=True)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    hue_order = sorted(dat["coinLabel"].unique().tolist())
    palette = dict(zip(hue_order, sns.color_palette("tab10", n_colors=len(hue_order))))
    sns.set_theme(style="whitegrid")

    fig, ax = plt.subplots(figsize=(12, 6))
    for k, sub in dat.groupby("coinLabel", sort=True):
        ax.scatter(sub["trueSession_elapsed_s"], sub[variableOfInterest], s=18, alpha=0.5, label=k)

    title_bits = [t for t in [title_prefix.strip(), f"{voi_str} vs Overall Session Elapsed Time"] if t]
    ax.set_title(" — ".join(title_bits), fontsize=14)
    ax.set_xlabel(f"Overall Session Elapsed Time (s)")
    ax.set_ylabel(f"{voi_str} {voi_unit}".strip())
    ax.legend(title="Coin Type")
    fig.tight_layout()
    plt.show()
