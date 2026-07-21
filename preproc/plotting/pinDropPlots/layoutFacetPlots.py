# layoutFacetPlots.py
from __future__ import annotations

import math

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from histoHelpers_v2 import annotate_hist_bins


PARTICIPANT_METRIC_COLUMNS = (
    "totScore",
    "swapRate_tot",
    "PVSS_TotalScore",
    "PVSS_AvgScore",
)


def _prep_plot_data(df: pd.DataFrame, variable_of_interest: str) -> pd.DataFrame:
    req = [variable_of_interest, "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for plotting: {missing}")

    out = df.copy()
    out[variable_of_interest] = pd.to_numeric(
        out[variable_of_interest], errors="coerce"
    )
    out["coinLabel"] = out["coinLabel"].astype("string").str.strip()
    out["dropQual"] = out["dropQual"].astype("string").str.strip().str.lower()

    mask = (
        out[variable_of_interest].notna()
        & out["coinLabel"].notna()
        & out["dropQual"].isin(["good", "bad"])
    )
    return out.loc[mask].copy()


def _sample_stats_text(
    dat: pd.DataFrame,
    *,
    value_col: str,
    group_col: str,
    group_order: list[str] | None = None,
) -> str:
    """Return one line per group with n, mean, and sample SD."""
    work = dat[[value_col, group_col]].copy()
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    work = work.dropna(subset=[value_col, group_col])
    work[group_col] = work[group_col].astype(str)

    if group_order is None:
        group_order = sorted(work[group_col].unique().tolist())

    lines: list[str] = []
    for group in group_order:
        values = work.loc[work[group_col] == str(group), value_col]
        n = int(values.count())
        if n == 0:
            continue
        mean = float(values.mean())
        sd = float(values.std(ddof=1)) if n > 1 else float("nan")
        sd_text = f"{sd:.3g}" if np.isfinite(sd) else "NA"
        lines.append(f"{group}: n={n:,}, μ={mean:.3g}, σ={sd_text}")
    return "\n".join(lines)


def _hist_stats_text(
    dat: pd.DataFrame,
    *,
    value_col: str,
    group_col: str,
    bins: int | str = "auto",
    group_order: list[str] | None = None,
) -> str:
    """Return histogram stats with n, mean, sample SD, and common bin width."""
    work = dat[[value_col, group_col]].copy()
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    work = work.dropna(subset=[value_col, group_col])
    work[group_col] = work[group_col].astype(str)

    all_values = work[value_col].to_numpy(dtype=float)
    if all_values.size == 0:
        return ""

    edges = np.histogram_bin_edges(all_values, bins=bins)
    widths = np.diff(edges)
    bin_width = (
        float(widths[0])
        if widths.size and np.allclose(widths, widths[0])
        else float(np.median(widths))
    )

    if group_order is None:
        group_order = sorted(work[group_col].unique().tolist())

    lines: list[str] = []
    for group in group_order:
        values = work.loc[work[group_col] == str(group), value_col]
        n = int(values.count())
        if n == 0:
            continue
        mean = float(values.mean())
        sd = float(values.std(ddof=1)) if n > 1 else float("nan")
        sd_text = f"{sd:.3g}" if np.isfinite(sd) else "NA"
        lines.append(
            f"{group}: n={n:,}, μ={mean:.3g}, σ={sd_text}, bin≈{bin_width:.3g}"
        )
    return "\n".join(lines)


def _set_panel_header(
    ax: plt.Axes,
    *,
    title: str,
    stats_text: str = "",
    participant_text: str = "",
) -> None:
    """Put annotations in the panel header so legends/data cannot cover them."""
    parts = [title]
    if stats_text:
        parts.append(stats_text)
    if participant_text:
        parts.append(participant_text)

    ax.set_title(
        "\n".join(parts),
        fontsize=9 if len(parts) > 1 else 12,
        loc="left",
        pad=8,
    )


def _add_stats_box(ax: plt.Axes, text: str, *, location: str = "upper right") -> None:
    if not text:
        return

    positions = {
        "upper right": (0.98, 0.98, "right", "top"),
        "upper left": (0.02, 0.98, "left", "top"),
        "lower right": (0.98, 0.02, "right", "bottom"),
        "lower left": (0.02, 0.02, "left", "bottom"),
    }
    x, y, ha, va = positions[location]
    ax.text(
        x,
        y,
        text,
        transform=ax.transAxes,
        ha=ha,
        va=va,
        fontsize=8.5,
        family="monospace",
        bbox={
            "boxstyle": "round,pad=0.35",
            "facecolor": "white",
            "edgecolor": "0.65",
            "alpha": 0.88,
        },
        zorder=20,
    )


def _format_metric(value: object, digits: int = 3) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "NA"
    return f"{float(numeric):.{digits}g}"


def _participant_metrics_text(sub: pd.DataFrame) -> str:
    """Create participant metadata text from values already merged into the data."""
    required = ["participantID", *PARTICIPANT_METRIC_COLUMNS]
    if any(col not in sub.columns for col in required):
        return ""

    cols = required.copy()
    if "sessionID" in sub.columns:
        cols.insert(1, "sessionID")

    unique_rows = sub[cols].drop_duplicates()
    if unique_rows.empty:
        return ""

    # A session facet should normally map to one participant/summary row.
    # If several remain, show each participant on a separate compact line.
    lines: list[str] = []
    for _, row in unique_rows.head(6).iterrows():
        pid = row.get("participantID", "NA")
        total_score = _format_metric(row.get("totScore"))
        swap = _format_metric(row.get("swapRate_tot"))
        pvss_total = _format_metric(row.get("PVSS_TotalScore"))
        pvss_avg = _format_metric(row.get("PVSS_AvgScore"))
        lines.append(
            f"participantID={pid} | "
            f"totScore={total_score} | "
            f"swapRate_tot={swap} | "
            f"PVSS_TotalScore={pvss_total} | "
            f"PVSS_AvgScore={pvss_avg}"
        )

    if len(unique_rows) > 6:
        lines.append(f"… plus {len(unique_rows) - 6} more participant rows")
    return "\n".join(lines)


def _add_panel_annotations(
    ax: plt.Axes,
    sub: pd.DataFrame,
    *,
    variable_of_interest: str,
    group_col: str,
    group_order: list[str],
    show_stats: bool,
    show_participant_metrics: bool,
) -> None:
    if show_stats:
        stats_text = _sample_stats_text(
            sub,
            value_col=variable_of_interest,
            group_col=group_col,
            group_order=group_order,
        )
        _add_stats_box(ax, stats_text, location="upper right")

    if show_participant_metrics:
        metrics_text = _participant_metrics_text(sub)
        _add_stats_box(ax, metrics_text, location="lower left")


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
    show_stats: bool = True,
    show_participant_metrics: bool = False,
) -> None:
    if layout_col not in df.columns:
        raise ValueError(f"Missing layout column: {layout_col}")
    if show_participant_metrics and layout_col != "sessionID":
        raise ValueError(
            "--show-participant-metrics is only valid when --layout-col sessionID."
        )

    dat = _prep_plot_data(df, variable_of_interest)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    levels = sorted(
        dat[layout_col].dropna().unique().tolist(), key=lambda x: str(x)
    )
    panels = [("All data", dat)] + [
        (f"{layout_col}={v}", dat.loc[dat[layout_col] == v].copy())
        for v in levels
    ]

    hue_order = sorted(dat["coinLabel"].astype(str).unique().tolist())
    if isinstance(palette, dict):
        color_map = {k: palette.get(k, "#333333") for k in hue_order}
    else:
        color_map = dict(
            zip(hue_order, sns.color_palette(palette, n_colors=len(hue_order)))
        )

    n_panels = len(panels)
    nrows = math.ceil(n_panels / ncols)

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(5.3 * ncols, 4.2 * nrows),
        squeeze=False,
    )
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

        hist_stats_text = (
            _hist_stats_text(
                sub,
                value_col=variable_of_interest,
                group_col="coinLabel",
                bins=bins,
                group_order=hue_order,
            )
            if show_stats
            else ""
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
            warn_singular=False,
        )

        participant_text = (
            _participant_metrics_text(sub)
            if show_participant_metrics and title != "All data"
            else ""
        )
        _set_panel_header(
            ax,
            title=title,
            stats_text=hist_stats_text,
            participant_text=participant_text,
        )
        ax.set_xlabel(f"{voi_str} {voi_unit}".strip())
        ax.set_ylabel("Density")

    for ax in axes_flat[n_panels:]:
        ax.set_visible(False)

    fig.suptitle(
        f"{voi_str} distribution by coin type within layout",
        fontsize=15,
        y=0.995,
    )
    fig.tight_layout(h_pad=3.0)
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
    show_stats: bool = True,
    show_participant_metrics: bool = False,
) -> None:
    if layout_col not in df.columns:
        raise ValueError(f"Missing layout column: {layout_col}")
    if show_participant_metrics and layout_col != "sessionID":
        raise ValueError(
            "--show-participant-metrics is only valid when --layout-col sessionID."
        )

    dat = _prep_plot_data(df, variable_of_interest)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    levels = sorted(
        dat[layout_col].dropna().unique().tolist(), key=lambda x: str(x)
    )
    panels = [("All data", dat)] + [
        (f"{layout_col}={v}", dat.loc[dat[layout_col] == v].copy())
        for v in levels
    ]

    hue_order = sorted(dat["coinLabel"].astype(str).unique().tolist())
    if isinstance(palette, dict):
        color_map = {k: palette.get(k, "#333333") for k in hue_order}
    else:
        color_map = dict(
            zip(hue_order, sns.color_palette(palette, n_colors=len(hue_order)))
        )

    n_panels = len(panels)
    nrows = math.ceil(n_panels / ncols)

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(5.1 * ncols, 4.2 * nrows),
        squeeze=False,
    )
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

        violin_stats_text = (
            _sample_stats_text(
                sub,
                value_col=variable_of_interest,
                group_col="coinLabel",
                group_order=hue_order,
            )
            if show_stats
            else ""
        )
        participant_text = (
            _participant_metrics_text(sub)
            if show_participant_metrics and title != "All data"
            else ""
        )
        _set_panel_header(
            ax,
            title=title,
            stats_text=violin_stats_text,
            participant_text=participant_text,
        )
        ax.set_xlabel("Coin Type")
        ax.set_ylabel(f"{voi_str} {voi_unit}".strip())

    for ax in axes_flat[n_panels:]:
        ax.set_visible(False)

    fig.suptitle(
        f"{voi_str} by coin type within layout (violin)",
        fontsize=15,
        y=0.995,
    )
    fig.tight_layout(h_pad=3.0)
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
    show_stats: bool = True,
    show_participant_metrics: bool = False,
) -> None:
    if layout_col not in df.columns:
        raise ValueError(f"Missing layout column: {layout_col}")

    dat = _prep_plot_data(df, variable_of_interest)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    coin_levels = sorted(
        dat["coinLabel"].dropna().astype(str).unique().tolist()
    )
    layout_levels = sorted(
        dat[layout_col].dropna().unique().tolist(), key=lambda x: str(x)
    )
    layout_order = [str(x) for x in layout_levels]

    if isinstance(palette, dict):
        color_map = {str(k): palette.get(k, "#333333") for k in layout_levels}
    else:
        color_map = dict(
            zip(
                layout_order,
                sns.color_palette(palette, n_colors=len(layout_levels)),
            )
        )

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(
        nrows=1,
        ncols=len(coin_levels),
        figsize=(5.6 * len(coin_levels), 4.6),
        squeeze=False,
    )
    axes_flat = axes.ravel()

    for ax, coin_label in zip(axes_flat, coin_levels):
        sub = dat.loc[dat["coinLabel"] == coin_label].copy()
        sub["_layout_str"] = sub[layout_col].astype("string")

        sns.histplot(
            data=sub,
            x=variable_of_interest,
            hue="_layout_str",
            hue_order=layout_order,
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

        hist_stats_text = (
            _hist_stats_text(
                sub,
                value_col=variable_of_interest,
                group_col="_layout_str",
                bins=bins,
                group_order=layout_order,
            )
            if show_stats
            else ""
        )

        sns.kdeplot(
            data=sub,
            x=variable_of_interest,
            hue="_layout_str",
            hue_order=layout_order,
            common_norm=False,
            palette=color_map,
            lw=2,
            legend=False,
            ax=ax,
            warn_singular=False,
        )
        _set_panel_header(
            ax,
            title=f"coinLabel={coin_label}",
            stats_text=hist_stats_text,
        )
        ax.set_xlabel(f"{voi_str} {voi_unit}".strip())
        ax.set_ylabel("Density")

    fig.suptitle(
        f"{voi_str} distribution by layout within coin type",
        fontsize=15,
        y=0.995,
    )
    fig.tight_layout(h_pad=3.0)
    plt.show()


def plot_violin_coin_type_first(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    layout_col: str,
    voi_str: str = "Measure",
    voi_unit: str = "",
    palette: str | dict = "tab10",
    show_stats: bool = True,
    show_participant_metrics: bool = False,
) -> None:
    if layout_col not in df.columns:
        raise ValueError(f"Missing layout column: {layout_col}")

    dat = _prep_plot_data(df, variable_of_interest)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    coin_levels = sorted(
        dat["coinLabel"].dropna().astype(str).unique().tolist()
    )
    layout_levels = sorted(
        dat[layout_col].dropna().unique().tolist(), key=lambda x: str(x)
    )
    layout_order = [str(x) for x in layout_levels]

    if isinstance(palette, dict):
        color_map = {str(k): palette.get(k, "#333333") for k in layout_levels}
    else:
        color_map = dict(
            zip(
                layout_order,
                sns.color_palette(palette, n_colors=len(layout_levels)),
            )
        )

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(
        nrows=1,
        ncols=len(coin_levels),
        figsize=(5.4 * len(coin_levels), 4.8),
        squeeze=False,
    )
    axes_flat = axes.ravel()

    for ax, coin_label in zip(axes_flat, coin_levels):
        sub = dat.loc[dat["coinLabel"] == coin_label].copy()
        sub["_layout_str"] = sub[layout_col].astype("string")

        sns.violinplot(
            data=sub,
            x="_layout_str",
            y=variable_of_interest,
            order=layout_order,
            palette=color_map,
            inner="quartile",
            ax=ax,
        )
        sns.stripplot(
            data=sub,
            x="_layout_str",
            y=variable_of_interest,
            order=layout_order,
            color="0.1",
            jitter=0.25,
            alpha=0.22,
            ax=ax,
        )

        violin_stats_text = (
            _sample_stats_text(
                sub,
                value_col=variable_of_interest,
                group_col="_layout_str",
                group_order=layout_order,
            )
            if show_stats
            else ""
        )
        _set_panel_header(
            ax,
            title=f"coinLabel={coin_label}",
            stats_text=violin_stats_text,
        )
        ax.set_xlabel(layout_col)
        ax.set_ylabel(f"{voi_str} {voi_unit}".strip())
        ax.tick_params(axis="x", rotation=45)

    fig.suptitle(
        f"{voi_str} by layout within coin type (violin)",
        fontsize=15,
        y=0.995,
    )
    fig.tight_layout(h_pad=3.0)
    plt.show()
