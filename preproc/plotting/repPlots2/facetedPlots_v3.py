from __future__ import annotations

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.gridspec import GridSpec

from facetedHelpers_v3 import (
    RepresentativePair,
    SharedPlotSpec,
    annotate_hist_bins,
    draw_dots_strip,
)


def _prepare_plot_data(
    df: pd.DataFrame,
    *,
    voi: str,
    session_column: str,
    extra_columns: tuple[str, ...] = (),
) -> pd.DataFrame:
    required = [voi, "coinLabel", session_column, *extra_columns]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for faceted histogram/KDE: {missing}")

    data = df.copy()
    data[voi] = pd.to_numeric(data[voi], errors="coerce")
    mask = data[voi].notna() & data["coinLabel"].notna()
    if "dropQual" in data.columns:
        mask &= data["dropQual"].astype("string").str.strip().str.lower().isin(["good", "bad"])

    columns = list(dict.fromkeys([voi, "coinLabel", session_column, *extra_columns]))
    return data.loc[mask, columns].reset_index(drop=True)


def _representative_facets(
    data: pd.DataFrame,
    *,
    pair: RepresentativePair,
    session_column: str,
) -> list[tuple[str, pd.DataFrame]]:
    session_ids = data[session_column].astype("string").str.strip()
    main = data.loc[session_ids.eq(pair.main_session_id)].copy()
    rr = data.loc[session_ids.eq(pair.rr_session_id)].copy()
    if main.empty or rr.empty:
        raise ValueError(
            f"Representative data became empty for {pair.mode}: "
            f"main={pair.main_session_id} ({len(main)} rows), "
            f"RR={pair.rr_session_id} ({len(rr)} rows)."
        )
    return [
        ("Whole dataset", data),
        (f"Main representative\n{pair.main_session_id}", main),
        (f"RR representative\n{pair.rr_session_id}", rr),
    ]



def _cohort_representative_facets(
    data: pd.DataFrame,
    *,
    pair: RepresentativePair,
    session_column: str,
    cohort_column: str,
    main_cohort_value: str,
    rr_cohort_value: str,
) -> list[tuple[str, pd.DataFrame]]:
    """Build Main/RR cohort facets and their matched representative facets."""
    cohort_values = data[cohort_column].astype("string").str.strip().str.casefold()
    main_value = str(main_cohort_value).strip().casefold()
    rr_value = str(rr_cohort_value).strip().casefold()

    main_cohort = data.loc[cohort_values.eq(main_value)].copy()
    rr_cohort = data.loc[cohort_values.eq(rr_value)].copy()

    session_ids = data[session_column].astype("string").str.strip()
    main_rep = data.loc[session_ids.eq(pair.main_session_id)].copy()
    rr_rep = data.loc[session_ids.eq(pair.rr_session_id)].copy()

    failures: list[str] = []
    if main_cohort.empty:
        failures.append(
            f"Main cohort '{main_cohort_value}' has no usable rows in '{cohort_column}'"
        )
    if rr_cohort.empty:
        failures.append(
            f"RR cohort '{rr_cohort_value}' has no usable rows in '{cohort_column}'"
        )
    if main_rep.empty:
        failures.append(
            f"Main representative '{pair.main_session_id}' has no usable rows"
        )
    if rr_rep.empty:
        failures.append(
            f"RR representative '{pair.rr_session_id}' has no usable rows"
        )
    if failures:
        raise ValueError("; ".join(failures))

    return [
        ("Main cohort", main_cohort),
        ("RR cohort", rr_cohort),
        (f"Main representative\n{pair.main_session_id}", main_rep),
        (f"RR representative\n{pair.rr_session_id}", rr_rep),
    ]


def plot_faceted_histkde_cohorts(
    df: pd.DataFrame,
    *,
    pair: RepresentativePair,
    plot_spec: SharedPlotSpec,
    variableOfInterest: str,
    session_column: str = "sessionID",
    cohort_column: str = "main_RR",
    main_cohort_value: str = "main",
    rr_cohort_value: str = "RR",
    voi_str: str = "Measure",
    voi_unit: str = "",
    stat: str = "density",
    palette: str | dict = "tab10",
    dot_mode: str = "panel",
    dot_alpha: float = 0.6,
    dot_size: float = 12,
    dot_jitter: float = 0.15,
    max_points_per_group: int | None = 4000,
    show_bin_stats: bool = True,
    ylim: float | None = None,
    title_prefix: str = "",
):
    """Draw Main/RR cohorts above their matched representative facets."""
    if dot_mode not in {"panel", "baseline", "none"}:
        raise ValueError("dot_mode must be one of: panel, baseline, none")

    data = _prepare_plot_data(
        df,
        voi=variableOfInterest,
        session_column=session_column,
        extra_columns=(cohort_column,),
    )
    if data.empty:
        raise ValueError("No data remain after plot filtering.")

    facets = _cohort_representative_facets(
        data,
        pair=pair,
        session_column=session_column,
        cohort_column=cohort_column,
        main_cohort_value=main_cohort_value,
        rr_cohort_value=rr_cohort_value,
    )

    coin_order = list(plot_spec.coin_order)
    if not coin_order:
        coin_order = sorted(data["coinLabel"].astype(str).unique().tolist())

    if isinstance(palette, dict):
        color_map = {coin: palette.get(coin, "#333333") for coin in coin_order}
    else:
        color_map = dict(
            zip(coin_order, sns.color_palette(palette, n_colors=len(coin_order)))
        )

    sns.set_theme(style="whitegrid")
    has_dot_panel = dot_mode == "panel"

    if has_dot_panel:
        figure = plt.figure(figsize=(14, 15))
        grid = GridSpec(
            4,
            2,
            figure=figure,
            height_ratios=(6, 1, 6, 1),
            hspace=0.50,
            wspace=0.22,
        )
        main_axes = [
            figure.add_subplot(grid[0, 0]),
            figure.add_subplot(grid[0, 1]),
            figure.add_subplot(grid[2, 0]),
            figure.add_subplot(grid[2, 1]),
        ]
        dot_axes = [
            figure.add_subplot(grid[1, 0], sharex=main_axes[0]),
            figure.add_subplot(grid[1, 1], sharex=main_axes[1]),
            figure.add_subplot(grid[3, 0], sharex=main_axes[2]),
            figure.add_subplot(grid[3, 1], sharex=main_axes[3]),
        ]
    else:
        figure, axes = plt.subplots(
            2,
            2,
            figsize=(14, 11),
            sharex=True,
            squeeze=False,
        )
        main_axes = list(axes.ravel())
        dot_axes = [None] * 4
        figure.subplots_adjust(hspace=0.32, wspace=0.22)

    observed_y_max = 0.0
    for index, ((facet_title, facet_data), axis, dot_axis) in enumerate(
        zip(facets, main_axes, dot_axes)
    ):
        sns.histplot(
            data=facet_data,
            x=variableOfInterest,
            hue="coinLabel",
            hue_order=coin_order,
            bins=plot_spec.bin_edges,
            stat=stat,
            common_norm=False,
            element="step",
            alpha=0.35,
            multiple="layer",
            ax=axis,
            palette=color_map,
            legend=index == 0,
        )

        if show_bin_stats:
            annotate_hist_bins(
                axis,
                facet_data,
                x=variableOfInterest,
                bins=plot_spec.bin_edges,
                hue="coinLabel",
                stat=stat,
                framealpha=0.5,
            )

        for coin in coin_order:
            coin_data = facet_data.loc[
                facet_data["coinLabel"].astype(str).eq(str(coin))
            ]
            if len(coin_data) >= 2 and coin_data[variableOfInterest].nunique() >= 2:
                sns.kdeplot(
                    data=coin_data,
                    x=variableOfInterest,
                    ax=axis,
                    color=color_map[coin],
                    lw=2,
                    legend=False,
                    clip=plot_spec.xlim,
                )

        if dot_mode == "panel" and dot_axis is not None:
            draw_dots_strip(
                dot_axis,
                facet_data,
                x_col=variableOfInterest,
                group_col="coinLabel",
                color_map=color_map,
                dot_size=dot_size,
                dot_alpha=dot_alpha,
                dot_jitter=dot_jitter,
                max_points_per_group=max_points_per_group,
            )
            dot_axis.set_xlim(plot_spec.xlim)
            dot_axis.set_xlabel(f"{voi_str} {voi_unit}".strip())
            dot_axis.set_ylabel("")
            dot_axis.set_yticks([])
            sns.despine(ax=dot_axis, left=True)
            axis.tick_params(axis="x", labelbottom=False)
            axis.set_xlabel("")
        elif dot_mode == "baseline":
            for coin_index, coin in enumerate(coin_order):
                coin_values = pd.to_numeric(
                    facet_data.loc[
                        facet_data["coinLabel"].astype(str).eq(str(coin)),
                        variableOfInterest,
                    ],
                    errors="coerce",
                ).dropna()
                axis.scatter(
                    coin_values,
                    np.full(len(coin_values), -0.015 * (coin_index + 1)),
                    s=dot_size,
                    alpha=dot_alpha,
                    color=color_map[coin],
                    clip_on=False,
                )
            axis.set_xlabel(f"{voi_str} {voi_unit}".strip())
        else:
            axis.set_xlabel(f"{voi_str} {voi_unit}".strip())

        axis.set_title(
            f"{facet_title}\nN = {len(facet_data):,} Pin Drops",
            fontsize=13,
        )
        axis.set_ylabel("Density" if stat == "density" else stat.capitalize())
        axis.set_xlim(plot_spec.xlim)
        observed_y_max = max(observed_y_max, float(axis.get_ylim()[1]))

    shared_y_max = float(ylim) if ylim is not None else observed_y_max * 1.05
    if shared_y_max <= 0 or not np.isfinite(shared_y_max):
        shared_y_max = 1.0

    for axis in main_axes:
        axis.set_ylim(0, shared_y_max)

    for axis in (main_axes[1], main_axes[3]):
        axis.set_ylabel("")

    for axis in main_axes[1:]:
        if axis.get_legend() is not None:
            axis.get_legend().remove()

    title_bits = [
        part
        for part in (
            title_prefix.strip(),
            f"{voi_str} Distribution by Coin Type",
            f"{pair.mode.replace('_', ' ').title()} cohort comparison",
        )
        if part
    ]
    figure.suptitle(" — ".join(title_bits), fontsize=16, y=0.995)
    figure.text(
        0.5,
        0.01,
        (
            f"Shared bin width: {plot_spec.bin_width:.6g} {voi_unit}".strip()
            + f" | Role: {pair.role}"
            + f" | Cohort column: {cohort_column}"
        ),
        ha="center",
        fontsize=9,
    )
    figure.tight_layout(rect=(0, 0.035, 1, 0.97))
    plt.show()


def plot_faceted_histkde_representatives(
    df: pd.DataFrame,
    *,
    pair: RepresentativePair,
    plot_spec: SharedPlotSpec,
    variableOfInterest: str,
    session_column: str = "sessionID",
    voi_str: str = "Measure",
    voi_unit: str = "",
    stat: str = "density",
    palette: str | dict = "tab10",
    dot_mode: str = "panel",
    dot_alpha: float = 0.6,
    dot_size: float = 12,
    dot_jitter: float = 0.15,
    max_points_per_group: int | None = 4000,
    show_bin_stats: bool = True,
    ylim: float | None = None,
    title_prefix: str = "",
):
    """Draw whole-data, main-representative, and RR-representative facets."""
    if dot_mode not in {"panel", "baseline", "none"}:
        raise ValueError("dot_mode must be one of: panel, baseline, none")

    data = _prepare_plot_data(
        df,
        voi=variableOfInterest,
        session_column=session_column,
    )
    if data.empty:
        raise ValueError("No data remain after plot filtering.")

    facets = _representative_facets(
        data,
        pair=pair,
        session_column=session_column,
    )

    coin_order = list(plot_spec.coin_order)
    if not coin_order:
        coin_order = sorted(data["coinLabel"].astype(str).unique().tolist())

    if isinstance(palette, dict):
        color_map = {coin: palette.get(coin, "#333333") for coin in coin_order}
    else:
        color_map = dict(
            zip(coin_order, sns.color_palette(palette, n_colors=len(coin_order)))
        )

    sns.set_theme(style="whitegrid")
    has_dot_panel = dot_mode == "panel"
    if has_dot_panel:
        figure = plt.figure(figsize=(18, 8.5))
        grid = GridSpec(
            2,
            3,
            figure=figure,
            height_ratios=(6, 1),
            hspace=0.36,
            wspace=0.22,
        )
        main_axes = [figure.add_subplot(grid[0, index]) for index in range(3)]
        dot_axes = [
            figure.add_subplot(grid[1, index], sharex=main_axes[index])
            for index in range(3)
        ]
    else:
        figure, axes = plt.subplots(1, 3, figsize=(18, 6.5), sharex=True)
        main_axes = list(np.atleast_1d(axes))
        dot_axes = [None, None, None]
        figure.subplots_adjust(wspace=0.22)

    observed_y_max = 0.0
    for index, ((facet_title, facet_data), axis, dot_axis) in enumerate(
        zip(facets, main_axes, dot_axes)
    ):
        sns.histplot(
            data=facet_data,
            x=variableOfInterest,
            hue="coinLabel",
            hue_order=coin_order,
            bins=plot_spec.bin_edges,
            stat=stat,
            common_norm=False,
            element="step",
            alpha=0.35,
            multiple="layer",
            ax=axis,
            palette=color_map,
            legend=index == 0,
        )

        if show_bin_stats:
            annotate_hist_bins(
                axis,
                facet_data,
                x=variableOfInterest,
                bins=plot_spec.bin_edges,
                hue="coinLabel",
                stat=stat,
                framealpha=0.5,
            )

        for coin in coin_order:
            coin_data = facet_data.loc[
                facet_data["coinLabel"].astype(str).eq(str(coin))
            ]
            if len(coin_data) >= 2 and coin_data[variableOfInterest].nunique() >= 2:
                sns.kdeplot(
                    data=coin_data,
                    x=variableOfInterest,
                    ax=axis,
                    color=color_map[coin],
                    lw=2,
                    legend=False,
                    clip=plot_spec.xlim,
                )

        if dot_mode == "panel" and dot_axis is not None:
            draw_dots_strip(
                dot_axis,
                facet_data,
                x_col=variableOfInterest,
                group_col="coinLabel",
                color_map=color_map,
                dot_size=dot_size,
                dot_alpha=dot_alpha,
                dot_jitter=dot_jitter,
                max_points_per_group=max_points_per_group,
            )
            dot_axis.set_xlim(plot_spec.xlim)
            dot_axis.set_xlabel(f"{voi_str} {voi_unit}".strip())
            dot_axis.set_ylabel("")
            dot_axis.set_yticks([])
            sns.despine(ax=dot_axis, left=True)
            axis.tick_params(axis="x", labelbottom=False)
            axis.set_xlabel("")
        elif dot_mode == "baseline":
            for coin_index, coin in enumerate(coin_order):
                coin_values = pd.to_numeric(
                    facet_data.loc[
                        facet_data["coinLabel"].astype(str).eq(str(coin)),
                        variableOfInterest,
                    ],
                    errors="coerce",
                ).dropna()
                axis.scatter(
                    coin_values,
                    np.full(len(coin_values), -0.015 * (coin_index + 1)),
                    s=dot_size,
                    alpha=dot_alpha,
                    color=color_map[coin],
                    clip_on=False,
                )
            axis.set_xlabel(f"{voi_str} {voi_unit}".strip())
        else:
            axis.set_xlabel(f"{voi_str} {voi_unit}".strip())

        axis.set_title(f"{facet_title}\nN = {len(facet_data):,} Pin Drops", fontsize=13)
        axis.set_ylabel("Density" if stat == "density" else stat.capitalize())
        axis.set_xlim(plot_spec.xlim)
        observed_y_max = max(observed_y_max, float(axis.get_ylim()[1]))

    shared_y_max = float(ylim) if ylim is not None else observed_y_max * 1.05
    if shared_y_max <= 0 or not np.isfinite(shared_y_max):
        shared_y_max = 1.0
    for axis in main_axes:
        axis.set_ylim(0, shared_y_max)

    for axis in main_axes[1:]:
        if axis.get_legend() is not None:
            axis.get_legend().remove()
        axis.set_ylabel("")

    title_bits = [
        part
        for part in (
            title_prefix.strip(),
            f"{voi_str} Distribution by Coin Type",
            f"{pair.mode.replace('_', ' ').title()} representatives",
        )
        if part
    ]
    figure.suptitle(" — ".join(title_bits), fontsize=16, y=0.995)
    figure.text(
        0.5,
        0.01,
        (
            f"Shared bin width: {plot_spec.bin_width:.6g} {voi_unit}".strip()
            + f" | Role: {pair.role}"
        ),
        ha="center",
        fontsize=9,
    )
    figure.tight_layout(rect=(0, 0.035, 1, 0.96))
    plt.show()


def build_representative_descriptives(
    df: pd.DataFrame,
    *,
    pair: RepresentativePair,
    variableOfInterest: str,
    session_column: str = "sessionID",
) -> pd.DataFrame:
    """Return facet-by-coin descriptive statistics for audit-ready CSV output."""
    data = _prepare_plot_data(
        df,
        voi=variableOfInterest,
        session_column=session_column,
    )
    facets = _representative_facets(data, pair=pair, session_column=session_column)

    rows: list[pd.DataFrame] = []
    for facet_name, facet_data in facets:
        summary = (
            facet_data.groupby("coinLabel", dropna=False)[variableOfInterest]
            .agg(["count", "mean", "std", "median", "min", "max"])
            .reset_index()
        )
        summary.insert(0, "facet", facet_name.replace("\n", " "))
        summary.insert(0, "role", pair.role)
        summary.insert(0, "representative_mode", pair.mode)
        rows.append(summary)

    return pd.concat(rows, ignore_index=True)
