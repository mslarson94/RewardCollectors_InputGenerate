# =========================
# file: pinDropSuite.py
# =========================
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable
import contextlib
import json

import matplotlib.pyplot as plt
import pandas as pd

from pinDropHelpers import DEFAULT_LABELS, prepare_event_data, read_table, slugify
from pinDropPlots import (
    plot_block3_round_num,
    plot_block3_round_time,
    plot_blocks_gt3_block_facets,
    plot_blocks_gt3_vs_block_num,
    plot_blocks_gt3_vs_time,
    plot_hist_kde_by_coin,
)
from pinDropStats import test_coin_distributions


def _ensure_dirs(*paths: Path) -> None:
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)


def _save_fig(
    fig: plt.Figure,
    *,
    output_base: Path,
    formats: Iterable[str] = ("png", "pdf"),
    dpi: int = 220,
) -> list[str]:
    saved: list[str] = []
    output_base.parent.mkdir(parents=True, exist_ok=True)

    with contextlib.suppress(Exception):
        fig.tight_layout()

    for ext in formats:
        path = output_base.with_suffix(f".{ext}")
        fig.savefig(path, dpi=dpi, bbox_inches="tight")
        saved.append(str(path))

    plt.close(fig)
    return saved


def _save_figs(
    figures: list[plt.Figure],
    *,
    output_base: Path,
    formats: Iterable[str] = ("png", "pdf"),
    dpi: int = 220,
) -> list[str]:
    saved: list[str] = []
    multi = len(figures) > 1

    for index, fig in enumerate(figures, start=1):
        base = output_base.parent / (
            f"{output_base.name}_p{index:02d}" if multi else output_base.name
        )
        saved.extend(_save_fig(fig, output_base=base, formats=formats, dpi=dpi))

    return saved


def run_suite_for_file(
    csv_path: Path | str,
    *,
    out_root: Path | str = "plots_out",
    use_outlier_filter: bool = False,
    outlier_groupby: str | list[str] | None = "coinLabel",
    outlier_method: str = "median",
    outlier_sigma: float = 2.0,
    outlier_ddof: int = 1,
    outlier_keep_na: bool = False,
    filter_columns: Iterable[str] = ("truecontent_elapsed_s", "dropDist"),
    groupby: str | list[str] | None = None,
    group_subdirs: bool = True,
    formats: Iterable[str] = ("png", "pdf"),
    dpi: int = 220,
    blocks_per_facet: int = 20,
    variable_of_interest: str = "truecontent_elapsed_s",
    y_label_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    csv_path = Path(csv_path)
    out_root = Path(out_root)

    raw = read_table(csv_path)
    df = prepare_event_data(
        raw,
        use_outlier_filter=use_outlier_filter,
        filter_columns=filter_columns,
        outlier_groupby=outlier_groupby,
        outlier_method=outlier_method,
        outlier_sigma=outlier_sigma,
        outlier_ddof=outlier_ddof,
        outlier_keep_na=outlier_keep_na,
    )

    labels = dict(DEFAULT_LABELS)
    if y_label_map:
        labels.update(y_label_map)
    y_label = labels.get(variable_of_interest, variable_of_interest)

    common_dir = out_root / "_ALL"
    per_file_dir = out_root / csv_path.stem
    _ensure_dirs(common_dir, per_file_dir)

    if groupby is None:
        groups: list[tuple[Any, pd.DataFrame]] = [(None, df)]
    else:
        groups = list(df.groupby(groupby, dropna=False, sort=True))

    manifest: dict[str, Any] = {"file": str(csv_path), "outputs": []}

    for group_key, group_df in groups:
        group_tag = None if group_key is None else slugify(group_key)
        target_dir = per_file_dir / group_tag if (group_tag and group_subdirs) else per_file_dir
        _ensure_dirs(target_dir)

        def tag(base: str) -> str:
            return f"{base}__grp-{group_tag}" if group_tag else base

        stats = test_coin_distributions(
            group_df,
            variable_of_interest=variable_of_interest,
            blocks_min=3,
            min_n_per_group=10,
            alpha=0.05,
        )
        stats_name = tag(f"stats_pairwise__{variable_of_interest}")
        stats_path = target_dir / f"{stats_name}.csv"
        stats["pairwise"].to_csv(stats_path, index=False)
        manifest["outputs"].append({"type": "stats", "path": str(stats_path), "rows": int(stats["pairwise"].shape[0])})

        fig = plot_hist_kde_by_coin(group_df, variable_of_interest=variable_of_interest)
        paths = _save_fig(
            fig,
            output_base=target_dir / tag(f"hist_kde__{variable_of_interest}"),
            formats=formats,
            dpi=dpi,
        )
        manifest["outputs"].append({"type": "plot", "paths": paths})

        fig = plot_blocks_gt3_vs_time(
            group_df,
            variable_of_interest=variable_of_interest,
            y_label=y_label,
        )
        paths = _save_fig(
            fig,
            output_base=target_dir / tag(f"tp2_time__{variable_of_interest}"),
            formats=formats,
            dpi=dpi,
        )
        manifest["outputs"].append({"type": "plot", "paths": paths})

        fig = plot_blocks_gt3_vs_block_num(
            group_df,
            variable_of_interest=variable_of_interest,
            y_label=y_label,
        )
        paths = _save_fig(
            fig,
            output_base=target_dir / tag(f"tp2_blocknum__{variable_of_interest}"),
            formats=formats,
            dpi=dpi,
        )
        manifest["outputs"].append({"type": "plot", "paths": paths})

        figs = plot_blocks_gt3_block_facets(
            group_df,
            variable_of_interest=variable_of_interest,
            y_label=y_label,
            blocks_per_facet=blocks_per_facet,
        )
        paths = _save_figs(
            figs,
            output_base=target_dir / tag(f"tp2_facets__{variable_of_interest}"),
            formats=formats,
            dpi=dpi,
        )
        manifest["outputs"].append({"type": "plot", "paths": paths})

        fig = plot_block3_round_num(
            group_df,
            variable_of_interest=variable_of_interest,
            y_label=y_label,
        )
        paths = _save_fig(
            fig,
            output_base=target_dir / tag(f"block3_roundnum__{variable_of_interest}"),
            formats=formats,
            dpi=dpi,
        )
        manifest["outputs"].append({"type": "plot", "paths": paths})

        fig = plot_block3_round_time(
            group_df,
            variable_of_interest=variable_of_interest,
            y_label=y_label,
        )
        paths = _save_fig(
            fig,
            output_base=target_dir / tag(f"block3_roundtime__{variable_of_interest}"),
            formats=formats,
            dpi=dpi,
        )
        manifest["outputs"].append({"type": "plot", "paths": paths})

    manifest_path = per_file_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


if __name__ == "__main__":
    result = run_suite_for_file(
        "AN_PinDropsKnottedFiltered_1st50Rds_noCD_main.csv",
        out_root="plots_out",
        variable_of_interest="truecontent_elapsed_s",
    )
    print(json.dumps(result, indent=2))