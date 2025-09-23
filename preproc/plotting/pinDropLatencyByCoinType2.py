# === Add below your helpers (read_data, add_true_session_elapsed*, _run_and_collect_figs, _save_figs, etc.) ===
from __future__ import annotations
from pathlib import Path
from typing import Iterable, Sequence, Callable, Any
import re
import pandas as pd
import matplotlib.pyplot as plt

# Type alias for plot functions that accept (df, **kwargs)
PlotFn = Callable[..., Any]

def _slugify(x: Any, maxlen: int = 64) -> str:
    s = re.sub(r"\W+", "_", str(x)).strip("_")
    return (s[:maxlen] or "NA")

def _prepare_subset(
    df: pd.DataFrame,
    *,
    filter_columns: Iterable[str] = ("truecontent_elapsed_s", "dropDist"),
    use_outlier_filter: bool = False,
    outlier_method: str = "median",
    outlier_sigma: float = 2.0,
    outlier_ddof: int = 1,
    outlier_keep_na: bool = False,
    outlier_groupby: str | list[str] | None = "coinLabel",
) -> pd.DataFrame:
    """
    Apply a single pass of outlier removal over the given columns (intersection keep).
    If `outlier_groupby` is not None, outlier detection is done within each group.
    """
    out = df.copy()
    if use_outlier_filter:
        for col in filter_columns:
            if col in out.columns:
                out = exclude_outliers(
                    out,
                    col,
                    method=outlier_method,
                    sigma=outlier_sigma,
                    ddof=outlier_ddof,
                    groupby=outlier_groupby,
                    keep_na=outlier_keep_na,
                )
    return out

def run_plots_and_stats_for_file(
    csv_path: Path | str,
    out_root: Path | str = "plots_out",
    *,
    # grouping/splitting
    groupby: str | list[str] | None = None,
    group_subdirs: bool = False,      # if True, create per-group subfolders under per-file dir
    # outlier controls (applied ONCE per file+group)
    use_outlier_filter: bool = False,
    outlier_groupby: str | list[str] | None = "coinLabel",
    outlier_method: str = "median",   # "mean" | "median"
    outlier_sigma: float = 2.0,
    outlier_ddof: int = 1,
    outlier_keep_na: bool = False,
    filter_columns: Iterable[str] = ("truecontent_elapsed_s", "dropDist"),
    # plotting controls
    blocks_per_facet: int = 20,
    formats: Iterable[str] = ("png", "pdf"),
    dpi: int = 220,
    # choose which plots/stats to run
    which_plots: tuple[str, ...] = (
        "block3_round_vs_block",
        "blocks_gt3_overall_vs_block",
        "blocks_gt3_overall_vs_session",
        "blocks_gt3_overall_vs_block_facet",
        "blocks_gt3_overall_vs_BlockNum",
        "hist_kde_dropDist",
        "hist_kde_truecontent",
    ),
    which_stats: tuple[str, ...] = ("latency", "dropdist"),
) -> dict:
    """
    Preps data once per file (+ per-group if `groupby` is given),
    then calls the selected plotting & stats functions. All figures are saved
    to both a common and a per-file directory. Stats tables are saved as CSV.
    Returns a small manifest with where things were written.
    """
    csv_path = Path(csv_path)
    out_root = Path(out_root)
    common_dir = out_root / "_ALL"
    per_file_dir = out_root / csv_path.stem
    _ensure_dirs(common_dir, per_file_dir)

    df = read_data(csv_path)
    if "coinLabel" in df.columns:
        df = df[df["coinLabel"].astype(str).str.len() > 0]

    df_elapsed = add_true_session_elapsed(df)
    df_elapsed = add_true_session_elapsed_by_block_events(df_elapsed)

    # groups to iterate
    if groupby is None:
        groups = [(None, df_elapsed)]
    else:
        groups = list(df_elapsed.groupby(groupby, dropna=False, sort=True))

    manifest: dict = {"file": str(csv_path), "outputs": []}

    for gkey, gdf in groups:
        # Prepare once per file+group
        prepped = _prepare_subset(
            gdf,
            filter_columns=filter_columns,
            use_outlier_filter=use_outlier_filter,
            outlier_method=outlier_method,
            outlier_sigma=outlier_sigma,
            outlier_ddof=outlier_ddof,
            outlier_keep_na=outlier_keep_na,
            outlier_groupby=outlier_groupby,
        )

        # Where to drop group results
        group_tag = None if gkey is None else _slugify(gkey)
        target_dir = per_file_dir / group_tag if (group_tag and group_subdirs) else per_file_dir
        _ensure_dirs(target_dir)

        def _tag(base: str) -> str:
            return f"{base}__grp-{group_tag}" if group_tag else base

        # ---- Plots (each call may create 1+ figures) ----
        jobs: list[tuple[PlotFn, dict, str]] = []
        if "block3_round_vs_block" in which_plots:
            jobs.append((plot_block3_round_vs_block, {}, _tag("block3_round_vs_block")))
        if "blocks_gt3_overall_vs_block" in which_plots:
            jobs.append((plot_blocks_gt3_overall_vs_block, {}, _tag("blocks_gt3_overall_vs_block")))
        if "blocks_gt3_overall_vs_session" in which_plots:
            jobs.append((plot_blocks_gt3_overall_vs_session, {}, _tag("blocks_gt3_overall_vs_session")))
        if "blocks_gt3_overall_vs_block_facet" in which_plots:
            jobs.append((plot_blocks_gt3_overall_vs_block_facet, {"blocks_per_facet": blocks_per_facet}, _tag(f"blocks_gt3_facet_{blocks_per_facet}")))
        if "blocks_gt3_overall_vs_BlockNum" in which_plots:
            jobs.append((plot_blocks_gt3_overall_vs_BlockNum, {}, _tag("blocks_gt3_vs_BlockNum")))
        if "hist_kde_dropDist" in which_plots:
            jobs.append((plot_hist_kde_by_coin, {"variableOfInterest": "dropDist"}, _tag("hist_kde_dropDist")))
        if "hist_kde_truecontent" in which_plots:
            jobs.append((plot_hist_kde_by_coin, {"variableOfInterest": "truecontent_elapsed_s"}, _tag("hist_kde_truecontent")))

        for fn, kwargs, tag in jobs:
            figs = _run_and_collect_figs(fn, prepped, **kwargs)
            _save_figs(
                figs,
                common_dir=common_dir,
                per_file_dir=target_dir,
                stem=csv_path.stem,
                tag=tag,
                formats=formats,
                dpi=dpi,
            )
            plt.close("all")
            manifest["outputs"].append({"type": "plot", "tag": tag, "count": len(figs)})

        # ---- Stats (saved as CSV) ----
        if "latency" in which_stats:
            res = test_coin_latency_distributions(prepped, blocks_min=3, min_n_per_group=10, alpha=0.05, verbose=False)
            pair_df = res["pairwise"]
            stats_name = _tag("stats_pairwise_truecontent")
            pair_df.to_csv(target_dir / f"{stats_name}.csv", index=False)
            manifest["outputs"].append({"type": "stats", "tag": stats_name, "rows": int(getattr(pair_df, "shape", (0, 0))[0])})

        if "dropdist" in which_stats:
            res2 = test_coin_distributions(prepped, variableOfInterest="dropDist", blocks_min=3, min_n_per_group=10, alpha=0.05, verbose=False)
            pair_df2 = res2["pairwise"]
            stats_name2 = _tag("stats_pairwise_dropDist")
            pair_df2.to_csv(target_dir / f"{stats_name2}.csv", index=False)
            manifest["outputs"].append({"type": "stats", "tag": stats_name2, "rows": int(getattr(pair_df2, "shape", (0, 0))[0])})

    return manifest

def run_plots_and_stats_for_directory(
    input_dir: Path | str,
    pattern: str = "*.csv",
    *,
    recursive: bool = False,
    **kwargs,   # forwarded to run_plots_and_stats_for_file
) -> list[dict]:
    """
    Iterates over all CSVs in a directory (optionally recursively) and runs
    the per-file wrapper for each.
    """
    root = Path(input_dir)
    paths = sorted(root.rglob(pattern) if recursive else root.glob(pattern))
    results = []
    for p in paths:
        if p.is_file():
            print(f"[run] {p}")
            results.append(run_plots_and_stats_for_file(p, **kwargs))
    print(f"Done. Processed {len(results)} files.")
    return results

# --- Example invocations (uncomment to use) ---
# if __name__ == "__main__":
#     # Single file, grouped by Participant (example), remove outliers per coin within each Participant
#     run_plots_and_stats_for_file(
#         CSV_PATH,
#         out_root="plots_out",
#         groupby="Participant",
#         group_subdirs=True,
#         use_outlier_filter=True,
#         outlier_groupby="coinLabel",   # outliers detected per coin within each Participant subset
#         outlier_method="median",
#         outlier_sigma=2.0,
#         blocks_per_facet=20,
#     )
#
#     # Whole directory, no grouping, outliers off
#     run_plots_and_stats_for_directory(
#         "/path/to/csvs",
#         pattern="*.csv",
#         recursive=False,
#         out_root="plots_out",
#         groupby=None,
#         use_outlier_filter=False,
#     )
