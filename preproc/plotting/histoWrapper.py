from __future__ import annotations
from pathlib import Path
from typing import Iterable, Sequence, Any
import contextlib
import json

import matplotlib.pyplot as plt
import pandas as pd

from histoHelpers import (
    read_data, _prep_df_for_file, _slugify, _ensure_dirs, log,
)
from histoFlexPlots import (
    plot_hist_kde_by_coin,
    plot_blocks_gt3_overall_vs_TP2Blocks_Time,
    plot_blocks_gt3_overall_vs_TP2Blocks_BlockNum,
    plot_blocks_gt3_overall_vs_block_facet,
    plot_block3_roundNum,
    plot_block3_roundTime,
)
from histoStats import test_coin_distributions, _enough_for_stats

import io
import contextlib


# ---------- figure capture / saving ----------
PlotFn = Any

def _run_and_collect_figs(fn: PlotFn, *args, **kwargs) -> list[plt.Figure]:
    before = set(plt.get_fignums())
    _orig_show = plt.show
    try:
        plt.show = lambda *a, **k: None
        fn(*args, **kwargs)
    except Exception as e:
        print(f"[plot] {fn.__name__} skipped: {e}", flush=True)
        return []
    finally:
        plt.show = _orig_show
    after = set(plt.get_fignums())
    new_nums = sorted(after - before)
    return [plt.figure(n) for n in new_nums]

def _save_figs_v1a(
    figs: Sequence[plt.Figure],
    *,
    common_dir: Path,
    common_dirBool: bool = True,
    per_file_dir: Path,
    stem: str,
    tag: str,
    formats: Iterable[str] = ("png", "pdf"),
    dpi: int = 220,
) -> None:
    _ensure_dirs(common_dir, per_file_dir)
    multi = len(figs) > 1
    for i, fig in enumerate(figs, 1):
        suffix = f"_p{i:02d}" if multi else ""
        base_common = f"{stem}__{tag}{suffix}"
        base_specific = f"{tag}{suffix}"
        with contextlib.suppress(Exception):
            fig.tight_layout()
        for ext in formats:
            #fig.savefig(Path(common_dir) / f"{base_common}.{ext}", dpi=dpi, bbox_inches="tight")
            fig.savefig(Path(per_file_dir) / f"{base_specific}.{ext}", dpi=dpi, bbox_inches="tight")
            if common_dirBool == True:
                fig.savefig(Path(common_dir) / f"{base_common}.{ext}", dpi=dpi, bbox_inches="tight")
            else:
                continue

    plt.close("all")

def _save_figs(
    figs: Sequence[plt.Figure],
    *,
    common_dir: Path,
    per_file_dir: Path,
    stem: str,
    tag: str,
    formats: Iterable[str] = ("png", "pdf"),
    dpi: int = 220,
    common_dirBool: bool = True,   # <--- default + proper type
) -> None:
    # ensure only what we need
    _ensure_dirs(per_file_dir)
    if common_dirBool and common_dir is not None:
        _ensure_dirs(common_dir)

    multi = len(figs) > 1
    for i, fig in enumerate(figs, 1):
        suffix = f"_p{i:02d}" if multi else ""
        base_common = f"{stem}_{tag}{suffix}"
        base_specific = f"{tag}{suffix}"
        with contextlib.suppress(Exception):
            fig.tight_layout()
        for ext in formats:
            # always save per-file
            fig.savefig(Path(per_file_dir) / f"{base_specific}.{ext}", dpi=dpi, bbox_inches="tight")
            # optionally save to common
            if common_dirBool and common_dir is not None:
                fig.savefig(Path(common_dir) / f"{base_common}.{ext}", dpi=dpi, bbox_inches="tight")

    plt.close("all")

# ---------- main suite ----------
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
    variableOfInterest: str = "truecontent_elapsed_s",
    voi_str: str = "Round Elapsed Time",
    voi_UnitStr: str = "(s)",
    yLabel_map: dict[str, str] | None = None,
    allowed_status: Iterable[str] | None = ("complete",),
    # ---------- NEW/Fixed ----------
    bin_width: Optional[float] = None,
    fix_xlim: bool = False,
    xlim: Optional[tuple[float, float]] = None,
    fd_source: Optional[str] = None,
) -> dict:

    """
    Executes (conditionally):
      test_coin_distributions
      plot_hist_kde_by_coin
      plot_blocks_gt3_overall_vs_TP2Blocks_Time
      plot_blocks_gt3_overall_vs_TP2Blocks_BlockNum
      plot_blocks_gt3_overall_vs_block_facet
      plot_block3_roundNum
      plot_block3_roundTime
    """
    csv_path = Path(csv_path)
    out_root = Path(out_root)
    common_dir = out_root / "_ALL"
    per_file_dir = out_root / csv_path.stem
    _ensure_dirs(common_dir, per_file_dir)

    # read + prep
    df = read_data(csv_path)
    df = _prep_df_for_file(
        df,
        use_outlier_filter=use_outlier_filter,
        filter_columns=filter_columns,
        outlier_groupby=outlier_groupby,
        outlier_method=outlier_method,
        outlier_sigma=outlier_sigma,
        outlier_ddof=outlier_ddof,
        outlier_keep_na=outlier_keep_na,
    )
    # Compute bin_width from giant file if requested and not explicitly set
    if fd_source and bin_width is None:
        big_path = Path(fd_source)
        big = pd.read_parquet(big_path) if big_path.suffix.lower() in {".parquet", ".pq"} else pd.read_csv(big_path)
        vec = _collect_numeric(big, variableOfInterest) if variableOfInterest in big.columns else np.array([])
        width = _freedman_diaconis_width(vec) if vec.size else 0.0
        if np.isfinite(width) and width > 0:
            bin_width = float(width)

    # Validate xlim
    if xlim is not None:
        if not (isinstance(xlim, tuple) and len(xlim) == 2 and all(isinstance(v, (int, float)) for v in xlim)):
            raise ValueError("xlim must be a tuple of two numbers, e.g., (0.0, 10.0)")
        lo, hi = float(xlim[0]), float(xlim[1])
        if not np.isfinite(lo) or not np.isfinite(hi):
            raise ValueError("xlim values must be finite numbers")
        if lo == hi:
            hi = lo + 1.0
        # enforce ordered tuple
        xlim = (min(lo, hi), max(lo, hi))
        fix_xlim = True  # explicit xlim implies fixed axes

    # grouping
    groups: list[tuple[Any, pd.DataFrame]]
    if groupby is None:
        groups = [(None, df)]
    else:
        groups = list(df.groupby(groupby, dropna=False, sort=True))

    # labels
    default_labels = {
        "truecontent_elapsed_s": "Pin Drop Latency Within Round (s)",
        "dropDist": "Pin Drop Distance to Closest Coin Not Yet Collected (m)",
    }
    if yLabel_map:
        default_labels.update(yLabel_map)
    yLabel = default_labels.get(variableOfInterest, variableOfInterest)

    manifest: dict = {"file": str(csv_path), "outputs": []}

    for gkey, gdf in groups:
        group_tag = None if gkey is None else _slugify(gkey)
        target_dir = per_file_dir / group_tag if (group_tag and group_subdirs) else per_file_dir
        _ensure_dirs(target_dir)

        def _tag(base: str) -> str:
            return f"{base}__grp-{group_tag}" if group_tag else base

        # 1) STATS — only run if dataset is meaningful; otherwise skip
        ok, why_not = _enough_for_stats(
            gdf,
            variableOfInterest,
            blocks_min=3,
            min_n_per_group=10,
            allowed_status=allowed_status,
        )
        group_desc = f" [{gkey}]" if gkey is not None else ""
        # if ok:
        #     log(f"[stats] {csv_path.name}{group_desc}: running")
        #     res = test_coin_distributions(
        #         gdf,
        #         variableOfInterest=variableOfInterest,
        #         blocks_min=3,
        #         min_n_per_group=10,
        #         alpha=0.05,
        #         verbose=False,
        #     )
        #     pair_df = res["pairwise"]
        #     stats_name = _tag(f"stats_pairwise__{variableOfInterest}")
        #     pair_df.to_csv(target_dir / f"{stats_name}.csv", index=False)
        #     manifest["outputs"].append({"type": "stats", "tag": stats_name, "rows": int(pair_df.shape[0])})

        if ok:
            log(f"[stats] {csv_path.name}{group_desc}: running")

            # capture full verbose report
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                res = test_coin_distributions(
                    gdf,
                    variableOfInterest=variableOfInterest,
                    blocks_min=3,
                    min_n_per_group=10,
                    alpha=0.05,
                    verbose=True,  # turn on the prints we want to capture
                )
            report_txt = buf.getvalue()

            # # save the pairwise CSV as before
            # pair_df = res["pairwise"]
            # stats_name = _tag(f"stats_pairwise__{variableOfInterest}")
            # pair_df.to_csv(target_dir / f"{stats_name}.csv", index=False)
            # manifest["outputs"].append({"type": "stats", "tag": stats_name, "rows": int(pair_df.shape[0])})

            # NEW: also save the verbose text report
            report_tag = _tag(f"stats_verbose__{variableOfInterest}")
            local_txt = target_dir / f"{report_tag}.txt"
            local_txt.write_text(report_txt, encoding="utf-8")

            # common copy (nest under _ALL/Stats/, include file stem to avoid collisions)
            stats_common_dir = common_dir / "Stats"
            stats_common_dir.mkdir(parents=True, exist_ok=True)
            common_txt = stats_common_dir / f"{csv_path.stem}__{report_tag}.txt"
            common_txt.write_text(report_txt, encoding="utf-8")

            manifest["outputs"].append({
                "type": "stats_report",
                "tag": report_tag,
                "bytes": len(report_txt),
                "paths": [str(local_txt), str(common_txt)],
            })
            
        else:
            log(f"[stats] {csv_path.name}{group_desc}: SKIPPED — {why_not}")
            manifest["outputs"].append({
                "type": "stats",
                "tag": _tag(f"stats_pairwise_{variableOfInterest}"),
                "skipped": True,
                "reason": why_not,
            })

        # 2) plot_hist_kde_by_coin
        figs = _run_and_collect_figs(
            plot_hist_kde_by_coin,
            gdf,
            variableOfInterest=variableOfInterest,
            voi_str= voi_str,
            voi_UnitStr=voi_UnitStr,
            bin_width=bin_width,
            fix_xlim=bool(fix_xlim or xlim),
            xlim=tuple(xlim) if xlim else None,
        )
        _save_figs(figs, common_dir=common_dir / "HistKDE", per_file_dir=target_dir,
                   stem=csv_path.stem, tag=_tag(f"hist_kde_{variableOfInterest}"),
                   formats=formats, dpi=dpi)
        manifest["outputs"].append({"type": "plot", "tag": _tag(f"hist_kde_{variableOfInterest}"), "count": len(figs)})

        # 3) plot_blocks_gt3_overall_vs_TP2Blocks_Time
        figs = _run_and_collect_figs(
            plot_blocks_gt3_overall_vs_TP2Blocks_Time,
            gdf, variableOfInterest=variableOfInterest, yLabel=yLabel, voi_str= voi_str, voi_UnitStr=voi_UnitStr)

        _save_figs(figs, common_dir=common_dir / "TP2_BlocksTime", per_file_dir=target_dir,
                   stem=csv_path.stem, tag=_tag(f"tp2_time_{variableOfInterest}"),
                   formats=formats, dpi=dpi)
        manifest["outputs"].append({"type": "plot", "tag": _tag(f"tp2_time_{variableOfInterest}"), "count": len(figs)})

        # 4) plot_blocks_gt3_overall_vs_TP2Blocks_BlockNum
        figs = _run_and_collect_figs(
            plot_blocks_gt3_overall_vs_TP2Blocks_BlockNum,
            gdf, variableOfInterest=variableOfInterest, yLabel=yLabel, voi_str= voi_str, voi_UnitStr=voi_UnitStr)
        
        _save_figs(figs, common_dir=common_dir / "TP2_BlocksNum", per_file_dir=target_dir,
                   stem=csv_path.stem, tag=_tag(f"tp2_blocknum_{variableOfInterest}"),
                   formats=formats, dpi=dpi)
        manifest["outputs"].append({"type": "plot", "tag": _tag(f"tp2_blocknum_{variableOfInterest}"), "count": len(figs)})

        # 5) plot_blocks_gt3_overall_vs_block_facet
        figs = _run_and_collect_figs(
            plot_blocks_gt3_overall_vs_block_facet,
            gdf, variableOfInterest=variableOfInterest, yLabel=yLabel, blocks_per_facet=blocks_per_facet, voi_str= voi_str, voi_UnitStr=voi_UnitStr)
        
        _save_figs(figs, common_dir=common_dir / "TP2_FacetBlocks", per_file_dir=target_dir, common_dirBool = False,
                   stem=csv_path.stem, tag=_tag(f"tp2_facet{blocks_per_facet}_{variableOfInterest}"),
                   formats=formats, dpi=dpi)
        manifest["outputs"].append({"type": "plot", "tag": _tag(f"tp2_facet{blocks_per_facet}_{variableOfInterest}"), "count": len(figs)})

        # 6) plot_block3_roundNum
        figs = _run_and_collect_figs(
            plot_block3_roundNum,
            gdf, variableOfInterest=variableOfInterest, yLabel=yLabel, voi_str= voi_str, voi_UnitStr=voi_UnitStr)

        _save_figs(figs, common_dir=common_dir / "Block3_RoundNum", per_file_dir=target_dir,
                   stem=csv_path.stem, tag=_tag(f"block3_roundNum_{variableOfInterest}"),
                   formats=formats, dpi=dpi)
        manifest["outputs"].append({"type": "plot", "tag": _tag(f"block3_roundNum_{variableOfInterest}"), "count": len(figs)})

        # 7) plot_block3_roundTime
        figs = _run_and_collect_figs(
            plot_block3_roundTime,
            gdf, variableOfInterest=variableOfInterest, yLabel=yLabel, voi_str= voi_str, voi_UnitStr=voi_UnitStr)

        _save_figs(figs, common_dir=common_dir / "Block3_RoundTime", per_file_dir=target_dir,
                   stem=csv_path.stem, tag=_tag(f"block3_roundTime_{variableOfInterest}"),
                   formats=formats, dpi=dpi)
        manifest["outputs"].append({"type": "plot", "tag": _tag(f"block3_roundTime_{variableOfInterest}"), "count": len(figs)})

    return manifest

def run_suite_for_directory(
    input_dir: Path | str,
    pattern: str = "*.csv",
    *,
    recursive: bool = False,
    **kwargs,   # forwarded to run_suite_for_file
) -> list[dict]:
    root = Path(input_dir)
    paths = sorted(root.rglob(pattern) if recursive else root.glob(pattern))
    results = []
    for p in paths:
        if p.is_file():
            print(f"[run] {p}")
            results.append(run_suite_for_file(p, **kwargs))
    print(f"Done. Processed {len(results)} files.")
    return results

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Run plotting/stat suite for a CSV file or a directory of CSVs.")
    ap.add_argument("--input", required=True, help="Path to a CSV file or a directory containing CSVs")
    ap.add_argument("-p", "--pattern", default="*.csv", help="Glob pattern when input is a directory")
    ap.add_argument("-r", "--recursive", action="store_true", help="Recurse into subdirectories when input is a directory")
    ap.add_argument("-o", "--out-root", default="plots_out", help="Output root directory")

    # Suite options
    ap.add_argument("--variable-of-interest", dest="voi", default="truecontent_elapsed_s",
                    help="Column to analyze/plot (e.g., truecontent_elapsed_s or dropDist)")
    ap.add_argument("--voi_str", default="Round Elapsed Time", help="The string you want for your actual plot titles")
    ap.add_argument("--voi_UnitStr", default="(s)", help="units you want shown in your actual plots")
    ap.add_argument("--blocks-per-facet", type=int, default=20, help="Facet size for block facet plots")
    ap.add_argument("--formats", default="png,pdf", help="Comma-separated output formats (default: png,pdf)")

    # Grouping
    ap.add_argument("--groupby", nargs="*", default=None, help="Columns to group results by")
    ap.add_argument("--no-group-subdirs", dest="group_subdirs", action="store_false",
                    help="Do not create per-group subdirectories")

    # Outlier filtering
    ap.add_argument("--use-outlier-filter", action="store_true", help="Enable outlier removal")
    ap.add_argument("--outlier-groupby", nargs="*", default=["coinLabel"], help="Columns for outlier grouping")
    ap.add_argument("--outlier-method", choices=["mean", "median"], default="median")
    ap.add_argument("--outlier-sigma", type=float, default=2.0)
    ap.add_argument("--outlier-ddof", type=int, default=1)
    ap.add_argument("--outlier-keep-na", action="store_true", help="Keep NaNs in outlier column(s)")
    ap.add_argument("--filter-columns", nargs="*", default=["truecontent_elapsed_s", "dropDist"],
                    help="Columns to apply outlier filtering to")
    ap.add_argument("--bin-width", type=float, default=None)
    ap.add_argument("--fix-xlim", action="store_true")
    ap.add_argument("--xlim", type=float, nargs=2, metavar=("MIN","MAX"))
    ap.add_argument("--fd-source", type=str, default=None)
    args = ap.parse_args()

    args = ap.parse_args()

    formats = tuple(s.strip() for s in args.formats.split(",") if s.strip())
    gby = args.groupby if args.groupby else None
    ogby = args.outlier_groupby if args.outlier_groupby else None
    if args.fd_source and args.bin_width is None:
        big = pd.read_parquet(args.fd_source) if args.fd_source.endswith((".parquet",".pq")) else pd.read_csv(args.fd_source)
        from histoHelpers import _freedman_diaconis_width, _collect_numeric
        width = _freedman_diaconis_width(_collect_numeric(big, args.variableOfInterest))
        if np.isfinite(width) and width > 0:
            args.bin_width = width

    p = Path(args.input)
    if p.is_file():
        manifest = run_suite_for_file(
            p,
            out_root=args.out_root,
            variableOfInterest=args.voi,
            bin_width=args.bin_width,
            fix_xlim=bool(args.fix_xlim or args.xlim),
            xlim=tuple(args.xlim) if args.xlim else None,
            voi_str=args.voi_str,
            voi_UnitStr=args.voi_UnitStr,
            blocks_per_facet=args.blocks_per_facet,
            formats=formats,
            groupby=gby,
            group_subdirs=args.group_subdirs,
            use_outlier_filter=args.use_outlier_filter,
            outlier_groupby=ogby,
            outlier_method=args.outlier_method,
            outlier_sigma=args.outlier_sigma,
            outlier_ddof=args.outlier_ddof,
            outlier_keep_na=args.outlier_keep_na,
            filter_columns=args.filter_columns,
        )
        print(json.dumps(manifest, indent=2))
    elif p.is_dir():
        results = run_suite_for_directory(
            p,
            pattern=args.pattern,
            recursive=args.recursive,
            out_root=args.out_root,
            variableOfInterest=args.voi,
            bin_width=args.bin_width,
            fix_xlim=bool(args.fix_xlim or args.xlim),
            xlim=tuple(args.xlim) if args.xlim else None,
            voi_str=args.voi_str,
            voi_UnitStr=args.voi_UnitStr,
            blocks_per_facet=args.blocks_per_facet,
            formats=formats,
            groupby=gby,
            group_subdirs=args.group_subdirs,
            use_outlier_filter=args.use_outlier_filter,
            outlier_groupby=ogby,
            outlier_method=args.outlier_method,
            outlier_sigma=args.outlier_sigma,
            outlier_ddof=args.outlier_ddof,
            outlier_keep_na=args.outlier_keep_na,
            filter_columns=args.filter_columns,
        )
        print(json.dumps(results, indent=2))
    else:
        raise SystemExit(f"Input path not found: {p}")

if __name__ == "__main__":
    main()
