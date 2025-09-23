#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import argparse, json
import pandas as pd

from histoHelpers import (
    _slugify, _ensure_dirs, _run_and_collect_figs, _save_figs,
    exclude_outliers,  # assuming you have this in helpers; if not, copy from histo modules
)
from histoStats import test_coin_distributions, _enough_for_stats  # uses same VOI contract  :contentReference[oaicite:0]{index=0}
from pinDropPlots import (
    plot_histkde_allsubjects,
    plot_tp2_scatter_allsubjects,
)

# def read_csv_loose(path: Path) -> pd.DataFrame:
#     df = pd.read_csv(path)
#     # light normalization
#     if "coinLabel" in df.columns:
#         df["coinLabel"] = df["coinLabel"].astype(str).str.strip()
#     if "dropQual" in df.columns:
#         df["dropQual"] = df["dropQual"].astype(str).str.strip().lower()
#     return df

def read_csv_loose(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # light normalization
    if "coinLabel" in df.columns:
        df["coinLabel"] = df["coinLabel"].astype("string").str.strip()
    if "dropQual" in df.columns:
        df["dropQual"] = df["dropQual"].astype("string").str.strip().str.lower()
    return df



def maybe_filter_outliers(df: pd.DataFrame, cols: list[str], *, groupby: str | list[str] | None) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out = exclude_outliers(out, c, method="median", sigma=2.0, ddof=1, groupby=groupby, keep_na=False)
    return out

def main():
    ap = argparse.ArgumentParser(description="All-subjects pin-drop plotting wrapper (VOI-driven).")
    ap.add_argument("--input", required=True, help="Path to PinDrops_ALL.csv")
    ap.add_argument("--out-root", required=True, help="Output root directory")
    ap.add_argument("--formats", default="pdf", help="Comma-separated formats, e.g., pdf or png,pdf")
    ap.add_argument("--voi", required=True, help="Variable of interest column name (e.g., dropDist)")
    ap.add_argument("--voi-str", default="Measure", help="Human-readable name for VOI")
    ap.add_argument("--voi-unit", default="", help="Units string, e.g. (m)")
    ap.add_argument("--facet-by", default="", help="Optional grouping column (e.g., coinSet). Leave blank for no facet.")
    # plotting knobs (keep minimal and consistent)
    ap.add_argument("--dot-mode", default="panel", choices=["panel", "baseline", "none"])
    # outlier toggle
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--use-outlier-filter", dest="use_outliers", action="store_true")
    g.add_argument("--no-outlier-filter", dest="use_outliers", action="store_false")
    ap.set_defaults(use_outliers=False)
    ap.add_argument("--filter-columns", nargs="*", default=None,
                    help="Columns to apply outlier filtering to (default: [voi])")
    args = ap.parse_args()

    in_path = Path(args.input)
    out_root = Path(args.out_root)
    formats = tuple(s.strip() for s in args.formats.split(",") if s.strip())
    voi = args.voi
    voi_str = args.voi_str
    voi_unit = args.voi_unit
    facet_by = args.facet_by.strip()
    dot_mode = args.dot_mode

    df = read_csv_loose(in_path)

    # choose columns to filter
    filter_cols = args.filter_columns if args.filter_columns else [voi]
    if args.use_outliers:
        # if faceting, filter within facet+coinLabel; else within coinLabel
        gb_cols = [c for c in [facet_by, "coinLabel"] if c and c in df.columns] or "coinLabel"
        df = maybe_filter_outliers(df, filter_cols, groupby=gb_cols)

    # set up output dirs
    common_dir = out_root / "_ALL"
    per_file_dir = out_root / in_path.stem
    _ensure_dirs(common_dir, per_file_dir)

    # group by facet (or not)
    if facet_by and facet_by in df.columns:
        groups = list(df.groupby(facet_by, dropna=False, sort=True))
    else:
        groups = [(None, df)]

    manifest: dict = {"file": str(in_path), "outputs": []}

    for gkey, gdf in groups:
        tag_suffix = f"__{facet_by}-{_slugify(gkey)}" if gkey is not None else ""
        target_dir = per_file_dir if gkey is None else (per_file_dir / _slugify(gkey))
        _ensure_dirs(target_dir)

        # 1) HISTKDE
        figs = _run_and_collect_figs(
            plot_histkde_allsubjects,
            gdf,
            variableOfInterest=voi,
            voi_str=voi_str,
            voi_unit=voi_unit,
            dot_mode=("panel" if dot_mode == "panel" else "baseline" if dot_mode == "baseline" else "none"),
            title_prefix=(f"{facet_by}: {gkey}" if gkey is not None else ""),
        )
        _save_figs(
            figs,
            common_dir=common_dir, per_file_dir=target_dir,
            stem=in_path.stem, tag=f"histkde_{voi}{tag_suffix}",
            formats=formats, dpi=220, write_common=True
        )
        manifest["outputs"].append({"type": "plot", "tag": f"histkde_{voi}{tag_suffix}", "count": len(figs)})

        # 2) TP2 SCATTER (only if time column exists)
        if "trueSession_elapsed_s" in gdf.columns:
            figs = _run_and_collect_figs(
                plot_tp2_scatter_allsubjects,
                gdf,
                variableOfInterest=voi,
                voi_str=voi_str,
                voi_unit=voi_unit,
                title_prefix=(f"{facet_by}: {gkey}" if gkey is not None else ""),
            )
            _save_figs(
                figs,
                common_dir=common_dir, per_file_dir=target_dir,
                stem=in_path.stem, tag=f"tp2scatter_{voi}{tag_suffix}",
                formats=formats, dpi=220, write_common=True
            )
            manifest["outputs"].append({"type": "plot", "tag": f"tp2scatter_{voi}{tag_suffix}", "count": len(figs)})

        # 3) STATS (same VOI contract as histo scripts)
        ok, why_not = _enough_for_stats(gdf, voi, blocks_min=3, min_n_per_group=10, allowed_status=("complete",))
        if ok:
            res = test_coin_distributions(
                gdf, variableOfInterest=voi, voi_str=voi_str, blocks_min=3, min_n_per_group=10, alpha=0.05, verbose=False
            )
            pair_df = res["pairwise"]
            stats_csv = f"stats_pairwise__{voi}{tag_suffix}.csv"
            pair_df.to_csv(target_dir / stats_csv, index=False)
            manifest["outputs"].append({"type": "stats_csv", "tag": stats_csv, "rows": int(pair_df.shape[0])})

            # verbose report
            from io import StringIO
            buf = StringIO()
            # re-run with verbose= True but capture prints
            try:
                import sys, contextlib
                with contextlib.redirect_stdout(buf):
                    test_coin_distributions(
                        gdf, variableOfInterest=voi, voi_str=voi_str, blocks_min=3, min_n_per_group=10, alpha=0.05, verbose=True
                    )
            except Exception:
                pass
            report_txt = buf.getvalue() or "No verbose output captured."
            report_name = f"stats_verbose__{voi}{tag_suffix}"
            # ensure Stats subdir in common_dir
            stats_common = common_dir / "Stats"
            _ensure_dirs(stats_common)
            (target_dir / f"{report_name}.txt").write_text(report_txt, encoding="utf-8")
            (stats_common / f"{in_path.stem}__{report_name}.txt").write_text(report_txt, encoding="utf-8")
            manifest["outputs"].append({"type": "stats_report", "tag": report_name, "bytes": len(report_txt)})
        else:
            manifest["outputs"].append({"type": "stats", "tag": f"stats_pairwise__{voi}{tag_suffix}", "skipped": True, "reason": why_not})

    print(json.dumps(manifest, indent=2))

if __name__ == "__main__":
    main()
