#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import argparse
import json
from io import StringIO
import contextlib

import numpy as np
import pandas as pd

from histoHelpers_v2 import (
    _slugify,
    _ensure_dirs,
    _run_and_collect_figs,
    _save_figs,
    _freedman_diaconis_width,
    _collect_numeric,
)
from histoStats_v2 import test_coin_distributions_tp2, test_coin_distributions_all,  _enough_for_stats
from pinDropPlots_v2 import (
    plot_histkde_allsubjects,
    plot_tp2_scatter_allsubjects,
    plot_pinDrop_block3_lines_by_round,
    plot_pinDrop_blocks_lines_by_block,
    plot_violin_allsubjects,
)


def read_csv_loose(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "coinLabel" in df.columns:
        df["coinLabel"] = df["coinLabel"].astype("string").str.strip()
    if "dropQual" in df.columns:
        df["dropQual"] = df["dropQual"].astype("string").str.strip().str.lower()
    return df


def _series_truthy(s: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(s):
        return s.fillna(False)
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce").fillna(0).astype(float) != 0.0
    text = s.astype("string").str.strip().str.lower()
    return text.isin({"1", "true", "t", "yes", "y"})


def apply_cli_filters(
    df: pd.DataFrame,
    *,
    require_cols: list[str],
    exclude_true_cols: list[str],
) -> pd.DataFrame:
    out = df.copy()

    missing = [c for c in require_cols + exclude_true_cols if c not in out.columns]
    if missing:
        raise ValueError(f"Missing filter columns in input data: {missing}")

    mask = pd.Series(True, index=out.index)

    for col in require_cols:
        mask &= _series_truthy(out[col])

    for col in exclude_true_cols:
        mask &= ~_series_truthy(out[col])

    return out.loc[mask].copy()


def build_stats_header(
    *,
    voi: str,
    voi_str: str,
    voi_unit: str,
    facet_by: str,
    require_cols: list[str],
    exclude_true_cols: list[str],
    outlier_method: str,
    n_rows_before: int,
    n_rows_after: int,
) -> str:
    require_text = ", ".join(require_cols) if require_cols else "<none>"
    exclude_text = ", ".join(exclude_true_cols) if exclude_true_cols else "<none>"

    lines = [
        "Pin-drop stats report",
        f"VOI column: {voi}",
        f"VOI label: {voi_str}",
        f"VOI unit: {voi_unit or '<none>'}",
        f"Facet column: {facet_by or '<none>'}",
        f"Require-true filter columns: {require_text}",
        f"Exclude-true filter columns: {exclude_text}",
        f"Outlier detection method used: {outlier_method}",
        f"Rows before wrapper filtering: {n_rows_before}",
        f"Rows after wrapper filtering: {n_rows_after}",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="All-subjects pin-drop plotting wrapper (VOI-driven).")
    ap.add_argument("--input", required=True, help="Path to input CSV")
    ap.add_argument("--out-root", required=True, help="Output root directory")
    ap.add_argument("--formats", default="pdf", help="Comma-separated formats, e.g., pdf or png,pdf")
    ap.add_argument("--voi", required=True, help="Variable of interest column name (e.g., dropDist)")
    ap.add_argument("--voi-str", default="Measure", help="Human-readable name for VOI")
    ap.add_argument("--voi-unit", default="", help="Units string, e.g. (m)")
    ap.add_argument("--facet-by", default="", help="Optional grouping column (e.g., coinSet). Leave blank for no facet.")
    ap.add_argument("--dot-mode", default="panel", choices=["panel", "baseline", "none"])
    ap.add_argument(
        "--require-cols",
        nargs="*",
        default=[],
        help="Keep rows where all of these columns are truthy. Example: --require-cols isEligibleBase isEligibleRoundDur",
    )
    ap.add_argument(
        "--exclude-true-cols",
        nargs="*",
        default=[],
        help="Drop rows where any of these columns are truthy. Example: --exclude-true-cols roundDur_pref_out",
    )
    ap.add_argument(
        "--outlier-method",
        default="none",
        help="Name of the external outlier detection method used. Recorded in stats.txt only.",
    )
    ap.add_argument(
        "--bin-width",
        type=float,
        default=None,
        help="Fixed histogram bin width (in VOI units). If omitted, FD width is used when --fix-xlim or --fd-source present.",
    )
    ap.add_argument("--fix-xlim", action="store_true", help="Lock x-axis limits so all plots share the same range.")
    ap.add_argument("--xlim", type=float, nargs=2, metavar=("MIN", "MAX"), help="Explicit x-axis limits. Implies --fix-xlim.")
    ap.add_argument("--fd-source", type=str, default=None, help="CSV/Parquet to compute global FD bin width/edges from.")
    ap.add_argument("--ylim", type=float, default=None)
    ap.add_argument("--blockmin", type=int, default=4)
    ap.add_argument("--blockmax", type=int, default=24)
    args = ap.parse_args()

    in_path = Path(args.input)
    out_root = Path(args.out_root)
    formats = tuple(s.strip() for s in args.formats.split(",") if s.strip())

    voi = args.voi
    voi_str = args.voi_str
    voi_unit = args.voi_unit
    facet_by = args.facet_by.strip()
    dot_mode = args.dot_mode

    raw_df = read_csv_loose(in_path)
    df = apply_cli_filters(
        raw_df,
        require_cols=args.require_cols,
        exclude_true_cols=args.exclude_true_cols,
    )

    if args.fd_source:
        src = args.fd_source
        big = pd.read_parquet(src) if src.lower().endswith((".parquet", ".pq")) else pd.read_csv(src)
        if args.bin_width is None:
            width = _freedman_diaconis_width(_collect_numeric(big, voi))
            if np.isfinite(width) and width > 0:
                args.bin_width = width

    common_dir = out_root / "_ALL"
    per_file_dir = out_root / in_path.stem
    _ensure_dirs(common_dir, per_file_dir)

    if facet_by and facet_by in df.columns:
        df[facet_by] = df[facet_by].astype("category")
        groups = list(df.groupby(facet_by, dropna=False, sort=True))
    else:
        groups = [(None, df)]

    manifest: dict[str, object] = {"file": str(in_path), "outputs": []}

    for gkey, gdf in groups:
        tag_suffix = f"__{facet_by}-{_slugify(gkey)}" if gkey is not None else ""
        target_dir = per_file_dir if gkey is None else (per_file_dir / _slugify(gkey))
        _ensure_dirs(target_dir)

        ##### histkde labeled
        figs = _run_and_collect_figs(
            plot_histkde_allsubjects,
            gdf,
            variableOfInterest=voi,
            voi_str=voi_str,
            voi_unit=voi_unit,
            dot_mode=("panel" if dot_mode == "panel" else "baseline" if dot_mode == "baseline" else "none"),
            title_prefix=(f"{facet_by}: {gkey}" if gkey is not None else ""),
            bin_width=args.bin_width,
            fix_xlim=bool(args.fix_xlim or args.xlim),
            xlim=tuple(args.xlim) if args.xlim else None,
        )
        _save_figs(
            figs,
            common_dir=common_dir,
            per_file_dir=target_dir,
            stem=in_path.stem,
            tag=f"histkde_{voi}{tag_suffix}",
            formats=formats,
            dpi=220,
            write_common=True,
        )
        manifest["outputs"].append({"type": "plot", "tag": f"histkde_{voi}{tag_suffix}", "count": len(figs)})
        ################

        ##### histkde unlabeled
        figs = _run_and_collect_figs(
            plot_histkde_allsubjects,
            gdf,
            variableOfInterest=voi,
            voi_str=voi_str,
            voi_unit=voi_unit,
            dot_mode=("panel" if dot_mode == "panel" else "baseline" if dot_mode == "baseline" else "none"),
            title_prefix=(f"{facet_by}: {gkey}" if gkey is not None else ""),
            bin_width=args.bin_width,
            fix_xlim=bool(args.fix_xlim or args.xlim),
            xlim=tuple(args.xlim) if args.xlim else None,
            show_bin_stats=False,
        )
        _save_figs(
            figs,
            common_dir=common_dir,
            per_file_dir=target_dir,
            stem=in_path.stem,
            tag=f"histkde_{voi}{tag_suffix}_unlabeled",
            formats=formats,
            dpi=220,
            write_common=True,
        )
        manifest["outputs"].append({"type": "plot", "tag": f"histkde_{voi}{tag_suffix}_unlabeled", "count": len(figs)})
        ################

        ######### violin labeled
        figs_violin = _run_and_collect_figs(
            plot_violin_allsubjects,
            gdf,
            variableOfInterest=voi,
            voi_str=voi_str,
            voi_unit=voi_unit,
            title_prefix=(f"{facet_by}: {gkey}" if gkey is not None else ""),
        )
        _save_figs(
            figs_violin,
            common_dir=common_dir,
            per_file_dir=target_dir,
            stem=in_path.stem,
            tag=f"violin_{voi}{tag_suffix}",
            formats=formats,
            dpi=220,
            write_common=True,
        )
        manifest["outputs"].append({"type": "plot", "tag": f"violin_{voi}{tag_suffix}", "count": len(figs_violin)})
        ################

        ######### TP2 scatter labeled
        figs_scatter = _run_and_collect_figs(
            plot_tp2_scatter_allsubjects,
            gdf,
            variableOfInterest=voi,
            voi_str=voi_str,
            voi_unit=voi_unit,
            title_prefix=(f"{facet_by}: {gkey}" if gkey is not None else ""),
        )
        _save_figs(
            figs_scatter,
            common_dir=common_dir,
            per_file_dir=target_dir,
            stem=in_path.stem,
            tag=f"tp2_scatter_{voi}{tag_suffix}",
            formats=formats,
            dpi=220,
            write_common=True,
        )
        manifest["outputs"].append({"type": "plot", "tag": f"tp2_scatter_{voi}{tag_suffix}", "count": len(figs_scatter)})
        ################


        stats_common = common_dir / "Stats"
        _ensure_dirs(stats_common)

        stats_header = build_stats_header(
            voi=voi,
            voi_str=voi_str,
            voi_unit=voi_unit,
            facet_by=facet_by,
            require_cols=args.require_cols,
            exclude_true_cols=args.exclude_true_cols,
            outlier_method=args.outlier_method,
            n_rows_before=len(raw_df),
            n_rows_after=len(df),
        )

        stats_ok, why_not = _enough_for_stats(gdf, voi, blocks_min=3, min_n_per_group=10)

        # --- TP2 stats: preserves old behavior ---
        if stats_ok:
            tp2_pair_df = None
            try:
                tp2_results = test_coin_distributions_tp2(
                    gdf,
                    variableOfInterest=voi,
                    voi_str=voi_str,
                    max_totalRounds=1,
                    min_n_per_group=10,
                    alpha=0.05,
                    verbose=False,
                )
                if isinstance(tp2_results, dict):
                    tp2_pair_df = tp2_results.get("pairwise")
            except Exception as exc:
                why_not = f"TP2 stats generation failed: {exc}"

            if isinstance(tp2_pair_df, pd.DataFrame) and not tp2_pair_df.empty:
                stats_csv = target_dir / f"stats_pairwise_TP2__{voi}{tag_suffix}.csv"
                tp2_pair_df.to_csv(stats_csv, index=False)
                common_stats_csv = stats_common / f"{in_path.stem}__stats_pairwise_TP2__{voi}{tag_suffix}.csv"
                tp2_pair_df.to_csv(common_stats_csv, index=False)
                manifest["outputs"].append(
                    {"type": "stats_csv", "tag": f"stats_pairwise_TP2__{voi}{tag_suffix}", "rows": int(tp2_pair_df.shape[0])}
                )

            buf = StringIO()
            try:
                with contextlib.redirect_stdout(buf):
                    test_coin_distributions_tp2(
                        gdf,
                        variableOfInterest=voi,
                        voi_str=voi_str,
                        max_totalRounds=1,
                        min_n_per_group=10,
                        alpha=0.05,
                        verbose=True,
                    )
            except Exception:
                pass

            report_txt = stats_header + (buf.getvalue() or "No verbose output captured.")
            report_name = f"stats_verbose_TP2__{voi}{tag_suffix}"
            (target_dir / f"{report_name}.txt").write_text(report_txt, encoding="utf-8")
            (stats_common / f"{in_path.stem}__{report_name}.txt").write_text(report_txt, encoding="utf-8")
            manifest["outputs"].append({"type": "stats_report", "tag": report_name, "bytes": len(report_txt)})
        else:
            manifest["outputs"].append(
                {"type": "stats", "tag": f"stats_pairwise_TP2__{voi}{tag_suffix}", "skipped": True, "reason": why_not}
            )

        # --- ALL stats: no BlockNum > 3 restriction ---
        all_pair_df = None
        all_why_not = ""
        try:
            all_results = test_coin_distributions_all(
                gdf,
                variableOfInterest=voi,
                voi_str=voi_str,
                min_n_per_group=10,
                alpha=0.05,
                verbose=False,
            )
            if isinstance(all_results, dict):
                all_pair_df = all_results.get("pairwise")
        except Exception as exc:
            all_why_not = f"ALL stats generation failed: {exc}"

        if isinstance(all_pair_df, pd.DataFrame) and not all_pair_df.empty:
            stats_csv = target_dir / f"stats_pairwise_all__{voi}{tag_suffix}.csv"
            all_pair_df.to_csv(stats_csv, index=False)
            common_stats_csv = stats_common / f"{in_path.stem}__stats_pairwise_all__{voi}{tag_suffix}.csv"
            all_pair_df.to_csv(common_stats_csv, index=False)
            manifest["outputs"].append(
                {"type": "stats_csv", "tag": f"stats_pairwise_all__{voi}{tag_suffix}", "rows": int(all_pair_df.shape[0])}
            )

            buf = StringIO()
            try:
                with contextlib.redirect_stdout(buf):
                    test_coin_distributions_all(
                        gdf,
                        variableOfInterest=voi,
                        voi_str=voi_str,
                        min_n_per_group=10,
                        alpha=0.05,
                        verbose=True,
                    )
            except Exception:
                pass

            report_txt = stats_header + (buf.getvalue() or "No verbose output captured.")
            report_name = f"stats_verbose_all__{voi}{tag_suffix}"
            (target_dir / f"{report_name}.txt").write_text(report_txt, encoding="utf-8")
            (stats_common / f"{in_path.stem}__{report_name}.txt").write_text(report_txt, encoding="utf-8")
            manifest["outputs"].append({"type": "stats_report", "tag": report_name, "bytes": len(report_txt)})
        else:
            manifest["outputs"].append(
                {"type": "stats", "tag": f"stats_pairwise_all__{voi}{tag_suffix}", "skipped": True, "reason": all_why_not or "insufficient data"}
            )


        figs1 = _run_and_collect_figs(
            plot_pinDrop_block3_lines_by_round,
            gdf,
            variableOfInterest="dropDist",
            yLabel="Pin Drop Distance to Closest Coin (m)",
            ylim=args.ylim,
        )
        _save_figs(
            figs1,
            common_dir=common_dir,
            per_file_dir=target_dir,
            stem=in_path.stem,
            tag=f"tp1_line_{voi}{tag_suffix}",
            formats=formats,
            dpi=220,
            write_common=True,
        )
        manifest["outputs"].append({"type": "plot", "tag": f"tp1_line_{voi}{tag_suffix}", "count": len(figs1)})

        figs2 = _run_and_collect_figs(
            plot_pinDrop_blocks_lines_by_block,
            gdf,
            variableOfInterest="dropDist",
            yLabel="Pin Drop Distance to Closest Coin (m)",
            block_min=args.blockmin,
            block_max=args.blockmax,
            ylim=args.ylim,
        )
        _save_figs(
            figs2,
            common_dir=common_dir,
            per_file_dir=target_dir,
            stem=in_path.stem,
            tag=f"tp2_line_{voi}{tag_suffix}",
            formats=formats,
            dpi=220,
            write_common=True,
        )
        manifest["outputs"].append({"type": "plot", "tag": f"tp2_line_{voi}{tag_suffix}", "count": len(figs2)})

    manifest_path = per_file_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()