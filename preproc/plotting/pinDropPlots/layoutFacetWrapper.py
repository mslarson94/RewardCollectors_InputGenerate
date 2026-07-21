# layoutFacetWrapper.py
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from histoHelpers_v2 import _ensure_dirs, _run_and_collect_figs, _save_figs
from layoutFacetPlots import (
    plot_histkde_layout_first,
    plot_violin_layout_first,
    plot_histkde_coin_type_first,
    plot_violin_coin_type_first,
)
from layoutFacetStats import (
    run_layout_first_stats,
    run_coin_type_first_stats,
)


def read_csv_loose(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "coinLabel" in df.columns:
        df["coinLabel"] = df["coinLabel"].astype("string").str.strip()
    if "dropQual" in df.columns:
        df["dropQual"] = df["dropQual"].astype("string").str.strip().str.lower()
    return df


def attach_participant_metrics(
    df: pd.DataFrame,
    summary_path: Path,
) -> pd.DataFrame:
    """Attach swapRate_tot and PVSS_AvgScore without changing row count."""
    summary = pd.read_csv(summary_path)

    required_main = {"participantID", "sessionID"}
    required_summary = {
        "participantID",
        "totScore",
        "swapRate_tot",
        "PVSS_TotalScore",
        "PVSS_AvgScore",
    }

    missing_main = required_main - set(df.columns)
    if missing_main:
        raise ValueError(
            "Participant metrics require these input columns: "
            f"{sorted(missing_main)}"
        )

    missing_summary = required_summary - set(summary.columns)
    if missing_summary:
        raise ValueError(
            "Participant summary is missing columns: "
            f"{sorted(missing_summary)}"
        )

    # Normalize identifiers to avoid int/string merge mismatches.
    out = df.copy()
    out["participantID"] = out["participantID"].astype("string").str.strip()
    out["sessionID"] = out["sessionID"].astype("string").str.strip()

    summary = summary.copy()
    summary["participantID"] = (
        summary["participantID"].astype("string").str.strip()
    )

    merge_keys = ["participantID"]
    if "sessionID" in summary.columns:
        summary["sessionID"] = summary["sessionID"].astype("string").str.strip()
        merge_keys.append("sessionID")

    keep_cols = merge_keys + [
        "totScore",
        "swapRate_tot",
        "PVSS_TotalScore",
        "PVSS_AvgScore",
    ]
    summary = summary[keep_cols].copy()

    # Multiple identical summary rows are harmless; conflicting rows are not.
    summary = summary.drop_duplicates()
    duplicate_keys = summary.duplicated(merge_keys, keep=False)
    if duplicate_keys.any():
        conflicts = summary.loc[duplicate_keys, merge_keys].drop_duplicates()
        raise ValueError(
            "Participant summary contains multiple metric rows for the same "
            f"merge key(s) {merge_keys}. Conflicting keys include:\n"
            f"{conflicts.head(10).to_string(index=False)}"
        )

    before = len(out)
    out = out.merge(
        summary,
        on=merge_keys,
        how="left",
        validate="many_to_one",
    )
    if len(out) != before:
        raise RuntimeError(
            "Participant metric merge unexpectedly changed the input row count."
        )

    return out


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
    layout_col: str,
    require_cols: list[str],
    exclude_true_cols: list[str],
    n_rows_before: int,
    n_rows_after: int,
) -> str:
    require_text = ", ".join(require_cols) if require_cols else "<none>"
    exclude_text = ", ".join(exclude_true_cols) if exclude_true_cols else "<none>"

    lines = [
        "Layout-facet stats report",
        f"VOI column: {voi}",
        f"VOI label: {voi_str}",
        f"VOI unit: {voi_unit or '<none>'}",
        f"Layout column: {layout_col}",
        f"Require-true filter columns: {require_text}",
        f"Exclude-true filter columns: {exclude_text}",
        f"Rows before wrapper filtering: {n_rows_before}",
        f"Rows after wrapper filtering: {n_rows_after}",
        "Stats framework: descriptive + facetwise inferential (nonparametric/distributional)",
        "",
    ]
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Generate layout-first and coin-type-first faceted histkde/violin plots, "
            "plus matching descriptive and facetwise nonparametric stats."
        )
    )
    ap.add_argument("--input", required=True, help="Path to input CSV")
    ap.add_argument("--out-root", required=True, help="Output root directory")
    ap.add_argument("--formats", default="pdf", help="Comma-separated formats, e.g. pdf or png,pdf")
    ap.add_argument("--voi", required=True, help="Variable of interest column")
    ap.add_argument("--voi-str", default="Measure", help="Human-readable VOI label")
    ap.add_argument("--voi-unit", default="", help="VOI units")
    ap.add_argument("--layout-col", default="coinSet", help="Layout column, e.g. coinSet or coinSetID")
    ap.add_argument("--require-cols", nargs="*", default=[], help="Keep rows where all columns are truthy")
    ap.add_argument("--exclude-true-cols", nargs="*", default=[], help="Drop rows where any columns are truthy")
    ap.add_argument("--min-n-per-group", type=int, default=10, help="Minimum n per group for stats")
    ap.add_argument("--alpha", type=float, default=0.05, help="Alpha for reporting")
    ap.add_argument("--layout-ncols", type=int, default=3, help="Number of columns for layout-first facet grids")
    ap.add_argument(
        "--show-plot-stats",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Show n, mean, SD, and histogram bin size annotations (default: on).",
    )
    ap.add_argument(
        "--show-participant-metrics",
        action="store_true",
        help=(
            "Show swapRate_tot and PVSS_AvgScore in sessionID facets. "
            "Requires --participant-summary."
        ),
    )
    ap.add_argument(
        "--participant-summary",
        type=Path,
        help="CSV containing participantID, totScore, swapRate_tot, PVSS_TotalScore, and PVSS_AvgScore.",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    in_path = Path(args.input)
    out_root = Path(args.out_root)
    formats = tuple(x.strip() for x in args.formats.split(",") if x.strip())

    raw_df = read_csv_loose(in_path)
    df = apply_cli_filters(
        raw_df,
        require_cols=args.require_cols,
        exclude_true_cols=args.exclude_true_cols,
    )

    if args.show_participant_metrics:
        if args.layout_col != "sessionID":
            raise ValueError(
                "--show-participant-metrics requires --layout-col sessionID."
            )
        if args.participant_summary is None:
            raise ValueError(
                "--show-participant-metrics requires --participant-summary PATH."
            )
        df = attach_participant_metrics(df, args.participant_summary)

    if args.layout_col not in df.columns:
        raise ValueError(f"Missing layout column: {args.layout_col}")

    common_dir = out_root / "_ALL"
    per_file_dir = out_root / in_path.stem
    stats_dir = common_dir / "Stats"
    _ensure_dirs(common_dir, per_file_dir, stats_dir)

    manifest: dict[str, object] = {"file": str(in_path), "outputs": []}

    figs = _run_and_collect_figs(
        plot_histkde_layout_first,
        df,
        variable_of_interest=args.voi,
        layout_col=args.layout_col,
        voi_str=args.voi_str,
        voi_unit=args.voi_unit,
        ncols=args.layout_ncols,
        show_stats=args.show_plot_stats,
        show_participant_metrics=args.show_participant_metrics,
    )
    _save_figs(
        figs,
        common_dir=common_dir,
        per_file_dir=per_file_dir,
        stem=in_path.stem,
        tag=f"histkde_layoutFirst__{args.layout_col}__{args.voi}",
        formats=formats,
        dpi=220,
        write_common=True,
    )
    manifest["outputs"].append({"type": "plot", "tag": f"histkde_layoutFirst__{args.layout_col}__{args.voi}", "count": len(figs)})

    figs = _run_and_collect_figs(
        plot_violin_layout_first,
        df,
        variable_of_interest=args.voi,
        layout_col=args.layout_col,
        voi_str=args.voi_str,
        voi_unit=args.voi_unit,
        ncols=args.layout_ncols,
        show_stats=args.show_plot_stats,
        show_participant_metrics=args.show_participant_metrics,
    )
    _save_figs(
        figs,
        common_dir=common_dir,
        per_file_dir=per_file_dir,
        stem=in_path.stem,
        tag=f"violin_layoutFirst__{args.layout_col}__{args.voi}",
        formats=formats,
        dpi=220,
        write_common=True,
    )
    manifest["outputs"].append({"type": "plot", "tag": f"violin_layoutFirst__{args.layout_col}__{args.voi}", "count": len(figs)})

    figs = _run_and_collect_figs(
        plot_histkde_coin_type_first,
        df,
        variable_of_interest=args.voi,
        layout_col=args.layout_col,
        voi_str=args.voi_str,
        voi_unit=args.voi_unit,
        show_stats=args.show_plot_stats,
        show_participant_metrics=False,
    )
    _save_figs(
        figs,
        common_dir=common_dir,
        per_file_dir=per_file_dir,
        stem=in_path.stem,
        tag=f"histkde_coinTypeFirst__{args.layout_col}__{args.voi}",
        formats=formats,
        dpi=220,
        write_common=True,
    )
    manifest["outputs"].append({"type": "plot", "tag": f"histkde_coinTypeFirst__{args.layout_col}__{args.voi}", "count": len(figs)})

    figs = _run_and_collect_figs(
        plot_violin_coin_type_first,
        df,
        variable_of_interest=args.voi,
        layout_col=args.layout_col,
        voi_str=args.voi_str,
        voi_unit=args.voi_unit,
        show_stats=args.show_plot_stats,
        show_participant_metrics=False,
    )
    _save_figs(
        figs,
        common_dir=common_dir,
        per_file_dir=per_file_dir,
        stem=in_path.stem,
        tag=f"violin_coinTypeFirst__{args.layout_col}__{args.voi}",
        formats=formats,
        dpi=220,
        write_common=True,
    )
    manifest["outputs"].append({"type": "plot", "tag": f"violin_coinTypeFirst__{args.layout_col}__{args.voi}", "count": len(figs)})

    layout_stats = run_layout_first_stats(
        df,
        variable_of_interest=args.voi,
        layout_col=args.layout_col,
        min_n_per_group=args.min_n_per_group,
        alpha=args.alpha,
    )
    coin_stats = run_coin_type_first_stats(
        df,
        variable_of_interest=args.voi,
        layout_col=args.layout_col,
        min_n_per_group=args.min_n_per_group,
        alpha=args.alpha,
    )

    header = build_stats_header(
        voi=args.voi,
        voi_str=args.voi_str,
        voi_unit=args.voi_unit,
        layout_col=args.layout_col,
        require_cols=args.require_cols,
        exclude_true_cols=args.exclude_true_cols,
        n_rows_before=len(raw_df),
        n_rows_after=len(df),
    )

    outputs = [
        ("layoutFirst_summary", layout_stats["summary"]),
        ("layoutFirst_omnibus", layout_stats["omnibus"]),
        ("layoutFirst_pairwise", layout_stats["pairwise"]),
        ("coinTypeFirst_summary", coin_stats["summary"]),
        ("coinTypeFirst_omnibus", coin_stats["omnibus"]),
        ("coinTypeFirst_pairwise", coin_stats["pairwise"]),
    ]

    for stem_suffix, table in outputs:
        out1 = per_file_dir / f"stats_{stem_suffix}__{args.layout_col}__{args.voi}.csv"
        out2 = stats_dir / f"{in_path.stem}__stats_{stem_suffix}__{args.layout_col}__{args.voi}.csv"
        table.to_csv(out1, index=False)
        table.to_csv(out2, index=False)
        manifest["outputs"].append({"type": "stats_csv", "tag": out1.stem, "rows": int(len(table))})

    verbose_layout = header + layout_stats["verbose_text"]
    verbose_coin = header + coin_stats["verbose_text"]

    verbose_layout_path = per_file_dir / f"stats_verbose_layoutFirst__{args.layout_col}__{args.voi}.txt"
    verbose_coin_path = per_file_dir / f"stats_verbose_coinTypeFirst__{args.layout_col}__{args.voi}.txt"
    verbose_layout_common = stats_dir / f"{in_path.stem}__stats_verbose_layoutFirst__{args.layout_col}__{args.voi}.txt"
    verbose_coin_common = stats_dir / f"{in_path.stem}__stats_verbose_coinTypeFirst__{args.layout_col}__{args.voi}.txt"

    verbose_layout_path.write_text(verbose_layout, encoding="utf-8")
    verbose_coin_path.write_text(verbose_coin, encoding="utf-8")
    verbose_layout_common.write_text(verbose_layout, encoding="utf-8")
    verbose_coin_common.write_text(verbose_coin, encoding="utf-8")

    manifest["outputs"].append({"type": "stats_report", "tag": verbose_layout_path.stem, "bytes": len(verbose_layout)})
    manifest["outputs"].append({"type": "stats_report", "tag": verbose_coin_path.stem, "bytes": len(verbose_coin)})

    manifest_path = per_file_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()