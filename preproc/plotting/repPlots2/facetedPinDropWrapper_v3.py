#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import json
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd

from facetedHelpers_v3 import (
    RepresentativePair,
    _ensure_dirs,
    _run_and_collect_figs,
    _save_figs,
    build_shared_plot_spec,
    read_roles_table,
    resolve_representative_pair,
    validate_representative_pair,
)
from facetedPlots_v3 import (
    build_representative_descriptives,
    plot_faceted_histkde_cohorts,
    plot_faceted_histkde_representatives,
)
from facetedStats_v3 import (
    _enough_for_stats,
    test_coin_distributions_all,
    test_coin_distributions_tp2,
)


def read_csv_loose(path: Path) -> pd.DataFrame:
    """Read the analysis CSV and normalize common categorical columns."""
    data = pd.read_csv(path)
    for column in ("coinLabel", "sessionID", "main_RR"):
        if column in data.columns:
            data[column] = data[column].astype("string").str.strip()
    if "dropQual" in data.columns:
        data["dropQual"] = (
            data["dropQual"].astype("string").str.strip().str.lower()
        )
    return data


def _series_truthy(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce").fillna(0).ne(0)
    text = series.astype("string").str.strip().str.lower()
    return text.isin({"1", "true", "t", "yes", "y"})


def apply_cli_filters(
    data: pd.DataFrame,
    *,
    require_cols: list[str],
    exclude_true_cols: list[str],
) -> pd.DataFrame:
    """Apply the original wrapper's truthy include/exclude filters."""
    missing = [
        column
        for column in require_cols + exclude_true_cols
        if column not in data.columns
    ]
    if missing:
        raise ValueError(f"Missing filter columns in input data: {missing}")

    mask = pd.Series(True, index=data.index)
    for column in require_cols:
        mask &= _series_truthy(data[column])
    for column in exclude_true_cols:
        mask &= ~_series_truthy(data[column])
    return data.loc[mask].copy()


def _selected_pairs(
    roles: pd.DataFrame,
    *,
    voi: str,
    representative_mode: str,
    general_role: str,
    role_column: str,
    main_role_column: str,
    rr_role_column: str,
) -> list[RepresentativePair]:
    pairs: list[RepresentativePair] = []
    if representative_mode in {"both", "voi-specific"}:
        pairs.append(
            resolve_representative_pair(
                roles,
                role=voi,
                mode="voi_specific",
                role_column=role_column,
                main_column=main_role_column,
                rr_column=rr_role_column,
            )
        )
    if representative_mode in {"both", "general"}:
        pairs.append(
            resolve_representative_pair(
                roles,
                role=general_role,
                mode="general",
                role_column=role_column,
                main_column=main_role_column,
                rr_column=rr_role_column,
            )
        )
    return pairs


def _annotation_variants(mode: str) -> list[tuple[str, bool]]:
    if mode == "both":
        return [("with_bin_stats", True), ("without_bin_stats", False)]
    if mode == "labeled":
        return [("with_bin_stats", True)]
    return [("without_bin_stats", False)]


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _stats_header(
    *,
    args: argparse.Namespace,
    pair: RepresentativePair,
    rows_before: int,
    rows_after: int,
    bin_edges: np.ndarray,
    bin_width: float,
) -> str:
    return "\n".join(
        [
            "Faceted representative stats report",
            f"VOI column: {args.voi}",
            f"VOI label: {args.voi_str}",
            f"VOI unit: {args.voi_unit or '<none>'}",
            f"Representative mode: {pair.mode}",
            f"Role row: {pair.role}",
            f"Main sessionID: {pair.main_session_id}",
            f"RR sessionID: {pair.rr_session_id}",
            f"Session column: {args.session_column}",
            f"Rows before wrapper filtering: {rows_before}",
            f"Rows after wrapper filtering: {rows_after}",
            f"Require-true columns: {', '.join(args.require_cols) or '<none>'}",
            f"Exclude-true columns: {', '.join(args.exclude_true_cols) or '<none>'}",
            f"Outlier method: {args.outlier_method}",
            f"Shared bin width: {bin_width:.12g}",
            "Shared bin edges: " + ", ".join(f"{value:.12g}" for value in bin_edges),
            "",
        ]
    )


def _write_pair_stats(
    *,
    data: pd.DataFrame,
    pair: RepresentativePair,
    args: argparse.Namespace,
    target_dir: Path,
    common_stats_dir: Path,
    input_stem: str,
    rows_before: int,
    rows_after: int,
    bin_edges: np.ndarray,
    bin_width: float,
    manifest: dict[str, object],
) -> None:
    descriptive = build_representative_descriptives(
        data,
        pair=pair,
        variableOfInterest=args.voi,
        session_column=args.session_column,
    )
    tag = f"representative_descriptives__{args.voi}__{pair.mode}"
    local_csv = target_dir / f"{tag}.csv"
    common_csv = common_stats_dir / f"{input_stem}__{tag}.csv"
    descriptive.to_csv(local_csv, index=False)
    descriptive.to_csv(common_csv, index=False)

    report = _stats_header(
        args=args,
        pair=pair,
        rows_before=rows_before,
        rows_after=rows_after,
        bin_edges=bin_edges,
        bin_width=bin_width,
    )
    report += descriptive.to_string(index=False)
    report_tag = f"representative_report__{args.voi}__{pair.mode}"
    _write_text(target_dir / f"{report_tag}.txt", report)
    _write_text(common_stats_dir / f"{input_stem}__{report_tag}.txt", report)

    manifest["outputs"].extend(
        [
            {"type": "stats_csv", "tag": tag, "rows": len(descriptive)},
            {"type": "stats_report", "tag": report_tag},
        ]
    )


def _write_inferential_stats(
    *,
    data: pd.DataFrame,
    args: argparse.Namespace,
    target_dir: Path,
    common_stats_dir: Path,
    input_stem: str,
    manifest: dict[str, object],
) -> None:
    stats_ok, reason = _enough_for_stats(
        data,
        args.voi,
        blocks_min=3,
        min_n_per_group=args.min_n_per_group,
    )

    jobs = [
        ("TP2", test_coin_distributions_tp2),
        ("all", test_coin_distributions_all),
    ]
    for label, function in jobs:
        if label == "TP2" and not stats_ok:
            manifest["outputs"].append(
                {
                    "type": "stats",
                    "tag": f"stats_pairwise_{label}__{args.voi}",
                    "skipped": True,
                    "reason": reason,
                }
            )
            continue

        kwargs = {
            "variableOfInterest": args.voi,
            "voi_str": args.voi_str,
            "min_n_per_group": args.min_n_per_group,
            "alpha": args.alpha,
            "verbose": False,
        }
        if label == "TP2":
            kwargs["max_totalRounds"] = 1

        try:
            result = function(data, **kwargs)
            pairwise = result.get("pairwise") if isinstance(result, dict) else None
        except Exception as exc:
            manifest["outputs"].append(
                {
                    "type": "stats",
                    "tag": f"stats_pairwise_{label}__{args.voi}",
                    "skipped": True,
                    "reason": str(exc),
                }
            )
            continue

        if isinstance(pairwise, pd.DataFrame) and not pairwise.empty:
            tag = f"stats_pairwise_{label}__{args.voi}"
            pairwise.to_csv(target_dir / f"{tag}.csv", index=False)
            pairwise.to_csv(
                common_stats_dir / f"{input_stem}__{tag}.csv",
                index=False,
            )
            manifest["outputs"].append(
                {"type": "stats_csv", "tag": tag, "rows": len(pairwise)}
            )

        verbose_kwargs = dict(kwargs)
        verbose_kwargs["verbose"] = True
        buffer = StringIO()
        try:
            with contextlib.redirect_stdout(buffer):
                function(data, **verbose_kwargs)
        except Exception as exc:
            buffer.write(f"Verbose statistics generation failed: {exc}\n")

        tag = f"stats_verbose_{label}__{args.voi}"
        text = buffer.getvalue() or "No verbose output captured.\n"
        _write_text(target_dir / f"{tag}.txt", text)
        _write_text(common_stats_dir / f"{input_stem}__{tag}.txt", text)
        manifest["outputs"].append({"type": "stats_report", "tag": tag})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate comparable faceted plots for VOI-specific and fixed "
            "general representative pairs."
        )
    )
    parser.add_argument("--input", required=True, help="Analysis CSV path")
    parser.add_argument("--roles", required=True, help="Randomized roles CSV/TSV path")
    parser.add_argument("--out-root", required=True, help="Output root directory")
    parser.add_argument("--voi", required=True, help="Variable-of-interest column")
    parser.add_argument("--voi-str", default="Measure", help="Human-readable VOI label")
    parser.add_argument("--voi-unit", default="", help="VOI units")
    parser.add_argument("--formats", default="png", help="Comma-separated: png,pdf")
    parser.add_argument(
        "--representative-mode",
        choices=["both", "voi-specific", "general"],
        default="both",
    )
    parser.add_argument(
        "--figure-mode",
        choices=["both", "representatives", "cohorts"],
        default="both",
        help=(
            "Generate the original 3-panel representative figure, the new "
            "4-panel cohort-vs-representative figure, or both."
        ),
    )
    parser.add_argument(
        "--annotation-mode",
        choices=["both", "labeled", "unlabeled"],
        default="both",
        help="Generate plots with bin annotations, without them, or both.",
    )
    parser.add_argument(
        "--no-write-stats",
        action="store_true",
        help="Skip descriptive and inferential stats files.",
    )
    parser.add_argument("--session-column", default="sessionID")
    parser.add_argument("--role-column", default="role")
    parser.add_argument("--main-role-column", default="main_seed42")
    parser.add_argument("--rr-role-column", default="RR_seed50")
    parser.add_argument("--general-role", default="generalRepresentative")
    parser.add_argument(
        "--cohort-column",
        default="main_RR",
        help="Column distinguishing Main and RR cohort membership.",
    )
    parser.add_argument(
        "--main-cohort-value",
        default="main",
        help="Value in --cohort-column identifying the Main cohort.",
    )
    parser.add_argument(
        "--rr-cohort-value",
        default="RR",
        help="Value in --cohort-column identifying the RR cohort.",
    )
    parser.add_argument(
        "--dot-mode",
        choices=["panel", "baseline", "none"],
        default="panel",
    )
    parser.add_argument("--bin-width", type=float, default=None)
    parser.add_argument("--xlim", type=float, nargs=2, metavar=("MIN", "MAX"))
    parser.add_argument(
        "--ylim",
        type=float,
        default=None,
        help="Shared upper y-axis limit. Otherwise derived jointly from all facets.",
    )
    parser.add_argument("--stat", choices=["density", "count", "probability"], default="density")
    parser.add_argument("--require-cols", nargs="*", default=[])
    parser.add_argument("--exclude-true-cols", nargs="*", default=[])
    parser.add_argument("--outlier-method", default="none")
    parser.add_argument("--min-n-per-group", type=int, default=10)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--dpi", type=int, default=220)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    input_path = Path(args.input)
    roles_path = Path(args.roles)
    out_root = Path(args.out_root)
    formats = tuple(
        item.strip().lower()
        for item in args.formats.split(",")
        if item.strip()
    )
    unsupported = sorted(set(formats).difference({"png", "pdf", "svg"}))
    if unsupported:
        raise ValueError(f"Unsupported output formats: {unsupported}")
    if args.bin_width is not None and args.bin_width <= 0:
        raise ValueError("--bin-width must be greater than zero.")
    if args.ylim is not None and args.ylim <= 0:
        raise ValueError("--ylim must be greater than zero.")

    raw_data = read_csv_loose(input_path)
    if args.voi not in raw_data.columns:
        raise ValueError(f"VOI '{args.voi}' is not present in the input CSV.")

    filtered_data = apply_cli_filters(
        raw_data,
        require_cols=args.require_cols,
        exclude_true_cols=args.exclude_true_cols,
    )
    if filtered_data.empty:
        raise ValueError("Wrapper filtering removed every row.")

    if args.figure_mode in {"both", "cohorts"}:
        if args.cohort_column not in filtered_data.columns:
            raise ValueError(
                f"Cohort figure requested but column '{args.cohort_column}' "
                "is not present in the filtered data."
            )

    roles = read_roles_table(roles_path)
    pairs = _selected_pairs(
        roles,
        voi=args.voi,
        representative_mode=args.representative_mode,
        general_role=args.general_role,
        role_column=args.role_column,
        main_role_column=args.main_role_column,
        rr_role_column=args.rr_role_column,
    )
    for pair in pairs:
        validate_representative_pair(
            filtered_data,
            pair,
            session_column=args.session_column,
            voi=args.voi,
        )

    plot_spec = build_shared_plot_spec(
        filtered_data,
        voi=args.voi,
        bin_width=args.bin_width,
        xlim=tuple(args.xlim) if args.xlim else None,
    )

    common_dir = out_root / "_ALL"
    target_dir = out_root / input_path.stem / args.voi
    common_stats_dir = common_dir / "Stats"
    _ensure_dirs(common_dir, target_dir, common_stats_dir)

    manifest: dict[str, object] = {
        "input": str(input_path),
        "roles": str(roles_path),
        "voi": args.voi,
        "figure_mode": args.figure_mode,
        "cohort": {
            "column": args.cohort_column,
            "main_value": args.main_cohort_value,
            "rr_value": args.rr_cohort_value,
        },
        "rows_before_filtering": len(raw_data),
        "rows_after_filtering": len(filtered_data),
        "shared_plot_spec": {
            "bin_width": plot_spec.bin_width,
            "bin_edges": plot_spec.bin_edges.tolist(),
            "xlim": list(plot_spec.xlim),
            "ylim": args.ylim,
            "coin_order": list(plot_spec.coin_order),
        },
        "representatives": [
            {
                "mode": pair.mode,
                "role": pair.role,
                "main_session_id": pair.main_session_id,
                "rr_session_id": pair.rr_session_id,
            }
            for pair in pairs
        ],
        "outputs": [],
    }

    for pair in pairs:
        for annotation_tag, show_bin_stats in _annotation_variants(args.annotation_mode):
            if args.figure_mode in {"both", "representatives"}:
                tag = (
                    f"faceted_histkde_{args.voi}__{pair.mode}__"
                    f"representatives__{annotation_tag}"
                )
                figures = _run_and_collect_figs(
                    plot_faceted_histkde_representatives,
                    filtered_data,
                    pair=pair,
                    plot_spec=plot_spec,
                    variableOfInterest=args.voi,
                    session_column=args.session_column,
                    voi_str=args.voi_str,
                    voi_unit=args.voi_unit,
                    stat=args.stat,
                    dot_mode=args.dot_mode,
                    show_bin_stats=show_bin_stats,
                    ylim=args.ylim,
                )
                if not figures:
                    raise RuntimeError(f"Plot generation failed for '{tag}'.")
                _save_figs(
                    figures,
                    common_dir=common_dir,
                    per_file_dir=target_dir,
                    stem=input_path.stem,
                    tag=tag,
                    formats=formats,
                    dpi=args.dpi,
                    write_common=True,
                )
                manifest["outputs"].append(
                    {
                        "type": "plot",
                        "figure_mode": "representatives",
                        "tag": tag,
                        "formats": list(formats),
                        "count": len(figures),
                    }
                )

            if args.figure_mode in {"both", "cohorts"}:
                tag = (
                    f"faceted_histkde_{args.voi}__{pair.mode}__"
                    f"cohorts__{annotation_tag}"
                )
                figures = _run_and_collect_figs(
                    plot_faceted_histkde_cohorts,
                    filtered_data,
                    pair=pair,
                    plot_spec=plot_spec,
                    variableOfInterest=args.voi,
                    session_column=args.session_column,
                    cohort_column=args.cohort_column,
                    main_cohort_value=args.main_cohort_value,
                    rr_cohort_value=args.rr_cohort_value,
                    voi_str=args.voi_str,
                    voi_unit=args.voi_unit,
                    stat=args.stat,
                    dot_mode=args.dot_mode,
                    show_bin_stats=show_bin_stats,
                    ylim=args.ylim,
                )
                if not figures:
                    raise RuntimeError(f"Plot generation failed for '{tag}'.")
                _save_figs(
                    figures,
                    common_dir=common_dir,
                    per_file_dir=target_dir,
                    stem=input_path.stem,
                    tag=tag,
                    formats=formats,
                    dpi=args.dpi,
                    write_common=True,
                )
                manifest["outputs"].append(
                    {
                        "type": "plot",
                        "figure_mode": "cohorts",
                        "tag": tag,
                        "formats": list(formats),
                        "count": len(figures),
                    }
                )

        if not args.no_write_stats:
            _write_pair_stats(
                data=filtered_data,
                pair=pair,
                args=args,
                target_dir=target_dir,
                common_stats_dir=common_stats_dir,
                input_stem=input_path.stem,
                rows_before=len(raw_data),
                rows_after=len(filtered_data),
                bin_edges=plot_spec.bin_edges,
                bin_width=plot_spec.bin_width,
                manifest=manifest,
            )

    if not args.no_write_stats:
        _write_inferential_stats(
            data=filtered_data,
            args=args,
            target_dir=target_dir,
            common_stats_dir=common_stats_dir,
            input_stem=input_path.stem,
            manifest=manifest,
        )

    manifest_path = target_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Wrote {len(manifest['outputs'])} output records to {target_dir}")


if __name__ == "__main__":
    main()
