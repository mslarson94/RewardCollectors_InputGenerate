# ==========================================
# file: regressionWrapper.py
# ==========================================
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from regressionHelpers import (
    DEFAULT_COHORT_COLUMN,
    DEFAULT_COIN_COLUMN,
    DEFAULT_CORRECTNESS_COLUMN,
    DEFAULT_OUTCOME_COLUMN,
    DEFAULT_SUBJECT_COLUMN,
    DEFAULT_TP2_COLUMN,
    DEFAULT_TASKPROGRESSION_COLUMN,
    prepare_analysis_frame,
    prettify_name,
    read_roles_table,
    read_table,
    resolve_representative_pair,
    slugify,
    validate_axis_limits,
    validate_columns,
    validate_representative_pair,
)
from regressionPlotting import (
    create_cohort_regression_figure,
    create_representative_regression_figure,
)
from regressionStats import (
    build_cohort_facets,
    build_representative_facets,
    facet_fit_manifest_row,
    fit_facets,
)


def parse_args() -> argparse.Namespace:
    """Build the command-line interface."""

    parser = argparse.ArgumentParser(
        description=(
            "Generate faceted regression plots parallel to the faceted distribution "
            "suite, using Main/RR representative mappings and cohort membership."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to input CSV/XLSX/Parquet.",
    )
    parser.add_argument(
        "--roles",
        type=Path,
        required=True,
        help="CSV/TSV containing representative role mappings.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory where plots and manifests will be saved.",
    )

    parser.add_argument(
        "--outcome-column",
        default=DEFAULT_OUTCOME_COLUMN,
        help=f"Continuous outcome column. Default: {DEFAULT_OUTCOME_COLUMN}",
    )
    parser.add_argument(
        "--outcome-label",
        default=None,
        help="Optional human-readable outcome label.",
    )
    parser.add_argument(
        "--session-column",
        "--subject-column",
        dest="session_column",
        default=DEFAULT_SUBJECT_COLUMN,
        help=f"Session/subject identifier. Default: {DEFAULT_SUBJECT_COLUMN}",
    )
    parser.add_argument(
        "--cohort-column",
        default=DEFAULT_COHORT_COLUMN,
        help=f"Main/RR cohort column. Default: {DEFAULT_COHORT_COLUMN}",
    )
    parser.add_argument(
        "--main-cohort-value",
        default="main",
        help="Value in --cohort-column identifying Main cohort rows.",
    )
    parser.add_argument(
        "--rr-cohort-value",
        default="RR",
        help="Value in --cohort-column identifying RR cohort rows.",
    )
    parser.add_argument(
        "--coin-column",
        default=DEFAULT_COIN_COLUMN,
        help=f"Coin column. Default: {DEFAULT_COIN_COLUMN}",
    )
    parser.add_argument(
        "--correctness-column",
        default=DEFAULT_CORRECTNESS_COLUMN,
        help=f"Correctness column. Default: {DEFAULT_CORRECTNESS_COLUMN}",
    )
    parser.add_argument(
        "--tp2-column",
        default=DEFAULT_TP2_COLUMN,
        help=f"TP2 flag column. Default: {DEFAULT_TP2_COLUMN}",
    )
    parser.add_argument(
        "--task-progression-column",
        default=DEFAULT_TASKPROGRESSION_COLUMN,
        help=(
            "Task progression variable. "
            f"Default: {DEFAULT_TASKPROGRESSION_COLUMN}"
        ),
    )

    parser.add_argument(
        "--representative-mode",
        choices=("both", "voi-specific", "general"),
        default="both",
        help="Generate outcome-specific representatives, general representatives, or both.",
    )
    parser.add_argument(
        "--general-role",
        default="generalRepresentative",
        help="Roles-table row used for fixed general representatives.",
    )
    parser.add_argument("--role-column", default="role")
    parser.add_argument("--main-role-column", default="main_seed42")
    parser.add_argument("--rr-role-column", default="RR_seed50")

    parser.add_argument(
        "--figure-mode",
        choices=("both", "representatives", "cohorts"),
        default="both",
        help=(
            "representatives: 3-panel All/Main Rep/RR Rep; "
            "cohorts: 2x2 Main/RR cohort + representatives; both: export both."
        ),
    )
    parser.add_argument(
        "--ci-style",
        choices=("ribbon", "dashed"),
        default="dashed",
        help="How to render the 95% confidence interval.",
    )
    parser.add_argument(
        "--show-raw",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Show styled raw observations. Use --no-show-raw to suppress them.",
    )
    parser.add_argument(
        "--include-tp1",
        action="store_true",
        help="Include TP1 rows. By default, only TP2 rows are retained.",
    )
    parser.add_argument(
        "--correct-only",
        action="store_true",
        help="Keep only correct pin drops.",
    )
    parser.add_argument(
        "--limit-coins",
        nargs="*",
        default=None,
        help="Optional global subset of coin labels, e.g. --limit-coins HV LV.",
    )

    parser.add_argument(
        "--x-axis-mode",
        choices=("shared", "facet"),
        default="shared",
        help=(
            "shared: use one x-axis range across all facets; "
            "facet: auto-fit each facet independently. Explicit --xlim overrides "
            "this setting and is shared across facets."
        ),
    )
    parser.add_argument(
        "--xlim",
        type=float,
        nargs=2,
        metavar=("LOWER", "UPPER"),
        default=None,
        help="Optional explicit shared x-axis limits.",
    )
    parser.add_argument(
        "--ylim",
        type=float,
        nargs=2,
        metavar=("LOWER", "UPPER"),
        default=None,
        help=(
            "Optional explicit shared y-axis limits. If omitted, each figure uses "
            "shared limits derived jointly from its raw observations and fitted CIs."
        ),
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=8,
        help="Minimum rows required to fit a facet regression.",
    )
    parser.add_argument(
        "--min-unique-x",
        type=int,
        default=2,
        help="Minimum unique task-progression values required to fit a facet.",
    )

    parser.add_argument(
        "--formats",
        default="png",
        help="Comma-separated plot formats: png,pdf,svg.",
    )
    parser.add_argument("--dpi", type=int, default=300, help="Raster export DPI.")
    return parser.parse_args()


def _selected_pairs(
    roles: pd.DataFrame,
    *,
    outcome_column: str,
    representative_mode: str,
    general_role: str,
    role_column: str,
    main_role_column: str,
    rr_role_column: str,
):
    """Resolve the representative pairs requested by the CLI."""

    pairs = []

    if representative_mode in {"both", "voi-specific"}:
        pairs.append(
            resolve_representative_pair(
                roles,
                role=outcome_column,
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


def _selected_figure_types(mode: str) -> tuple[str, ...]:
    if mode == "representatives":
        return ("representatives",)
    if mode == "cohorts":
        return ("cohorts",)
    return ("representatives", "cohorts")


def _output_stem(
    *,
    outcome_column: str,
    figure_type: str,
    representative_mode: str,
    include_tp1: bool,
    correct_only: bool,
) -> str:
    tp_part = "tp1_tp2" if include_tp1 else "tp2_only"
    correctness_part = "correct_only" if correct_only else "all_drops"
    return (
        f"regression_faceted__{slugify(outcome_column)}__{figure_type}__"
        f"{slugify(representative_mode)}__{tp_part}__{correctness_part}"
    )


def _save_figure(
    figure: plt.Figure,
    *,
    output_dir: Path,
    stem: str,
    formats: tuple[str, ...],
    dpi: int,
) -> list[str]:
    output_files: list[str] = []
    for extension in formats:
        path = output_dir / f"{stem}.{extension}"
        save_kwargs = {"bbox_inches": "tight"}
        if extension == "png":
            save_kwargs["dpi"] = dpi
        figure.savefig(path, **save_kwargs)
        output_files.append(path.name)
    plt.close(figure)
    return output_files


def main() -> None:
    args = parse_args()

    if args.min_rows < 2:
        raise ValueError("--min-rows must be at least 2.")
    if args.min_unique_x < 2:
        raise ValueError("--min-unique-x must be at least 2.")

    xlim = validate_axis_limits(
        tuple(args.xlim) if args.xlim is not None else None,
        argument_name="--xlim",
    )
    ylim = validate_axis_limits(
        tuple(args.ylim) if args.ylim is not None else None,
        argument_name="--ylim",
    )

    formats = tuple(
        value.strip().lower()
        for value in args.formats.split(",")
        if value.strip()
    )
    unsupported = sorted(set(formats).difference({"png", "pdf", "svg"}))
    if unsupported:
        raise ValueError(f"Unsupported output formats: {unsupported}")
    if not formats:
        raise ValueError("--formats must contain at least one format.")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    raw = read_table(args.input)
    validate_columns(
        raw,
        [
            args.outcome_column,
            args.task_progression_column,
            args.session_column,
            args.coin_column,
            args.correctness_column,
            args.tp2_column,
            args.cohort_column,
        ],
    )

    data = prepare_analysis_frame(
        raw,
        outcome_column=args.outcome_column,
        task_progression_column=args.task_progression_column,
        subject_column=args.session_column,
        coin_column=args.coin_column,
        correctness_column=args.correctness_column,
        tp2_column=args.tp2_column,
        cohort_column=args.cohort_column,
        include_tp1=args.include_tp1,
        correct_only=args.correct_only,
        limit_coins=args.limit_coins,
    )
    if data.empty:
        raise ValueError("Analysis filtering removed every row.")

    roles = read_roles_table(args.roles)
    pairs = _selected_pairs(
        roles,
        outcome_column=args.outcome_column,
        representative_mode=args.representative_mode,
        general_role=args.general_role,
        role_column=args.role_column,
        main_role_column=args.main_role_column,
        rr_role_column=args.rr_role_column,
    )

    for pair in pairs:
        validate_representative_pair(
            data,
            pair,
            session_column=args.session_column,
            outcome_column=args.outcome_column,
            task_progression_column=args.task_progression_column,
        )

    outcome_label = args.outcome_label or prettify_name(args.outcome_column)
    figure_types = _selected_figure_types(args.figure_mode)
    manifest_rows: list[dict[str, object]] = []
    output_records: list[dict[str, object]] = []

    for pair in pairs:
        for figure_type in figure_types:
            if figure_type == "representatives":
                facets = build_representative_facets(
                    data,
                    pair=pair,
                    session_column=args.session_column,
                )
            else:
                facets = build_cohort_facets(
                    data,
                    pair=pair,
                    session_column=args.session_column,
                    cohort_column=args.cohort_column,
                    main_cohort_value=args.main_cohort_value,
                    rr_cohort_value=args.rr_cohort_value,
                )

            fits = fit_facets(
                facets,
                outcome_column=args.outcome_column,
                task_progression_column=args.task_progression_column,
                min_rows=args.min_rows,
                min_unique_x=args.min_unique_x,
            )

            if figure_type == "representatives":
                figure = create_representative_regression_figure(
                    fits=fits,
                    pair=pair,
                    outcome_column=args.outcome_column,
                    task_progression_column=args.task_progression_column,
                    outcome_label=outcome_label,
                    coin_column=args.coin_column,
                    include_tp1=args.include_tp1,
                    correct_only=args.correct_only,
                    ci_style=args.ci_style,
                    show_raw=args.show_raw,
                    x_axis_mode=args.x_axis_mode,
                    xlim=xlim,
                    ylim=ylim,
                )
            else:
                figure = create_cohort_regression_figure(
                    fits=fits,
                    pair=pair,
                    outcome_column=args.outcome_column,
                    task_progression_column=args.task_progression_column,
                    outcome_label=outcome_label,
                    coin_column=args.coin_column,
                    include_tp1=args.include_tp1,
                    correct_only=args.correct_only,
                    ci_style=args.ci_style,
                    show_raw=args.show_raw,
                    x_axis_mode=args.x_axis_mode,
                    xlim=xlim,
                    ylim=ylim,
                )

            stem = _output_stem(
                outcome_column=args.outcome_column,
                figure_type=figure_type,
                representative_mode=pair.mode,
                include_tp1=args.include_tp1,
                correct_only=args.correct_only,
            )
            files = _save_figure(
                figure,
                output_dir=args.output_dir,
                stem=stem,
                formats=formats,
                dpi=args.dpi,
            )

            output_records.append(
                {
                    "figure_type": figure_type,
                    "representative_mode": pair.mode,
                    "role": pair.role,
                    "files": files,
                }
            )

            for fit in fits:
                row = facet_fit_manifest_row(
                    fit,
                    figure_type=figure_type,
                    pair=pair,
                    outcome_column=args.outcome_column,
                    task_progression_column=args.task_progression_column,
                    include_tp1=args.include_tp1,
                    correct_only=args.correct_only,
                )
                row["output_files"] = ";".join(files)
                manifest_rows.append(row)

    manifest = pd.DataFrame(manifest_rows)
    manifest_csv = args.output_dir / (
        f"regression_faceted_manifest__{slugify(args.outcome_column)}.csv"
    )
    manifest.to_csv(manifest_csv, index=False)

    manifest_json = args.output_dir / (
        f"regression_faceted_manifest__{slugify(args.outcome_column)}.json"
    )
    manifest_json.write_text(
        json.dumps(
            {
                "input": str(args.input),
                "roles": str(args.roles),
                "outcome_column": args.outcome_column,
                "outcome_label": outcome_label,
                "task_progression_column": args.task_progression_column,
                "session_column": args.session_column,
                "cohort_column": args.cohort_column,
                "main_cohort_value": args.main_cohort_value,
                "rr_cohort_value": args.rr_cohort_value,
                "representative_mode": args.representative_mode,
                "figure_mode": args.figure_mode,
                "xlim": list(xlim) if xlim else None,
                "ylim": list(ylim) if ylim else None,
                "include_tp1": args.include_tp1,
                "correct_only": args.correct_only,
                "show_raw": args.show_raw,
                "ci_style": args.ci_style,
                "rows_before_filtering": len(raw),
                "rows_after_filtering": len(data),
                "outputs": output_records,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    fit_count = int((manifest["status"] == "fit").sum()) if not manifest.empty else 0
    skipped_count = len(manifest) - fit_count
    print(
        f"Done. Figures: {len(output_records)}, "
        f"Facet models fit: {fit_count}, skipped: {skipped_count}"
    )
    print(f"CSV manifest: {manifest_csv}")
    print(f"JSON manifest: {manifest_json}")


if __name__ == "__main__":
    main()
