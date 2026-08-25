#!/usr/bin/env python3
"""Command-line runner for participant and representative/cohort path-choice plots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt

from pathChoiceHelpers import (
    RepresentativePair,
    merge_rounds_with_summary,
    normalize_cohort_labels,
    prepare_round_level_data,
    prepare_summary_data,
    read_roles_table,
    read_table,
    resolve_representative_pair,
    validate_representative_pair,
)
from pathChoicePlots import (
    plot_faceted_path_proportions,
    plot_faceted_path_violins,
    plot_path_comparison_proportions,
    plot_path_comparison_violins,
)
from pathChoiceStats import summarize_participants, summarize_path_choices


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create participant path-choice plots plus representative/cohort "
            "comparison figures."
        )
    )
    parser.add_argument("--input", required=True, help="Pin-drop CSV/TSV/Excel/Parquet file.")
    parser.add_argument("--summary", required=True, help="Participant summary CSV/TSV/Excel/Parquet file.")
    parser.add_argument("--roles", required=True, help="Representative roles CSV/TSV file.")
    parser.add_argument(
        "--summary-sheet",
        default=None,
        help="Excel summary sheet name. Ignored for non-Excel files.",
    )
    parser.add_argument("--out-dir", required=True, help="Output directory.")
    parser.add_argument("--formats", default="png,pdf", help="Comma-separated plot formats.")

    parser.add_argument(
        "--figure-mode",
        choices=("both", "representatives", "cohorts"),
        default="both",
        help=(
            "Comparison figures to create: 3-panel all/representatives, "
            "2x2 cohorts/representatives, or both."
        ),
    )
    parser.add_argument(
        "--representative-mode",
        choices=("both", "path-specific", "general"),
        default="both",
        help="Use the path-specific role, fixed general representative role, or both.",
    )
    parser.add_argument(
        "--representative-role",
        default="pathChoice",
        help=(
            "Roles-table row for the path-specific representative pair. "
            "Change this to the exact role value used by your roles table."
        ),
    )
    parser.add_argument("--general-role", default="generalRepresentative")
    parser.add_argument("--role-column", default="role")
    parser.add_argument("--main-role-column", default="main_seed42")
    parser.add_argument("--rr-role-column", default="RR_seed50")
    parser.add_argument("--session-column", default="sessionID")

    parser.add_argument("--cohort-column", default="main_RR")
    parser.add_argument("--main-cohort-value", default="main")
    parser.add_argument("--rr-cohort-value", default="RR")

    parser.add_argument(
        "--no-individual-plots",
        action="store_true",
        help="Skip the original participant/session faceted plots.",
    )
    parser.add_argument("--ncols", type=int, default=3, help="Columns for original individual facets.")
    parser.add_argument("--panel-width", type=float, default=5.1, help="Width of each facet in inches.")
    parser.add_argument("--panel-height", type=float, default=4.2, help="Height of each violin facet in inches.")
    parser.add_argument(
        "--annotation-fontsize",
        type=float,
        default=7.2,
        help="Font size for representative/participant summary annotations.",
    )
    parser.add_argument(
        "--no-proportion-plot",
        action="store_true",
        help="Skip all categorical proportion plots.",
    )
    return parser.parse_args()


def _selected_pairs(args: argparse.Namespace, roles) -> list[RepresentativePair]:
    pairs: list[RepresentativePair] = []

    if args.representative_mode in {"both", "path-specific"}:
        pairs.append(
            resolve_representative_pair(
                roles,
                role=args.representative_role,
                mode="path_specific",
                role_column=args.role_column,
                main_column=args.main_role_column,
                rr_column=args.rr_role_column,
            )
        )

    if args.representative_mode in {"both", "general"}:
        pairs.append(
            resolve_representative_pair(
                roles,
                role=args.general_role,
                mode="general",
                role_column=args.role_column,
                main_column=args.main_role_column,
                rr_column=args.rr_role_column,
            )
        )

    return pairs


def _figure_types(mode: str) -> list[str]:
    if mode == "both":
        return ["representatives", "cohorts"]
    return [mode]


def _validate_formats(value: str) -> list[str]:
    formats = [item.strip().lower() for item in value.split(",") if item.strip()]
    unsupported = sorted(set(formats).difference({"png", "pdf", "svg"}))
    if unsupported:
        raise ValueError(f"Unsupported plot formats: {unsupported}")
    if not formats:
        raise ValueError("At least one plot format is required.")
    return formats


def main() -> None:
    args = parse_args()
    output_dir = Path(args.out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    formats = _validate_formats(args.formats)

    raw_rounds = read_table(args.input)
    raw_summary = read_table(args.summary, sheet_name=args.summary_sheet)

    rounds = prepare_round_level_data(
        raw_rounds,
        cohort_column=args.cohort_column,
    )
    summary = prepare_summary_data(
        raw_summary,
        cohort_column=args.cohort_column,
    )
    merged = merge_rounds_with_summary(
        rounds,
        summary,
        cohort_column=args.cohort_column,
    )
    merged = normalize_cohort_labels(
        merged,
        cohort_column=args.cohort_column,
    )

    roles = read_roles_table(args.roles)
    pairs = _selected_pairs(args, roles)
    for pair in pairs:
        validate_representative_pair(
            merged,
            pair,
            session_column=args.session_column,
        )

    merged.to_csv(output_dir / "path_choice_round_level_merged.csv", index=False)
    (
        merged[["path_code", "path_label"]]
        .drop_duplicates()
        .sort_values("path_code")
        .to_csv(output_dir / "path_choice_code_lookup.csv", index=False)
    )
    summarize_path_choices(merged).to_csv(
        output_dir / "path_choice_counts_proportions.csv", index=False
    )
    summarize_participants(merged).to_csv(
        output_dir / "path_choice_participant_summary.csv", index=False
    )

    generated_plots: list[str] = []

    if not args.no_individual_plots:
        for extension in formats:
            output = output_dir / f"path_choice_faceted_violins.{extension}"
            figure = plot_faceted_path_violins(
                merged,
                output_path=output,
                ncols=args.ncols,
                panel_width=args.panel_width,
                panel_height=args.panel_height,
                annotation_fontsize=args.annotation_fontsize,
            )
            plt.close(figure)
            generated_plots.append(output.name)

            if not args.no_proportion_plot:
                output = output_dir / f"path_choice_faceted_proportions.{extension}"
                figure = plot_faceted_path_proportions(
                    merged,
                    output_path=output,
                    ncols=args.ncols,
                    panel_width=args.panel_width,
                    panel_height=max(3.6, args.panel_height - 0.2),
                    annotation_fontsize=args.annotation_fontsize,
                )
                plt.close(figure)
                generated_plots.append(output.name)

    for pair in pairs:
        for figure_type in _figure_types(args.figure_mode):
            for extension in formats:
                stem = f"path_choice_{figure_type}__{pair.mode}"

                violin_output = output_dir / f"{stem}__violins.{extension}"
                figure = plot_path_comparison_violins(
                    merged,
                    pair=pair,
                    figure_type=figure_type,
                    output_path=violin_output,
                    session_column=args.session_column,
                    cohort_column=args.cohort_column,
                    main_cohort_value=args.main_cohort_value,
                    rr_cohort_value=args.rr_cohort_value,
                    panel_width=args.panel_width,
                    panel_height=args.panel_height,
                    annotation_fontsize=args.annotation_fontsize,
                )
                plt.close(figure)
                generated_plots.append(violin_output.name)

                if not args.no_proportion_plot:
                    proportion_output = output_dir / f"{stem}__proportions.{extension}"
                    figure = plot_path_comparison_proportions(
                        merged,
                        pair=pair,
                        figure_type=figure_type,
                        output_path=proportion_output,
                        session_column=args.session_column,
                        cohort_column=args.cohort_column,
                        main_cohort_value=args.main_cohort_value,
                        rr_cohort_value=args.rr_cohort_value,
                        panel_width=args.panel_width,
                        panel_height=max(3.6, args.panel_height - 0.2),
                        annotation_fontsize=args.annotation_fontsize,
                    )
                    plt.close(figure)
                    generated_plots.append(proportion_output.name)

    cohort_counts = (
        merged[[args.cohort_column, "participantID", args.session_column]]
        .drop_duplicates()
        .groupby(args.cohort_column)
        .size()
        .to_dict()
    )

    manifest = {
        "input": str(Path(args.input)),
        "summary": str(Path(args.summary)),
        "roles": str(Path(args.roles)),
        "summary_sheet": args.summary_sheet,
        "rows_raw": len(raw_rounds),
        "unique_valid_rounds": len(rounds),
        "participant_sessions": int(
            merged[["participantID", args.session_column]].drop_duplicates().shape[0]
        ),
        "cohort_column": args.cohort_column,
        "cohort_counts": cohort_counts,
        "figure_mode": args.figure_mode,
        "representative_mode": args.representative_mode,
        "representatives": [
            {
                "mode": pair.mode,
                "role": pair.role,
                "main_session_id": pair.main_session_id,
                "rr_session_id": pair.rr_session_id,
            }
            for pair in pairs
        ],
        "excluded_path_codes": [888, 999],
        "summary_merge_counts": merged["_summary_merge"].value_counts().to_dict(),
        "plots": sorted(generated_plots),
        "outputs": sorted(path.name for path in output_dir.iterdir()),
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, default=str),
        encoding="utf-8",
    )
    print(json.dumps(manifest, indent=2, default=str))


if __name__ == "__main__":
    main()
