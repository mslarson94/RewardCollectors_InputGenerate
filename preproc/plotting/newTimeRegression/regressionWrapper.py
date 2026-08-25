# ==========================================
# file: regressionWrapper.py
# ==========================================
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from regressionHelpers import (
    DEFAULT_COIN_COLUMN,
    DEFAULT_CORRECTNESS_COLUMN,
    DEFAULT_OUTCOME_COLUMN,
    DEFAULT_SUBJECT_COLUMN,
    DEFAULT_TP2_COLUMN,
    DEFAULT_TASKPROGRESSION_COLUMN,
    prepare_analysis_frame,
    prettify_name,
    read_table,
    slugify,
    validate_columns,
)
from regressionPlotting import create_regression_summary_plot
from regressionStats import build_plot_specs, filter_for_spec, fit_linear_model


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate regression-summary plots for outcome ~ task progression, "
            "with raw points formatted by coin type and corrected drop quality."
        )
    )
    parser.add_argument("--input", type=Path, required=True, help="Path to input CSV/XLSX/Parquet.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory where plots and manifest will be saved.",
    )
    parser.add_argument(
        "--outcome-column",
        type=str,
        default=DEFAULT_OUTCOME_COLUMN,
        help=f"Continuous outcome column. Default: {DEFAULT_OUTCOME_COLUMN}",
    )
    parser.add_argument(
        "--outcome-label",
        type=str,
        default=None,
        help="Optional human-readable outcome label.",
    )
    parser.add_argument(
        "--subject-column",
        type=str,
        default=DEFAULT_SUBJECT_COLUMN,
        help=f"Subject/group column. Default: {DEFAULT_SUBJECT_COLUMN}",
    )
    parser.add_argument(
        "--coin-column",
        type=str,
        default=DEFAULT_COIN_COLUMN,
        help=f"Coin column. Default: {DEFAULT_COIN_COLUMN}",
    )
    parser.add_argument(
        "--correctness-column",
        type=str,
        default=DEFAULT_CORRECTNESS_COLUMN,
        help=f"Correctness column. Default: {DEFAULT_CORRECTNESS_COLUMN}",
    )
    parser.add_argument(
        "--tp2-column",
        type=str,
        default=DEFAULT_TP2_COLUMN,
        help=f"TP2 flag column. Default: {DEFAULT_TP2_COLUMN}",
    )
    parser.add_argument(
        "--task-progression-column",
        type=str,
        default=DEFAULT_TASKPROGRESSION_COLUMN,
        help=f"task progression variable. Default: {DEFAULT_TASKPROGRESSION_COLUMN}",
    )
    parser.add_argument(
        "--ci-style",
        choices=("ribbon", "dashed"),
        default="dashed",
        help="How to render the 95% confidence interval.",
    )
    parser.add_argument(
        "--show-raw",
        action="store_true",
        help="Draw styled raw observations in the background.",
    )
    parser.add_argument(
        "--include-tp1",
        action="store_true",
        help="Include TP1 rows. By default, only TP2 rows are kept.",
    )
    parser.add_argument(
        "--correct-only",
        action="store_true",
        help="Keep only correct pin drops.",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=8,
        help="Minimum row count required to fit/export a plot.",
    )
    parser.add_argument(
        "--min-unique-x",
        type=int,
        default=2,
        help="Minimum unique task progression values required to fit/export a plot.",
    )
    parser.add_argument("--dpi", type=int, default=300, help="Export DPI.")
    parser.add_argument("--width", type=float, default=7.0, help="Figure width in inches.")
    parser.add_argument("--height", type=float, default=6.5, help="Figure height in inches.")
    parser.add_argument(
        "--include-all-subjects",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include an All Subjects aggregate.",
    )
    parser.add_argument(
        "--include-all-coins",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include an All Coins aggregate.",
    )
    parser.add_argument(
        "--limit-subjects",
        nargs="*",
        default=None,
        help="Optional subset of subject values to include.",
    )
    parser.add_argument(
        "--limit-coins",
        nargs="*",
        default=None,
        help="Optional subset of coin values to include.",
    )
    return parser.parse_args()


def build_output_name(
    *,
    outcome_column: str,
    spec_subject_value: str | None,
    spec_coin_value: str | None,
    include_tp1: bool,
    correct_only: bool,
) -> str:
    outcome_part = slugify(outcome_column)
    subject_part = (
        "all_subjects"
        if spec_subject_value is None
        else f"subject_{slugify(spec_subject_value)}"
    )
    coin_part = (
        "all_coins"
        if spec_coin_value is None
        else f"coin_{slugify(spec_coin_value)}"
    )
    tp_part = "tp1_tp2" if include_tp1 else "tp2_only"
    correctness_part = "correct_only" if correct_only else "all_drops"
    return (
        f"regression_summary__{outcome_part}__{subject_part}__"
        f"{coin_part}__{tp_part}__{correctness_part}.png"
    )


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    df = read_table(args.input)

    validate_columns(
        df,
        [
            args.outcome_column,
            args.task_progression_column,
            args.subject_column,
            args.coin_column,
            args.correctness_column,
            args.tp2_column,
        ],
    )


    df = prepare_analysis_frame(
        df=df,
        outcome_column=args.outcome_column,
        subject_column=args.subject_column,
        task_progression_column=args.task_progression_column,
        coin_column=args.coin_column,
        correctness_column=args.correctness_column,
        tp2_column=args.tp2_column,
        include_tp1=args.include_tp1,
        correct_only=args.correct_only,
    )
    

    outcome_label = args.outcome_label or prettify_name(args.outcome_column)

    specs = build_plot_specs(
        df=df,
        subject_column=args.subject_column,
        coin_column=args.coin_column,
        include_all_subjects=args.include_all_subjects,
        include_all_coins=args.include_all_coins,
        limit_subjects=args.limit_subjects,
        limit_coins=args.limit_coins,
    )

    manifest_rows: list[dict[str, object]] = []

    for spec in specs:
        subset = filter_for_spec(
            df=df,
            spec=spec,
            subject_column=args.subject_column,
            coin_column=args.coin_column,
        )

        n_rows = len(subset)
        n_unique_x = subset[args.task_progression_column].nunique()

        if n_rows < args.min_rows or n_unique_x < args.min_unique_x:
            manifest_rows.append(
                {
                    "file_name": None,
                    "outcome_column": args.outcome_column,
                    "outcome_label": outcome_label,
                    "subject_subset": spec.subject_label,
                    "coin_subset": spec.coin_label,
                    "tp_scope": "TP1+TP2" if args.include_tp1 else "TP2 only",
                    "drop_scope": "Correct only" if args.correct_only else "All drops",
                    "n_rows": n_rows,
                    "n_unique_task_progression": n_unique_x,
                    "status": "skipped_insufficient_data",
                }
            )
            continue

        try:
            model, pred_df = fit_linear_model(
                df=subset,
                outcome_column=args.outcome_column,
                task_progression_column=args.task_progression_column,
            )
            
        except Exception as exc:
            manifest_rows.append(
                {
                    "file_name": None,
                    "outcome_column": args.outcome_column,
                    "outcome_label": outcome_label,
                    "subject_subset": spec.subject_label,
                    "coin_subset": spec.coin_label,
                    "tp_scope": "TP1+TP2" if args.include_tp1 else "TP2 only",
                    "drop_scope": "Correct only" if args.correct_only else "All drops",
                    "n_rows": n_rows,
                    "n_unique_task_progression": n_unique_x,
                    "status": f"skipped_model_error: {exc}",
                }
            )
            continue

        fig = create_regression_summary_plot(
            df=subset,
            pred_df=pred_df,
            outcome_column=args.outcome_column,
            coin_column=args.coin_column,
            outcome_label=outcome_label,
            task_progression_column=args.task_progression_column,
            spec=spec,
            include_tp1=args.include_tp1,
            correct_only=args.correct_only,
            ci_style=args.ci_style,
            show_raw=args.show_raw,
            width=args.width,
            height=args.height,
        )

        file_name = build_output_name(
            outcome_column=args.outcome_column,
            spec_subject_value=spec.subject_value,
            spec_coin_value=spec.coin_value,
            include_tp1=args.include_tp1,
            correct_only=args.correct_only,
        )
        output_path = args.output_dir / file_name
        fig.savefig(output_path, dpi=args.dpi, bbox_inches="tight")
        plt.close(fig)

        manifest_rows.append(
            {
                "file_name": file_name,
                "outcome_column": args.outcome_column,
                "outcome_label": outcome_label,
                "subject_subset": spec.subject_label,
                "coin_subset": spec.coin_label,
                "tp_scope": "TP1+TP2" if args.include_tp1 else "TP2 only",
                "drop_scope": "Correct only" if args.correct_only else "All drops",
                "n_rows": n_rows,
                "n_unique_task_progression": n_unique_x,
                "slope": model.params.get(args.task_progression_column, np.nan),
                "p_value_slope": model.pvalues.get(args.task_progression_column, np.nan),
                "intercept": model.params.get("const", np.nan),
                "r_squared": model.rsquared,
                "status": "exported",
            }
        )

    manifest = pd.DataFrame(manifest_rows)
    manifest_path = args.output_dir / (
        f"regression_summary_manifest__{slugify(args.outcome_column)}__"
        f"{'tp1_tp2' if args.include_tp1 else 'tp2_only'}__"
        f"{'correct_only' if args.correct_only else 'all_drops'}.csv"
    )
    manifest.to_csv(manifest_path, index=False)

    exported = int((manifest["status"] == "exported").sum()) if not manifest.empty else 0
    skipped = len(manifest) - exported
    print(f"Done. Exported: {exported}, Skipped: {skipped}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()