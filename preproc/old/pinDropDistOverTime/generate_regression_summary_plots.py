# generate_regression_summary_plots.py

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm


DEFAULT_OUTCOME_COLUMN = "dropDist"
DEFAULT_SESSION_COLUMN = "sessionID"
DEFAULT_ROUND_COLUMN = "roundID"
DEFAULT_SUBJECT_COLUMN = "participantID"
DEFAULT_COIN_COLUMN = "coinLabel"


@dataclass(frozen=True)
class PlotSpec:
    subject_value: Optional[str]
    subject_label: str
    coin_value: Optional[str]
    coin_label: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate standalone regression-summary plots for outcome ~ task progression."
    )
    parser.add_argument("--input", type=Path, required=True, help="Path to input CSV.")
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
        help="Optional human-readable outcome label for axis/text. Example: 'Pin Drop Distance'.",
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
        help=f"Coin subset column. Default: {DEFAULT_COIN_COLUMN}",
    )
    parser.add_argument(
        "--session-column",
        type=str,
        default=DEFAULT_SESSION_COLUMN,
        help=f"Session column. Default: {DEFAULT_SESSION_COLUMN}",
    )
    parser.add_argument(
        "--round-column",
        type=str,
        default=DEFAULT_ROUND_COLUMN,
        help=f"Round column used to derive task progression. Default: {DEFAULT_ROUND_COLUMN}",
    )
    parser.add_argument(
        "--ci-style",
        choices=("ribbon", "dashed"),
        default="dashed",
        help="How to render the 95%% confidence interval.",
    )
    parser.add_argument(
        "--show-raw",
        action="store_true",
        help="Draw faint raw observations in the background.",
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
    parser.add_argument(
        "--width",
        type=float,
        default=7.0,
        help="Figure width in inches.",
    )
    parser.add_argument(
        "--height",
        type=float,
        default=6.5,
        help="Figure height in inches.",
    )
    parser.add_argument(
        "--include-all-subjects",
        action="store_true",
        default=True,
        help="Include an All Subjects aggregate.",
    )
    parser.add_argument(
        "--include-all-coins",
        action="store_true",
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


def validate_columns(df: pd.DataFrame, required_columns: Iterable[str]) -> None:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def coerce_string_label(value: object) -> str:
    if pd.isna(value):
        return "Missing"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def prettify_name(value: str) -> str:
    value = value.replace("_", " ")
    value = re.sub(r"([a-z])([A-Z])", r"\1 \2", value)
    return value.strip().title()


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "value"


def derive_task_progression(
    df: pd.DataFrame,
    session_column: str,
    round_column: str,
) -> pd.DataFrame:
    ordered_rounds = (
        df.reset_index(names="_row_order")
        [[session_column, round_column, "_row_order"]]
        .dropna(subset=[session_column, round_column])
        .drop_duplicates(subset=[session_column, round_column], keep="first")
        .sort_values([session_column, "_row_order"], kind="stable")
        .copy()
    )

    ordered_rounds["taskProgression"] = (
        ordered_rounds.groupby(session_column).cumcount() + 1
    )

    merged = df.merge(
        ordered_rounds[[session_column, round_column, "taskProgression"]],
        on=[session_column, round_column],
        how="left",
        validate="many_to_one",
    )

    return merged


def prepare_analysis_frame(
    df: pd.DataFrame,
    outcome_column: str,
    subject_column: str,
    coin_column: str,
) -> pd.DataFrame:
    frame = df.copy()
    frame[outcome_column] = pd.to_numeric(frame[outcome_column], errors="coerce")
    frame["taskProgression"] = pd.to_numeric(frame["taskProgression"], errors="coerce")

    frame = frame.dropna(
        subset=["taskProgression", outcome_column, subject_column, coin_column]
    ).copy()

    frame[subject_column] = frame[subject_column].map(coerce_string_label)
    frame[coin_column] = frame[coin_column].map(coerce_string_label)

    return frame


def build_plot_specs(
    df: pd.DataFrame,
    subject_column: str,
    coin_column: str,
    include_all_subjects: bool,
    include_all_coins: bool,
    limit_subjects: Optional[list[str]],
    limit_coins: Optional[list[str]],
) -> list[PlotSpec]:
    subject_values = sorted(df[subject_column].dropna().unique().tolist(), key=str)
    coin_values = sorted(df[coin_column].dropna().unique().tolist(), key=str)

    if limit_subjects is not None:
        allowed_subjects = {coerce_string_label(value) for value in limit_subjects}
        subject_values = [value for value in subject_values if value in allowed_subjects]

    if limit_coins is not None:
        allowed_coins = {coerce_string_label(value) for value in limit_coins}
        coin_values = [value for value in coin_values if value in allowed_coins]

    subject_specs: list[tuple[Optional[str], str]] = []
    coin_specs: list[tuple[Optional[str], str]] = []

    if include_all_subjects:
        subject_specs.append((None, "All Subjects"))
    subject_specs.extend((value, f"Subject: {value}") for value in subject_values)

    if include_all_coins:
        coin_specs.append((None, "All Coins"))
    coin_specs.extend((value, f"Coin: {value}") for value in coin_values)

    specs: list[PlotSpec] = []
    for subject_value, subject_label in subject_specs:
        for coin_value, coin_label in coin_specs:
            specs.append(
                PlotSpec(
                    subject_value=subject_value,
                    subject_label=subject_label,
                    coin_value=coin_value,
                    coin_label=coin_label,
                )
            )
    return specs


def filter_for_spec(
    df: pd.DataFrame,
    spec: PlotSpec,
    subject_column: str,
    coin_column: str,
) -> pd.DataFrame:
    filtered = df
    if spec.subject_value is not None:
        filtered = filtered.loc[filtered[subject_column] == spec.subject_value]
    if spec.coin_value is not None:
        filtered = filtered.loc[filtered[coin_column] == spec.coin_value]
    return filtered.copy()


def fit_linear_model(
    df: pd.DataFrame,
    outcome_column: str,
) -> tuple[sm.regression.linear_model.RegressionResultsWrapper, pd.DataFrame]:
    x = df["taskProgression"].astype(float)
    y = df[outcome_column].astype(float)

    design = sm.add_constant(x)
    model = sm.OLS(y, design).fit()

    x_grid = np.linspace(x.min(), x.max(), 200)
    pred_design = sm.add_constant(pd.Series(x_grid, name="taskProgression"))
    prediction = model.get_prediction(pred_design).summary_frame(alpha=0.05)

    pred_df = pd.DataFrame(
        {
            "taskProgression": x_grid,
            "mean": prediction["mean"].to_numpy(),
            "mean_ci_lower": prediction["mean_ci_lower"].to_numpy(),
            "mean_ci_upper": prediction["mean_ci_upper"].to_numpy(),
        }
    )

    return model, pred_df


def make_description_text(
    outcome_label: str,
    predictor_label: str,
    spec: PlotSpec,
) -> str:
    return (
        f"{outcome_label} × {predictor_label}\n"
        f"{spec.subject_label}\n"
        f"{spec.coin_label}"
    )


def create_plot(
    df: pd.DataFrame,
    pred_df: pd.DataFrame,
    outcome_column: str,
    outcome_label: str,
    spec: PlotSpec,
    ci_style: str,
    show_raw: bool,
    width: float,
    height: float,
) -> plt.Figure:
    fig = plt.figure(figsize=(width, height))
    ax = fig.add_axes([0.12, 0.36, 0.78, 0.55])

    if show_raw:
        ax.scatter(
            df["taskProgression"],
            df[outcome_column],
            alpha=0.18,
            s=14,
            linewidths=0,
        )

    ax.plot(
        pred_df["taskProgression"],
        pred_df["mean"],
        linewidth=2.0,
    )

    if ci_style == "ribbon":
        ax.fill_between(
            pred_df["taskProgression"],
            pred_df["mean_ci_lower"],
            pred_df["mean_ci_upper"],
            alpha=0.18,
        )
    else:
        ax.plot(
            pred_df["taskProgression"],
            pred_df["mean_ci_lower"],
            linestyle=(0, (3, 3)),
            linewidth=1.4,
        )
        ax.plot(
            pred_df["taskProgression"],
            pred_df["mean_ci_upper"],
            linestyle=(0, (3, 3)),
            linewidth=1.4,
        )

    ax.set_xlabel("Task Progression")
    ax.set_ylabel(outcome_label)
    ax.grid(True, alpha=0.25)

    description = make_description_text(
        outcome_label=outcome_label,
        predictor_label="Task Progression",
        spec=spec,
    )

    fig.text(
        0.12,
        0.14,
        description,
        ha="left",
        va="bottom",
        fontsize=10,
    )

    return fig


def build_output_name(spec: PlotSpec, outcome_column: str) -> str:
    outcome_part = slugify(outcome_column)
    subject_part = (
        "all_subjects"
        if spec.subject_value is None
        else f"subject_{slugify(spec.subject_value)}"
    )
    coin_part = (
        "all_coins"
        if spec.coin_value is None
        else f"coin_{slugify(spec.coin_value)}"
    )
    return f"regression_summary__{outcome_part}__{subject_part}__{coin_part}.png"


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.input)

    validate_columns(
        df,
        [
            args.outcome_column,
            args.session_column,
            args.round_column,
            args.subject_column,
            args.coin_column,
        ],
    )

    df = derive_task_progression(
        df=df,
        session_column=args.session_column,
        round_column=args.round_column,
    )

    df = prepare_analysis_frame(
        df=df,
        outcome_column=args.outcome_column,
        subject_column=args.subject_column,
        coin_column=args.coin_column,
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
        n_unique_x = subset["taskProgression"].nunique()

        if n_rows < args.min_rows or n_unique_x < args.min_unique_x:
            manifest_rows.append(
                {
                    "file_name": None,
                    "outcome_column": args.outcome_column,
                    "outcome_label": outcome_label,
                    "subject_subset": spec.subject_label,
                    "coin_subset": spec.coin_label,
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
            )
        except Exception as exc:
            manifest_rows.append(
                {
                    "file_name": None,
                    "outcome_column": args.outcome_column,
                    "outcome_label": outcome_label,
                    "subject_subset": spec.subject_label,
                    "coin_subset": spec.coin_label,
                    "n_rows": n_rows,
                    "n_unique_task_progression": n_unique_x,
                    "status": f"skipped_model_error: {exc}",
                }
            )
            continue

        fig = create_plot(
            df=subset,
            pred_df=pred_df,
            outcome_column=args.outcome_column,
            outcome_label=outcome_label,
            spec=spec,
            ci_style=args.ci_style,
            show_raw=args.show_raw,
            width=args.width,
            height=args.height,
        )

        file_name = build_output_name(spec, args.outcome_column)
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
                "n_rows": n_rows,
                "n_unique_task_progression": n_unique_x,
                "slope": model.params.get("taskProgression", np.nan),
                "intercept": model.params.get("const", np.nan),
                "p_value_slope": model.pvalues.get("taskProgression", np.nan),
                "r_squared": model.rsquared,
                "status": "exported",
            }
        )

    manifest = pd.DataFrame(manifest_rows)
    manifest_path = args.output_dir / f"regression_summary_manifest__{slugify(args.outcome_column)}.csv"
    manifest.to_csv(manifest_path, index=False)

    exported = int((manifest["status"] == "exported").sum()) if not manifest.empty else 0
    skipped = len(manifest) - exported
    print(f"Done. Exported: {exported}, Skipped: {skipped}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()