# plot_round_duration_flags.py

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats


REQUIRED_COLUMNS = {
    "roundID",
    "roundID_int",
    "round_dur_s",
    "roundDur_pref_groupEligible",
    "roundDur_iqr_rr_sess_out",
}


def parse_boolean(series: pd.Series) -> pd.Series:
    """Convert common Boolean representations to pandas nullable Boolean values."""
    if pd.api.types.is_bool_dtype(series):
        return series.astype("boolean")

    normalized = series.astype("string").str.strip().str.upper()

    value_map = {
        "TRUE": True,
        "T": True,
        "1": True,
        "YES": True,
        "Y": True,
        "FALSE": False,
        "F": False,
        "0": False,
        "NO": False,
        "N": False,
    }

    return normalized.map(value_map).astype("boolean")


def validate_columns(data: pd.DataFrame) -> None:
    """Raise a readable error when required columns are absent."""
    missing_columns = REQUIRED_COLUMNS.difference(data.columns)

    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing required columns: {missing}")


def verify_round_consistency(data: pd.DataFrame) -> None:
    """Ensure round-level variables do not vary within a round."""
    round_level_columns = [
        "roundID_int",
        "round_dur_s",
        "roundDur_pref_groupEligible",
        "roundDur_iqr_rr_sess_out",
    ]

    inconsistencies = (
        data.groupby("roundID", dropna=False)[round_level_columns]
        .nunique(dropna=False)
        .gt(1)
    )

    problematic_rounds = inconsistencies.any(axis=1)

    if problematic_rounds.any():
        example_ids = problematic_rounds[problematic_rounds].index[:10].tolist()
        raise ValueError(
            "Round-level values differ within some roundID groups. "
            f"Examples: {example_ids}"
        )


def prepare_round_data(data: pd.DataFrame) -> pd.DataFrame:
    """Return one validated observation per unique round."""
    validate_columns(data)

    prepared = data.copy()
    prepared["round_dur_s"] = pd.to_numeric(
        prepared["round_dur_s"],
        errors="coerce",
    )
    prepared["roundID_int"] = pd.to_numeric(
        prepared["roundID_int"],
        errors="coerce",
    )
    prepared["roundDur_pref_groupEligible"] = parse_boolean(
        prepared["roundDur_pref_groupEligible"]
    )
    prepared["roundDur_iqr_rr_sess_out"] = parse_boolean(
        prepared["roundDur_iqr_rr_sess_out"]
    )

    verify_round_consistency(prepared)

    round_data = (
        prepared[
            [
                "roundID",
                "roundID_int",
                "round_dur_s",
                "roundDur_pref_groupEligible",
                "roundDur_iqr_rr_sess_out",
            ]
        ]
        .drop_duplicates(subset="roundID")
        .dropna(subset=["roundID", "roundID_int", "round_dur_s"])
        .sort_values("roundID_int")
        .reset_index(drop=True)
    )

    round_data["roundID_int"] = round_data["roundID_int"].astype(int)

    return round_data


def descriptive_statistics(
    data: pd.DataFrame,
    flag_column: str,
    false_is_highlighted: bool = True,
) -> pd.DataFrame:
    """Calculate descriptive statistics for each Boolean flag group."""
    working = data.dropna(subset=[flag_column, "round_dur_s"]).copy()

    summaries = (
        working.groupby(flag_column, observed=True)["round_dur_s"]
        .agg(
            n="count",
            mean="mean",
            std="std",
            median="median",
            minimum="min",
            maximum="max",
        )
        .reset_index()
    )

    quantiles = (
        working.groupby(flag_column, observed=True)["round_dur_s"]
        .quantile([0.25, 0.75])
        .unstack()
        .rename(columns={0.25: "q1", 0.75: "q3"})
        .reset_index()
    )

    summaries = summaries.merge(quantiles, on=flag_column, how="left")
    summaries["iqr"] = summaries["q3"] - summaries["q1"]
    summaries["sem"] = summaries["std"] / np.sqrt(summaries["n"])

    summaries["plot_color"] = np.where(
        summaries[flag_column].eq(False) if false_is_highlighted
        else summaries[flag_column].eq(True),
        "red",
        "black",
    )

    return summaries


def cohens_d(group_a: np.ndarray, group_b: np.ndarray) -> float:
    """Calculate Cohen's d using the pooled standard deviation."""
    n_a = len(group_a)
    n_b = len(group_b)

    if n_a < 2 or n_b < 2:
        return np.nan

    variance_a = np.var(group_a, ddof=1)
    variance_b = np.var(group_b, ddof=1)

    pooled_variance = (
        ((n_a - 1) * variance_a) + ((n_b - 1) * variance_b)
    ) / (n_a + n_b - 2)

    if pooled_variance <= 0:
        return np.nan

    return float(
        (np.mean(group_a) - np.mean(group_b))
        / np.sqrt(pooled_variance)
    )


def comparison_statistics(
    data: pd.DataFrame,
    flag_column: str,
) -> pd.DataFrame:
    """Compare FALSE and TRUE groups using parametric and rank tests."""
    working = data.dropna(subset=[flag_column, "round_dur_s"])

    false_values = working.loc[
        working[flag_column].eq(False),
        "round_dur_s",
    ].to_numpy(dtype=float)

    true_values = working.loc[
        working[flag_column].eq(True),
        "round_dur_s",
    ].to_numpy(dtype=float)

    result = {
        "flag": flag_column,
        "false_n": len(false_values),
        "true_n": len(true_values),
        "false_mean": np.mean(false_values) if len(false_values) else np.nan,
        "true_mean": np.mean(true_values) if len(true_values) else np.nan,
        "mean_difference_false_minus_true": (
            np.mean(false_values) - np.mean(true_values)
            if len(false_values) and len(true_values)
            else np.nan
        ),
        "cohens_d_false_minus_true": cohens_d(
            false_values,
            true_values,
        ),
    }

    if len(false_values) >= 2 and len(true_values) >= 2:
        t_test = stats.ttest_ind(
            false_values,
            true_values,
            equal_var=False,
            nan_policy="omit",
        )

        mann_whitney = stats.mannwhitneyu(
            false_values,
            true_values,
            alternative="two-sided",
        )

        rank_biserial = (
            (2 * mann_whitney.statistic)
            / (len(false_values) * len(true_values))
        ) - 1

        result.update(
            {
                "welch_t": t_test.statistic,
                "welch_p": t_test.pvalue,
                "mann_whitney_u": mann_whitney.statistic,
                "mann_whitney_p": mann_whitney.pvalue,
                "rank_biserial_false_minus_true": rank_biserial,
            }
        )
    else:
        result.update(
            {
                "welch_t": np.nan,
                "welch_p": np.nan,
                "mann_whitney_u": np.nan,
                "mann_whitney_p": np.nan,
                "rank_biserial_false_minus_true": np.nan,
            }
        )

    return pd.DataFrame([result])


def plot_round_durations(
    data: pd.DataFrame,
    flag_column: str,
    title: str,
    output_path: Path,
    highlight_false: bool = True,
    random_seed: int = 42,
) -> None:
    """Create a violin plot with deterministic jittered round-level points."""
    plot_data = data.dropna(
        subset=["roundID_int", "round_dur_s", flag_column]
    ).copy()

    if plot_data.empty:
        raise ValueError(f"No plottable data available for {flag_column}.")

    sns.set_theme(style="whitegrid")

    figure_width = max(16, min(40, len(plot_data) / 25))
    figure, axis = plt.subplots(figsize=(figure_width, 8))

    sns.violinplot(
        data=plot_data,
        x="roundID_int",
        y="round_dur_s",
        color="lightgray",
        inner=None,
        cut=0,
        linewidth=0.5,
        scale="width",
        ax=axis,
    )

    category_order = sorted(plot_data["roundID_int"].unique())
    category_positions = {
        round_id: position
        for position, round_id in enumerate(category_order)
    }

    rng = np.random.default_rng(random_seed)
    jitter = rng.uniform(-0.18, 0.18, size=len(plot_data))

    base_positions = (
        plot_data["roundID_int"]
        .map(category_positions)
        .to_numpy(dtype=float)
    )

    highlighted = (
        plot_data[flag_column].eq(False)
        if highlight_false
        else plot_data[flag_column].eq(True)
    )

    colors = np.where(highlighted, "red", "black")

    axis.scatter(
        base_positions + jitter,
        plot_data["round_dur_s"],
        c=colors,
        s=22,
        alpha=0.8,
        linewidths=0,
        zorder=3,
    )

    from matplotlib.lines import Line2D

    legend_elements = [
        Line2D(
            [0],
            [0],
            marker="o",
            linestyle="",
            markerfacecolor="black",
            markeredgecolor="black",
            label="Default",
        ),
        Line2D(
            [0],
            [0],
            marker="o",
            linestyle="",
            markerfacecolor="red",
            markeredgecolor="red",
            label=f"{flag_column} = FALSE",
        ),
    ]

    axis.legend(handles=legend_elements, loc="upper right")
    axis.set_title(title)
    axis.set_xlabel("Round ID")
    axis.set_ylabel("Round duration (seconds)")

    label_step = max(1, len(category_order) // 25)
    visible_positions = np.arange(0, len(category_order), label_step)
    visible_labels = [
        str(category_order[position])
        for position in visible_positions
    ]

    axis.set_xticks(visible_positions)
    axis.set_xticklabels(visible_labels, rotation=45, ha="right")

    figure.tight_layout()
    figure.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(figure)


def save_statistics(
    round_data: pd.DataFrame,
    output_directory: Path,
) -> None:
    """Save round-level data and all requested statistical summaries."""
    round_data.to_csv(
        output_directory / "round_level_data.csv",
        index=False,
    )

    overall_stats = round_data["round_dur_s"].describe(
        percentiles=[0.25, 0.5, 0.75]
    )
    overall_stats.to_csv(
        output_directory / "overall_round_duration_stats.csv",
        header=["value"],
    )

    flag_columns = [
        "roundDur_pref_groupEligible",
        "roundDur_iqr_rr_sess_out",
    ]

    descriptive_frames = []
    comparison_frames = []

    for flag_column in flag_columns:
        description = descriptive_statistics(
            round_data,
            flag_column,
            false_is_highlighted=True,
        )
        description.insert(0, "flag", flag_column)
        descriptive_frames.append(description)

        comparison_frames.append(
            comparison_statistics(round_data, flag_column)
        )

    descriptive_results = pd.concat(
        descriptive_frames,
        ignore_index=True,
    )
    comparison_results = pd.concat(
        comparison_frames,
        ignore_index=True,
    )

    descriptive_results.to_csv(
        output_directory / "round_duration_descriptive_stats.csv",
        index=False,
    )
    comparison_results.to_csv(
        output_directory / "round_duration_group_comparisons.csv",
        index=False,
    )

    missingness = pd.DataFrame(
        {
            "column": round_data.columns,
            "missing_n": round_data.isna().sum().values,
            "missing_percent": (
                round_data.isna().mean().values * 100
            ),
        }
    )

    missingness.to_csv(
        output_directory / "round_level_missingness.csv",
        index=False,
    )

    print("\nOverall round-duration statistics:")
    print(overall_stats.to_string())

    print("\nDescriptive statistics by flag:")
    print(descriptive_results.to_string(index=False))

    print("\nGroup comparisons:")
    print(comparison_results.to_string(index=False))


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Plot round durations and report statistics for "
            "round-duration quality flags."
        )
    )
    parser.add_argument(
        "csv_path",
        type=Path,
        help="Path to the input CSV file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("round_duration_results"),
        help="Directory for plots and CSV reports.",
    )
    return parser.parse_args()


def main() -> None:
    """Run plotting and statistical reporting."""
    arguments = parse_arguments()

    if not arguments.csv_path.exists():
        raise FileNotFoundError(
            f"Input file does not exist: {arguments.csv_path}"
        )

    arguments.output_dir.mkdir(parents=True, exist_ok=True)

    raw_data = pd.read_csv(arguments.csv_path)
    round_data = prepare_round_data(raw_data)

    print(f"Input interval rows: {len(raw_data):,}")
    print(f"Unique analyzed rounds: {len(round_data):,}")

    plot_round_durations(
        data=round_data,
        flag_column="roundDur_pref_groupEligible",
        title=(
            "Round Duration by Round ID\n"
            "Red = roundDur_pref_groupEligible is FALSE"
        ),
        output_path=(
            arguments.output_dir
            / "round_duration_pref_group_eligible.png"
        ),
    )

    plot_round_durations(
        data=round_data,
        flag_column="roundDur_iqr_rr_sess_out",
        title=(
            "Round Duration by Round ID\n"
            "Red = roundDur_iqr_rr_sess_out is FALSE"
        ),
        output_path=(
            arguments.output_dir
            / "round_duration_iqr_rr_sess_out.png"
        ),
    )

    save_statistics(round_data, arguments.output_dir)

    print(f"\nResults saved to: {arguments.output_dir.resolve()}")


if __name__ == "__main__":
    main()