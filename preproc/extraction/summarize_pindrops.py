#!/usr/bin/env python3
# summarize_pindrops.py

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


DEMOGRAPHIC_COLUMNS = [
    "currentRole",
    "device",
    "main_RR",
    "pairID",
    "participantID",
    "ptIsAorB",
    "sessionID",
    "testingDate",
    "PVSS_TotalScore",
    "PVSS_AvgScore",
    "Age",
    "Gender",
    "SpatialMemRating",
]

PATH_ORDER_LABELS = {
    "num_HVLVNV": "HV -> LV -> NV",
    "num_LVHVNV": "LV -> HV -> NV",
    "num_HVNVLV": "HV -> NV -> LV",
    "num_NVHVLV": "NV -> HV -> LV",
    "num_LVNVHV": "LV -> NV -> HV",
    "num_NVLVHV": "NV -> LV -> HV",
}

DEFAULT_CRITERION_COLUMNS = [
    "rounds2criterion",
    "restartsTP1",
    "LongShortRR",
    "coinSet"
]


def normalize_session_id(value: object) -> str:
    """Normalize session IDs for reliable cross-file matching."""
    if pd.isna(value):
        return ""

    text = str(value).strip()

    if text.endswith(".0"):
        candidate = text[:-2]

        if candidate.replace("-", "").isdigit():
            return candidate

    return text


def parse_criterion_columns(
    value: str | None,
) -> list[str]:
    """Parse a comma-separated criterion column list."""
    if value is None:
        return DEFAULT_CRITERION_COLUMNS.copy()

    columns = [
        column.strip()
        for column in value.split(",")
        if column.strip()
    ]

    if not columns:
        raise ValueError(
            "--criterion-columns must contain at least one column name."
        )

    return columns


def load_criterion_lookup(
    criterion_path: Path,
    criterion_columns: list[str],
) -> pd.DataFrame:
    """Load one main-cohort criterion record per normalized session ID."""
    criterion_df = pd.read_csv(
        criterion_path,
        low_memory=False,
    )

    required = {
        "sessionID",
        *criterion_columns,
    }

    missing = sorted(
        required - set(criterion_df.columns)
    )

    if missing:
        raise ValueError(
            f"{criterion_path.name}: "
            f"missing criterion columns: {missing}"
        )

    working = criterion_df.copy()

    if "main_RR" in working.columns:
        main_mask = (
            working["main_RR"]
            .astype(str)
            .str.strip()
            .str.lower()
            .eq("main")
        )

        working = working.loc[
            main_mask
        ].copy()

    working["_criterionSessionKey"] = (
        working["sessionID"]
        .map(normalize_session_id)
    )

    working = working.loc[
        working["_criterionSessionKey"].ne("")
    ].copy()

    duplicate_mask = working[
        "_criterionSessionKey"
    ].duplicated(
        keep=False
    )

    if duplicate_mask.any():
        duplicates = working.loc[
            duplicate_mask,
            [
                "sessionID",
                "_criterionSessionKey",
            ],
        ]

        raise ValueError(
            "Criterion report contains duplicate main-cohort "
            "sessionID values after normalization:\n"
            f"{duplicates.to_string(index=False)}"
        )

    selected_columns = [
        "_criterionSessionKey",
        *criterion_columns,
    ]

    lookup = working[
        selected_columns
    ].copy()


    return lookup


def attach_criterion_data(
    summary_df: pd.DataFrame,
    criterion_lookup: pd.DataFrame,
    criterion_columns: list[str],
) -> pd.DataFrame:
    """Attach main-cohort criterion data to participant summaries."""
    if "sessionID" not in summary_df.columns:
        raise ValueError(
            "Summary output has no sessionID column "
            "for criterion matching."
        )

    output = summary_df.copy()

    output["_criterionSessionKey"] = (
        output["sessionID"]
        .map(normalize_session_id)
    )

    if "main_RR" in output.columns:
        output["_isMainCohort"] = (
            output["main_RR"]
            .astype(str)
            .str.strip()
            .str.lower()
            .eq("main")
        )
    else:
        output["_isMainCohort"] = True

    output = output.merge(
        criterion_lookup,
        on="_criterionSessionKey",
        how="left",
        validate="m:1",
    )

    criterion_output_columns = criterion_columns

    output["criterionMatch"] = (
        output["_isMainCohort"]
        & output[
            criterion_output_columns
        ].notna().any(axis=1)
    )

    output["criterionMatchStatus"] = np.select(
        [
            ~output["_isMainCohort"],
            output["criterionMatch"],
        ],
        [
            "not_main_cohort",
            "matched",
        ],
        default="main_session_not_found",
    )

    output = output.drop(
        columns=[
            "_criterionSessionKey",
            "_isMainCohort",
        ]
    )

    return output


def print_criterion_merge_summary(
    summary_df: pd.DataFrame,
) -> None:
    """Print criterion merge counts."""
    if "criterionMatchStatus" not in summary_df.columns:
        return

    counts = (
        summary_df["criterionMatchStatus"]
        .value_counts(dropna=False)
        .to_dict()
    )

    print(
        "Criterion matches: "
        f"{counts.get('matched', 0)}"
    )

    print(
        "Main sessions not found in criterion report: "
        f"{counts.get('main_session_not_found', 0)}"
    )

    print(
        "Non-main sessions left blank: "
        f"{counts.get('not_main_cohort', 0)}"
    )


def safe_ratio(
    numerator: float,
    denominator: float,
):
    """Return a ratio or NaN when the denominator is zero."""
    if denominator == 0:
        return np.nan

    return numerator / denominator


def get_single_unique_value(
    series: pd.Series,
):
    """Return one unique value or a list when values differ."""
    values = series.dropna()

    if values.empty:
        return None

    unique_values = pd.unique(values)

    if len(unique_values) == 1:
        value = unique_values[0]

        return (
            value.item()
            if hasattr(value, "item")
            else value
        )

    result = []

    for value in unique_values:
        result.append(
            value.item()
            if hasattr(value, "item")
            else value
        )

    return result


def build_round_id(
    df: pd.DataFrame,
) -> pd.Series:
    """Construct a unique round identifier."""
    if (
        "TotSesh_actTest_RoundNum" in df.columns
        and df[
            "TotSesh_actTest_RoundNum"
        ].notna().all()
    ):
        numeric_rounds = pd.to_numeric(
            df["TotSesh_actTest_RoundNum"],
            errors="coerce",
        )

        if numeric_rounds.notna().all():
            return (
                numeric_rounds
                .astype(int)
                .astype(str)
            )

        return df[
            "TotSesh_actTest_RoundNum"
        ].astype(str)

    if (
        "source_file" in df.columns
        and "RoundNum" in df.columns
    ):
        round_num = (
            pd.to_numeric(
                df["RoundNum"],
                errors="coerce",
            )
            .astype("Int64")
            .astype(str)
        )

        return (
            df["source_file"].astype(str)
            + "::"
            + round_num
        )

    raise ValueError(
        "Could not construct a unique round identifier. "
        "Expected either 'TotSesh_actTest_RoundNum' "
        "or both 'source_file' and 'RoundNum'."
    )


def get_actual_test_rounds(
    working: pd.DataFrame,
    max_rounds: int | None,
) -> pd.DataFrame:
    """Restrict rows to valid main actual-test rounds."""
    actual_round = pd.to_numeric(
        working["TotSesh_actTest_RoundNum"],
        errors="coerce",
    )

    actual_test = working.loc[
        actual_round.notna()
    ].copy()

    actual_test["actual_test_round_num"] = (
        actual_round.loc[
            actual_round.notna()
        ]
    )

    if max_rounds is not None:
        actual_test = actual_test.loc[
            actual_test[
                "actual_test_round_num"
            ]
            <= max_rounds
        ].copy()

    return actual_test


def build_actual_test_round_level(
    actual_test: pd.DataFrame,
) -> pd.DataFrame:
    """Collapse repeated pin rows to one row per actual-test round."""
    return (
        actual_test.sort_values(
            [
                "actual_test_round_num",
                "_orig_index",
            ],
            kind="stable",
        )
        .drop_duplicates(
            subset=[
                "actual_test_round_num"
            ],
            keep="last",
        )
        .copy()
    )


def summarize_dataframe(
    df: pd.DataFrame,
    file_label: str,
    max_rounds: int | None = None,
) -> dict:
    """Summarize one participant file."""
    required_columns = [
        "CoinSetID",
        "BlockType",
        "dropDist",
        "isSwap",
        "swapType",
        "SwapVote",
        "SwapVoteScore",
        "source_file",
        "roundGrandTotal",
        "avgRoundSpeed",
        "path_eff_raw",
        "path_order_round",
        "TotSesh_actTest_RoundNum",
    ]

    missing = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required columns: {missing}"
        )

    if (
        max_rounds is not None
        and max_rounds <= 0
    ):
        raise ValueError(
            "max_rounds must be a positive integer."
        )

    working = (
        df.copy()
        .reset_index()
        .rename(
            columns={
                "index": "_orig_index"
            }
        )
    )

    working["CoinSetID_num"] = pd.to_numeric(
        working["CoinSetID"],
        errors="coerce",
    )

    working["dropDist_num"] = pd.to_numeric(
        working["dropDist"],
        errors="coerce",
    )

    working["isSwap_num"] = (
        pd.to_numeric(
            working["isSwap"],
            errors="coerce",
        )
        .fillna(0)
        .astype(int)
    )

    working["avgRoundSpeed_num"] = pd.to_numeric(
        working["avgRoundSpeed"],
        errors="coerce",
    )

    working["path_eff_raw_num"] = pd.to_numeric(
        working["path_eff_raw"],
        errors="coerce",
    )

    working["roundGrandTotal_num"] = pd.to_numeric(
        working["roundGrandTotal"],
        errors="coerce",
    )

    all_actual_round_numbers = pd.to_numeric(
        working["TotSesh_actTest_RoundNum"],
        errors="coerce",
    ).dropna()

    total_actual_test_rounds_available = int(
        all_actual_round_numbers.nunique()
    )

    actual_test = get_actual_test_rounds(
        working,
        max_rounds=max_rounds,
    )

    if actual_test.empty:
        cutoff_text = (
            f"first {max_rounds} actual-test rounds"
            if max_rounds is not None
            else "actual-test rounds"
        )

        raise ValueError(
            f"No rows matched the requested {cutoff_text}."
        )

    actual_round_level = (
        build_actual_test_round_level(
            actual_test
        )
    )

    actual_test_rounds_included = int(
        len(actual_round_level)
    )

    swap_vote_registered_n = int(
        actual_round_level[
            "SwapVote"
        ].notna().sum()
    )

    swap_vote_registered_d = (
        actual_test_rounds_included
    )

    swap_vote_registered_recalc = safe_ratio(
        swap_vote_registered_n,
        swap_vote_registered_d,
    )

    filtered = actual_test.loc[
        (
            actual_test[
                "CoinSetID_num"
            ]
            < 4
        )
        & (
            actual_test[
                "BlockType"
            ]
            .astype(str)
            .str.strip()
            .str.lower()
            == "pindropping"
        )
    ].copy()

    if filtered.empty:
        raise ValueError(
            "No pindropping rows with CoinSetID < 4 "
            "matched the requested actual-test round range."
        )

    filtered["round_id"] = build_round_id(
        filtered
    )

    tot_pin_drops = int(
        len(filtered)
    )

    tot_correct = int(
        (
            filtered[
                "dropDist_num"
            ]
            <= 1.1
        ).sum()
    )

    tot_score = safe_ratio(
        tot_correct,
        tot_pin_drops,
    )

    tot_swap_all = int(
        filtered[
            "isSwap_num"
        ].sum()
    )

    tot_normal = int(
        tot_pin_drops
        - tot_swap_all
    )

    swap_type_lower = (
        filtered[
            "swapType"
        ]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    tot_swap_neg = int(
        (
            swap_type_lower
            == "neg"
        ).sum()
    )

    tot_swap_pos = int(
        (
            swap_type_lower
            == "pos"
        ).sum()
    )

    swap_rate_tot = safe_ratio(
        tot_swap_all,
        tot_pin_drops,
    )

    swap_ratio = (
        f"{tot_normal}:"
        f"{tot_swap_neg}:"
        f"{tot_swap_pos}"
    )

    tot_rounds = int(
        filtered[
            "round_id"
        ].nunique()
    )

    swap_round_ids = filtered.loc[
        filtered[
            "isSwap_num"
        ]
        == 1,
        "round_id",
    ].unique()

    tot_swap_rounds = int(
        len(swap_round_ids)
    )

    tot_normal_rounds = int(
        tot_rounds
        - tot_swap_rounds
    )

    tot_swap_round_ratio = (
        f"{tot_normal_rounds}:"
        f"{tot_swap_neg}:"
        f"{tot_swap_pos}"
    )

    correct_vote_rounds = int(
        filtered.loc[
            (
                filtered[
                    "SwapVoteScore"
                ]
                .astype(str)
                .str.strip()
                .str.lower()
                == "correct"
            ),
            "round_id",
        ].nunique()
    )

    swap_vote_score = safe_ratio(
        correct_vote_rounds,
        tot_rounds,
    )

    source_files = (
        filtered[
            "source_file"
        ]
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    num_files = int(
        len(source_files)
    )

    source_file_tot_points = {}

    for source_file, group in actual_test.groupby(
        "source_file",
        sort=False,
    ):
        group_non_null = group.loc[
            group[
                "roundGrandTotal_num"
            ].notna()
        ].sort_values(
            [
                "actual_test_round_num",
                "_orig_index",
            ],
            kind="stable",
        )

        if group_non_null.empty:
            source_file_tot_points[
                source_file
            ] = None
        else:
            source_file_tot_points[
                source_file
            ] = float(
                group_non_null.iloc[-1][
                    "roundGrandTotal_num"
                ]
            )

    cumulative_points_rows = actual_test.loc[
        actual_test[
            "roundGrandTotal_num"
        ].notna()
    ].sort_values(
        [
            "actual_test_round_num",
            "_orig_index",
        ],
        kind="stable",
    )

    if cumulative_points_rows.empty:
        tot_points = None
    else:
        tot_points = float(
            cumulative_points_rows.iloc[-1][
                "roundGrandTotal_num"
            ]
        )

    round_level = (
        filtered.sort_values(
            [
                "actual_test_round_num",
                "_orig_index",
            ],
            kind="stable",
        )
        .drop_duplicates(
            subset=[
                "round_id"
            ],
            keep="last",
        )
        .copy()
    )

    avg_round_speed = float(
        round_level[
            "avgRoundSpeed_num"
        ].mean()
    )

    std_round_speed = float(
        round_level[
            "avgRoundSpeed_num"
        ].std()
    )

    avg_path_eff = float(
        round_level[
            "path_eff_raw_num"
        ].mean()
    )

    std_path_eff = float(
        round_level[
            "path_eff_raw_num"
        ].std()
    )

    path_order_counts = (
        round_level[
            "path_order_round"
        ]
        .value_counts(
            dropna=False
        )
        .to_dict()
    )

    summary = {
        "inputFile": file_label,
        "maxActualTestRoundsRequested": max_rounds,
        "actualTestRoundsAvailable":
            total_actual_test_rounds_available,
        "actualTestRoundsIncluded":
            actual_test_rounds_included,
        "totPinDrops": tot_pin_drops,
        "totCorrect": tot_correct,
        "totScore": tot_score,
        "totSwap_all": tot_swap_all,
        "totNormal": tot_normal,
        "totSwap_neg": tot_swap_neg,
        "totSwap_pos": tot_swap_pos,
        "swapRate_tot": swap_rate_tot,
        "swapRatio": swap_ratio,
        "totRounds": tot_rounds,
        "totSwap_Rounds": tot_swap_rounds,
        "totNormal_Rounds": tot_normal_rounds,
        "totSwap_RoundRatio":
            tot_swap_round_ratio,
        "swapVoteScore":
            swap_vote_score,
        "swapVoteRegistered_n_sum":
            swap_vote_registered_n,
        "swapVoteRegistered_d_sum":
            swap_vote_registered_d,
        "swapVoteRegistered_recalc":
            swap_vote_registered_recalc,
        "sourceFiles": json.dumps(
            source_files,
            ensure_ascii=False,
        ),
        "numFiles": num_files,
        "sourceFile_totPoints":
            json.dumps(
                source_file_tot_points,
                ensure_ascii=False,
            ),
        "totPoints": tot_points,
        "avgRoundSpeed": avg_round_speed,
        "stdRoundSpeed": std_round_speed,
        "avgPathEff": avg_path_eff,
        "stdPathEff": std_path_eff,
    }

    for output_name, path_label in (
        PATH_ORDER_LABELS.items()
    ):
        summary[
            output_name
        ] = int(
            path_order_counts.get(
                path_label,
                0,
            )
        )

    for column in DEMOGRAPHIC_COLUMNS:
        summary[column] = (
            get_single_unique_value(
                filtered[column]
            )
            if column in filtered.columns
            else None
        )

    return summary


def summarize_csv_file(
    csv_path: Path,
    max_rounds: int | None = None,
) -> dict:
    """Read and summarize one CSV file."""
    df = pd.read_csv(
        csv_path,
        low_memory=False,
    )

    return summarize_dataframe(
        df,
        csv_path.name,
        max_rounds=max_rounds,
    )


def summarize_folder(
    folder_path: Path,
    pattern: str = "*.csv",
    max_rounds: int | None = None,
) -> pd.DataFrame:
    """Summarize all matching CSV files in a folder."""
    csv_files = sorted(
        folder_path.glob(pattern)
    )

    if not csv_files:
        raise ValueError(
            f"No files matched pattern "
            f"{pattern!r} in {folder_path}"
        )

    summaries = []
    errors = []

    for csv_file in csv_files:
        try:
            summaries.append(
                summarize_csv_file(
                    csv_file,
                    max_rounds=max_rounds,
                )
            )
        except Exception as exc:
            errors.append(
                {
                    "inputFile":
                        csv_file.name,
                    "error":
                        str(exc),
                }
            )

    if not summaries:
        error_text = "\n".join(
            f"{item['inputFile']}: "
            f"{item['error']}"
            for item in errors
        )

        raise ValueError(
            "No files were summarized "
            f"successfully.\n{error_text}"
        )

    summary_df = pd.DataFrame(
        summaries
    )

    if errors:
        error_df = pd.DataFrame(
            errors
        )

        error_path = (
            folder_path
            / "summary_errors.csv"
        )

        error_df.to_csv(
            error_path,
            index=False,
        )

        print(
            f"Wrote error log: "
            f"{error_path}"
        )

    return summary_df


def write_output(
    summary_df: pd.DataFrame,
    out_path: Path,
) -> None:
    """Write a summary DataFrame as CSV or JSON."""
    out_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if out_path.suffix.lower() == ".json":
        records = summary_df.to_dict(
            orient="records"
        )

        with open(
            out_path,
            "w",
            encoding="utf-8",
        ) as handle:
            json.dump(
                records,
                handle,
                indent=2,
                ensure_ascii=False,
            )
    else:
        summary_df.to_csv(
            out_path,
            index=False,
        )


def main() -> None:
    """Run the command-line summarizer."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "input_path",
        help=(
            "Path to a CSV file or a folder "
            "containing CSV files"
        ),
    )

    parser.add_argument(
        "--pattern",
        default="*.csv",
        help=(
            "Glob pattern for folder mode "
            "(default: *.csv)"
        ),
    )

    parser.add_argument(
        "--max-rounds",
        type=int,
        default=None,
        help=(
            "Limit analysis to the first N actual-test "
            "rounds using TotSesh_actTest_RoundNum. "
            "Participants with fewer than N rounds are "
            "retained and summarized using all "
            "available rounds."
        ),
    )

    parser.add_argument(
        "--criterion-report",
        type=Path,
        default=None,
        help=(
            "Optional criterionReporting.csv path. "
            "Main-cohort criterion fields are matched "
            "by sessionID."
        ),
    )

    parser.add_argument(
        "--criterion-columns",
        default=None,
        help=(
            "Comma-separated criterion columns to attach. "
            "Default: rounds2criterion,restartsTP1,"
            "LongShortRR,coinSet"
        ),
    )

    parser.add_argument(
        "--out",
        default=None,
        help=(
            "Optional output file (.csv or .json). "
            "In folder mode, defaults to "
            "<folder>/folder_summary.csv. "
            "In file mode, defaults to stdout."
        ),
    )

    args = parser.parse_args()

    input_path = Path(
        args.input_path
    )

    criterion_columns = (
        parse_criterion_columns(
            args.criterion_columns
        )
    )

    criterion_lookup = None

    if args.criterion_report is not None:
        if not args.criterion_report.is_file():
            parser.error(
                "Criterion report does not exist: "
                f"{args.criterion_report}"
            )

        criterion_lookup = load_criterion_lookup(
            criterion_path=
                args.criterion_report,
            criterion_columns=
                criterion_columns,
        )

    if input_path.is_dir():
        summary_df = summarize_folder(
            input_path,
            pattern=args.pattern,
            max_rounds=args.max_rounds,
        )

        if criterion_lookup is not None:
            summary_df = attach_criterion_data(
                summary_df=summary_df,
                criterion_lookup=
                    criterion_lookup,
                criterion_columns=
                    criterion_columns,
            )

        out_path = (
            Path(args.out)
            if args.out
            else input_path
            / "folder_summary.csv"
        )

        write_output(
            summary_df,
            out_path,
        )

        print(
            f"Wrote summary: {out_path}"
        )

        print_criterion_merge_summary(
            summary_df
        )

    else:
        summary = summarize_csv_file(
            input_path,
            max_rounds=args.max_rounds,
        )

        summary_df = pd.DataFrame(
            [summary]
        )

        if criterion_lookup is not None:
            summary_df = attach_criterion_data(
                summary_df=summary_df,
                criterion_lookup=
                    criterion_lookup,
                criterion_columns=
                    criterion_columns,
            )

        if args.out:
            write_output(
                summary_df,
                Path(args.out),
            )

            print(
                f"Wrote summary: "
                f"{args.out}"
            )

            print_criterion_merge_summary(
                summary_df
            )
        else:
            print(
                summary_df.to_string(
                    index=False
                )
            )


if __name__ == "__main__":
    main()