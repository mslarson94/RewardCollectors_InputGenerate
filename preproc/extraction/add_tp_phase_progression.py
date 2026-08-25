#!/usr/bin/env python3
"""
add_tp_phase_progression.py

Create mutually exclusive collecting, TP1, and TP2 round classifications and
phase-specific progression variables.

Each input row represents one pin drop. Classification and progression are
computed once per unique roundID and merged back onto every pin-drop row.

Classification:
    isCollecting:
        BlockType == "collecting"

    isTP1:
        BlockType == "pindropping" and totalRounds > 1

    isTP2:
        BlockType == "pindropping" and totalRounds == 1

    isTutorial:
        CoinSetID >= 4

    isExcludedTruncatedRound:
        isLastRoundOfSourceFile is true
        and BlockStatus == "truncated"

Progression variables:
    TotSesh_TP1_roundNum_all:
        All TP1 rounds, including tutorial and excluded truncated rounds.

    TotSesh_TP1_roundNum:
        TP1 rounds excluding tutorial and excluded truncated rounds.

    TotSesh_TP2_roundNum_all:
        All TP2 rounds, including tutorial and excluded truncated rounds.

    TotSesh_TP2_roundNum:
        TP2 rounds excluding tutorial and excluded truncated rounds.

Collecting rounds remain blank in all four phase progression variables.

totalRounds may be missing for collecting rounds, but must be a positive integer
for pindropping rounds.

Usage:
    python add_tp_phase_progression.py \
        --input allIntervalData.csv \
        --output allIntervalData_withTPProgression.csv

Optional QC output:
    python add_tp_phase_progression.py \
        --input allIntervalData.csv \
        --output allIntervalData_withTPProgression.csv \
        --qc-output tp_phase_round_qc.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd


ROUND_ID_COLUMN = "roundID"
SESSION_ID_COLUMN = "sessionID"
UNIVERSAL_ROUND_COLUMN = "TotSesh_runTot_RoundNum_all"

IS_COLLECTING_COLUMN = "isCollecting"
IS_TP1_COLUMN = "isTP1"
IS_TP2_COLUMN = "isTP2"
IS_TUTORIAL_COLUMN = "isTutorial"
IS_EXCLUDED_TRUNCATED_COLUMN = "isExcludedTruncatedRound"

TP1_ALL_COLUMN = "TotSesh_TP1_roundNum_all"
TP1_READY_COLUMN = "TotSesh_TP1_roundNum"
TP2_ALL_COLUMN = "TotSesh_TP2_roundNum_all"
TP2_READY_COLUMN = "TotSesh_TP2_roundNum"

MAX_ROWS_PER_ROUND = 3

REQUIRED_COLUMNS = [
    ROUND_ID_COLUMN,
    SESSION_ID_COLUMN,
    UNIVERSAL_ROUND_COLUMN,
    "totalRounds",
    "BlockType",
    "CoinSetID",
    "BlockStatus",
    "isLastRoundOfSourceFile",
]

OUTPUT_COLUMNS = [
    IS_COLLECTING_COLUMN,
    IS_TP1_COLUMN,
    IS_TP2_COLUMN,
    IS_TUTORIAL_COLUMN,
    IS_EXCLUDED_TRUNCATED_COLUMN,
    TP1_ALL_COLUMN,
    TP1_READY_COLUMN,
    TP2_ALL_COLUMN,
    TP2_READY_COLUMN,
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create collecting, TP1, and TP2 classifications and phase-specific "
            "round progression variables."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Input CSV containing corrected roundID values",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output CSV with TP phase progression variables",
    )
    parser.add_argument(
        "--qc-output",
        type=Path,
        default=None,
        help="Optional session-level QC summary CSV",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="CSV encoding",
    )
    return parser.parse_args()


def require_columns(
    dataframe: pd.DataFrame,
    required_columns: Iterable[str],
) -> None:
    missing = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def normalize_required_text(
    series: pd.Series,
    column_name: str,
) -> pd.Series:
    normalized = series.astype("string").str.strip()
    invalid = normalized.isna() | normalized.eq("")

    if invalid.any():
        indexes = normalized.index[invalid].tolist()[:20]
        raise ValueError(
            f"{column_name} is missing or blank at row indexes: {indexes}"
        )

    return normalized


def normalize_integer_column(
    series: pd.Series,
    column_name: str,
    *,
    minimum: int | None = None,
) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")

    missing = numeric.isna()
    if missing.any():
        sample = series.loc[missing].head(20)
        raise ValueError(
            f"{column_name} contains missing or invalid values:\n"
            f"{sample.to_string()}"
        )

    non_integer = numeric.mod(1).ne(0)
    if non_integer.any():
        sample = numeric.loc[non_integer].head(20)
        raise ValueError(
            f"{column_name} contains non-integer values:\n"
            f"{sample.to_string()}"
        )

    if minimum is not None:
        below_minimum = numeric.lt(minimum)
        if below_minimum.any():
            sample = numeric.loc[below_minimum].head(20)
            raise ValueError(
                f"{column_name} contains values below {minimum}:\n"
                f"{sample.to_string()}"
            )

    return numeric.astype("Int64")


def normalize_total_rounds(
    series: pd.Series,
    block_type: pd.Series,
) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    is_pindropping = block_type.eq("pindropping")

    missing_for_pindropping = is_pindropping & numeric.isna()
    if missing_for_pindropping.any():
        sample = pd.DataFrame(
            {
                "BlockType": block_type.loc[missing_for_pindropping],
                "totalRounds": series.loc[missing_for_pindropping],
            }
        ).head(20)

        raise ValueError(
            "totalRounds is missing or invalid for pindropping rows:\n"
            f"{sample.to_string()}"
        )

    non_integer = (
        is_pindropping
        & numeric.notna()
        & numeric.mod(1).ne(0)
    )
    if non_integer.any():
        sample = numeric.loc[non_integer].head(20)
        raise ValueError(
            "totalRounds contains non-integer values for pindropping rows:\n"
            f"{sample.to_string()}"
        )

    non_positive = (
        is_pindropping
        & numeric.notna()
        & numeric.lt(1)
    )
    if non_positive.any():
        sample = numeric.loc[non_positive].head(20)
        raise ValueError(
            "totalRounds must be at least 1 for pindropping rows:\n"
            f"{sample.to_string()}"
        )

    return numeric.astype("Int64")


def parse_boolish(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False).astype(bool)

    if pd.api.types.is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors="coerce")
        return numeric.fillna(0).ne(0)

    normalized = series.astype("string").str.strip().str.lower()

    true_values = {"1", "true", "t", "yes", "y"}
    false_values = {"0", "false", "f", "no", "n", ""}

    unknown = (
        normalized.notna()
        & ~normalized.isin(true_values)
        & ~normalized.isin(false_values)
    )

    if unknown.any():
        sample = normalized.loc[unknown].drop_duplicates().head(20).tolist()
        raise ValueError(
            "isLastRoundOfSourceFile contains unrecognized values: "
            f"{sample}"
        )

    return normalized.isin(true_values)


def validate_round_sizes(dataframe: pd.DataFrame) -> None:
    round_sizes = (
        dataframe.groupby(
            ROUND_ID_COLUMN,
            dropna=False,
            sort=False,
        )
        .size()
    )

    oversized = round_sizes[round_sizes > MAX_ROWS_PER_ROUND]

    if not oversized.empty:
        raise ValueError(
            f"Some {ROUND_ID_COLUMN} values contain more than "
            f"{MAX_ROWS_PER_ROUND} rows:\n"
            f"{oversized.head(20).to_string()}"
        )


def validate_constant_within_round(
    dataframe: pd.DataFrame,
    columns: Iterable[str],
) -> None:
    problems: list[str] = []

    for column in columns:
        unique_counts = (
            dataframe.groupby(
                ROUND_ID_COLUMN,
                dropna=False,
                sort=False,
            )[column]
            .nunique(dropna=False)
        )

        inconsistent = unique_counts[unique_counts != 1]

        if not inconsistent.empty:
            problems.append(
                f"{column}:\n{inconsistent.head(20).to_string()}"
            )

    if problems:
        raise ValueError(
            "Values are not constant within roundID:\n\n"
            + "\n\n".join(problems)
        )


def validate_universal_round_progression(
    dataframe: pd.DataFrame,
) -> None:
    round_table = dataframe[
        [
            ROUND_ID_COLUMN,
            SESSION_ID_COLUMN,
            UNIVERSAL_ROUND_COLUMN,
        ]
    ].drop_duplicates()

    duplicate_session_rounds = (
        round_table.groupby(
            [SESSION_ID_COLUMN, UNIVERSAL_ROUND_COLUMN],
            dropna=False,
            sort=False,
        )[ROUND_ID_COLUMN]
        .nunique(dropna=False)
    )

    invalid_mapping = duplicate_session_rounds[
        duplicate_session_rounds != 1
    ]

    if not invalid_mapping.empty:
        raise ValueError(
            "Some sessionID and universal round-number combinations map to "
            "multiple roundID values:\n"
            f"{invalid_mapping.head(20).to_string()}"
        )

    progression_errors: list[str] = []

    for session_id, session_rounds in round_table.groupby(
        SESSION_ID_COLUMN,
        dropna=False,
        sort=False,
    ):
        observed = sorted(
            session_rounds[UNIVERSAL_ROUND_COLUMN]
            .astype(int)
            .unique()
            .tolist()
        )

        expected = list(range(1, len(observed) + 1))

        if observed != expected:
            progression_errors.append(
                f"sessionID={session_id!r}: "
                f"observed={observed[:20]}, expected={expected[:20]}"
            )

    if progression_errors:
        raise ValueError(
            f"{UNIVERSAL_ROUND_COLUMN} is not consecutive from 1 within "
            "one or more sessions:\n"
            + "\n".join(progression_errors[:20])
        )


def create_round_table(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    round_columns = [
        ROUND_ID_COLUMN,
        SESSION_ID_COLUMN,
        UNIVERSAL_ROUND_COLUMN,
        "totalRounds",
        "BlockType",
        "CoinSetID",
        "BlockStatus",
        "isLastRoundOfSourceFile",
    ]

    if "roundID_int" in dataframe.columns:
        round_columns.insert(1, "roundID_int")

    return (
        dataframe.sort_values(
            [
                SESSION_ID_COLUMN,
                UNIVERSAL_ROUND_COLUMN,
                ROUND_ID_COLUMN,
            ],
            kind="mergesort",
        )
        .drop_duplicates(
            subset=[ROUND_ID_COLUMN],
            keep="first",
        )[round_columns]
        .copy()
        .reset_index(drop=True)
    )


def add_round_classifications(
    round_table: pd.DataFrame,
) -> pd.DataFrame:
    result = round_table.copy()

    block_type = (
        result["BlockType"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    valid_block_types = {"collecting", "pindropping"}
    invalid_block_type = (
        block_type.isna()
        | ~block_type.isin(valid_block_types)
    )

    if invalid_block_type.any():
        details = result.loc[
            invalid_block_type,
            [ROUND_ID_COLUMN, "BlockType"],
        ].head(20)

        raise ValueError(
            "BlockType must be either 'collecting' or 'pindropping':\n"
            f"{details.to_string(index=False)}"
        )

    total_rounds = normalize_total_rounds(
        result["totalRounds"],
        block_type,
    )

    coin_set_id = normalize_integer_column(
        result["CoinSetID"],
        "CoinSetID",
        minimum=0,
    )

    block_status = (
        result["BlockStatus"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    invalid_status = block_status.isna() | block_status.eq("")
    if invalid_status.any():
        details = result.loc[
            invalid_status,
            [ROUND_ID_COLUMN, "BlockStatus"],
        ].head(20)

        raise ValueError(
            "BlockStatus is missing or blank:\n"
            f"{details.to_string(index=False)}"
        )

    is_last_round = parse_boolish(
        result["isLastRoundOfSourceFile"]
    )

    is_collecting = block_type.eq("collecting")
    is_pindropping = block_type.eq("pindropping")

    is_tp1 = (
        is_pindropping
        & total_rounds.gt(1).fillna(False)
    )

    is_tp2 = (
        is_pindropping
        & total_rounds.eq(1).fillna(False)
    )

    result["totalRounds"] = total_rounds
    result["CoinSetID"] = coin_set_id

    result[IS_COLLECTING_COLUMN] = (
        is_collecting.astype("int8")
    )
    result[IS_TP1_COLUMN] = is_tp1.astype("int8")
    result[IS_TP2_COLUMN] = is_tp2.astype("int8")

    result[IS_TUTORIAL_COLUMN] = (
        coin_set_id.ge(4).astype("int8")
    )

    result[IS_EXCLUDED_TRUNCATED_COLUMN] = (
        is_last_round
        & block_status.eq("truncated")
    ).astype("int8")

    classification_total = result[
        [
            IS_COLLECTING_COLUMN,
            IS_TP1_COLUMN,
            IS_TP2_COLUMN,
        ]
    ].sum(axis=1)

    invalid_classification = classification_total.ne(1)

    if invalid_classification.any():
        details = result.loc[
            invalid_classification,
            [
                ROUND_ID_COLUMN,
                "BlockType",
                "totalRounds",
                IS_COLLECTING_COLUMN,
                IS_TP1_COLUMN,
                IS_TP2_COLUMN,
            ],
        ].head(20)

        raise ValueError(
            "Every round must be exactly one of collecting, TP1, or TP2:\n"
            f"{details.to_string(index=False)}"
        )

    return result


def assign_phase_progression(
    round_table: pd.DataFrame,
    *,
    eligibility_mask: pd.Series,
    output_column: str,
) -> pd.DataFrame:
    result = round_table.copy()

    result[output_column] = pd.Series(
        pd.NA,
        index=result.index,
        dtype="Int64",
    )

    eligible = result.loc[eligibility_mask].copy()

    eligible = eligible.sort_values(
        [
            SESSION_ID_COLUMN,
            UNIVERSAL_ROUND_COLUMN,
            ROUND_ID_COLUMN,
        ],
        kind="mergesort",
    )

    eligible[output_column] = (
        eligible.groupby(
            SESSION_ID_COLUMN,
            dropna=False,
            sort=False,
        )
        .cumcount()
        .add(1)
        .astype("Int64")
    )

    result.loc[
        eligible.index,
        output_column,
    ] = eligible[output_column]

    return result


def add_phase_progression(
    round_table: pd.DataFrame,
) -> pd.DataFrame:
    result = round_table.copy()

    tp1_all_eligible = result[IS_TP1_COLUMN].eq(1)

    tp1_ready_eligible = (
        result[IS_TP1_COLUMN].eq(1)
        & result[IS_TUTORIAL_COLUMN].eq(0)
        & result[IS_EXCLUDED_TRUNCATED_COLUMN].eq(0)
    )

    tp2_all_eligible = result[IS_TP2_COLUMN].eq(1)

    tp2_ready_eligible = (
        result[IS_TP2_COLUMN].eq(1)
        & result[IS_TUTORIAL_COLUMN].eq(0)
        & result[IS_EXCLUDED_TRUNCATED_COLUMN].eq(0)
    )

    result = assign_phase_progression(
        result,
        eligibility_mask=tp1_all_eligible,
        output_column=TP1_ALL_COLUMN,
    )

    result = assign_phase_progression(
        result,
        eligibility_mask=tp1_ready_eligible,
        output_column=TP1_READY_COLUMN,
    )

    result = assign_phase_progression(
        result,
        eligibility_mask=tp2_all_eligible,
        output_column=TP2_ALL_COLUMN,
    )

    result = assign_phase_progression(
        result,
        eligibility_mask=tp2_ready_eligible,
        output_column=TP2_READY_COLUMN,
    )

    return result


def validate_progression_column(
    round_table: pd.DataFrame,
    *,
    output_column: str,
    eligibility_mask: pd.Series,
) -> None:
    eligible = round_table.loc[
        eligibility_mask,
        [
            ROUND_ID_COLUMN,
            SESSION_ID_COLUMN,
            UNIVERSAL_ROUND_COLUMN,
            output_column,
        ],
    ].copy()

    ineligible = round_table.loc[
        ~eligibility_mask,
        [
            ROUND_ID_COLUMN,
            SESSION_ID_COLUMN,
            output_column,
        ],
    ].copy()

    missing_eligible = eligible[output_column].isna()

    if missing_eligible.any():
        details = eligible.loc[missing_eligible].head(20)

        raise ValueError(
            f"{output_column} is blank for eligible rounds:\n"
            f"{details.to_string(index=False)}"
        )

    populated_ineligible = ineligible[output_column].notna()

    if populated_ineligible.any():
        details = ineligible.loc[
            populated_ineligible
        ].head(20)

        raise ValueError(
            f"{output_column} is populated for ineligible rounds:\n"
            f"{details.to_string(index=False)}"
        )

    for session_id, session_rounds in eligible.groupby(
        SESSION_ID_COLUMN,
        dropna=False,
        sort=False,
    ):
        ordered = session_rounds.sort_values(
            UNIVERSAL_ROUND_COLUMN,
            kind="mergesort",
        )

        observed = ordered[output_column].astype(int).tolist()
        expected = list(range(1, len(ordered) + 1))

        if observed != expected:
            raise ValueError(
                f"{output_column} is not consecutive within "
                f"sessionID={session_id!r}. "
                f"Observed={observed[:20]}, expected={expected[:20]}"
            )


def validate_phase_progressions(
    round_table: pd.DataFrame,
) -> None:
    tp1_all_eligible = round_table[IS_TP1_COLUMN].eq(1)

    tp1_ready_eligible = (
        round_table[IS_TP1_COLUMN].eq(1)
        & round_table[IS_TUTORIAL_COLUMN].eq(0)
        & round_table[IS_EXCLUDED_TRUNCATED_COLUMN].eq(0)
    )

    tp2_all_eligible = round_table[IS_TP2_COLUMN].eq(1)

    tp2_ready_eligible = (
        round_table[IS_TP2_COLUMN].eq(1)
        & round_table[IS_TUTORIAL_COLUMN].eq(0)
        & round_table[IS_EXCLUDED_TRUNCATED_COLUMN].eq(0)
    )

    validations = [
        (TP1_ALL_COLUMN, tp1_all_eligible),
        (TP1_READY_COLUMN, tp1_ready_eligible),
        (TP2_ALL_COLUMN, tp2_all_eligible),
        (TP2_READY_COLUMN, tp2_ready_eligible),
    ]

    for output_column, eligibility_mask in validations:
        validate_progression_column(
            round_table,
            output_column=output_column,
            eligibility_mask=eligibility_mask,
        )


def merge_round_values_to_rows(
    dataframe: pd.DataFrame,
    round_table: pd.DataFrame,
) -> pd.DataFrame:
    result = dataframe.drop(
        columns=OUTPUT_COLUMNS,
        errors="ignore",
    ).copy()

    result = result.merge(
        round_table[
            [ROUND_ID_COLUMN] + OUTPUT_COLUMNS
        ],
        on=ROUND_ID_COLUMN,
        how="left",
        validate="m:1",
        sort=False,
    )

    missing_merge = result[IS_COLLECTING_COLUMN].isna()

    if missing_merge.any():
        details = result.loc[
            missing_merge,
            [ROUND_ID_COLUMN, SESSION_ID_COLUMN],
        ].head(20)

        raise ValueError(
            "Some rows did not receive round-level phase values:\n"
            f"{details.to_string(index=False)}"
        )

    flag_columns = [
        IS_COLLECTING_COLUMN,
        IS_TP1_COLUMN,
        IS_TP2_COLUMN,
        IS_TUTORIAL_COLUMN,
        IS_EXCLUDED_TRUNCATED_COLUMN,
    ]

    for column in flag_columns:
        result[column] = result[column].astype("int8")

    progression_columns = [
        TP1_ALL_COLUMN,
        TP1_READY_COLUMN,
        TP2_ALL_COLUMN,
        TP2_READY_COLUMN,
    ]

    for column in progression_columns:
        result[column] = result[column].astype("Int64")

    return result


def validate_merged_round_values(
    dataframe: pd.DataFrame,
) -> None:
    validate_constant_within_round(
        dataframe,
        OUTPUT_COLUMNS,
    )

    classification_total = dataframe[
        [
            IS_COLLECTING_COLUMN,
            IS_TP1_COLUMN,
            IS_TP2_COLUMN,
        ]
    ].sum(axis=1)

    invalid = classification_total.ne(1)

    if invalid.any():
        details = dataframe.loc[
            invalid,
            [
                ROUND_ID_COLUMN,
                IS_COLLECTING_COLUMN,
                IS_TP1_COLUMN,
                IS_TP2_COLUMN,
            ],
        ].head(20)

        raise ValueError(
            "Merged rows violate collecting/TP1/TP2 exclusivity:\n"
            f"{details.to_string(index=False)}"
        )


def build_round_qc_summary(
    round_table: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for session_id, session_rounds in round_table.groupby(
        SESSION_ID_COLUMN,
        dropna=False,
        sort=False,
    ):
        n_total = int(len(session_rounds))
        n_collecting = int(
            session_rounds[IS_COLLECTING_COLUMN].sum()
        )
        n_tp1 = int(session_rounds[IS_TP1_COLUMN].sum())
        n_tp2 = int(session_rounds[IS_TP2_COLUMN].sum())

        rows.append(
            {
                SESSION_ID_COLUMN: session_id,
                "n_rounds_total": n_total,
                "n_collecting_rounds": n_collecting,
                "n_tp1_rounds": n_tp1,
                "n_tp2_rounds": n_tp2,
                "classification_total": (
                    n_collecting + n_tp1 + n_tp2
                ),
                "n_tutorial_rounds": int(
                    session_rounds[IS_TUTORIAL_COLUMN].sum()
                ),
                "n_excluded_truncated_rounds": int(
                    session_rounds[
                        IS_EXCLUDED_TRUNCATED_COLUMN
                    ].sum()
                ),
                "n_tp1_all_numbered": int(
                    session_rounds[TP1_ALL_COLUMN]
                    .notna()
                    .sum()
                ),
                "n_tp1_ready_numbered": int(
                    session_rounds[TP1_READY_COLUMN]
                    .notna()
                    .sum()
                ),
                "n_tp2_all_numbered": int(
                    session_rounds[TP2_ALL_COLUMN]
                    .notna()
                    .sum()
                ),
                "n_tp2_ready_numbered": int(
                    session_rounds[TP2_READY_COLUMN]
                    .notna()
                    .sum()
                ),
                "max_universal_round_number": int(
                    session_rounds[
                        UNIVERSAL_ROUND_COLUMN
                    ].max()
                ),
            }
        )

    return pd.DataFrame(rows)


def process_dataframe(
    dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    require_columns(dataframe, REQUIRED_COLUMNS)

    working = dataframe.copy()

    working[ROUND_ID_COLUMN] = normalize_required_text(
        working[ROUND_ID_COLUMN],
        ROUND_ID_COLUMN,
    )

    working[SESSION_ID_COLUMN] = normalize_required_text(
        working[SESSION_ID_COLUMN],
        SESSION_ID_COLUMN,
    )

    working[UNIVERSAL_ROUND_COLUMN] = normalize_integer_column(
        working[UNIVERSAL_ROUND_COLUMN],
        UNIVERSAL_ROUND_COLUMN,
        minimum=1,
    )

    working["_BlockType_normalized"] = (
        working["BlockType"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    working["_BlockStatus_normalized"] = (
        working["BlockStatus"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    working["_totalRounds_normalized"] = normalize_total_rounds(
        working["totalRounds"],
        working["_BlockType_normalized"],
    )

    working["_CoinSetID_normalized"] = normalize_integer_column(
        working["CoinSetID"],
        "CoinSetID",
        minimum=0,
    )

    working["_isLastRound_normalized"] = parse_boolish(
        working["isLastRoundOfSourceFile"]
    )

    validate_round_sizes(working)

    validate_constant_within_round(
        working,
        [
            SESSION_ID_COLUMN,
            UNIVERSAL_ROUND_COLUMN,
            "_BlockType_normalized",
            "_BlockStatus_normalized",
            "_totalRounds_normalized",
            "_CoinSetID_normalized",
            "_isLastRound_normalized",
        ],
    )

    validate_universal_round_progression(working)

    round_table = create_round_table(working)
    round_table = add_round_classifications(round_table)
    round_table = add_phase_progression(round_table)

    validate_phase_progressions(round_table)

    working = working.drop(
        columns=[
            "_BlockType_normalized",
            "_BlockStatus_normalized",
            "_totalRounds_normalized",
            "_CoinSetID_normalized",
            "_isLastRound_normalized",
        ],
        errors="ignore",
    )

    output = merge_round_values_to_rows(
        working,
        round_table,
    )

    validate_merged_round_values(output)

    return output, round_table


def main() -> int:
    args = parse_args()

    input_path = args.input.expanduser().resolve()
    output_path = args.output.expanduser().resolve()

    dataframe = pd.read_csv(
        input_path,
        encoding=args.encoding,
    )

    output, round_table = process_dataframe(dataframe)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output.to_csv(
        output_path,
        index=False,
        encoding=args.encoding,
    )

    if args.qc_output is not None:
        qc_output_path = args.qc_output.expanduser().resolve()

        qc_output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        qc_summary = build_round_qc_summary(round_table)

        qc_summary.to_csv(
            qc_output_path,
            index=False,
            encoding=args.encoding,
        )

        print(f"Wrote QC summary: {qc_output_path}")

    print(f"Wrote: {output_path}")
    print(f"Rows: {len(output)}")
    print(
        "Unique rounds: "
        f"{round_table[ROUND_ID_COLUMN].nunique()}"
    )
    print(
        "Collecting rounds: "
        f"{int(round_table[IS_COLLECTING_COLUMN].sum())}"
    )
    print(
        "TP1 rounds: "
        f"{int(round_table[IS_TP1_COLUMN].sum())}"
    )
    print(
        "TP2 rounds: "
        f"{int(round_table[IS_TP2_COLUMN].sum())}"
    )
    print(
        "Tutorial rounds: "
        f"{int(round_table[IS_TUTORIAL_COLUMN].sum())}"
    )
    print(
        "Excluded truncated rounds: "
        f"{int(round_table[IS_EXCLUDED_TRUNCATED_COLUMN].sum())}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())