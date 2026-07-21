#!/usr/bin/env python3
"""
concat_csvs.py

Concatenate CSV files and create a universal roundID for every round type.

roundID is constructed from:
    participantID + sessionID + TotSesh_runTot_RoundNum_all

TotSesh_runTot_RoundNum_all includes normal, tutorial, collecting,
incomplete, and truncated-block round attempts.

Usage:
    python concat_csvs.py --indir /path/to/csvs --out merged.csv

    python concat_csvs.py \
        --indir . \
        --out merged.csv \
        --pattern "*__withDemo.csv" \
        --add-source-file

    python concat_csvs.py \
        --indir . \
        --out merged.csv \
        --require-same-header
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandas as pd


ROUND_NUMBER_COLUMN = "TotSesh_runTot_RoundNum_all"
ROUND_ID_COLUMN = "roundID"
ROUND_ID_INT_COLUMN = "roundID_int"
MAX_ROWS_PER_ROUND = 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Concatenate CSV files and create universal round IDs."
    )
    parser.add_argument(
        "--indir",
        type=Path,
        required=True,
        help="Directory containing CSV files",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output CSV path",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default="*.csv",
        help="Glob pattern",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Search recursively",
    )
    parser.add_argument(
        "--encoding",
        type=str,
        default="utf-8",
        help="CSV encoding",
    )
    parser.add_argument(
        "--require-same-header",
        action="store_true",
        help="Fail when input headers differ in columns or order",
    )
    parser.add_argument(
        "--add-source-file",
        action="store_true",
        help="Add the input filename to every row",
    )
    parser.add_argument(
        "--source-col",
        type=str,
        default="intervalSourceFile",
        help="Column used by --add-source-file",
    )
    return parser.parse_args()


def list_files(
    input_directory: Path,
    pattern: str,
    recursive: bool,
    output_path: Path,
) -> list[Path]:
    globber = input_directory.rglob if recursive else input_directory.glob
    resolved_output = output_path.resolve()

    return sorted(
        path
        for path in globber(pattern)
        if path.is_file() and path.resolve() != resolved_output
    )


def read_header(path: Path, encoding: str) -> list[str]:
    return list(pd.read_csv(path, nrows=0, encoding=encoding).columns)


def normalize_identifier(value: object) -> str:
    if value is None or pd.isna(value):
        return ""

    if isinstance(value, float) and math.isfinite(value) and value.is_integer():
        return str(int(value))

    return str(value).strip()


def require_columns(
    dataframe: pd.DataFrame,
    required_columns: list[str],
) -> None:
    missing = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def validate_headers(
    files: list[Path],
    expected_header: list[str],
    encoding: str,
) -> None:
    mismatches: list[tuple[str, list[str]]] = []

    for path in files[1:]:
        header = read_header(path, encoding)
        if header != expected_header:
            mismatches.append((path.name, header))

    if not mismatches:
        return

    lines = [
        "Header mismatch detected.",
        f"First file: {files[0].name}",
        f"Expected header: {expected_header}",
        "",
        "Mismatching files:",
    ]

    for filename, header in mismatches[:20]:
        lines.append(f"- {filename}: {header}")

    if len(mismatches) > 20:
        lines.append(f"... and {len(mismatches) - 20} more")

    raise ValueError("\n".join(lines))


def read_and_concatenate(
    files: list[Path],
    encoding: str,
    add_source_file: bool,
    source_column: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for path in files:
        dataframe = pd.read_csv(path, encoding=encoding)

        if add_source_file:
            dataframe[source_column] = path.name

        frames.append(dataframe)

    return pd.concat(frames, ignore_index=True, sort=False)


def normalize_round_numbers(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")

    missing = numeric.isna()
    if missing.any():
        sample = series.loc[missing].head(20)

        raise ValueError(
            f"{ROUND_NUMBER_COLUMN} contains missing or invalid values:\n"
            f"{sample.to_string()}"
        )

    non_integer = numeric.mod(1).ne(0)
    if non_integer.any():
        sample = numeric.loc[non_integer].head(20)

        raise ValueError(
            f"{ROUND_NUMBER_COLUMN} contains non-integer values:\n"
            f"{sample.to_string()}"
        )

    non_positive = numeric.lt(1)
    if non_positive.any():
        sample = numeric.loc[non_positive].head(20)

        raise ValueError(
            f"{ROUND_NUMBER_COLUMN} contains values below 1:\n"
            f"{sample.to_string()}"
        )

    return numeric.astype("Int64")


def normalize_required_identifier(
    series: pd.Series,
    column_name: str,
) -> pd.Series:
    normalized = series.map(normalize_identifier).astype("string")
    invalid = normalized.isna() | normalized.eq("")

    if invalid.any():
        indexes = normalized.index[invalid].tolist()[:20]

        raise ValueError(
            f"{column_name} is missing or blank at row indexes: {indexes}"
        )

    return normalized


def add_round_ids(dataframe: pd.DataFrame) -> pd.DataFrame:
    require_columns(
        dataframe,
        [
            "participantID",
            "sessionID",
            ROUND_NUMBER_COLUMN,
        ],
    )

    result = dataframe.drop(
        columns=[ROUND_ID_COLUMN, ROUND_ID_INT_COLUMN],
        errors="ignore",
    ).copy()

    participant_id = normalize_required_identifier(
        result["participantID"],
        "participantID",
    )
    session_id = normalize_required_identifier(
        result["sessionID"],
        "sessionID",
    )
    round_number = normalize_round_numbers(
        result[ROUND_NUMBER_COLUMN]
    )

    result[ROUND_NUMBER_COLUMN] = round_number

    result[ROUND_ID_COLUMN] = (
        participant_id
        + "_"
        + session_id
        + "_"
        + round_number.astype("string")
    )

    validate_round_ids(result)

    round_mapping = {
        round_id: integer_id
        for integer_id, round_id in enumerate(
            result[ROUND_ID_COLUMN].drop_duplicates()
        )
    }

    result[ROUND_ID_INT_COLUMN] = (
        result[ROUND_ID_COLUMN]
        .map(round_mapping)
        .astype("int64")
    )

    return result


def validate_round_ids(dataframe: pd.DataFrame) -> None:
    round_sizes = dataframe.groupby(
        ROUND_ID_COLUMN,
        dropna=False,
        sort=False,
    ).size()

    oversized = round_sizes[round_sizes > MAX_ROWS_PER_ROUND]

    if not oversized.empty:
        raise ValueError(
            f"Some {ROUND_ID_COLUMN} values contain more than "
            f"{MAX_ROWS_PER_ROUND} rows:\n"
            f"{oversized.head(20).to_string()}"
        )

    consistency_columns = [
        "participantID",
        "sessionID",
        ROUND_NUMBER_COLUMN,
    ]

    inconsistencies: list[str] = []

    for column in consistency_columns:
        unique_counts = (
            dataframe.groupby(
                ROUND_ID_COLUMN,
                dropna=False,
                sort=False,
            )[column]
            .nunique(dropna=False)
        )

        invalid = unique_counts[unique_counts != 1]

        if not invalid.empty:
            inconsistencies.append(
                f"{column}:\n{invalid.head(20).to_string()}"
            )

    if inconsistencies:
        raise ValueError(
            "Some roundID values map to conflicting source values:\n\n"
            + "\n\n".join(inconsistencies)
        )


def main() -> int:
    args = parse_args()

    input_directory = args.indir.expanduser().resolve()
    output_path = args.out.expanduser().resolve()

    files = list_files(
        input_directory=input_directory,
        pattern=args.pattern,
        recursive=args.recursive,
        output_path=output_path,
    )

    if not files:
        raise SystemExit(
            f"No CSV files matched {args.pattern!r} in {input_directory}"
        )

    first_header = read_header(files[0], args.encoding)

    if args.require_same_header:
        validate_headers(
            files=files,
            expected_header=first_header,
            encoding=args.encoding,
        )

    concatenated = read_and_concatenate(
        files=files,
        encoding=args.encoding,
        add_source_file=args.add_source_file,
        source_column=args.source_col,
    )

    output = add_round_ids(concatenated)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(
        output_path,
        index=False,
        encoding=args.encoding,
    )

    print(
        f"Wrote {output_path} from {len(files)} file(s). "
        f"Rows={len(output)}, "
        f"unique rounds={output[ROUND_ID_COLUMN].nunique()}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())