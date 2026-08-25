#!/usr/bin/env python3
# add_session_running_totals.py

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


ROUND_ATTEMPT_KEY = [
    "RoundNum",
    "BlockNum",
    "BlockInstance",
    "source_file",
    "BlockStatus",
]

BLOCK_ATTEMPT_KEY = [
    "BlockNum",
    "BlockInstance",
    "source_file",
    "BlockStatus",
]


def _ensure_required_cols(
    df: pd.DataFrame,
    input_path: Path,
) -> None:
    """Validate columns required to construct session counters."""
    required = {
        "RoundNum",
        "BlockNum",
        "BlockInstance",
        "source_file",
        "BlockStatus",
        "BlockType",
        "CoinSetID",
    }

    missing = sorted(required - set(df.columns))

    if missing:
        raise ValueError(
            f"{input_path.name}: missing required columns: {missing}"
        )


def _first_rows_by_key(
    df: pd.DataFrame,
    key_cols: list[str],
) -> pd.DataFrame:
    """Return one chronologically ordered row per logical attempt."""
    return (
        df.sort_values(
            "TotSesh_rowIndex",
            kind="mergesort",
        )
        .drop_duplicates(
            subset=key_cols,
            keep="first",
        )
        .copy()
    )


def _assign_running_total_by_starts(
    df: pd.DataFrame,
    starts: pd.DataFrame,
    key_cols: list[str],
    out_col: str,
) -> pd.DataFrame:
    """Assign sequential numbers to every attempt in chronological order."""
    numbered = starts.copy()

    numbered[out_col] = np.arange(
        1,
        len(numbered) + 1,
        dtype=int,
    )

    return df.merge(
        numbered[key_cols + [out_col]],
        on=key_cols,
        how="left",
        validate="m:1",
    )


def _assign_eligible_round_numbers(
    df: pd.DataFrame,
    starts: pd.DataFrame,
    key_cols: list[str],
    out_col: str,
    exclude_mask_in_starts: pd.Series,
) -> pd.DataFrame:
    """Number eligible attempts and leave excluded attempts unnumbered."""
    numbered = starts.copy()

    exclude_mask = exclude_mask_in_starts.to_numpy(
        dtype=bool
    )

    numbered[out_col] = pd.Series(
        pd.NA,
        index=numbered.index,
        dtype="Int64",
    )

    included_indices = numbered.index[
        ~exclude_mask
    ]

    numbered.loc[
        included_indices,
        out_col,
    ] = np.arange(
        1,
        len(included_indices) + 1,
        dtype=int,
    )

    return df.merge(
        numbered[key_cols + [out_col]],
        on=key_cols,
        how="left",
        validate="m:1",
    )


def _build_round_size_flags(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Add round row-count and incomplete-round indicators."""
    round_sizes = (
        df.groupby(
            ROUND_ATTEMPT_KEY,
            dropna=False,
            sort=False,
        )
        .size()
        .reset_index(
            name="round_rowCount"
        )
    )

    round_sizes["isIncompleteRound"] = (
        round_sizes["round_rowCount"] < 3
    )

    return df.merge(
        round_sizes,
        on=ROUND_ATTEMPT_KEY,
        how="left",
        validate="m:1",
    )


def _add_last_source_file_round_flag(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Mark the final logical round within each source file."""
    round_last_rows = (
        df.groupby(
            ROUND_ATTEMPT_KEY,
            dropna=False,
            sort=False,
        )["TotSesh_rowIndex"]
        .max()
        .reset_index(
            name="_roundLastRowIndex"
        )
    )

    last_round_per_source = (
        round_last_rows.sort_values(
            [
                "source_file",
                "_roundLastRowIndex",
            ],
            kind="mergesort",
        )
        .groupby(
            "source_file",
            dropna=False,
            sort=False,
        )
        .tail(1)
        .copy()
    )

    last_round_per_source[
        "isLastRoundOfSourceFile"
    ] = True

    last_round_per_source = (
        last_round_per_source[
            ROUND_ATTEMPT_KEY
            + ["isLastRoundOfSourceFile"]
        ]
    )

    df = df.merge(
        last_round_per_source,
        on=ROUND_ATTEMPT_KEY,
        how="left",
        validate="m:1",
    )

    df["isLastRoundOfSourceFile"] = (
        df["isLastRoundOfSourceFile"]
        .fillna(False)
        .astype(bool)
    )

    return df


def _identify_final_rounds_of_truncated_blocks(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Return the final logical round from each truncated block."""
    truncated_rows = df.loc[
        (
            df["BlockStatus"]
            .astype(str)
            .str.strip()
            .str.lower()
            == "truncated"
        )
    ].copy()

    if truncated_rows.empty:
        excluded = pd.DataFrame(
            columns=ROUND_ATTEMPT_KEY
        )

        excluded[
            "isExcludedFinalRoundOfTruncatedBlock"
        ] = pd.Series(dtype=bool)

        return excluded

    round_last_rows = (
        truncated_rows.groupby(
            BLOCK_ATTEMPT_KEY + ["RoundNum"],
            dropna=False,
            sort=False,
        )["TotSesh_rowIndex"]
        .max()
        .reset_index()
        .rename(
            columns={
                "TotSesh_rowIndex":
                    "round_last_rowIndex_in_block"
            }
        )
    )

    final_rounds = (
        round_last_rows.sort_values(
            BLOCK_ATTEMPT_KEY
            + ["round_last_rowIndex_in_block"],
            kind="mergesort",
        )
        .groupby(
            BLOCK_ATTEMPT_KEY,
            dropna=False,
            sort=False,
        )
        .tail(1)
        .reset_index(drop=True)
    )

    excluded = (
        final_rounds[
            ROUND_ATTEMPT_KEY
        ]
        .drop_duplicates()
        .copy()
    )

    excluded[
        "isExcludedFinalRoundOfTruncatedBlock"
    ] = True

    return excluded


def _add_round_totals(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Add all-attempt and valid-attempt session round counters."""
    round_starts = _first_rows_by_key(
        df,
        ROUND_ATTEMPT_KEY,
    )

    df = _assign_running_total_by_starts(
        df=df,
        starts=round_starts,
        key_cols=ROUND_ATTEMPT_KEY,
        out_col="TotSesh_runTot_RoundNum_all",
    )

    excluded_round_attempts = (
        _identify_final_rounds_of_truncated_blocks(
            df
        )
    )

    round_starts_main = round_starts.merge(
        excluded_round_attempts,
        on=ROUND_ATTEMPT_KEY,
        how="left",
        validate="1:1",
    )

    exclude_mask = (
        round_starts_main[
            "isExcludedFinalRoundOfTruncatedBlock"
        ]
        .fillna(False)
        .astype(bool)
    )

    df = _assign_eligible_round_numbers(
        df=df.drop(
            columns=["TotSesh_runTot_RoundNum"],
            errors="ignore",
        ),
        starts=round_starts_main.drop(
            columns=[
                "isExcludedFinalRoundOfTruncatedBlock"
            ],
            errors="ignore",
        ),
        key_cols=ROUND_ATTEMPT_KEY,
        out_col="TotSesh_runTot_RoundNum",
        exclude_mask_in_starts=exclude_mask,
    )

    df = df.merge(
        excluded_round_attempts,
        on=ROUND_ATTEMPT_KEY,
        how="left",
        validate="m:1",
    )

    df[
        "isExcludedFinalRoundOfTruncatedBlock"
    ] = (
        df[
            "isExcludedFinalRoundOfTruncatedBlock"
        ]
        .fillna(False)
        .astype(bool)
    )

    return df


def _add_block_totals(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Add all-attempt and valid block sequence counters."""
    block_starts = _first_rows_by_key(
        df,
        BLOCK_ATTEMPT_KEY,
    )

    df = _assign_running_total_by_starts(
        df=df,
        starts=block_starts,
        key_cols=BLOCK_ATTEMPT_KEY,
        out_col="TotSesh_runTot_BlockNum_all",
    )

    valid_block_starts = (
        block_starts.loc[
            ~(
                block_starts[
                    "BlockStatus"
                ]
                .astype(str)
                .str.strip()
                .str.lower()
                == "truncated"
            )
        ]
        .copy()
    )

    valid_block_starts[
        "TotSesh_runTot_BlockNum"
    ] = np.arange(
        1,
        len(valid_block_starts) + 1,
        dtype=int,
    )

    df = df.merge(
        valid_block_starts[
            BLOCK_ATTEMPT_KEY
            + ["TotSesh_runTot_BlockNum"]
        ],
        on=BLOCK_ATTEMPT_KEY,
        how="left",
        validate="m:1",
    )

    return df


def _make_actual_test_mapping(
    round_starts: pd.DataFrame,
    base_column: str,
    output_column: str,
) -> pd.DataFrame:
    """Build one sequential actual-test number per eligible round."""
    starts = round_starts.copy()

    starts["_BlockType_norm"] = (
        starts["BlockType"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    starts["_CoinSetID_num"] = pd.to_numeric(
        starts["CoinSetID"],
        errors="coerce",
    )

    base_values = pd.to_numeric(
        starts[base_column],
        errors="coerce",
    )

    keep = (
        base_values.notna()
        & (
            starts["_BlockType_norm"]
            != "collecting"
        )
        & (
            starts["_CoinSetID_num"] < 4
        )
    )

    kept = starts.loc[
        keep,
        ROUND_ATTEMPT_KEY + [base_column],
    ].copy()

    kept[base_column] = pd.to_numeric(
        kept[base_column],
        errors="coerce",
    )

    kept = (
        kept.sort_values(
            base_column,
            kind="mergesort",
        )
        .reset_index(drop=True)
    )

    kept[output_column] = np.arange(
        1,
        len(kept) + 1,
        dtype=int,
    )

    return kept[
        ROUND_ATTEMPT_KEY
        + [output_column]
    ]


def add_act_test_roundnums(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Add main and all-attempt actual-test round numbers."""
    round_starts = _first_rows_by_key(
        df,
        ROUND_ATTEMPT_KEY,
    )

    required_round_columns = [
        "BlockType",
        "CoinSetID",
        "TotSesh_runTot_RoundNum",
        "TotSesh_runTot_RoundNum_all",
    ]

    round_metadata = (
        df.sort_values(
            "TotSesh_rowIndex",
            kind="mergesort",
        )
        .drop_duplicates(
            subset=ROUND_ATTEMPT_KEY,
            keep="first",
        )[
            ROUND_ATTEMPT_KEY
            + required_round_columns
        ]
        .copy()
    )

    round_starts = round_starts[
        ROUND_ATTEMPT_KEY
    ].merge(
        round_metadata,
        on=ROUND_ATTEMPT_KEY,
        how="left",
        validate="1:1",
    )

    main_mapping = _make_actual_test_mapping(
        round_starts=round_starts,
        base_column="TotSesh_runTot_RoundNum",
        output_column="TotSesh_actTest_RoundNum",
    )

    all_mapping = _make_actual_test_mapping(
        round_starts=round_starts,
        base_column="TotSesh_runTot_RoundNum_all",
        output_column="TotSesh_actTest_RoundNum_all",
    )

    df = df.drop(
        columns=[
            "TotSesh_actTest_RoundNum",
            "TotSesh_actTest_RoundNum_all",
        ],
        errors="ignore",
    )

    df = df.merge(
        main_mapping,
        on=ROUND_ATTEMPT_KEY,
        how="left",
        validate="m:1",
    )

    df = df.merge(
        all_mapping,
        on=ROUND_ATTEMPT_KEY,
        how="left",
        validate="m:1",
    )

    return df


def process_file(
    input_path: Path,
    output_path: Path,
) -> dict:
    """Generate corrected session counters for one CSV."""
    df = pd.read_csv(input_path)

    _ensure_required_cols(
        df,
        input_path,
    )

    generated_columns = [
        "TotSesh_rowIndex",
        "round_rowCount",
        "isIncompleteRound",
        "isLastRoundOfSourceFile",
        "isExcludedFinalRoundOfTruncatedBlock",
        "TotSesh_runTot_RoundNum_all",
        "TotSesh_runTot_RoundNum",
        "TotSesh_runTot_BlockNum_all",
        "TotSesh_runTot_BlockNum",
        "TotSesh_actTest_RoundNum",
        "TotSesh_actTest_RoundNum_all",
    ]

    df = df.drop(
        columns=generated_columns,
        errors="ignore",
    )

    df["TotSesh_rowIndex"] = np.arange(
        1,
        len(df) + 1,
        dtype=int,
    )

    df = _build_round_size_flags(df)
    df = _add_last_source_file_round_flag(df)
    df = _add_round_totals(df)
    df = _add_block_totals(df)
    df = add_act_test_roundnums(df)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_csv(
        output_path,
        index=False,
    )

    round_level = _first_rows_by_key(
        df,
        ROUND_ATTEMPT_KEY,
    )

    return {
        "inputFile": input_path.name,
        "outputFile": output_path.name,
        "rows": int(len(df)),
        "roundAttemptsAll": int(
            round_level[
                "TotSesh_runTot_RoundNum_all"
            ].notna().sum()
        ),
        "roundAttemptsMain": int(
            round_level[
                "TotSesh_runTot_RoundNum"
            ].notna().sum()
        ),
        "excludedFinalTruncatedRounds": int(
            round_level[
                "isExcludedFinalRoundOfTruncatedBlock"
            ].sum()
        ),
        "actualTestRoundsAll": int(
            round_level[
                "TotSesh_actTest_RoundNum_all"
            ].notna().sum()
        ),
        "actualTestRoundsMain": int(
            round_level[
                "TotSesh_actTest_RoundNum"
            ].notna().sum()
        ),
    }


def process_folder(
    input_folder: Path,
    output_folder: Path,
    pattern: str,
) -> pd.DataFrame:
    """Process all matching CSV files in a folder."""
    files = sorted(
        input_folder.glob(pattern)
    )

    if not files:
        raise ValueError(
            f"No files matched {pattern!r} "
            f"in {input_folder}"
        )

    records = []
    errors = []

    for input_path in files:
        output_path = (
            output_folder
            / input_path.name
        )

        try:
            records.append(
                process_file(
                    input_path,
                    output_path,
                )
            )
        except Exception as exc:
            errors.append(
                {
                    "inputFile": input_path.name,
                    "error": str(exc),
                }
            )

    if errors:
        output_folder.mkdir(
            parents=True,
            exist_ok=True,
        )

        error_path = (
            output_folder
            / "counter_generation_errors.csv"
        )

        pd.DataFrame(errors).to_csv(
            error_path,
            index=False,
        )

        print(
            f"Wrote error log: {error_path}"
        )

    if not records:
        error_text = "\n".join(
            f"{item['inputFile']}: "
            f"{item['error']}"
            for item in errors
        )

        raise RuntimeError(
            "No files were processed successfully."
            f"\n{error_text}"
        )

    return pd.DataFrame(records)


def write_manifest(
    manifest: pd.DataFrame,
    output_path: Path,
) -> None:
    """Write processing manifest."""
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    manifest.to_csv(
        output_path,
        index=False,
    )


def main() -> None:
    """Run corrected session-counter generation."""
    parser = argparse.ArgumentParser(
        description=(
            "Generate corrected total-session and "
            "actual-test round counters."
        )
    )

    parser.add_argument(
        "input_path",
        nargs="?",
        type=Path,
        help=(
            "Input CSV file or folder containing CSV files. "
            "May be used instead of --input_dir."
        ),
    )

    parser.add_argument(
        "--input_dir",
        type=Path,
        default=None,
        help="Input directory containing CSV files.",
    )

    parser.add_argument(
        "--output_dir",
        type=Path,
        default=None,
        help="Output directory for processed CSV files.",
    )

    parser.add_argument(
        "--manifest_dir",
        type=Path,
        default=None,
        help="Directory where the processing manifest is written.",
    )

    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help=(
            "Backward-compatible output file for single-file mode "
            "or output folder for directory mode."
        ),
    )

    parser.add_argument(
        "--pattern",
        default="*.csv",
        help="CSV glob pattern in directory mode (default: *.csv).",
    )

    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Optional explicit manifest CSV path.",
    )

    args = parser.parse_args()

    if args.input_dir is not None and args.input_path is not None:
        parser.error(
            "Use either positional input_path or --input_dir, not both."
        )

    input_path = args.input_dir or args.input_path

    if input_path is None:
        parser.error(
            "An input path is required. Use input_path or --input_dir."
        )

    if args.output_dir is not None and args.out is not None:
        parser.error(
            "Use either --output_dir or --out, not both."
        )

    if args.manifest_dir is not None and args.manifest is not None:
        parser.error(
            "Use either --manifest_dir or --manifest, not both."
        )

    if input_path.is_dir():
        output_folder = (
            args.output_dir
            or args.out
            or input_path / "with_corrected_counters"
        )

        manifest = process_folder(
            input_folder=input_path,
            output_folder=output_folder,
            pattern=args.pattern,
        )

        if args.manifest is not None:
            manifest_path = args.manifest
        elif args.manifest_dir is not None:
            manifest_path = (
                args.manifest_dir
                / "add_session_running_totals_manifest.csv"
            )
        else:
            manifest_path = (
                output_folder
                / "counter_generation_manifest.csv"
            )

        write_manifest(
            manifest,
            manifest_path,
        )

        print(
            f"Wrote processed files to: {output_folder}"
        )
        print(
            f"Wrote manifest: {manifest_path}"
        )

    else:
        if args.input_dir is not None:
            parser.error(
                "--input_dir must point to a directory."
            )

        if args.output_dir is not None:
            output_path = (
                args.output_dir
                / f"{input_path.stem}_withCorrectedCounters.csv"
            )
        else:
            output_path = (
                args.out
                or input_path.with_name(
                    f"{input_path.stem}"
                    "_withCorrectedCounters.csv"
                )
            )

        record = process_file(
            input_path=input_path,
            output_path=output_path,
        )

        print(
            f"Wrote: {output_path}"
        )

        for key, value in record.items():
            print(
                f"{key}: {value}"
            )


if __name__ == "__main__":
    main()