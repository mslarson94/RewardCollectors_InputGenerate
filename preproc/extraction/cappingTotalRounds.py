```python
#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Keep all rows associated with the first N unique round IDs "
            "within each session."
        )
    )

    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to the input CSV file.",
    )

    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help=(
            "Path for the filtered output CSV. "
            "Default: <input_stem>_first_<N>_rounds.csv"
        ),
    )

    parser.add_argument(
        "-n",
        "--max-rounds",
        type=int,
        default=50,
        help="Maximum number of unique round IDs to retain per session. Default: 50.",
    )

    parser.add_argument(
        "--session-column",
        default="sessionID",
        help="Name of the session ID column. Default: sessionID.",
    )

    parser.add_argument(
        "--round-column",
        default="roundID",
        help="Name of the round ID column. Default: roundID.",
    )

    parser.add_argument(
        "--summary",
        type=Path,
        help="Optional path for a per-session round-count summary CSV.",
    )

    return parser.parse_args()


def validate_arguments(args: argparse.Namespace) -> None:
    if not args.input_file.is_file():
        raise FileNotFoundError(
            f"Input file does not exist: {args.input_file}"
        )

    if args.max_rounds < 1:
        raise ValueError("--max-rounds must be greater than or equal to 1.")


def build_default_output_path(
    input_file: Path,
    max_rounds: int,
) -> Path:
    return input_file.with_name(
        f"{input_file.stem}_first_{max_rounds}_rounds.csv"
    )


def filter_first_unique_rounds(
    df: pd.DataFrame,
    session_column: str,
    round_column: str,
    max_rounds: int,
) -> pd.DataFrame:
    required_columns = {session_column, round_column}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required column(s): {sorted(missing_columns)}"
        )

    round_order = (
        df.groupby(session_column, dropna=False)[round_column]
        .transform(
            lambda values: pd.factorize(
                values,
                sort=False,
                use_na_sentinel=False,
            )[0]
        )
    )

    return df.loc[round_order < max_rounds].copy()


def create_summary(
    original_df: pd.DataFrame,
    filtered_df: pd.DataFrame,
    session_column: str,
    round_column: str,
) -> pd.DataFrame:
    original_counts = (
        original_df
        .groupby(session_column, dropna=False)[round_column]
        .nunique(dropna=False)
        .rename("total_unique_roundIDs")
        .reset_index()
    )

    retained_counts = (
        filtered_df
        .groupby(session_column, dropna=False)[round_column]
        .nunique(dropna=False)
        .rename("retained_unique_roundIDs")
        .reset_index()
    )

    summary = original_counts.merge(
        retained_counts,
        on=session_column,
        how="left",
    )

    summary["retained_unique_roundIDs"] = (
        summary["retained_unique_roundIDs"]
        .fillna(0)
        .astype(int)
    )

    summary["roundIDs_removed"] = (
        summary["total_unique_roundIDs"]
        - summary["retained_unique_roundIDs"]
    )

    return summary


def main() -> None:
    args = parse_arguments()
    validate_arguments(args)

    output_file = (
        args.output
        if args.output is not None
        else build_default_output_path(
            args.input_file,
            args.max_rounds,
        )
    )

    df = pd.read_csv(
        args.input_file,
        low_memory=False,
    )

    filtered_df = filter_first_unique_rounds(
        df=df,
        session_column=args.session_column,
        round_column=args.round_column,
        max_rounds=args.max_rounds,
    )

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    filtered_df.to_csv(
        output_file,
        index=False,
    )

    print(f"Input file:      {args.input_file}")
    print(f"Output file:     {output_file}")
    print(f"Original rows:   {len(df):,}")
    print(f"Retained rows:   {len(filtered_df):,}")
    print(f"Rows removed:    {len(df) - len(filtered_df):,}")
    print(f"Maximum rounds:  {args.max_rounds:,}")

    if args.summary is not None:
        summary = create_summary(
            original_df=df,
            filtered_df=filtered_df,
            session_column=args.session_column,
            round_column=args.round_column,
        )

        args.summary.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        summary.to_csv(
            args.summary,
            index=False,
        )

        print(f"Summary file:    {args.summary}")


if __name__ == "__main__":
    main()
```
