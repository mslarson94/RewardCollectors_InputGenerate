# add_pin_drop_vote_score.py

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd


KEY_COLUMNS = ["RoundNum", "BlockNum", "BlockInstance", "chestPin_num"]
REQUIRED_COLUMNS = KEY_COLUMNS + ["lo_eventType", "dropQual", "pinDropVote"]


def normalize_text(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.lower()


def build_composite_key(df: pd.DataFrame, key_columns: list[str]) -> pd.Series:
    return df[key_columns].astype("string").agg("|".join, axis=1)


def validate_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def add_pin_drop_vote_score(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int | str]]:
    validate_columns(df, REQUIRED_COLUMNS)

    result = df.copy()

    result["_event_key"] = build_composite_key(result, KEY_COLUMNS)
    result["_dropQual_norm"] = normalize_text(result["dropQual"])
    result["_pinDropVote_norm"] = normalize_text(result["pinDropVote"])

    is_drop = result["lo_eventType"].eq("PinDrop_Moment")
    is_vote = result["lo_eventType"].eq("PinDropVote_Moment")

    drop_lookup = (
        result.loc[is_drop, ["_event_key", "_dropQual_norm"]]
        .dropna(subset=["_dropQual_norm"])
        .drop_duplicates(subset=["_event_key"], keep="first")
        .set_index("_event_key")["_dropQual_norm"]
    )

    result["_matched_dropQual_norm"] = pd.NA
    result.loc[is_vote, "_matched_dropQual_norm"] = result.loc[is_vote, "_event_key"].map(drop_lookup)

    result["pinDropVoteScore"] = pd.NA

    has_matched_key = is_vote & result["_matched_dropQual_norm"].notna()
    comparable = has_matched_key & result["_pinDropVote_norm"].notna()

    result.loc[comparable, "pinDropVoteScore"] = (
        result.loc[comparable, "_matched_dropQual_norm"]
        .eq(result.loc[comparable, "_pinDropVote_norm"])
        .astype("Int64")
    )

    summary = {
        "total_rows": int(len(result)),
        "drop_rows": int(is_drop.sum()),
        "vote_rows": int(is_vote.sum()),
        "vote_rows_with_matched_key": int(has_matched_key.sum()),
        "vote_rows_without_matched_key": int((is_vote & result["_matched_dropQual_norm"].isna()).sum()),
        "comparable_vote_rows": int(comparable.sum()),
        "vote_rows_missing_pinDropVote": int((is_vote & result["_pinDropVote_norm"].isna()).sum()),
        "score_1_count": int((result["pinDropVoteScore"] == 1).sum()),
        "score_0_count": int((result["pinDropVoteScore"] == 0).sum()),
        "score_blank_count": int((is_vote & result["pinDropVoteScore"].isna()).sum()),
    }

    result = result.drop(
        columns=[
            "_event_key",
            "_dropQual_norm",
            "_pinDropVote_norm",
            "_matched_dropQual_norm",
        ]
    )

    return result, summary


def iter_input_files(input_path: Path, pattern: str = "*.csv") -> Iterable[Path]:
    if input_path.is_file():
        if input_path.suffix.lower() != ".csv":
            raise ValueError(f"Input file must be a CSV: {input_path}")
        yield input_path
        return

    if input_path.is_dir():
        files = sorted(p for p in input_path.glob(pattern) if p.is_file())

        if not files:
            raise ValueError(
                f"No files found in directory: {input_path} matching pattern: {pattern}"
            )

        yield from files
        return

    raise ValueError(f"Input path does not exist: {input_path}")


def build_output_path(input_file: Path, output_dir: Path | None) -> Path:
    target_dir = output_dir if output_dir is not None else input_file.parent
    target_dir.mkdir(parents=True, exist_ok=True)

    suffix = "_eventsWalks.csv"
    name = input_file.name
    if not name.endswith(suffix):
        raise ValueError(f"Expected *{suffix}, got: {name}")

    base = name[: -len(suffix)]
    return target_dir / f"{base}_with_pinDropVoteScore.csv"


def print_summary(file_path: Path, summary: dict[str, int | str]) -> None:
    print(f"\nProcessed: {file_path.name}")
    print(f"  total_rows: {summary['total_rows']}")
    print(f"  drop_rows: {summary['drop_rows']}")
    print(f"  vote_rows: {summary['vote_rows']}")
    print(f"  vote_rows_with_matched_key: {summary['vote_rows_with_matched_key']}")
    print(f"  vote_rows_without_matched_key: {summary['vote_rows_without_matched_key']}")
    print(f"  comparable_vote_rows: {summary['comparable_vote_rows']}")
    print(f"  vote_rows_missing_pinDropVote: {summary['vote_rows_missing_pinDropVote']}")
    print(f"  score_1_count: {summary['score_1_count']}")
    print(f"  score_0_count: {summary['score_0_count']}")
    print(f"  score_blank_count: {summary['score_blank_count']}")


def process_file(input_file: Path, output_dir: Path | None) -> dict[str, int | str]:
    df = pd.read_csv(input_file)
    result_df, summary = add_pin_drop_vote_score(df)

    output_file = build_output_path(input_file, output_dir)
    result_df.to_csv(output_file, index=False)

    summary_with_file = {"file_name": input_file.name, "output_file": str(output_file), **summary}
    print_summary(input_file, summary)
    print(f"  saved_to: {output_file}")

    return summary_with_file


def save_combined_summary(summaries: list[dict[str, int | str]], summary_path: Path) -> None:
    summary_df = pd.DataFrame(summaries)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(summary_path, index=False)
    print(f"\nCombined QA summary saved to: {summary_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Add pinDropVoteScore to one CSV or matching CSVs in a folder."
    )
    parser.add_argument(
        "input_path",
        type=Path,
        help="Path to a CSV file or a directory containing CSV files.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for processed CSV files. Defaults to each input file's directory.",
    )
    parser.add_argument(
        "--summary-csv",
        type=Path,
        default=None,
        help="Path to save the combined QA summary CSV. Defaults to output-dir/qa_summary.csv or input directory/qa_summary.csv.",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default="*.csv",
        help="Glob pattern for directory input. Example: 'ObsReward_B_*_filled.csv'",
    )
    args = parser.parse_args()

    input_files = list(iter_input_files(args.input_path, pattern=args.pattern))

    if args.input_path.is_dir():
        print("\nDirectory file selection")
        print(f"  pattern: {args.pattern}")
        print(f"  files_selected: {len(input_files)}")
        for file_path in input_files:
            print(f"    - {file_path.name}")

    summaries: list[dict[str, int | str]] = []

    for input_file in input_files:
        try:
            summaries.append(process_file(input_file, args.output_dir))
        except Exception as exc:
            error_summary = {
                "file_name": input_file.name,
                "output_file": "",
                "total_rows": "",
                "drop_rows": "",
                "vote_rows": "",
                "vote_rows_with_matched_key": "",
                "vote_rows_without_matched_key": "",
                "comparable_vote_rows": "",
                "vote_rows_missing_pinDropVote": "",
                "score_1_count": "",
                "score_0_count": "",
                "score_blank_count": "",
                "error": str(exc),
            }
            summaries.append(error_summary)
            print(f"\nFailed: {input_file.name}")
            print(f"  error: {exc}")

    if args.summary_csv is not None:
        summary_path = args.summary_csv
    elif args.output_dir is not None:
        summary_path = args.output_dir / "qa_summary.csv"
    elif args.input_path.is_dir():
        summary_path = args.input_path / "qa_summary.csv"
    else:
        summary_path = args.input_path.parent / "qa_summary.csv"

    save_combined_summary(summaries, summary_path)


if __name__ == "__main__":
    main()