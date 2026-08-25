# report_criterion.py

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = {
    "CoinSetID",
    "main_RR",
    "BlockType",
    "BlockNum",
    "BlockInstance",
    "RoundNum",
    "BlockStatus",
}

REPORT_COLUMNS = [
    "BlockInstance",
    "BlockNum",
    "RoundNum",
    "BlockStatus",
    "CoinSetID",
    "coinSet",
    "currentRole",
    "device", 
    "main_RR",
    "pairID",
    "participantID",
    "ptIsAorB",
    "sessionID", 
    "sessionType",
    "source_file",
    "taskNaive",
    "testingDate",
    "testingOrder",   
    "totalRounds",
    "PVSS_TotalScore",
    "PVSS_AvgScore",
    "Age",
    "Gender",
    "SpatialMemRating",

]

STATIC_MASTER_COLUMNS = [
    "coinSet",
    "currentRole",
    "device",
    "main_RR",
    "pairID",
    "participantID",
    "ptIsAorB",
    "sessionID",
    "sessionType",
    "testingDate",
    "PVSS_TotalScore",
    "PVSS_AvgScore",
    "Age",
    "Gender",
    "SpatialMemRating",
]

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate per-file reports of the highest RoundNum for each unique "
            "BlockNum + BlockInstance after exclusion filters, and also write "
            "a combined master summary CSV."
        )
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        type=Path,
        help="Directory containing input CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Directory where per-file output report CSVs will be written.",
    )
    parser.add_argument(
        "--master-out",
        required=True,
        type=Path,
        help="Path to the combined master summary CSV.",
    )
    parser.add_argument(
        "--pattern",
        default="*.csv",
        help="Glob pattern for matching input files. Default: *.csv",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Search input directory recursively.",
    )
    parser.add_argument(
        "--suffix",
        default="_max_roundnum_report.csv",
        help="Suffix appended to each per-file output filename stem.",
    )
    return parser.parse_args()


def find_input_files(input_dir: Path, pattern: str, recursive: bool) -> list[Path]:
    if recursive:
        return sorted(path for path in input_dir.rglob(pattern) if path.is_file())
    return sorted(path for path in input_dir.glob(pattern) if path.is_file())


def validate_columns(df: pd.DataFrame, file_path: Path) -> None:
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"Missing required columns in {file_path.name}: {missing_str}")


def build_report(df: pd.DataFrame) -> pd.DataFrame:
    filtered = df.loc[
        (df["CoinSetID"] <= 3)
        & (df["main_RR"] != "RR")
        & (df["BlockType"] != "collecting")
        & (df["RoundNum"] > 1)
    ].copy()

    if filtered.empty:
        return pd.DataFrame(columns=REPORT_COLUMNS)

    report = (
        filtered.sort_values(
            by=["BlockNum", "BlockInstance", "RoundNum"],
            kind="stable",
        )
        .groupby(["BlockNum", "BlockInstance"], as_index=False, sort=False)
        .tail(1)
        .loc[:, REPORT_COLUMNS]
        .sort_values(by=["BlockNum", "BlockInstance"], kind="stable")
        .reset_index(drop=True)
    )

    return report


def output_path_for(input_file: Path, output_dir: Path, suffix: str) -> Path:
    return output_dir / f"{input_file.stem}{suffix}"


def build_master_row(input_file: Path, input_df: pd.DataFrame, report_df: pd.DataFrame) -> dict[str, object]:
    first_row = input_df.iloc[0] if not input_df.empty else pd.Series(dtype="object")

    rounds2criterion = (
        pd.to_numeric(report_df["RoundNum"], errors="coerce").fillna(0).sum()
        if "RoundNum" in report_df.columns
        else 0
    )

    master_row: dict[str, object] = {
        "source_file": input_file.name,
        "source_stem": input_file.stem,
        "restartsTP1": int(len(report_df)),
        "rounds2criterion": float(rounds2criterion),
    }

    for column in STATIC_MASTER_COLUMNS:
        master_row[column] = first_row.get(column, pd.NA)

    return master_row


def process_file(
    input_file: Path,
    output_dir: Path,
    suffix: str,
) -> tuple[bool, str, dict[str, object] | None]:
    try:
        input_df = pd.read_csv(input_file)
        validate_columns(input_df, input_file)

        report_df = build_report(input_df)
        output_file = output_path_for(input_file, output_dir, suffix)
        report_df.to_csv(output_file, index=False)

        master_row = build_master_row(input_file, input_df, report_df)
        return True, f"Wrote {output_file}", master_row

    except Exception as exc:
        return False, f"Failed {input_file}: {exc}", None


def write_master_summary(master_rows: list[dict[str, object]], master_out: Path) -> None:
    master_out.parent.mkdir(parents=True, exist_ok=True)
    master_df = pd.DataFrame(master_rows)

    expected_columns = [
        "source_file",
        "source_stem",
        "restartsTP1",
        "rounds2criterion",
        *STATIC_MASTER_COLUMNS,
    ]

    if master_df.empty:
        master_df = pd.DataFrame(columns=expected_columns)
    else:
        for column in expected_columns:
            if column not in master_df.columns:
                master_df[column] = pd.NA
        master_df = master_df.loc[:, expected_columns]

    master_df = master_df.sort_values(by=["source_file"], kind="stable").reset_index(drop=True)
    master_df.to_csv(master_out, index=False)


def main() -> int:
    args = parse_args()

    if not args.input_dir.exists() or not args.input_dir.is_dir():
        print(f"Input directory does not exist or is not a directory: {args.input_dir}", file=sys.stderr)
        return 2

    args.output_dir.mkdir(parents=True, exist_ok=True)

    input_files = find_input_files(args.input_dir, args.pattern, args.recursive)
    if not input_files:
        print(
            f"No files matched pattern '{args.pattern}' in {args.input_dir}",
            file=sys.stderr,
        )
        return 1

    processed = 0
    failed = 0
    master_rows: list[dict[str, object]] = []

    for input_file in input_files:
        ok, message, master_row = process_file(input_file, args.output_dir, args.suffix)
        print(message)

        if ok:
            processed += 1
            if master_row is not None:
                master_rows.append(master_row)
        else:
            failed += 1

    write_master_summary(master_rows, args.master_out)
    print(f"Wrote master summary: {args.master_out}")
    print(f"Done. Processed: {processed}, Failed: {failed}, Total matched: {len(input_files)}")

    if processed == 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())