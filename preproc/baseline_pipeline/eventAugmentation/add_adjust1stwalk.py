# add_adjust1stwalk.py

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = {"chestPin_num", "lo_eventType", "totEventDist"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Add adjust1stWalk to matching CSV files. "
            "For rows where chestPin_num == 1 and lo_eventType == 'Walk_PinDrop', "
            "assign True when totEventDist <= 1 and False when totEventDist > 1."
        )
    )
    parser.add_argument(
        "--inputDir",
        required=True,
        type=Path,
        help="Directory containing CSV files.",
    )
    parser.add_argument(
        "--outputDir",
        type=Path,
        default=None,
        help="Directory for output CSVs. Default: <inputDir>/processed_adjust1stwalk",
    )
    parser.add_argument(
        "--pattern",
        default="*.csv",
        help="Glob pattern for input files. Default: *.csv",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively search subdirectories.",
    )
    parser.add_argument(
        "--suffix",
        default="_adjusted.csv",
        help="Full output filename suffix, including the file extension.",
    )
    parser.add_argument(
        "--replace-suffix",
        default=None,
        help=(
            "Optional ending to remove from the input filename stem before applying --suffix."
        ),
    )
    return parser.parse_args()


def validate_columns(df: pd.DataFrame, file_path: Path) -> None:
    missing = sorted(REQUIRED_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(f"{file_path.name}: missing required columns: {missing}")


def add_adjust1stwalk_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["adjust1stWalk"] = ""

    target_mask = (
        out["chestPin_num"].eq(1)
        & out["lo_eventType"].eq("Walk_PinDrop")
    )

    out.loc[target_mask & out["totEventDist"].le(1), "adjust1stWalk"] = "True"
    out.loc[target_mask & out["totEventDist"].gt(1), "adjust1stWalk"] = "False"

    return out


def iter_csv_files(input_dir: Path, pattern: str, recursive: bool) -> list[Path]:
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    if not input_dir.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {input_dir}")

    if recursive:
        files = sorted(path for path in input_dir.rglob(pattern) if path.is_file())
    else:
        files = sorted(path for path in input_dir.glob(pattern) if path.is_file())

    return files


def strip_optional_suffix(name_stem: str, replace_suffix: str | None) -> str:
    if not replace_suffix:
        return name_stem
    if name_stem.endswith(replace_suffix):
        return name_stem[: -len(replace_suffix)]
    return name_stem


def build_output_filename(
    input_file: Path,
    suffix: str,
    replace_suffix: str | None,
) -> str:
    if not suffix.lower().endswith(".csv"):
        raise ValueError("--suffix must include the .csv ending")

    base_stem = strip_optional_suffix(input_file.stem, replace_suffix)
    return f"{base_stem}{suffix}"


def build_output_path(
    input_file: Path,
    input_dir: Path,
    output_dir: Path,
    suffix: str,
    replace_suffix: str | None,
) -> Path:
    relative_parent = input_file.parent.relative_to(input_dir)
    target_dir = output_dir / relative_parent
    target_dir.mkdir(parents=True, exist_ok=True)

    filename = build_output_filename(
        input_file=input_file,
        suffix=suffix,
        replace_suffix=replace_suffix,
    )
    return target_dir / filename


def process_file(
    file_path: Path,
    input_dir: Path,
    output_dir: Path,
    suffix: str,
    replace_suffix: str | None,
) -> tuple[Path, int, int]:
    df = pd.read_csv(file_path)
    validate_columns(df, file_path)

    result = add_adjust1stwalk_column(df)
    out_path = build_output_path(
        input_file=file_path,
        input_dir=input_dir,
        output_dir=output_dir,
        suffix=suffix,
        replace_suffix=replace_suffix,
    )
    result.to_csv(out_path, index=False)

    true_count = int((result["adjust1stWalk"] == "True").sum())
    false_count = int((result["adjust1stWalk"] == "False").sum())
    return out_path, true_count, false_count


def main() -> None:
    args = parse_args()

    input_dir = args.inputDir
    output_dir = args.outputDir if args.outputDir else input_dir / "processed_adjust1stwalk"
    output_dir.mkdir(parents=True, exist_ok=True)

    files = iter_csv_files(input_dir, args.pattern, args.recursive)

    if not files:
        print(f"No files matched pattern '{args.pattern}' in {input_dir}")
        return

    success_count = 0
    failure_count = 0

    for file_path in files:
        try:
            out_path, true_count, false_count = process_file(
                file_path=file_path,
                input_dir=input_dir,
                output_dir=output_dir,
                suffix=args.suffix,
                replace_suffix=args.replace_suffix,
            )
            success_count += 1
            print(
                f"[OK] {file_path.name} -> {out_path.name} "
                f"(True: {true_count}, False: {false_count})"
            )
        except Exception as exc:
            failure_count += 1
            print(f"[ERROR] {file_path}: {exc}")

    print(
        f"\nDone. Processed: {success_count} succeeded, {failure_count} failed. "
        f"Output directory: {output_dir}"
    )


if __name__ == "__main__":
    main()


