# calculate_swapvote_latency.py

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


KEY_COLS = ["effectiveRoundNum", "BlockNum", "BlockInstance"]
REQUIRED_BASE_COLS = KEY_COLS + ["lo_eventType", "chestPin_num"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Iterate over a directory of CSV files and calculate swapVote_latency from "
            "'CoinCollect_Moment_PinDrop' (where chestPin_num == 3) to 'SwapVote_Moment', "
            "matched on effectiveRoundNum + BlockNum + BlockInstance."
        )
    )
    parser.add_argument(
        "--inputDir",
        help="Directory containing CSV files.",
    )
    parser.add_argument(
        "--outputDir",
        default=None,
        help="Directory for output CSVs. Default: <inputDir>/processed_swapvote_latency",
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
        "--time-col",
        default="AppTime",
        help="Column containing event time in seconds. Default: AppTime",
    )
    parser.add_argument(
        "--assign-to",
        choices=["all_rows", "swapvote_only"],
        default="all_rows",
        help=(
            "Where to write swapVote_latency. "
            "all_rows: every row in the matched round/block/instance. "
            "swapvote_only: only SwapVote_Moment rows. "
            "Default: all_rows."
        ),
    )
    parser.add_argument(
        "--suffix",
        default="_with_swapVote_latency.csv",
        help=(
            "Full output filename suffix, including the file extension. "
            "Example: _swapvote.csv"
        ),
    )
    parser.add_argument(
        "--replace-suffix",
        default=None,
        help=(
            "Optional ending to remove from the input filename stem before applying --suffix. "
            "Example input stem: session_filled_intervalProps "
            "--replace-suffix _filled_intervalProps "
            "--suffix _swapvote.csv "
            "=> session_swapvote.csv"
        ),
    )
    return parser.parse_args()


def validate_columns(df: pd.DataFrame, time_col: str, file_path: Path) -> None:
    required = REQUIRED_BASE_COLS + [time_col]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{file_path.name}: missing required columns: {missing}")


def build_latency_lookup(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    work = df.copy()

    for col in KEY_COLS + ["chestPin_num", time_col]:
        work[col] = pd.to_numeric(work[col], errors="coerce")

    coincollect = work[
        (work["lo_eventType"] == "CoinCollect_Moment_PinDrop")
        & (work["chestPin_num"] == 3)
    ][KEY_COLS + [time_col]].copy()

    swapvote = work[
        work["lo_eventType"] == "SwapVote_Moment"
    ][KEY_COLS + [time_col]].copy()

    coincollect = (
        coincollect
        .sort_values(KEY_COLS + [time_col])
        .drop_duplicates(subset=KEY_COLS, keep="first")
        .rename(columns={time_col: "pindrop_time"})
    )

    swapvote = (
        swapvote
        .sort_values(KEY_COLS + [time_col])
        .drop_duplicates(subset=KEY_COLS, keep="first")
        .rename(columns={time_col: "swapvote_time"})
    )

    lookup = coincollect.merge(swapvote, on=KEY_COLS, how="inner")
    lookup["swapVote_latency"] = lookup["swapvote_time"] - lookup["pindrop_time"]

    return lookup[KEY_COLS + ["swapVote_latency"]]


def apply_latency(
    df: pd.DataFrame,
    lookup: pd.DataFrame,
    assign_to: str,
) -> pd.DataFrame:
    out = df.copy()

    if lookup.empty:
        out["swapVote_latency"] = pd.NA
        return out

    out = out.merge(lookup, on=KEY_COLS, how="left")

    if assign_to == "swapvote_only":
        out.loc[out["lo_eventType"] != "SwapVote_Moment", "swapVote_latency"] = pd.NA
    elif assign_to != "all_rows":
        raise ValueError(f"Unsupported assign_to mode: {assign_to}")

    return out


def iter_csv_files(inputDir: Path, pattern: str, recursive: bool) -> list[Path]:
    if not inputDir.exists():
        raise FileNotFoundError(f"Input directory not found: {inputDir}")
    if not inputDir.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {inputDir}")

    if recursive:
        files = sorted(p for p in inputDir.rglob(pattern) if p.is_file())
    else:
        files = sorted(p for p in inputDir.glob(pattern) if p.is_file())

    return files


def strip_optional_suffix(name_stem: str, replace_suffix: str | None) -> str:
    if not replace_suffix:
        return name_stem
    print('name_stem', name_stem)
    print('bool: ', name_stem.endswith(replace_suffix) )
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
    print('base_stem', f"{base_stem}")
    return f"{base_stem}{suffix}"


def build_output_path(
    input_file: Path,
    inputDir: Path,
    outputDir: Path,
    suffix: str,
    replace_suffix: str | None,
) -> Path:
    relative_parent = input_file.parent.relative_to(inputDir)
    target_dir = outputDir / relative_parent
    target_dir.mkdir(parents=True, exist_ok=True)

    filename = build_output_filename(
        input_file=input_file,
        suffix=suffix,
        replace_suffix=replace_suffix,
    )
    return target_dir / filename


def process_file(
    file_path: Path,
    inputDir: Path,
    outputDir: Path,
    time_col: str,
    assign_to: str,
    suffix: str,
    replace_suffix: str | None,
) -> tuple[Path, int]:
    df = pd.read_csv(file_path)
    validate_columns(df, time_col, file_path)

    lookup = build_latency_lookup(df, time_col)
    result = apply_latency(df, lookup, assign_to=assign_to)

    out_path = build_output_path(
        input_file=file_path,
        inputDir=inputDir,
        outputDir=outputDir,
        suffix=suffix,
        replace_suffix=replace_suffix,
    )
    result.to_csv(out_path, index=False)

    matched_rounds = len(lookup)
    return out_path, matched_rounds


def main() -> None:
    args = parse_args()

    inputDir = Path(args.inputDir)
    outputDir = (
        Path(args.outputDir)
        if args.outputDir
        else inputDir / "processed_swapvote_latency"
    )
    outputDir.mkdir(parents=True, exist_ok=True)

    files = iter_csv_files(inputDir, args.pattern, args.recursive)

    if not files:
        print(f"No files matched pattern '{args.pattern}' in {inputDir}")
        return

    success_count = 0
    failure_count = 0

    for file_path in files:
        try:
            out_path, matched_rounds = process_file(
                file_path=file_path,
                inputDir=inputDir,
                outputDir=outputDir,
                time_col=args.time_col,
                assign_to=args.assign_to,
                suffix=args.suffix,
                replace_suffix=args.replace_suffix,
            )
            success_count += 1
            print(
                f"[OK] {file_path.name} -> {out_path.name} "
                f"(matched rounds: {matched_rounds})"
            )
        except Exception as exc:
            failure_count += 1
            print(f"[ERROR] {file_path}: {exc}")

    print(
        f"\nDone. Processed: {success_count} succeeded, {failure_count} failed. "
        f"Output directory: {outputDir}"
    )


if __name__ == "__main__":
    main()