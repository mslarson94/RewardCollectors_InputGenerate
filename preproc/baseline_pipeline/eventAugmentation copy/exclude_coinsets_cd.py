# scripts/exclude_coinsets_cd.py
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write six related CSVs from one base output stem: "
            "original all/main/RR and noCD all/main/RR."
        )
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input CSV",
    )
    parser.add_argument(
        "--output",
        required=True,
        help=(
            "Common base output CSV path. Example: "
            "allIntervalData_AN_baseQC_roundDur_optionA_patched.csv "
            "This will produce *_all.csv, *_main.csv, *_RR.csv, "
            "*_noCD_all.csv, *_noCD_main.csv, *_noCD_RR.csv"
        ),
    )
    parser.add_argument(
        "--coinset-col",
        default="coinSet",
        help="Column containing coinSet labels",
    )
    parser.add_argument(
        "--exclude",
        nargs="+",
        default=["C", "D"],
        help="coinSet values to exclude for the noCD outputs",
    )
    parser.add_argument(
        "--cohort-col",
        default="main_RR",
        help="Column containing cohort labels",
    )
    parser.add_argument(
        "--main-label",
        default="main",
        help="Value in cohort column corresponding to the main cohort",
    )
    parser.add_argument(
        "--rr-label",
        default="RR",
        help="Value in cohort column corresponding to the RR cohort",
    )
    return parser.parse_args()


def add_suffix(path: Path, suffix: str) -> Path:
    return path.with_name(f"{path.stem}_{suffix}{path.suffix}")


def summarize_output(df: pd.DataFrame, *, name: str, coinset_col: str, path: Path) -> None:
    print(f"\n{name}")
    print(f"Output file: {path}")
    print(f"Rows written: {len(df)}")

    remaining = (
        df[coinset_col]
        .astype("string")
        .str.strip()
        .dropna()
        .sort_values()
        .unique()
        .tolist()
    )
    print(f"Remaining {coinset_col} values: {remaining}")


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    output_base = Path(args.output)

    df = pd.read_csv(input_path)

    required = [args.coinset_col, args.cohort_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    coinset = df[args.coinset_col].astype("string").str.strip()
    cohort = df[args.cohort_col].astype("string").str.strip()
    exclude_values = {str(x).strip() for x in args.exclude}

    output_base.parent.mkdir(parents=True, exist_ok=True)

    # Original family
    original_all_df = df.copy()
    original_main_df = df.loc[cohort == args.main_label].copy()
    original_rr_df = df.loc[cohort == args.rr_label].copy()

    original_all_out = add_suffix(output_base, "all")
    original_main_out = add_suffix(output_base, "main")
    original_rr_out = add_suffix(output_base, "RR")

    # noCD family
    keep_mask = ~coinset.isin(exclude_values)
    filtered_df = df.loc[keep_mask].copy()
    filtered_cohort = filtered_df[args.cohort_col].astype("string").str.strip()

    no_cd_all_df = filtered_df.copy()
    no_cd_main_df = filtered_df.loc[filtered_cohort == args.main_label].copy()
    no_cd_rr_df = filtered_df.loc[filtered_cohort == args.rr_label].copy()

    no_cd_all_out = add_suffix(output_base, "noCD_all")
    no_cd_main_out = add_suffix(output_base, "noCD_main")
    no_cd_rr_out = add_suffix(output_base, "noCD_RR")

    # Write files
    original_all_df.to_csv(original_all_out, index=False)
    original_main_df.to_csv(original_main_out, index=False)
    original_rr_df.to_csv(original_rr_out, index=False)

    no_cd_all_df.to_csv(no_cd_all_out, index=False)
    no_cd_main_df.to_csv(no_cd_main_out, index=False)
    no_cd_rr_df.to_csv(no_cd_rr_out, index=False)

    print(f"Input file: {input_path}")
    print(f"Base output stem: {output_base}")
    print(f"Cohort column: {args.cohort_col}")
    print(f"Main label: {args.main_label}")
    print(f"RR label: {args.rr_label}")
    print(f"Excluded {args.coinset_col} values for noCD outputs: {sorted(exclude_values)}")
    print(f"Rows before exclusion: {len(df)}")
    print(f"Rows after exclusion: {len(filtered_df)}")
    print(f"Rows removed by exclusion: {len(df) - len(filtered_df)}")

    summarize_output(
        original_all_df,
        name="ORIGINAL INPUT - ALL",
        coinset_col=args.coinset_col,
        path=original_all_out,
    )
    summarize_output(
        original_main_df,
        name="ORIGINAL INPUT - MAIN",
        coinset_col=args.coinset_col,
        path=original_main_out,
    )
    summarize_output(
        original_rr_df,
        name="ORIGINAL INPUT - RR",
        coinset_col=args.coinset_col,
        path=original_rr_out,
    )
    summarize_output(
        no_cd_all_df,
        name="NO C/D - ALL",
        coinset_col=args.coinset_col,
        path=no_cd_all_out,
    )
    summarize_output(
        no_cd_main_df,
        name="NO C/D - MAIN",
        coinset_col=args.coinset_col,
        path=no_cd_main_out,
    )
    summarize_output(
        no_cd_rr_df,
        name="NO C/D - RR",
        coinset_col=args.coinset_col,
        path=no_cd_rr_out,
    )


if __name__ == "__main__":
    main()