#!/usr/bin/env python3
# summarize_pindrops.py
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


def safe_ratio(numerator: float, denominator: float):
    if denominator == 0:
        return np.nan
    return numerator / denominator


def get_single_unique_value(series: pd.Series):
    values = series.dropna()
    if values.empty:
        return None

    unique_values = pd.unique(values)
    if len(unique_values) == 1:
        value = unique_values[0]
        return value.item() if hasattr(value, "item") else value

    result = []
    for value in unique_values:
        result.append(value.item() if hasattr(value, "item") else value)
    return result


def get_single_unique_numeric_value(series: pd.Series):
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None

    unique_values = pd.unique(values)
    if len(unique_values) == 1:
        value = unique_values[0]
        return value.item() if hasattr(value, "item") else value

    raise ValueError(
        f"Expected one unique numeric value per group, found multiple: {list(unique_values)}"
    )


def build_round_id(df: pd.DataFrame) -> pd.Series:
    if "TotSesh_actTest_RoundNum" in df.columns and df["TotSesh_actTest_RoundNum"].notna().all():
        numeric_rounds = pd.to_numeric(df["TotSesh_actTest_RoundNum"], errors="coerce")
        if numeric_rounds.notna().all():
            return numeric_rounds.astype(int).astype(str)
        return df["TotSesh_actTest_RoundNum"].astype(str)

    if "source_file" in df.columns and "RoundNum" in df.columns:
        round_num = pd.to_numeric(df["RoundNum"], errors="coerce").astype("Int64").astype(str)
        return df["source_file"].astype(str) + "::" + round_num

    raise ValueError(
        "Could not construct a unique round identifier. "
        "Expected either 'TotSesh_actTest_RoundNum' or both 'source_file' and 'RoundNum'."
    )


def summarize_dataframe(df: pd.DataFrame, file_label: str) -> dict:
    required_columns = [
        "CoinSetID",
        "BlockStatus",
        "BlockType",
        "dropDist",
        "isSwap",
        "swapType",
        "SwapVoteScore",
        "swapVoteRegistered_n",
        "swapVoteRegistered_d",
        "swapVoteRegistered",
        "source_file",
        "roundGrandTotal",
        "avgRoundSpeed",
        "path_eff_raw",
        "path_order_round",
    ]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    working = df.copy()

    working["CoinSetID_num"] = pd.to_numeric(working["CoinSetID"], errors="coerce")
    working["dropDist_num"] = pd.to_numeric(working["dropDist"], errors="coerce")
    working["isSwap_num"] = pd.to_numeric(working["isSwap"], errors="coerce").fillna(0).astype(int)
    working["avgRoundSpeed_num"] = pd.to_numeric(working["avgRoundSpeed"], errors="coerce")
    working["path_eff_raw_num"] = pd.to_numeric(working["path_eff_raw"], errors="coerce")
    working["roundGrandTotal_num"] = pd.to_numeric(working["roundGrandTotal"], errors="coerce")

    filtered = working[
        (working["CoinSetID_num"] < 4)
        & (working["BlockStatus"].astype(str).str.lower() == "complete")
        & (working["BlockType"].astype(str).str.lower() == "pindropping")
    ].copy()

    if filtered.empty:
        raise ValueError("No rows matched the requested filter.")

    filtered["round_id"] = build_round_id(filtered)
    filtered = filtered.reset_index().rename(columns={"index": "_orig_index"})

    tot_pin_drops = int(len(filtered))
    tot_correct = int((filtered["dropDist_num"] <= 1.1).sum())
    tot_score = safe_ratio(tot_correct, tot_pin_drops)

    tot_swap_all = int(filtered["isSwap_num"].sum())
    tot_normal = int(tot_pin_drops - tot_swap_all)

    swap_type_lower = filtered["swapType"].astype(str).str.lower()
    tot_swap_neg = int((swap_type_lower == "neg").sum())
    tot_swap_pos = int((swap_type_lower == "pos").sum())

    swap_rate_tot = safe_ratio(tot_swap_all, tot_pin_drops)
    swap_ratio = f"{tot_normal}:{tot_swap_neg}:{tot_swap_pos}"

    tot_rounds = int(filtered["round_id"].nunique())

    swap_round_ids = filtered.loc[filtered["isSwap_num"] == 1, "round_id"].unique()
    tot_swap_rounds = int(len(swap_round_ids))
    tot_normal_rounds = int(tot_rounds - tot_swap_rounds)

    tot_swap_round_ratio = f"{tot_normal_rounds}:{tot_swap_neg}:{tot_swap_pos}"

    correct_vote_rounds = int(
        filtered.loc[
            filtered["SwapVoteScore"].astype(str).str.lower() == "correct",
            "round_id",
        ].nunique()
    )
    swap_vote_score = safe_ratio(correct_vote_rounds, tot_rounds)

    source_files = filtered["source_file"].dropna().drop_duplicates().tolist()
    num_files = int(len(source_files))

    source_file_swap_vote_registered_n = {}
    source_file_swap_vote_registered_d = {}

    for source_file, group in filtered.groupby("source_file", sort=False):
        n_value = get_single_unique_numeric_value(group["swapVoteRegistered_n"])
        d_value = get_single_unique_numeric_value(group["swapVoteRegistered_d"])

        source_file_swap_vote_registered_n[source_file] = None if n_value is None else int(n_value)
        source_file_swap_vote_registered_d[source_file] = None if d_value is None else int(d_value)

    total_swap_vote_registered_n = int(
        sum(v for v in source_file_swap_vote_registered_n.values() if v is not None)
    )
    total_swap_vote_registered_d = int(
        sum(v for v in source_file_swap_vote_registered_d.values() if v is not None)
    )
    total_swap_vote_registered = safe_ratio(
        total_swap_vote_registered_n,
        total_swap_vote_registered_d,
    )

    source_file_tot_points = {}
    for source_file, group in filtered.groupby("source_file", sort=False):
        group_non_null = group[group["roundGrandTotal_num"].notna()].sort_values("_orig_index")
        if group_non_null.empty:
            source_file_tot_points[source_file] = None
        else:
            source_file_tot_points[source_file] = float(group_non_null.iloc[-1]["roundGrandTotal_num"])

    if source_files:
        last_source_file = source_files[-1]
        tot_points = source_file_tot_points.get(last_source_file)
    else:
        tot_points = None

    avg_round_speed = float(filtered["avgRoundSpeed_num"].mean())
    std_round_speed = float(filtered["avgRoundSpeed_num"].std())

    avg_path_eff = float(filtered["path_eff_raw_num"].mean())
    std_path_eff = float(filtered["path_eff_raw_num"].std())

    round_level = (
        filtered.sort_values("_orig_index")
        .drop_duplicates(subset=["round_id"], keep="last")
        .copy()
    )

    path_order_counts = round_level["path_order_round"].value_counts(dropna=False).to_dict()

    summary = {
        "inputFile": file_label,
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
        "totSwap_RoundRatio": tot_swap_round_ratio,
        "swapVoteScore": swap_vote_score,
        "swapVoteRegistered_n_sum": total_swap_vote_registered_n,
        "swapVoteRegistered_d_sum": total_swap_vote_registered_d,
        "swapVoteRegistered_recalc": total_swap_vote_registered,
        "sourceFiles": json.dumps(source_files, ensure_ascii=False),
        "numFiles": num_files,
        "sourceFile_swapVoteRegistered_n": json.dumps(source_file_swap_vote_registered_n, ensure_ascii=False),
        "sourceFile_swapVoteRegistered_d": json.dumps(source_file_swap_vote_registered_d, ensure_ascii=False),
        "sourceFile_totPoints": json.dumps(source_file_tot_points, ensure_ascii=False),
        "totPoints": tot_points,
        "avgRoundSpeed": avg_round_speed,
        "stdRoundSpeed": std_round_speed,
        "avgPathEff": avg_path_eff,
        "stdPathEff": std_path_eff,
    }

    for output_name, path_label in PATH_ORDER_LABELS.items():
        summary[output_name] = int(path_order_counts.get(path_label, 0))

    for column in DEMOGRAPHIC_COLUMNS:
        summary[column] = get_single_unique_value(filtered[column]) if column in filtered.columns else None

    return summary


def summarize_csv_file(csv_path: Path) -> dict:
    df = pd.read_csv(csv_path)
    return summarize_dataframe(df, csv_path.name)


def summarize_folder(folder_path: Path, pattern: str = "*.csv") -> pd.DataFrame:
    csv_files = sorted(folder_path.glob(pattern))
    if not csv_files:
        raise ValueError(f"No files matched pattern {pattern!r} in {folder_path}")

    summaries = []
    errors = []

    for csv_file in csv_files:
        try:
            summaries.append(summarize_csv_file(csv_file))
        except Exception as exc:
            errors.append({"inputFile": csv_file.name, "error": str(exc)})

    if not summaries:
        error_text = "\n".join(f"{item['inputFile']}: {item['error']}" for item in errors)
        raise ValueError(f"No files were summarized successfully.\n{error_text}")

    summary_df = pd.DataFrame(summaries)

    if errors:
        error_df = pd.DataFrame(errors)
        error_path = folder_path / "summary_errors.csv"
        error_df.to_csv(error_path, index=False)
        print(f"Wrote error log: {error_path}")

    return summary_df


def write_output(summary_df: pd.DataFrame, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.suffix.lower() == ".json":
        records = summary_df.to_dict(orient="records")
        with open(out_path, "w", encoding="utf-8") as handle:
            json.dump(records, handle, indent=2, ensure_ascii=False)
    else:
        summary_df.to_csv(out_path, index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_path",
        help="Path to a CSV file or a folder containing CSV files",
    )
    parser.add_argument(
        "--pattern",
        default="*.csv",
        help="Glob pattern for folder mode (default: *.csv)",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Optional output file (.csv or .json). "
             "In folder mode, defaults to <folder>/folder_summary.csv. "
             "In file mode, defaults to stdout.",
    )
    args = parser.parse_args()

    input_path = Path(args.input_path)

    if input_path.is_dir():
        summary_df = summarize_folder(input_path, pattern=args.pattern)
        out_path = Path(args.out) if args.out else input_path / "folder_summary.csv"
        write_output(summary_df, out_path)
        print(f"Wrote summary: {out_path}")
    else:
        summary = summarize_csv_file(input_path)
        summary_df = pd.DataFrame([summary])

        if args.out:
            write_output(summary_df, Path(args.out))
            print(f"Wrote summary: {args.out}")
        else:
            print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()