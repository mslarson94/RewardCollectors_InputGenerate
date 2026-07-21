# scripts/generate_base_qc_flags.py
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Generate base QC / eligibility flags from allIntervalData_AN.csv without any "
            "outlier detection. Tutorial rounds are defined as CoinSetID > 3."
        )
    )
    ap.add_argument("--input", required=True, help="Path to allIntervalData_AN.csv")
    ap.add_argument("--outdir", required=True, help="Directory for output files")
    return ap.parse_args()


def require_columns(df: pd.DataFrame, columns: Iterable[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def parse_boolish(s: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(s):
        return s.fillna(False)

    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce").fillna(0).astype(float) != 0.0

    lowered = s.astype("string").str.strip().str.lower()
    return lowered.isin({"1", "true", "t", "yes", "y"})


def parse_bad_drop(drop_qual: pd.Series) -> pd.Series:
    text = drop_qual.astype("string").str.strip().str.lower()
    return text.eq("bad")


def first_nonnull(s: pd.Series):
    nonnull = s.dropna()
    return nonnull.iloc[0] if not nonnull.empty else np.nan


def add_base_flags(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    require_columns(
        df,
        [
            "roundID",
            "roundID_int",
            "participantID",
            "sessionID",
            "main_RR",
            "CoinSetID",
            "dropQual",
            "isIncompleteRound",
            "totalRounds",
        ],
    )

    out = df.copy()

    out["dropQual_isBad"] = parse_bad_drop(out["dropQual"])
    out["isIncompleteRound_bool"] = parse_boolish(out["isIncompleteRound"])
    out["isTutorial"] = (pd.to_numeric(out["CoinSetID"], errors="coerce") > 3).astype(int)
    out["isTP2"] = (pd.to_numeric(out["totalRounds"], errors="coerce") > 1).astype(int)

    round_group = out.groupby("roundID", dropna=False)

    round_summary = round_group.agg(
        roundID_int=("roundID_int", first_nonnull),
        participantID=("participantID", first_nonnull),
        sessionID=("sessionID", first_nonnull),
        main_RR=("main_RR", first_nonnull),
        sessionType=("sessionType", first_nonnull),
        coinSet=("coinSet", first_nonnull),
        CoinSetID=("CoinSetID", first_nonnull),
        isSwap=("isSwap", first_nonnull),
        startPos=("startPos", first_nonnull),
        totalRounds=("totalRounds", first_nonnull),
        isTP2=("isTP2", "max"),
        round_dur_s=("round_dur_s", first_nonnull),
        n_pinDrops=("roundID", "size"),
        n_badDrops=("dropQual_isBad", "sum"),
        isIncompleteRound=("isIncompleteRound_bool", "max"),
        isTutorial=("isTutorial", "max"),
    ).reset_index()

    round_summary["isPerfectRound"] = (round_summary["n_badDrops"] == 0).astype(int)

    round_summary["isEligibleBase"] = (
        ~round_summary["isIncompleteRound"] & (round_summary["isTutorial"] == 0)
    )

    round_summary["baseEligibilityReason"] = np.select(
        [
            round_summary["isIncompleteRound"] & (round_summary["isTutorial"] == 1),
            round_summary["isIncompleteRound"],
            round_summary["isTutorial"] == 1,
        ],
        [
            "incomplete+tutorial",
            "incomplete",
            "tutorial",
        ],
        default="eligible",
    )

    out = out.merge(
        round_summary[
            [
                "roundID",
                "isTP2",
                "isTutorial",
                "n_pinDrops",
                "n_badDrops",
                "isPerfectRound",
                "isEligibleBase",
                "baseEligibilityReason",
            ]
        ],
        on="roundID",
        how="left",
        suffixes=("", "_round"),
    )

    return out, round_summary


def build_overall_summary(pin_df: pd.DataFrame, round_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    round_keep = round_df["isEligibleBase"]
    pin_keep = pin_df["isEligibleBase"]

    rows.append({"section": "overall", "metric": "n_pinDrops_raw", "value": int(len(pin_df))})
    rows.append({"section": "overall", "metric": "n_pinDrops_post_base_filter", "value": int(pin_keep.sum())})
    rows.append({"section": "overall", "metric": "n_unique_roundIDs_raw", "value": int(round_df["roundID"].nunique(dropna=True))})
    rows.append({"section": "overall", "metric": "n_unique_roundIDs_post_base_filter", "value": int(round_df.loc[round_keep, "roundID"].nunique(dropna=True))})
    rows.append({"section": "overall", "metric": "n_rounds_raw", "value": int(len(round_df))})
    rows.append({"section": "overall", "metric": "n_rounds_post_base_filter", "value": int(round_keep.sum())})
    rows.append({"section": "overall", "metric": "n_rounds_incomplete", "value": int(round_df["isIncompleteRound"].sum())})
    rows.append({"section": "overall", "metric": "n_rounds_tutorial", "value": int((round_df["isTutorial"] == 1).sum())})
    rows.append({"section": "overall", "metric": "n_rounds_perfect", "value": int((round_df["isPerfectRound"] == 1).sum())})
    rows.append({"section": "overall", "metric": "n_rounds_not_perfect", "value": int((round_df["isPerfectRound"] == 0).sum())})
    rows.append({"section": "overall", "metric": "n_tp2_rounds", "value": int((round_df["isTP2"] == 1).sum())})
    rows.append({"section": "overall", "metric": "n_non_tp2_rounds", "value": int((round_df["isTP2"] == 0).sum())})

    for value, sub in round_df.groupby("main_RR", dropna=False):
        label = f"main_RR={value}"
        rows.append({"section": "by_main_RR", "group": label, "metric": "n_rounds_raw", "value": int(len(sub))})
        rows.append({"section": "by_main_RR", "group": label, "metric": "n_rounds_post_base_filter", "value": int(sub["isEligibleBase"].sum())})
        rows.append({"section": "by_main_RR", "group": label, "metric": "n_rounds_incomplete", "value": int(sub["isIncompleteRound"].sum())})
        rows.append({"section": "by_main_RR", "group": label, "metric": "n_rounds_tutorial", "value": int((sub["isTutorial"] == 1).sum())})
        rows.append({"section": "by_main_RR", "group": label, "metric": "n_rounds_perfect", "value": int((sub["isPerfectRound"] == 1).sum())})
        rows.append({"section": "by_main_RR", "group": label, "metric": "n_rounds_not_perfect", "value": int((sub["isPerfectRound"] == 0).sum())})

    for value, sub in round_df.groupby("sessionType", dropna=False):
        label = f"sessionType={value}"
        rows.append({"section": "by_sessionType", "group": label, "metric": "n_rounds_raw", "value": int(len(sub))})
        rows.append({"section": "by_sessionType", "group": label, "metric": "n_rounds_post_base_filter", "value": int(sub["isEligibleBase"].sum())})

    for value, sub in round_df.groupby("isTP2", dropna=False):
        label = f"isTP2={value}"
        rows.append({"section": "by_isTP2", "group": label, "metric": "n_rounds_raw", "value": int(len(sub))})
        rows.append({"section": "by_isTP2", "group": label, "metric": "n_rounds_post_base_filter", "value": int(sub["isEligibleBase"].sum())})

    for value, sub_round in round_df.groupby("sessionID", dropna=False):
        label = f"sessionID={value}"
        sub_pin = pin_df.loc[pin_df["sessionID"] == value]
        sub_pin_keep = sub_pin["isEligibleBase"]

        rows.append({"section": "by_sessionID", "group": label, "metric": "n_unique_roundIDs_raw", "value": int(sub_round["roundID"].nunique(dropna=True))})
        rows.append({"section": "by_sessionID", "group": label, "metric": "n_unique_roundIDs_post_base_filter", "value": int(sub_round.loc[sub_round["isEligibleBase"], "roundID"].nunique(dropna=True))})
        rows.append({"section": "by_sessionID", "group": label, "metric": "n_pinDrops_raw", "value": int(len(sub_pin))})
        rows.append({"section": "by_sessionID", "group": label, "metric": "n_pinDrops_post_base_filter", "value": int(sub_pin_keep.sum())})

    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    outdir_qc = Path(args.outdir, "RoundDurationQCFiles")
    outdir_qc.mkdir(parents=True, exist_ok=True)

    input_path = Path(args.input)
    df = pd.read_csv(input_path)

    pin_df, round_df = add_base_flags(df)
    overall_summary = build_overall_summary(pin_df, round_df)

    stem = input_path.stem
    augmented_path = outdir / f"{stem}_baseQC.csv"
    round_summary_path = outdir_qc / "round_base_qc_summary.csv"
    overall_summary_path = outdir_qc / "base_qc_overall_summary.csv"
    metadata_path = outdir_qc / "base_qc_run_metadata.json"

    pin_df.to_csv(augmented_path, index=False)
    round_df.to_csv(round_summary_path, index=False)
    overall_summary.to_csv(overall_summary_path, index=False)

    metadata = {
        "input": str(input_path.resolve()),
        "outputs": {
            "augmented_base_qc": str(augmented_path.resolve()),
            "round_base_qc_summary": str(round_summary_path.resolve()),
            "base_qc_overall_summary": str(overall_summary_path.resolve()),
        },
        "notes": {
            "dropQual_isBad": "True when dropQual == 'bad'; False when dropQual == 'good'",
            "isPerfectRound": "1 = all pin drops good, 0 = one or more bad pin drops in round",
            "isTP2": "Computed as totalRounds > 1, per current user request",
            "isTutorial": "Computed as 1 when CoinSetID > 3, else 0",
            "isEligibleBase": "True when round is not incomplete and not tutorial",
        },
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    print(f"Wrote: {augmented_path}")
    print(f"Wrote: {round_summary_path}")
    print(f"Wrote: {overall_summary_path}")
    print(f"Wrote: {metadata_path}")


if __name__ == "__main__":
    main()