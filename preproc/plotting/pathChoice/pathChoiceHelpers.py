#!/usr/bin/env python3
"""Data preparation helpers for participant-level path-choice plots."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

VALID_PATH_CODES = (1, 2, 3, 4, 5, 6)
SUMMARY_COLUMNS = (
    "PVSS_AvgScore",
    "swapRate_tot",
    "rounds2criterion",
    "totScore",
    "totRounds",
)
ROUND_KEY = ("participantID", "sessionID", "roundID_int")


def read_table(path: str | Path, *, sheet_name: str | int | None = None) -> pd.DataFrame:
    """Read CSV, TSV, XLS, XLSX, or Parquet data."""
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, low_memory=False)
    if suffix in {".tsv", ".txt"}:
        return pd.read_csv(path, sep="\t", low_memory=False)
    if suffix in {".xls", ".xlsx"}:
        return pd.read_excel(path, sheet_name=sheet_name or 0)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported input format: {suffix}")


def normalize_join_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize participant/session identifiers without changing their meaning."""
    out = df.copy()
    if "participantID" in out.columns:
        numeric = pd.to_numeric(out["participantID"], errors="coerce")
        out["participantID"] = numeric.astype("Int64").astype("string")
    if "sessionID" in out.columns:
        out["sessionID"] = out["sessionID"].astype("string").str.strip()
    return out


def _single_value(series: pd.Series, *, column: str, key: tuple[object, ...]) -> object:
    values = series.dropna().unique()
    if len(values) > 1:
        raise ValueError(f"Round {key} has conflicting {column} values: {values.tolist()}")
    return values[0] if len(values) == 1 else pd.NA


def prepare_round_level_data(
    raw: pd.DataFrame,
    *,
    valid_path_codes: Iterable[int] = VALID_PATH_CODES,
) -> pd.DataFrame:
    """Reduce pin-drop rows to one validated record per participant/session/round."""
    required = {*ROUND_KEY, "path_order_round", "path_order_round_num"}
    missing = sorted(required.difference(raw.columns))
    if missing:
        raise ValueError(f"Main data are missing required columns: {missing}")

    data = normalize_join_keys(raw)
    data["roundID_int"] = pd.to_numeric(data["roundID_int"], errors="coerce").astype("Int64")
    data["path_order_round_num"] = pd.to_numeric(
        data["path_order_round_num"], errors="coerce"
    ).astype("Int64")
    data["path_order_round"] = data["path_order_round"].astype("string").str.strip()

    valid_codes = {int(code) for code in valid_path_codes}
    data = data.loc[
        data["participantID"].notna()
        & data["sessionID"].notna()
        & data["roundID_int"].notna()
        & data["path_order_round_num"].isin(valid_codes)
    ].copy()

    optional = [column for column in ("RoundNum", "totalRounds", "totalRounds_round") if column in data.columns]
    keep = [*ROUND_KEY, "path_order_round", "path_order_round_num", *optional]
    data = data[keep]

    rows: list[dict[str, object]] = []
    for key, group in data.groupby(list(ROUND_KEY), sort=False, dropna=False):
        row = dict(zip(ROUND_KEY, key))
        for column in keep[3:]:
            row[column] = _single_value(group[column], column=column, key=key)
        rows.append(row)

    rounds = pd.DataFrame(rows)
    if rounds.empty:
        raise ValueError("No valid path-choice rounds remain after excluding non-1–6 codes.")

    path_map = (
        rounds[["path_order_round_num", "path_order_round"]]
        .dropna()
        .drop_duplicates()
    )
    conflicts = path_map.groupby("path_order_round_num")["path_order_round"].nunique()
    if (conflicts > 1).any():
        bad = conflicts[conflicts > 1].index.tolist()
        raise ValueError(f"Path codes map to multiple labels: {bad}")

    label_by_code = path_map.set_index("path_order_round_num")["path_order_round"].to_dict()
    rounds["path_label"] = rounds["path_order_round_num"].map(label_by_code).astype("string")
    rounds["path_code"] = rounds["path_order_round_num"].astype(int)
    rounds["roundID_int"] = rounds["roundID_int"].astype(int)

    rounds["round_plot_value"] = rounds["roundID_int"]
    rounds["round_axis_label"] = "roundID_int"
    sort_cols = ["participantID", "sessionID", "roundID_int"]
    return rounds.sort_values(sort_cols, kind="stable").reset_index(drop=True)


def prepare_summary_data(
    raw: pd.DataFrame,
    *,
    summary_columns: Iterable[str] = SUMMARY_COLUMNS,
) -> pd.DataFrame:
    """Select and validate one summary row per participant/session."""
    required = {"participantID", "sessionID"}
    missing = sorted(required.difference(raw.columns))
    if missing:
        raise ValueError(f"Summary data are missing join columns: {missing}")

    summary = normalize_join_keys(raw)
    available = [column for column in summary_columns if column in summary.columns]
    summary = summary[["participantID", "sessionID", *available]].copy()
    summary = summary.loc[summary["participantID"].notna() & summary["sessionID"].notna()]

    duplicate_mask = summary.duplicated(["participantID", "sessionID"], keep=False)
    if duplicate_mask.any():
        duplicate_keys = (
            summary.loc[duplicate_mask, ["participantID", "sessionID"]]
            .drop_duplicates()
            .to_dict("records")
        )
        raise ValueError(f"Summary join keys are not unique: {duplicate_keys}")

    return summary


def merge_rounds_with_summary(rounds: pd.DataFrame, summary: pd.DataFrame) -> pd.DataFrame:
    """Attach summary metrics with a validated many-to-one merge."""
    return rounds.merge(
        summary,
        on=["participantID", "sessionID"],
        how="left",
        validate="many_to_one",
        indicator="_summary_merge",
    )


def derive_path_order(rounds: pd.DataFrame) -> tuple[list[int], dict[int, str]]:
    """Return a fixed 1–6 path order and labels available in the data."""
    mapping = (
        rounds[["path_code", "path_label"]]
        .drop_duplicates()
        .set_index("path_code")["path_label"]
        .to_dict()
    )
    return list(VALID_PATH_CODES), {
        code: mapping.get(code, f"Path {code}") for code in VALID_PATH_CODES
    }
