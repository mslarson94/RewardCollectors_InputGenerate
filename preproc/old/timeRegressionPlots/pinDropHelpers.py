# =========================
# file: pinDropHelpers.py
# =========================
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable
import re

import numpy as np
import pandas as pd

MARKER_BY_COIN = {"HV": "*", "LV": "o", "NV": "o"}
FILLED_BY_COIN = {"HV": True, "LV": True, "NV": False}
COLOR_BY_QUAL = {"good": "blue", "bad": "red"}

ALPHA = 0.5
SIZE = 100

DEFAULT_LABELS = {
    "truecontent_elapsed_s": "Pin Drop Latency Within Round (s)",
    "dropDist": "Pin Drop Distance to Closest Coin Not Yet Collected (m)",
}


def read_table(path: str | Path, sheet_name: str | int | None = 0) -> pd.DataFrame:
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet_name)
    if suffix == ".parquet":
        return pd.read_parquet(path)

    raise ValueError(f"Unsupported file type: {suffix}")


def _coerce_seconds(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return numeric

    delta = pd.to_timedelta(series, errors="coerce")
    return delta.dt.total_seconds()


def normalize_event_data(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "coinLabel" in out.columns:
        out["coinLabel"] = out["coinLabel"].astype(str).str.strip()

    if "dropQual" in out.columns:
        out["dropQual"] = out["dropQual"].astype(str).str.strip().str.lower()

    for column in ("BlockNum", "RoundNum"):
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")

    if "mLTimestamp" in out.columns:
        out["mLTimestamp"] = pd.to_datetime(out["mLTimestamp"], errors="coerce")

    numeric_candidates = [
        "truecontent_elapsed_s",
        "dropDist",
        "BlockElapsedTime",
        "RoundElapsedTime",
        "TrueSessionElapsedTime",
    ]
    for column in numeric_candidates:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")

    return out


def add_true_session_elapsed(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "trueSession_elapsed_s" in out.columns:
        out["trueSession_elapsed_s"] = _coerce_seconds(out["trueSession_elapsed_s"])
        return out

    if "TrueSessionElapsedTime" in out.columns:
        out["trueSession_elapsed_s"] = _coerce_seconds(out["TrueSessionElapsedTime"])
        return out

    if "mLTimestamp" in out.columns:
        ts = pd.to_datetime(out["mLTimestamp"], errors="coerce")
        if ts.notna().any():
            out["trueSession_elapsed_s"] = (ts - ts.min()).dt.total_seconds()
            return out

    out["trueSession_elapsed_s"] = pd.NA
    return out


def add_true_session_elapsed_by_block_events(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "trueBlock_elapsed_s" in out.columns:
        out["trueBlock_elapsed_s"] = _coerce_seconds(out["trueBlock_elapsed_s"])
        return out

    if "BlockElapsedTime" in out.columns:
        out["trueBlock_elapsed_s"] = _coerce_seconds(out["BlockElapsedTime"])
        return out

    out["trueBlock_elapsed_s"] = pd.NA
    return out


def require_columns(df: pd.DataFrame, columns: Iterable[str], label: str = "Data") -> None:
    missing = sorted(set(columns).difference(df.columns))
    if missing:
        raise ValueError(f"{label} are missing columns: {missing}")


def filter_complete_rows(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    blocks_min: int | None = None,
    block_equals: int | None = None,
) -> pd.DataFrame:
    require_columns(
        df,
        ["BlockNum", "BlockStatus", variable_of_interest, "coinLabel", "dropQual"],
        label="Event data",
    )

    out = df.copy()
    out[variable_of_interest] = pd.to_numeric(out[variable_of_interest], errors="coerce")

    mask = (
        (out["BlockStatus"] == "complete")
        & out[variable_of_interest].notna()
        & out["coinLabel"].notna()
        & out["dropQual"].isin(["good", "bad"])
    )

    if block_equals is not None:
        mask &= out["BlockNum"] == block_equals

    if blocks_min is not None:
        mask &= out["BlockNum"] > blocks_min

    return out.loc[mask].copy()


def prepare_block3_round_time_data(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
) -> pd.DataFrame:
    out = filter_complete_rows(df, variable_of_interest=variable_of_interest, block_equals=3)
    require_columns(out, ["trueSession_elapsed_s"], label="Block 3 round-time data")
    out = out[out["trueSession_elapsed_s"].notna()].copy()
    return out[["trueSession_elapsed_s", variable_of_interest, "coinLabel", "dropQual"]].copy()


def prepare_block3_round_num_data(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    round_max: int = 100,
) -> pd.DataFrame:
    out = filter_complete_rows(df, variable_of_interest=variable_of_interest, block_equals=3)
    require_columns(out, ["RoundNum"], label="Block 3 round-number data")
    out["RoundNum"] = pd.to_numeric(out["RoundNum"], errors="coerce")
    out = out[out["RoundNum"].notna() & (out["RoundNum"] < round_max)].copy()
    return out[["RoundNum", variable_of_interest, "coinLabel", "dropQual"]].copy()


def prepare_blocks_gt3_time_data(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    blocks_min: int = 3,
) -> pd.DataFrame:
    out = filter_complete_rows(df, variable_of_interest=variable_of_interest, blocks_min=blocks_min)
    require_columns(out, ["trueSession_elapsed_s"], label="Blocks>3 time data")
    out = out[out["trueSession_elapsed_s"].notna()].copy()
    return out[["BlockNum", "trueSession_elapsed_s", variable_of_interest, "coinLabel", "dropQual"]].copy()


def prepare_blocks_gt3_blocknum_data(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    blocks_min: int = 3,
) -> pd.DataFrame:
    out = filter_complete_rows(df, variable_of_interest=variable_of_interest, blocks_min=blocks_min)
    return out[["BlockNum", variable_of_interest, "coinLabel", "dropQual"]].copy()


def prepare_hist_data(
    df: pd.DataFrame,
    *,
    variable_of_interest: str,
    blocks_min: int = 3,
) -> pd.DataFrame:
    out = filter_complete_rows(df, variable_of_interest=variable_of_interest, blocks_min=blocks_min)
    return out[[variable_of_interest, "coinLabel"]].copy()


def exclude_outliers(
    df: pd.DataFrame,
    column: str,
    *,
    method: str = "median",
    sigma: float = 2.0,
    ddof: int = 1,
    groupby: str | list[str] | None = None,
    keep_na: bool = False,
) -> pd.DataFrame:
    if column not in df.columns:
        return df.copy()

    out = df.copy()
    x = pd.to_numeric(out[column], errors="coerce")

    if groupby is None:
        center_value = x.mean() if method == "mean" else x.median()
        scale_value = x.std(ddof=ddof)
        center = pd.Series(center_value, index=out.index)
        scale = pd.Series(scale_value, index=out.index)
    else:
        center = out.groupby(groupby)[column].transform(
            lambda s: pd.to_numeric(s, errors="coerce").mean()
            if method == "mean"
            else pd.to_numeric(s, errors="coerce").median()
        )
        scale = out.groupby(groupby)[column].transform(
            lambda s: pd.to_numeric(s, errors="coerce").std(ddof=ddof)
        )

    scale = scale.replace(0, np.nan)
    z = (x - center).abs() / scale
    mask = (z <= sigma) | z.isna()

    if not keep_na:
        mask &= x.notna()

    return out.loc[mask].copy()


def prepare_event_data(
    raw: pd.DataFrame,
    *,
    use_outlier_filter: bool = False,
    filter_columns: Iterable[str] = ("truecontent_elapsed_s", "dropDist"),
    outlier_groupby: str | list[str] | None = "coinLabel",
    outlier_method: str = "median",
    outlier_sigma: float = 2.0,
    outlier_ddof: int = 1,
    outlier_keep_na: bool = False,
) -> pd.DataFrame:
    out = normalize_event_data(raw)
    out = add_true_session_elapsed(out)
    out = add_true_session_elapsed_by_block_events(out)

    if use_outlier_filter:
        for column in filter_columns:
            if column in out.columns:
                out = exclude_outliers(
                    out,
                    column,
                    method=outlier_method,
                    sigma=outlier_sigma,
                    ddof=outlier_ddof,
                    groupby=outlier_groupby,
                    keep_na=outlier_keep_na,
                )

    return out


def slugify(value: Any, maxlen: int = 64) -> str:
    text = re.sub(r"\W+", "_", str(value)).strip("_")
    return text[:maxlen] or "NA"


