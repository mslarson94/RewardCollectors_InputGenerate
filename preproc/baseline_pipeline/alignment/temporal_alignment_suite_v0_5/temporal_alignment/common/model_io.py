from __future__ import annotations
import pandas as pd


MATCH_REQUIRED = [
    "match_mode", "match_id", "ml_mark_id", "rpi_mark_id",
    "corrected_ml_time", "rpi_time", "raw_offset_s",
    "matched", "effective_excluded",
]


def require_columns(df: pd.DataFrame, cols, context: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"{context} missing required columns: {missing}")


def fit_mask(df: pd.DataFrame):
    return (
        df["matched"].fillna(False).astype(bool)
        & ~df["effective_excluded"].fillna(False).astype(bool)
        & df["corrected_ml_time"].notna()
        & df["rpi_time"].notna()
    )
