# =========================
# file: regressionHelpers.py
# =========================
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional
import re

import pandas as pd


DEFAULT_OUTCOME_COLUMN = "dropDist"
DEFAULT_SUBJECT_COLUMN = "sessionID"
DEFAULT_COIN_COLUMN = "coinLabel"
DEFAULT_CORRECTNESS_COLUMN = "dropQual"
DEFAULT_TP2_COLUMN = "isTP2"
DEFAULT_TASKPROGRESSION_COLUMN = "TotSesh_actTest_RoundNum"

MARKER_BY_COIN = {
    "HV": "*",
    "LV": "o",
    "NV": "o",
}
FILLED_BY_COIN = {
    "HV": True,
    "LV": True,
    "NV": False,
}
COLOR_BY_CORRECTNESS = {
    "correct": "blue",
    "incorrect": "red",
}

DEFAULT_POINT_ALPHA = 0.45
DEFAULT_POINT_SIZE = 20.0


def read_table(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)

    raise ValueError(f"Unsupported file type: {suffix}")


def validate_columns(df: pd.DataFrame, required_columns: Iterable[str]) -> None:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def coerce_string_label(value: object) -> str:
    if pd.isna(value):
        return "Missing"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def prettify_name(value: str) -> str:
    value = value.replace("_", " ")
    value = re.sub(r"([a-z])([A-Z])", r"\1 \2", value)
    return value.strip().title()


def slugify(value: object) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "value"


def normalize_coin_label(value: object) -> str:
    text = coerce_string_label(value).upper()
    return text


def normalize_correctness_label(value: object) -> Optional[str]:
    if pd.isna(value):
        return None

    text = str(value).strip().lower()
    if text == "":
        return None

    if text in {"correct", "good", "true", "1", "yes", "y"}:
        return "correct"
    if text in {"incorrect", "bad", "false", "0", "no", "n"}:
        return "incorrect"

    return text


def normalize_tp2_flag(value: object) -> Optional[bool]:
    if pd.isna(value):
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return bool(int(value))

    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "tp2"}:
        return True
    if text in {"0", "false", "no", "n", "tp1"}:
        return False

    return None




def prepare_analysis_frame(
    df: pd.DataFrame,
    *,
    outcome_column: str,
    task_progression_column: str,
    subject_column: str,
    coin_column: str,
    correctness_column: str,
    tp2_column: str,
    include_tp1: bool,
    correct_only: bool,
) -> pd.DataFrame:
    frame = df.copy()

    frame[outcome_column] = pd.to_numeric(frame[outcome_column], errors="coerce")
    frame[task_progression_column] = pd.to_numeric(
        frame[task_progression_column], errors="coerce"
    )

    frame[subject_column] = frame[subject_column].map(coerce_string_label)
    frame[coin_column] = frame[coin_column].map(normalize_coin_label)
    frame["dropCorrectness"] = frame[correctness_column].map(normalize_correctness_label)
    frame["isTP2"] = frame[tp2_column].map(normalize_tp2_flag)

    frame = frame.dropna(
        subset=[
            task_progression_column,
            outcome_column,
            subject_column,
            coin_column,
            "dropCorrectness",
        ]
    ).copy()

    if not include_tp1:
        frame = frame.loc[frame["isTP2"] == True].copy()

    if correct_only:
        frame = frame.loc[frame["dropCorrectness"] == "correct"].copy()

    return frame


