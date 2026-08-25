# =========================
# file: regressionHelpers.py
# =========================
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
import re

import numpy as np
import pandas as pd


DEFAULT_OUTCOME_COLUMN = "dropDist"
DEFAULT_SUBJECT_COLUMN = "sessionID"
DEFAULT_COIN_COLUMN = "coinLabel"
DEFAULT_CORRECTNESS_COLUMN = "dropQual"
DEFAULT_TP2_COLUMN = "isTP2"
DEFAULT_TASKPROGRESSION_COLUMN = "TotSesh_actTest_RoundNum"
DEFAULT_COHORT_COLUMN = "main_RR"

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


@dataclass(frozen=True)
class RepresentativePair:
    """Main/RR representative session IDs resolved from one roles-table row."""

    mode: str
    role: str
    main_session_id: str
    rr_session_id: str


def read_table(path: str | Path) -> pd.DataFrame:
    """Read a supported analysis table."""

    path = Path(path)
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)

    raise ValueError(f"Unsupported file type: {suffix}")


def read_roles_table(path: str | Path) -> pd.DataFrame:
    """Read comma- or tab-delimited representative role mappings."""

    roles = pd.read_csv(path, sep=None, engine="python", dtype="string")
    roles.columns = [str(column).strip() for column in roles.columns]
    return roles


def validate_columns(df: pd.DataFrame, required_columns: Iterable[str]) -> None:
    """Raise a clear error when required analysis columns are missing."""

    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def coerce_string_label(value: object) -> str:
    """Normalize identifiers and categorical labels to stripped strings."""

    if pd.isna(value):
        return "Missing"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def prettify_name(value: str) -> str:
    """Convert a column name into a readable plot label."""

    value = value.replace("_", " ")
    value = re.sub(r"([a-z])([A-Z])", r"\1 \2", value)
    return value.strip().title()


def slugify(value: object) -> str:
    """Create a filesystem-safe identifier."""

    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "value"


def normalize_coin_label(value: object) -> str:
    """Normalize coin labels to upper-case identifiers."""

    return coerce_string_label(value).upper()


def normalize_correctness_label(value: object) -> Optional[str]:
    """Normalize common correctness encodings."""

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
    """Normalize TP2 flags from booleans, numeric values, or strings."""

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


def normalize_cohort_label(value: object) -> Optional[str]:
    """Normalize cohort labels without imposing a specific spelling."""

    if pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def labels_equal(left: object, right: object) -> bool:
    """Compare categorical labels case-insensitively after trimming."""

    if pd.isna(left) or pd.isna(right):
        return False
    return str(left).strip().casefold() == str(right).strip().casefold()


def resolve_representative_pair(
    roles: pd.DataFrame,
    *,
    role: str,
    mode: str,
    role_column: str = "role",
    main_column: str = "main_seed42",
    rr_column: str = "RR_seed50",
) -> RepresentativePair:
    """Resolve exactly one Main/RR representative pair for a role."""

    for column in (role_column, main_column, rr_column):
        if column not in roles.columns:
            raise ValueError(f"Roles table does not contain '{column}'.")

    role_text = str(role).strip()
    role_values = roles[role_column].astype("string").str.strip()
    matches = roles.loc[role_values.eq(role_text)]

    if matches.empty:
        raise ValueError(f"No representative mapping found for role '{role_text}'.")
    if len(matches) != 1:
        raise ValueError(
            f"Expected one mapping for role '{role_text}', found {len(matches)}."
        )

    row = matches.iloc[0]
    main_id = str(row[main_column]).strip()
    rr_id = str(row[rr_column]).strip()

    invalid = {"", "nan", "<na>", "none"}
    if main_id.lower() in invalid or rr_id.lower() in invalid:
        raise ValueError(
            f"Role '{role_text}' has an empty representative session ID."
        )

    return RepresentativePair(
        mode=mode,
        role=role_text,
        main_session_id=main_id,
        rr_session_id=rr_id,
    )


def validate_representative_pair(
    df: pd.DataFrame,
    pair: RepresentativePair,
    *,
    session_column: str,
    outcome_column: str,
    task_progression_column: str,
) -> None:
    """Ensure both representative sessions have usable regression rows."""

    validate_columns(
        df,
        [session_column, outcome_column, task_progression_column],
    )

    session_values = df[session_column].astype("string").str.strip()
    failures: list[str] = []

    for label, session_id in (
        ("Main", pair.main_session_id),
        ("RR", pair.rr_session_id),
    ):
        selected = session_values.eq(session_id)
        if not selected.any():
            failures.append(f"{label} session '{session_id}' is absent")
            continue

        usable = (
            pd.to_numeric(df.loc[selected, outcome_column], errors="coerce").notna()
            & pd.to_numeric(
                df.loc[selected, task_progression_column], errors="coerce"
            ).notna()
        )
        if not usable.any():
            failures.append(
                f"{label} session '{session_id}' has no usable regression rows"
            )

    if failures:
        raise ValueError(
            f"Invalid {pair.mode} representative pair: " + "; ".join(failures)
        )


def prepare_analysis_frame(
    df: pd.DataFrame,
    *,
    outcome_column: str,
    task_progression_column: str,
    subject_column: str,
    coin_column: str,
    correctness_column: str,
    tp2_column: str,
    cohort_column: str,
    include_tp1: bool,
    correct_only: bool,
    limit_coins: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Normalize and filter the regression analysis frame."""

    frame = df.copy()

    frame[outcome_column] = pd.to_numeric(frame[outcome_column], errors="coerce")
    frame[task_progression_column] = pd.to_numeric(
        frame[task_progression_column],
        errors="coerce",
    )

    frame[subject_column] = frame[subject_column].map(coerce_string_label)
    frame[coin_column] = frame[coin_column].map(normalize_coin_label)
    frame[cohort_column] = frame[cohort_column].map(normalize_cohort_label)
    frame["dropCorrectness"] = frame[correctness_column].map(
        normalize_correctness_label
    )
    frame["isTP2"] = frame[tp2_column].map(normalize_tp2_flag)

    frame = frame.dropna(
        subset=[
            task_progression_column,
            outcome_column,
            subject_column,
            coin_column,
            cohort_column,
            "dropCorrectness",
        ]
    ).copy()

    if not include_tp1:
        frame = frame.loc[frame["isTP2"].eq(True)].copy()

    if correct_only:
        frame = frame.loc[frame["dropCorrectness"].eq("correct")].copy()

    if limit_coins is not None:
        allowed = {str(value).strip().upper() for value in limit_coins}
        frame = frame.loc[frame[coin_column].isin(allowed)].copy()

    return frame


def validate_axis_limits(
    limits: Optional[tuple[float, float]],
    *,
    argument_name: str,
) -> Optional[tuple[float, float]]:
    """Validate a two-value numeric axis limit."""

    if limits is None:
        return None

    lower, upper = map(float, limits)
    if not np.isfinite(lower) or not np.isfinite(upper):
        raise ValueError(f"{argument_name} values must be finite.")
    if lower >= upper:
        raise ValueError(
            f"{argument_name} requires LOWER < UPPER; received {lower}, {upper}."
        )
    return lower, upper
