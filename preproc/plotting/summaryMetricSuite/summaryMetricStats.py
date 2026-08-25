#!/usr/bin/env python3
"""Descriptive statistics for participant-summary variables."""

from __future__ import annotations

import numpy as np
import pandas as pd


def summarize_metric(
    data: pd.DataFrame,
    *,
    variable: str,
    group_column: str | None = None,
) -> pd.DataFrame:
    """Return descriptive statistics for a numeric summary variable."""
    if variable not in data.columns:
        raise ValueError(f"Missing variable '{variable}'.")

    frame = data.copy()
    frame["_metric"] = pd.to_numeric(frame[variable], errors="coerce")

    if group_column:
        if group_column not in frame.columns:
            raise ValueError(f"Missing group column '{group_column}'.")
        grouped = frame.groupby(group_column, dropna=False, sort=True)
    else:
        frame["_all"] = "ALL"
        group_column = "_all"
        grouped = frame.groupby(group_column, dropna=False, sort=True)

    rows: list[dict[str, object]] = []

    for group_value, group in grouped:
        values = group["_metric"].dropna()
        row: dict[str, object] = {
            "group": group_value,
            "n_rows": int(len(group)),
            "n_valid": int(values.size),
            "n_missing": int(group["_metric"].isna().sum()),
        }

        if values.empty:
            row.update(
                mean=np.nan,
                std=np.nan,
                median=np.nan,
                q1=np.nan,
                q3=np.nan,
                iqr=np.nan,
                min=np.nan,
                max=np.nan,
            )
        else:
            q1 = float(values.quantile(0.25))
            q3 = float(values.quantile(0.75))
            row.update(
                mean=float(values.mean()),
                std=float(values.std(ddof=1)) if len(values) > 1 else np.nan,
                median=float(values.median()),
                q1=q1,
                q3=q3,
                iqr=q3 - q1,
                min=float(values.min()),
                max=float(values.max()),
            )

        rows.append(row)

    result = pd.DataFrame(rows)
    if group_column == "_all":
        result["group"] = "ALL"
    return result
