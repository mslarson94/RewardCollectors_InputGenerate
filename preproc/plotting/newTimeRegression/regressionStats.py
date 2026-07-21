# =========================
# file: regressionStats.py
# =========================
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
import statsmodels.api as sm


@dataclass(frozen=True)
class PlotSpec:
    subject_value: Optional[str]
    subject_label: str
    coin_value: Optional[str]
    coin_label: str


def build_plot_specs(
    df: pd.DataFrame,
    *,
    subject_column: str,
    coin_column: str,
    include_all_subjects: bool,
    include_all_coins: bool,
    limit_subjects: Optional[list[str]],
    limit_coins: Optional[list[str]],
) -> list[PlotSpec]:
    subject_values = sorted(df[subject_column].dropna().unique().tolist(), key=str)
    coin_values = sorted(df[coin_column].dropna().unique().tolist(), key=str)

    if limit_subjects is not None:
        allowed_subjects = {str(value).strip() for value in limit_subjects}
        subject_values = [value for value in subject_values if value in allowed_subjects]

    if limit_coins is not None:
        allowed_coins = {str(value).strip().upper() for value in limit_coins}
        coin_values = [value for value in coin_values if value in allowed_coins]

    subject_specs: list[tuple[Optional[str], str]] = []
    coin_specs: list[tuple[Optional[str], str]] = []

    if include_all_subjects:
        subject_specs.append((None, "All Subjects"))
    subject_specs.extend((value, f"Subject: {value}") for value in subject_values)

    if include_all_coins:
        coin_specs.append((None, "All Coins"))
    coin_specs.extend((value, f"Coin: {value}") for value in coin_values)

    specs: list[PlotSpec] = []
    for subject_value, subject_label in subject_specs:
        for coin_value, coin_label in coin_specs:
            specs.append(
                PlotSpec(
                    subject_value=subject_value,
                    subject_label=subject_label,
                    coin_value=coin_value,
                    coin_label=coin_label,
                )
            )
    return specs


def filter_for_spec(
    df: pd.DataFrame,
    *,
    spec: PlotSpec,
    subject_column: str,
    coin_column: str,
) -> pd.DataFrame:
    filtered = df
    if spec.subject_value is not None:
        filtered = filtered.loc[filtered[subject_column] == spec.subject_value]
    if spec.coin_value is not None:
        filtered = filtered.loc[filtered[coin_column] == spec.coin_value]
    return filtered.copy()


def fit_linear_model(
    df: pd.DataFrame,
    *,
    outcome_column: str,
) -> tuple[sm.regression.linear_model.RegressionResultsWrapper, pd.DataFrame]:
    x = df["taskProgression"].astype(float)
    y = df[outcome_column].astype(float)

    design = sm.add_constant(x)
    model = sm.OLS(y, design).fit()

    x_grid = np.linspace(x.min(), x.max(), 200)
    pred_design = sm.add_constant(pd.Series(x_grid, name="taskProgression"))
    prediction = model.get_prediction(pred_design).summary_frame(alpha=0.05)

    pred_df = pd.DataFrame(
        {
            "taskProgression": x_grid,
            "mean": prediction["mean"].to_numpy(),
            "mean_ci_lower": prediction["mean_ci_lower"].to_numpy(),
            "mean_ci_upper": prediction["mean_ci_upper"].to_numpy(),
        }
    )
    return model, pred_df


