# =========================
# file: regressionStats.py
# =========================
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
import statsmodels.api as sm

from regressionHelpers import RepresentativePair, labels_equal


@dataclass(frozen=True)
class RegressionFacet:
    """One dataset and label used as a regression facet."""

    key: str
    title: str
    data: pd.DataFrame


@dataclass
class FacetFit:
    """Regression result for one facet, including skip/error state."""

    facet: RegressionFacet
    model: Optional[sm.regression.linear_model.RegressionResultsWrapper]
    predictions: Optional[pd.DataFrame]
    status: str

    @property
    def n_rows(self) -> int:
        return len(self.facet.data)


def build_representative_facets(
    df: pd.DataFrame,
    *,
    pair: RepresentativePair,
    session_column: str,
) -> list[RegressionFacet]:
    """Build All Participants, Main representative, and RR representative facets."""

    session_ids = df[session_column].astype("string").str.strip()
    main = df.loc[session_ids.eq(pair.main_session_id)].copy()
    rr = df.loc[session_ids.eq(pair.rr_session_id)].copy()

    return [
        RegressionFacet(
            key="all_participants",
            title="All Participants",
            data=df.copy(),
        ),
        RegressionFacet(
            key="main_representative",
            title=f"Main Representative\n{pair.main_session_id}",
            data=main,
        ),
        RegressionFacet(
            key="rr_representative",
            title=f"RR Representative\n{pair.rr_session_id}",
            data=rr,
        ),
    ]


def build_cohort_facets(
    df: pd.DataFrame,
    *,
    pair: RepresentativePair,
    session_column: str,
    cohort_column: str,
    main_cohort_value: str,
    rr_cohort_value: str,
) -> list[RegressionFacet]:
    """Build Main/RR cohort facets plus their representative-session facets."""

    main_mask = df[cohort_column].map(
        lambda value: labels_equal(value, main_cohort_value)
    )
    rr_mask = df[cohort_column].map(
        lambda value: labels_equal(value, rr_cohort_value)
    )
    session_ids = df[session_column].astype("string").str.strip()

    return [
        RegressionFacet(
            key="main_cohort",
            title="Main Cohort",
            data=df.loc[main_mask].copy(),
        ),
        RegressionFacet(
            key="rr_cohort",
            title="RR Cohort",
            data=df.loc[rr_mask].copy(),
        ),
        RegressionFacet(
            key="main_representative",
            title=f"Main Representative\n{pair.main_session_id}",
            data=df.loc[session_ids.eq(pair.main_session_id)].copy(),
        ),
        RegressionFacet(
            key="rr_representative",
            title=f"RR Representative\n{pair.rr_session_id}",
            data=df.loc[session_ids.eq(pair.rr_session_id)].copy(),
        ),
    ]


def fit_linear_model(
    df: pd.DataFrame,
    *,
    outcome_column: str,
    task_progression_column: str,
) -> tuple[
    sm.regression.linear_model.RegressionResultsWrapper,
    pd.DataFrame,
]:
    """Fit OLS and return a dense prediction grid with a 95% mean CI."""

    x = df[task_progression_column].astype(float)
    y = df[outcome_column].astype(float)

    design = sm.add_constant(x, has_constant="add")
    model = sm.OLS(y, design).fit()

    x_grid = np.linspace(float(x.min()), float(x.max()), 200)
    pred_design = sm.add_constant(
        pd.Series(x_grid, name=task_progression_column),
        has_constant="add",
    )
    prediction = model.get_prediction(pred_design).summary_frame(alpha=0.05)

    pred_df = pd.DataFrame(
        {
            task_progression_column: x_grid,
            "mean": prediction["mean"].to_numpy(),
            "mean_ci_lower": prediction["mean_ci_lower"].to_numpy(),
            "mean_ci_upper": prediction["mean_ci_upper"].to_numpy(),
        }
    )
    return model, pred_df


def fit_facets(
    facets: list[RegressionFacet],
    *,
    outcome_column: str,
    task_progression_column: str,
    min_rows: int,
    min_unique_x: int,
) -> list[FacetFit]:
    """Fit each facet independently while retaining skipped panels."""

    results: list[FacetFit] = []

    for facet in facets:
        n_rows = len(facet.data)
        n_unique_x = facet.data[task_progression_column].nunique()

        if n_rows < min_rows:
            results.append(
                FacetFit(
                    facet=facet,
                    model=None,
                    predictions=None,
                    status=f"skipped_insufficient_rows:{n_rows}<{min_rows}",
                )
            )
            continue

        if n_unique_x < min_unique_x:
            results.append(
                FacetFit(
                    facet=facet,
                    model=None,
                    predictions=None,
                    status=(
                        "skipped_insufficient_unique_x:"
                        f"{n_unique_x}<{min_unique_x}"
                    ),
                )
            )
            continue

        try:
            model, predictions = fit_linear_model(
                facet.data,
                outcome_column=outcome_column,
                task_progression_column=task_progression_column,
            )
        except Exception as exc:
            results.append(
                FacetFit(
                    facet=facet,
                    model=None,
                    predictions=None,
                    status=f"skipped_model_error:{exc}",
                )
            )
            continue

        results.append(
            FacetFit(
                facet=facet,
                model=model,
                predictions=predictions,
                status="fit",
            )
        )

    return results


def facet_fit_manifest_row(
    fit: FacetFit,
    *,
    figure_type: str,
    pair: RepresentativePair,
    outcome_column: str,
    task_progression_column: str,
    include_tp1: bool,
    correct_only: bool,
) -> dict[str, object]:
    """Convert one facet fit into an audit-friendly manifest row."""

    model = fit.model
    return {
        "figure_type": figure_type,
        "representative_mode": pair.mode,
        "role": pair.role,
        "facet": fit.facet.key,
        "facet_title": fit.facet.title.replace("\n", " "),
        "outcome_column": outcome_column,
        "task_progression_column": task_progression_column,
        "tp_scope": "TP1+TP2" if include_tp1 else "TP2 only",
        "drop_scope": "Correct only" if correct_only else "All drops",
        "n_rows": fit.n_rows,
        "n_unique_task_progression": (
            fit.facet.data[task_progression_column].nunique()
            if task_progression_column in fit.facet.data.columns
            else 0
        ),
        "slope": (
            model.params.get(task_progression_column, np.nan)
            if model is not None
            else np.nan
        ),
        "p_value_slope": (
            model.pvalues.get(task_progression_column, np.nan)
            if model is not None
            else np.nan
        ),
        "intercept": (
            model.params.get("const", np.nan) if model is not None else np.nan
        ),
        "r_squared": model.rsquared if model is not None else np.nan,
        "status": fit.status,
    }
