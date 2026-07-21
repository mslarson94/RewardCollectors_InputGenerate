#!/usr/bin/env python3
"""Descriptive summaries for participant path choices."""

from __future__ import annotations

import numpy as np
import pandas as pd

from pathChoiceHelpers import VALID_PATH_CODES, derive_path_order


def summarize_path_choices(data: pd.DataFrame) -> pd.DataFrame:
    """Return counts and within-participant/session proportions for all six paths."""
    required = {"participantID", "sessionID", "roundID_int", "path_code"}
    missing = sorted(required.difference(data.columns))
    if missing:
        raise ValueError(f"Summary data are missing columns: {missing}")

    path_order, path_labels = derive_path_order(data)
    keys = data[["participantID", "sessionID"]].drop_duplicates()
    categories = pd.DataFrame({"path_code": path_order})
    grid = keys.merge(categories, how="cross")

    observed = (
        data.groupby(["participantID", "sessionID", "path_code"], dropna=False)
        .agg(
            n_rounds=("roundID_int", "nunique"),
            first_round=("round_plot_value", "min"),
            last_round=("round_plot_value", "max"),
            median_round=("round_plot_value", "median"),
        )
        .reset_index()
    )
    result = grid.merge(
        observed,
        on=["participantID", "sessionID", "path_code"],
        how="left",
        validate="one_to_one",
    )
    result["n_rounds"] = result["n_rounds"].fillna(0).astype(int)
    totals = result.groupby(["participantID", "sessionID"])["n_rounds"].transform("sum")
    result["proportion"] = np.where(totals > 0, result["n_rounds"] / totals, np.nan)
    result["path_order_round"] = result["path_code"].map(path_labels)
    return result.sort_values(["participantID", "sessionID", "path_code"]).reset_index(drop=True)


def summarize_participants(data: pd.DataFrame) -> pd.DataFrame:
    """Return one row per participant/session with dominant path and coverage."""
    path_summary = summarize_path_choices(data)
    index = path_summary.groupby(["participantID", "sessionID"])["n_rounds"].idxmax()
    dominant = path_summary.loc[
        index,
        ["participantID", "sessionID", "path_code", "path_order_round", "proportion"],
    ].rename(
        columns={
            "path_code": "dominant_path_code",
            "path_order_round": "dominant_path",
            "proportion": "dominant_path_proportion",
        }
    )

    overview = (
        data.groupby(["participantID", "sessionID"], dropna=False)
        .agg(
            plotted_rounds=("roundID_int", "nunique"),
            observed_path_categories=("path_code", "nunique"),
        )
        .reset_index()
    )
    return overview.merge(
        dominant,
        on=["participantID", "sessionID"],
        how="left",
        validate="one_to_one",
    )
