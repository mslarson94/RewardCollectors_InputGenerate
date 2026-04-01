"""
fit_choice_models_with_summary.py

Fits conditional logit (6-alternative) models and writes a compact coefficient table to CSV.

Requires:
  - roundID or roundID_int: identifies each choice occasion (one round)
  - participantID: for cluster-robust SEs
  - alt in {1..6}, chosen in {0,1}, utility (alt-specific)
  - knot columns: t_early_K, t_late_K
  - recentSwapRate_all (optional for volatility models)

Models:
  M0: chosen ~ U
  M1(K): + U*t_early_K + U*t_late_K
  M2(K): + U*volatility   (drops choice occasions with missing volatility)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


ALT_COL = "alt"
Y_COL = "chosen"
U_COL = "utility"
VOL_COL = "recentSwapRate_all"
CLUSTER_COL = "participantID"
CHOICE_ID_COLS = ("roundID", "roundID_int")  # preference order


def pick_choice_id_col(df: pd.DataFrame) -> str:
    for c in CHOICE_ID_COLS:
        if c in df.columns:
            return c
    raise KeyError(f"Need one of {CHOICE_ID_COLS} in the CSV.")


def validate_choice_sets(df: pd.DataFrame, choice_id_col: str) -> None:
    g = df.groupby(choice_id_col, sort=False)

    n_alts = g.size()
    if not (n_alts == 6).all():
        bad = n_alts[n_alts != 6].head(10)
        raise ValueError(f"Not all choice sets have 6 rows. Examples:\n{bad}")

    chosen_sum = g[Y_COL].sum()
    if not (chosen_sum == 1).all():
        bad = chosen_sum[chosen_sum != 1].head(10)
        raise ValueError(f"Not all choice sets have exactly one chosen==1. Examples:\n{bad}")

    alt_ok = g[ALT_COL].apply(lambda s: set(s.tolist()) == {1, 2, 3, 4, 5, 6})
    if not alt_ok.all():
        bad_ids = alt_ok[~alt_ok].index[:10].tolist()
        raise ValueError(f"Some choice sets do not contain alt=1..6. Examples: {bad_ids}")

    if df[U_COL].isna().any():
        raise ValueError(f"Utility has missing values in {df[U_COL].isna().sum()} rows.")


def drop_choices_with_missing(df: pd.DataFrame, col: str, choice_id_col: str) -> pd.DataFrame:
    bad = df.groupby(choice_id_col, sort=False)[col].apply(lambda s: s.isna().any())
    bad_ids = bad[bad].index
    if len(bad_ids) == 0:
        return df
    return df.loc[~df[choice_id_col].isin(bad_ids)].copy()


def build_exog(df: pd.DataFrame, knot: int, include_learning: bool, include_vol: bool) -> pd.DataFrame:
    X = pd.DataFrame({"U": pd.to_numeric(df[U_COL], errors="coerce")})

    if include_learning:
        e = f"t_early_{knot}"
        l = f"t_late_{knot}"
        if e not in df.columns or l not in df.columns:
            raise KeyError(f"Missing knot columns for K={knot}: {e}, {l}")

        t_early = pd.to_numeric(df[e], errors="coerce")
        t_late = pd.to_numeric(df[l], errors="coerce")
        X[f"U_x_early_{knot}"] = X["U"] * t_early
        X[f"U_x_late_{knot}"] = X["U"] * t_late

    if include_vol:
        v = pd.to_numeric(df[VOL_COL], errors="coerce")
        X["U_x_vol"] = X["U"] * v

    if X.isna().any().any():
        bad = X.isna().sum()
        raise ValueError(f"Exog has missing values after coercion:\n{bad[bad > 0]}")
    return X


def fit_conditional_logit(df: pd.DataFrame, exog: pd.DataFrame, choice_id_col: str):
    try:
        from statsmodels.discrete.conditional_models import ConditionalLogit
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            "ConditionalLogit not available. Install/upgrade statsmodels>=0.14:\n"
            "  pip install -U statsmodels"
        ) from e

    endog = df[Y_COL].astype(int).to_numpy()
    groups = df[choice_id_col].to_numpy()

    model = ConditionalLogit(endog=endog, exog=exog.to_numpy(), groups=groups)
    res = model.fit(disp=False, cov_type="cluster", cov_kwds={"groups": df[CLUSTER_COL].to_numpy()})
    return res


def result_to_long_table(
    res,
    model_name: str,
    knot: int | None,
    n_rows: int,
    n_choices: int,
    choice_id_col: str,
    dropped_choices: int = 0,
) -> pd.DataFrame:
    # Statsmodels returns numpy arrays for params/bse/pvalues; preserve term names we created in exog.
    # We set exog columns in a fixed order when building X; res.params aligns to that order.
    params = np.asarray(res.params)
    bse = np.asarray(res.bse)
    pvalues = np.asarray(res.pvalues)

    # Try to recover names from res.model.exog_names if present; otherwise fallback.
    names = getattr(res.model, "exog_names", None)
    if not names:
        names = [f"x{i}" for i in range(len(params))]

    out = pd.DataFrame(
        {
            "model": model_name,
            "knot": knot,
            "term": names,
            "coef": params,
            "se_cluster_participant": bse,
            "p_value": pvalues,
            "n_rows": n_rows,
            "n_choices": n_choices,
            "choice_id_col": choice_id_col,
            "dropped_choices_missing_vol": dropped_choices,
            "llf": getattr(res, "llf", np.nan),
            "converged": getattr(res, "converged", np.nan),
        }
    )
    return out


def run_models(in_csv: Path, out_csv: Path, knots: Iterable[int]) -> None:
    df = pd.read_csv(in_csv)

    choice_id_col = pick_choice_id_col(df)
    validate_choice_sets(df, choice_id_col)

    print(f"Choice id column: {choice_id_col}")
    print(f"Rows: {len(df):,}")
    print(f"Choice occasions: {df[choice_id_col].nunique():,}\n")

    tables: list[pd.DataFrame] = []

    # M0
    X0 = build_exog(df, knot=0, include_learning=False, include_vol=False)
    res0 = fit_conditional_logit(df, X0, choice_id_col)
    print("=== M0: chosen ~ utility ===")
    print(res0.summary(), "\n")
    tables.append(
        result_to_long_table(
            res0,
            model_name="M0_U",
            knot=None,
            n_rows=len(df),
            n_choices=df[choice_id_col].nunique(),
            choice_id_col=choice_id_col,
        )
    )

    for k in knots:
        # M1(K)
        X1 = build_exog(df, knot=k, include_learning=True, include_vol=False)
        res1 = fit_conditional_logit(df, X1, choice_id_col)
        print(f"=== M1 (K={k}): + utility*t_early_{k} + utility*t_late_{k} ===")
        print(res1.summary(), "\n")
        tables.append(
            result_to_long_table(
                res1,
                model_name="M1_UxLearning",
                knot=k,
                n_rows=len(df),
                n_choices=df[choice_id_col].nunique(),
                choice_id_col=choice_id_col,
            )
        )

        # M2(K): drop missing volatility by choice occasion
        df2 = drop_choices_with_missing(df, VOL_COL, choice_id_col)
        validate_choice_sets(df2, choice_id_col)
        dropped = df[choice_id_col].nunique() - df2[choice_id_col].nunique()

        X2 = build_exog(df2, knot=k, include_learning=True, include_vol=True)
        res2 = fit_conditional_logit(df2, X2, choice_id_col)
        print(f"=== M2 (K={k}): + utility*volatility ===")
        print(f"Dropped choices with missing {VOL_COL}: {dropped}")
        print(res2.summary(), "\n")
        tables.append(
            result_to_long_table(
                res2,
                model_name="M2_UxLearning+Vol",
                knot=k,
                n_rows=len(df2),
                n_choices=df2[choice_id_col].nunique(),
                choice_id_col=choice_id_col,
                dropped_choices=dropped,
            )
        )

    out = pd.concat(tables, ignore_index=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)
    print(f"\nWrote summary table: {out_csv} (rows={len(out):,})")


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--in_csv", type=Path, required=True)
    p.add_argument("--out_summary_csv", type=Path, required=True)
    p.add_argument("--knots", type=int, nargs="+", default=[15, 20, 25])
    args = p.parse_args(argv)

    run_models(args.in_csv, args.out_summary_csv, args.knots)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))