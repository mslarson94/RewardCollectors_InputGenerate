from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import numpy as np

from temporal_alignment.common.io import read_csv, write_csv, write_json
from temporal_alignment.common.stats import summarize_residuals


def _infer_match_mode(df: pd.DataFrame, fallback: str = "") -> str:
    if "match_mode" in df.columns:
        vals = df["match_mode"].dropna().astype(str).unique()
        if len(vals) == 1:
            return vals[0]
    return fallback


def _infer_model_name(df: pd.DataFrame, fallback: str = "") -> str:
    if "model_name" in df.columns:
        vals = df["model_name"].dropna().astype(str).unique()
        if len(vals) == 1:
            return vals[0]
    return fallback


def _residual_slope(df: pd.DataFrame) -> float:
    if "corrected_ml_time" not in df or "residual_s" not in df or len(df) < 2:
        return np.nan
    t = pd.to_datetime(df["corrected_ml_time"], errors="coerce")
    r = pd.to_numeric(df["residual_s"], errors="coerce")
    mask = t.notna() & r.notna()
    if mask.sum() < 2:
        return np.nan
    x = (t.loc[mask] - t.loc[mask].min()).dt.total_seconds().to_numpy(float)
    y = r.loc[mask].to_numpy(float)
    if np.ptp(x) == 0:
        return np.nan
    return float(np.polyfit(x, y, 1)[0])


def summarize_alignment(path: str, evaluation_type: str, label: str = "") -> tuple[dict, pd.DataFrame]:
    df = read_csv(path)
    if "residual_s" not in df.columns:
        raise KeyError(f"{path} missing residual_s")
    model = _infer_model_name(df, label)
    match_mode = _infer_match_mode(df, "")
    metrics = summarize_residuals(pd.to_numeric(df["residual_s"], errors="coerce").to_numpy(float))
    row = {
        "evaluation_type": evaluation_type,
        "model_name": model,
        "match_mode": match_mode,
        "source_file": str(path),
        "residual_slope_s_per_s": _residual_slope(df),
        "residual_slope_ms_per_min": _residual_slope(df) * 60000.0 if np.isfinite(_residual_slope(df)) else np.nan,
        **metrics,
    }
    tagged = df.copy()
    tagged["evaluation_type"] = evaluation_type
    if "model_name" not in tagged:
        tagged["model_name"] = model
    if "match_mode" not in tagged:
        tagged["match_mode"] = match_mode
    return row, tagged


def main():
    ap = argparse.ArgumentParser(
        description="Build standardized comparison/validation tables from alignment outputs."
    )
    ap.add_argument("--insample", action="append", default=[],
                    help="Repeat for fit-diagnostic CSVs from any alignment model.")
    ap.add_argument("--heldout", action="append", default=[],
                    help="Repeat for held-out/CV prediction CSVs.")
    ap.add_argument("--matching-comparison", default="",
                    help="Optional manual-vs-automatic matching comparison CSV.")
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    if not args.insample and not args.heldout:
        raise SystemExit("Provide at least one --insample or --heldout file")

    summaries = []
    frames = []

    for path in args.insample:
        row, df = summarize_alignment(path, "in_sample")
        summaries.append(row)
        frames.append(df)

    for path in args.heldout:
        row, df = summarize_alignment(path, "held_out")
        summaries.append(row)
        frames.append(df)

    summary = pd.DataFrame(summaries)

    # Rank only within the same evaluation type; do not mix in-sample and held-out ranks.
    summary["rank_median_abs_residual"] = np.nan
    summary["rank_rmse"] = np.nan
    for eval_type, idx in summary.groupby("evaluation_type").groups.items():
        sub = summary.loc[idx]
        summary.loc[idx, "rank_median_abs_residual"] = (
            sub["median_abs_residual_s"].rank(method="min", ascending=True).to_numpy()
        )
        summary.loc[idx, "rank_rmse"] = (
            sub["rmse_s"].rank(method="min", ascending=True).to_numpy()
        )

    od = Path(args.out_dir)
    write_csv(summary.sort_values(
        ["evaluation_type", "rank_median_abs_residual", "rank_rmse"]
    ), od/"model_validation_summary.csv")

    all_points = pd.concat(frames, ignore_index=True)
    write_csv(all_points, od/"all_validation_points.csv")

    matching_stats = None
    if args.matching_comparison:
        m = read_csv(args.matching_comparison)
        matching_stats = {
            "n_rows": int(len(m)),
            "n_same_pair": int(m["same_pair"].fillna(False).astype(bool).sum()) if "same_pair" in m else None,
            "n_different_pair": int((~m["same_pair"].fillna(False).astype(bool)).sum()) if "same_pair" in m else None,
            "fraction_same_pair": float(m["same_pair"].fillna(False).astype(bool).mean()) if "same_pair" in m and len(m) else None,
            "median_abs_offset_difference_s": float(
                pd.to_numeric(m.get("offset_difference_s"), errors="coerce").abs().median()
            ) if "offset_difference_s" in m else None,
            "max_abs_offset_difference_s": float(
                pd.to_numeric(m.get("offset_difference_s"), errors="coerce").abs().max()
            ) if "offset_difference_s" in m else None,
        }
        write_json(matching_stats, od/"matching_agreement_summary.json")

    # Factor table: matching mode x model for held-out and in-sample separately.
    factor = summary.pivot_table(
        index=["evaluation_type", "match_mode"],
        columns="model_name",
        values=["median_abs_residual_s", "rmse_s", "p95_abs_residual_s"],
        aggfunc="first",
    )
    factor.to_csv(od/"matching_by_model_matrix.csv")

    run_summary = {
        "n_models_evaluated": int(len(summary)),
        "n_insample_files": len(args.insample),
        "n_heldout_files": len(args.heldout),
        "has_matching_comparison": bool(args.matching_comparison),
        "matching_agreement": matching_stats,
    }
    write_json(run_summary, od/"validation_run_summary.json")
    print(f"[ok] validation tables -> {od}")


if __name__ == "__main__":
    main()
