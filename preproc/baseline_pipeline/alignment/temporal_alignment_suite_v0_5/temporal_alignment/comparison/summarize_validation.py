from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv, write_json


def main():
    ap = argparse.ArgumentParser(description="Create compact validation leaderboard and best-model summary.")
    ap.add_argument("--summary", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    df = read_csv(args.summary)
    od = Path(args.out_dir)

    cols = [
        "evaluation_type","match_mode","model_name","n",
        "median_abs_residual_s","mad_residual_s","rmse_s",
        "p95_abs_residual_s","max_abs_residual_s",
        "residual_slope_s_per_s","residual_slope_ms_per_min",
        "rank_median_abs_residual","rank_rmse",
    ]
    keep = [c for c in cols if c in df]
    write_csv(df[keep], od/"validation_leaderboard.csv")

    best = {}
    for eval_type, g in df.groupby("evaluation_type"):
        good = g.dropna(subset=["median_abs_residual_s"])
        if good.empty:
            continue
        winner = good.sort_values(
            ["median_abs_residual_s","rmse_s","p95_abs_residual_s"]
        ).iloc[0]
        best[eval_type] = {
            "model_name": winner["model_name"],
            "match_mode": winner.get("match_mode", ""),
            "median_abs_residual_s": winner["median_abs_residual_s"],
            "rmse_s": winner["rmse_s"],
            "p95_abs_residual_s": winner["p95_abs_residual_s"],
            "note": (
                "Held-out results should be preferred for scientific model selection."
                if eval_type == "held_out"
                else "In-sample results describe fit quality only and should not determine the final model alone."
            ),
        }
    write_json(best, od/"best_models.json")
    print(f"[ok] validation summary -> {od}")


if __name__ == "__main__":
    main()
