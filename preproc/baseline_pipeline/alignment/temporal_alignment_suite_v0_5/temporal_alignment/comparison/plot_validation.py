from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from temporal_alignment.common.io import read_csv


def main():
    ap = argparse.ArgumentParser(description="Plot model-comparison and validation results.")
    ap.add_argument("--summary", required=True)
    ap.add_argument("--points", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    summary = read_csv(args.summary)
    points = read_csv(args.points)
    od = Path(args.out_dir)
    od.mkdir(parents=True, exist_ok=True)

    # Metric leaderboard per evaluation type.
    for eval_type, g in summary.groupby("evaluation_type", sort=False):
        labels = [
            f"{m}\n{mm}" if str(mm) not in {"", "nan"} else str(m)
            for m, mm in zip(g["model_name"], g["match_mode"])
        ]
        x = np.arange(len(g))

        fig, ax = plt.subplots(figsize=(10,4))
        ax.bar(x, g["median_abs_residual_s"].astype(float))
        ax.set_xticks(x, labels, rotation=30, ha="right")
        ax.set_ylabel("Median |residual| (s)")
        ax.set_title(f"Alignment model comparison — {eval_type}")
        ax.grid(True, axis="y", alpha=.3)
        fig.savefig(od/f"{eval_type}_median_abs_residual.png", dpi=160, bbox_inches="tight")
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(10,4))
        ax.bar(x, g["rmse_s"].astype(float))
        ax.set_xticks(x, labels, rotation=30, ha="right")
        ax.set_ylabel("RMSE (s)")
        ax.set_title(f"Alignment model RMSE — {eval_type}")
        ax.grid(True, axis="y", alpha=.3)
        fig.savefig(od/f"{eval_type}_rmse.png", dpi=160, bbox_inches="tight")
        plt.close(fig)

    # Residual-vs-time plot.
    if "corrected_ml_time" in points.columns:
        for eval_type, g0 in points.groupby("evaluation_type", sort=False):
            fig, ax = plt.subplots(figsize=(11,5))
            for (model, match_mode), g in g0.groupby(["model_name","match_mode"], dropna=False):
                t = pd.to_datetime(g["corrected_ml_time"], errors="coerce")
                mask = t.notna()
                if not mask.any():
                    continue
                x = (t.loc[mask] - t.loc[mask].min()).dt.total_seconds()/60.0
                ax.scatter(
                    x,
                    pd.to_numeric(g.loc[mask, "residual_s"], errors="coerce"),
                    s=18,
                    label=f"{model} | {match_mode}",
                )
            ax.axhline(0, linewidth=1)
            ax.set_xlabel("Elapsed corrected ML time (min)")
            ax.set_ylabel("Residual (s)")
            ax.set_title(f"Residuals over time — {eval_type}")
            ax.grid(True, alpha=.3)
            ax.legend(fontsize=8)
            fig.savefig(od/f"{eval_type}_residuals_over_time.png", dpi=160, bbox_inches="tight")
            plt.close(fig)

    print(f"[ok] validation plots -> {od}")


if __name__ == "__main__":
    main()
