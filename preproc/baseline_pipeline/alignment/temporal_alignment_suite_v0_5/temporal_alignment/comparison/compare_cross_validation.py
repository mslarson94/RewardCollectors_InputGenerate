from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

from temporal_alignment.common.io import read_csv, write_csv
from temporal_alignment.common.stats import summarize_residuals


def main():
    ap = argparse.ArgumentParser(description="Compare held-out validation outputs.")
    ap.add_argument("--cv", action="append", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    rows = []
    frames = []
    for path in args.cv:
        df = read_csv(path)
        for col in ["model_name", "residual_s", "cv_scheme"]:
            if col not in df:
                raise KeyError(f"{path} missing {col}")
        frames.append(df)
        for (model, scheme), g in df.groupby(["model_name", "cv_scheme"]):
            rows.append({
                "model_name": model,
                "cv_scheme": scheme,
                "source_file": str(path),
                **summarize_residuals(g["residual_s"].to_numpy(float)),
            })

    od = Path(args.out_dir)
    summary = pd.DataFrame(rows).sort_values(["median_abs_residual_s", "rmse_s"])
    write_csv(summary, od/"cross_validation_comparison.csv")

    all_df = pd.concat(frames, ignore_index=True)
    fig, ax = plt.subplots(figsize=(10,4))
    for (model, scheme), g in all_df.groupby(["model_name","cv_scheme"]):
        ax.scatter(range(len(g)), g["residual_s"].astype(float), s=18, label=f"{model} | {scheme}")
    ax.axhline(0, linewidth=1)
    ax.set_xlabel("Held-out prediction index")
    ax.set_ylabel("Residual (s)")
    ax.set_title("Held-out alignment residuals")
    ax.grid(True, alpha=.3)
    ax.legend()
    od.mkdir(parents=True, exist_ok=True)
    fig.savefig(od/"cross_validation_residuals.png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"[ok] cross-validation comparison -> {od}")


if __name__ == "__main__":
    main()
