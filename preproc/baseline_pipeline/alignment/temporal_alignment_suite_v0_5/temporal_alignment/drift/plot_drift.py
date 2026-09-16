from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

from temporal_alignment.common.io import read_csv


def main():
    ap = argparse.ArgumentParser(description="Plot raw clock difference, fitted drift, and residuals.")
    ap.add_argument("--drift-points", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    df = read_csv(args.drift_points)
    required = ["scope", "elapsed_s", "raw_offset_recomputed_s", "fitted_offset_s", "drift_residual_s"]
    missing = [c for c in required if c not in df]
    if missing:
        raise KeyError(f"Missing columns: {missing}")

    od = Path(args.out_dir)
    od.mkdir(parents=True, exist_ok=True)

    for scope, g in df.groupby("scope", sort=False):
        safe = str(scope).replace(":", "_").replace("/", "_")
        x = g["elapsed_s"].astype(float) / 60.0

        fig, ax = plt.subplots(figsize=(10,4))
        ax.scatter(x, g["raw_offset_recomputed_s"].astype(float), s=20, label="raw offset")
        ax.plot(x, g["fitted_offset_s"].astype(float), label="fitted offset")
        ax.set_xlabel("Elapsed corrected ML time (min)")
        ax.set_ylabel("RPi - ML (s)")
        ax.set_title(f"Clock offset and drift — {scope}")
        ax.grid(True, alpha=.3)
        ax.legend()
        fig.savefig(od / f"{safe}_offset_drift.png", dpi=160, bbox_inches="tight")
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(10,4))
        ax.scatter(x, g["drift_residual_s"].astype(float), s=20)
        ax.axhline(0, linewidth=1)
        ax.set_xlabel("Elapsed corrected ML time (min)")
        ax.set_ylabel("Residual (s)")
        ax.set_title(f"Residual around linear clock model — {scope}")
        ax.grid(True, alpha=.3)
        fig.savefig(od / f"{safe}_residuals.png", dpi=160, bbox_inches="tight")
        plt.close(fig)

    print(f"[ok] drift plots -> {od}")


if __name__ == "__main__":
    main()
