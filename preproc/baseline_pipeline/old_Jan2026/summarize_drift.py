#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
summarize_drift.py — Make drift plots and a stats CSV from a merged ML CSV (from script 2a).
"""

from __future__ import annotations

import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def _plot(ml_times: pd.Series, drift_sec: pd.Series, out_png: Path, title: str) -> None:
    idx = np.arange(1, len(ml_times) + 1)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 6), constrained_layout=True)
    ax1.scatter(idx, drift_sec)
    ax1.set_title(title)
    ax1.set_xlabel("Event Index")
    ax1.set_ylabel("Drift (s)")
    ax1.grid(True, alpha=0.4)

    delta = pd.Series(drift_sec).diff()
    ax2.plot(idx[1:], delta.iloc[1:], "-x")
    ax2.set_title("Derivative of Drift")
    ax2.set_xlabel("Event Index")
    ax2.set_ylabel("Δ Drift (s)")
    ax2.grid(True, alpha=0.4)
    ax2.axhline(1.0, linestyle="--", linewidth=1.0)

    fig.suptitle("Drift Analysis", fontsize=14, y=1.02)
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser(description="Plot and summarize drift from merged ML CSV")
    ap.add_argument("--merged_ml_csv", required=True)
    ap.add_argument("--label", required=True, help="BioPac or RNS")
    args = ap.parse_args()

    df = pd.read_csv(args.merged_ml_csv)
    label = args.label.strip()
    col_ts = f"{label}_RPi_Timestamp"
    col_drift = f"{label}_RPi_Timestamp_drift"

    if col_ts not in df.columns or col_drift not in df.columns:
        raise SystemExit(f"merged CSV missing required columns: {col_ts}, {col_drift}")

    ml_ts = pd.to_datetime(df.get("mLTimestamp", pd.Series([pd.NaT]*len(df))), errors="coerce")
    mask = (~df[col_ts].isna()) & (~ml_ts.isna())
    if not mask.any():
        raise SystemExit("no matched rows to summarize")

    drift = df.loc[mask, col_drift].astype(float)
    ml_times = ml_ts.loc[mask]

    ml_base = Path(args.merged_ml_csv).stem
    out_dir = Path(args.merged_ml_csv).parent

    out_png = out_dir / f"{ml_base}_{label}_DriftPlot.png"
    _plot(ml_times.reset_index(drop=True), drift.reset_index(drop=True), out_png, f"Drift vs Event Index — {ml_base}")

    summary = {
        "n_matched": int(mask.sum()),
        "mean_drift_s": float(np.nanmean(drift)),
        "median_drift_s": float(np.nanmedian(drift)),
        "max_abs_drift_s": float(np.nanmax(np.abs(drift))),
        "n_total_rows": int(len(df)),
    }
    out_csv = out_dir / f"{ml_base}_{label}_DriftSummary.csv"
    pd.DataFrame([summary]).to_csv(out_csv, index=False)

    print(out_png)
    print(out_csv)


if __name__ == "__main__":
    main()
