#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Summarize observed clock offset and post-affine residuals."
    )
    ap.add_argument("--merged_ml_csv", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--timeCol", default="mLT_orig")
    args = ap.parse_args()

    in_csv = Path(args.merged_ml_csv)
    df = pd.read_csv(in_csv)
    label = args.label
    time_col = args.timeCol

    offset_col = f"{label}_Observed_Offset_s"
    residual_col = f"{label}_Alignment_Residual_s"
    inlier_col = f"{label}_Affine_Inlier"
    drift_col = f"{label}_Clock_Drift_ppm"
    predicted_col = f"{label}_Predicted_Clock_Offset_s"

    required = [time_col, offset_col, residual_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required alignment columns: {missing}")

    t = pd.to_datetime(df[time_col], errors="coerce")
    mask = t.notna() & df[offset_col].notna()
    if not mask.any():
        raise ValueError("No matched marks with observed offsets")

    tm = t.loc[mask]
    x_min = (tm - tm.min()).dt.total_seconds().to_numpy() / 60.0
    observed = df.loc[mask, offset_col].astype(float).to_numpy()
    residual = df.loc[mask, residual_col].astype(float).to_numpy()
    inlier = (
        df.loc[mask, inlier_col].fillna(False).astype(bool).to_numpy()
        if inlier_col in df.columns
        else np.ones(len(observed), dtype=bool)
    )

    out_dir = in_csv.parent / "Drift"
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = in_csv.stem

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.scatter(x_min, observed, label="observed RPi - ML")
    if predicted_col in df.columns:
        pred = df.loc[mask, predicted_col].astype(float).to_numpy()
        ax.plot(x_min, pred, label="affine predicted offset")
    ax.set_title(f"Clock offset vs elapsed time — {stem} [{label}]")
    ax.set_xlabel("Elapsed ML time (min)")
    ax.set_ylabel("Offset (s)")
    ax.grid(True, alpha=0.4)
    ax.legend()
    offset_png = out_dir / f"{stem}_{label}_ClockOffset.png"
    fig.savefig(offset_png, dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.scatter(x_min[inlier], residual[inlier], label="affine inliers")
    if (~inlier).any():
        ax.scatter(x_min[~inlier], residual[~inlier], label="affine outliers")
    ax.axhline(0.0, linewidth=1)
    ax.set_title(f"Post-alignment residuals — {stem} [{label}]")
    ax.set_xlabel("Elapsed ML time (min)")
    ax.set_ylabel("Residual: observed RPi - predicted RPi (s)")
    ax.grid(True, alpha=0.4)
    ax.legend()
    residual_png = out_dir / f"{stem}_{label}_AlignmentResiduals.png"
    fig.savefig(residual_png, dpi=150)
    plt.close(fig)

    inlier_resid = residual[inlier & np.isfinite(residual)]
    stats = {
        "label": label,
        "n_matched": int(len(observed)),
        "n_inliers": int(inlier.sum()),
        "median_observed_offset_s": float(np.nanmedian(observed)),
        "offset_range_s": float(np.nanmax(observed) - np.nanmin(observed)),
        "residual_median_s": float(np.nanmedian(inlier_resid)) if len(inlier_resid) else np.nan,
        "residual_mad_s": (
            float(np.nanmedian(np.abs(inlier_resid - np.nanmedian(inlier_resid))))
            if len(inlier_resid)
            else np.nan),
        "residual_rmse_s": (
            float(np.sqrt(np.nanmean(np.square(inlier_resid))))
            if len(inlier_resid) else np.nan),
        "clock_drift_ppm": (
            float(pd.to_numeric(df[drift_col], errors="coerce").dropna().iloc[0])
            if drift_col in df.columns and pd.to_numeric(df[drift_col], errors="coerce").notna().any()
            else np.nan
        ),
    }
    out_csv = out_dir / f"{stem}_{label}_AlignmentSummary.csv"
    pd.DataFrame([stats]).to_csv(out_csv, index=False)

    print(f"[ok] wrote offset plot    -> {offset_png}")
    print(f"[ok] wrote residual plot  -> {residual_png}")
    print(f"[ok] wrote summary        -> {out_csv}")

if __name__ == "__main__":
    main()
