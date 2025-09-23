# ========================= summarize_drift.py =========================
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
summarize_drift.py — Plot and summarize drift from merged ML CSVs.

Usage examples:
  # per-source (backward compatible)
  python summarize_drift.py --merged_ml_csv <..._BioPac_events.csv> --label BioPac

  # multiple sources and combined overlay
  python summarize_drift.py --merged_ml_csv <..._BioPacRNS_events.csv> --labels BioPac,RNS,Combined

Outputs (per label L):
  <ml_base>_L_DriftPlot.png
  <ml_base>_L_DriftSummary.csv

If "Combined" is requested (or ≥2 labels are present), also writes:
  <ml_base>_Combined_DriftPlot.png
  <ml_base>_Combined_DriftSummary.csv  (one row per label)
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def _available_labels(df: pd.DataFrame) -> List[str]:
    labels = []
    for prefix in ("BioPac", "RNS"):
        if f"{prefix}_RPi_Timestamp_drift" in df.columns:
            labels.append(prefix)
    # allow any other prefixes that follow the pattern *_RPi_Timestamp_drift
    for col in df.columns:
        if col.endswith("_RPi_Timestamp_drift"):
            prefix = col[:-len("_RPi_Timestamp_drift")]
            if prefix not in labels:
                labels.append(prefix)
    return labels


essential_cols = ["mLTimestamp"]

def _series_for_label(df: pd.DataFrame, label: str):
    drift_col = f"{label}_RPi_Timestamp_drift"
    ts_col = f"{label}_RPi_Timestamp"
    if drift_col not in df.columns or ts_col not in df.columns:
        return None
    ml_ts = pd.to_datetime(df.get("mLTimestamp", pd.Series([pd.NaT]*len(df))), errors="coerce")
    mask = (~df[drift_col].isna()) & (~ml_ts.isna()) & (~df[ts_col].isna())
    if not mask.any():
        return None
    x = (np.arange(len(df)) + 1)[mask.to_numpy()]  # event index positions of this label's matched rows
    y = df.loc[mask, drift_col].astype(float).to_numpy()
    return x, y, ml_ts.loc[mask].reset_index(drop=True)


def _plot_single(label: str, x: np.ndarray, y: np.ndarray, title: str, out_png: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.scatter(x, y)
    ax.set_title(title)
    ax.set_xlabel("Event Index")
    ax.set_ylabel("Drift (s)")
    ax.grid(True, alpha=0.4)
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def _summarize(y: np.ndarray) -> dict:
    return {
        "n_matched": int(len(y)),
        "mean_drift_s": float(np.nanmean(y)) if len(y) else float("nan"),
        "median_drift_s": float(np.nanmedian(y)) if len(y) else float("nan"),
        "max_abs_drift_s": float(np.nanmax(np.abs(y))) if len(y) else float("nan"),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Plot and summarize drift from merged ML CSV (supports multiple labels and combined overlay)")
    ap.add_argument("--merged_ml_csv", required=True)
    ap.add_argument("--label", default="", help="Single label (BioPac or RNS). Deprecated if --labels is used.")
    ap.add_argument("--labels", default="", help="Comma-separated labels to include (e.g., 'BioPac,RNS,Combined')")
    args = ap.parse_args()

    df = pd.read_csv(args.merged_ml_csv)
    ml_base = Path(args.merged_ml_csv).stem
    out_dir = Path(args.merged_ml_csv).parent

    requested = [s.strip() for s in args.labels.split(",") if s.strip()] if args.labels else ([] if not args.label else [args.label.strip()])
    present = _available_labels(df)
    chosen = [L for L in requested if L != "Combined" and L in present] if requested else present

    # Per-label plots & summaries
    per_label_rows = []
    for L in chosen:
        series = _series_for_label(df, L)
        if series is None:
            continue
        x, y, _ = series
        out_png = out_dir / f"{ml_base}_{L}_DriftPlot.png"
        _plot_single(L, x, y, f"Drift vs Event Index — {ml_base} [{L}]", out_png)
        stats = _summarize(y)
        pd.DataFrame([stats]).to_csv(out_dir / f"{ml_base}_{L}_DriftSummary.csv", index=False)
        per_label_rows.append({"label": L, **stats})

    # Combined overlay (requested explicitly via 'Combined' or if ≥2 labels chosen)
    if ("Combined" in requested) or (len(chosen) >= 2):
        fig, ax = plt.subplots(figsize=(12, 4))
        combined_rows = []
        for L in chosen:
            series = _series_for_label(df, L)
            if series is None:
                continue
            x, y, _ = series
            ax.scatter(x, y, label=L)
            combined_rows.append({"label": L, **_summarize(y)})
        ax.set_title(f"Drift vs Event Index — {ml_base} [Combined]")
        ax.set_xlabel("Event Index")
        ax.set_ylabel("Drift (s)")
        ax.grid(True, alpha=0.4)
        ax.legend()
        fig.savefig(out_dir / f"{ml_base}_Combined_DriftPlot.png", dpi=150)
        plt.close(fig)
        if combined_rows:
            pd.DataFrame(combined_rows).to_csv(out_dir / f"{ml_base}_Combined_DriftSummary.csv", index=False)

    # If nothing produced, hint at missing columns
    if not per_label_rows and not (("Combined" in requested) or (len(chosen) >= 2)):
        print("[skip] no drift columns present for requested labels; nothing to summarize/plot")


if __name__ == "__main__":
    main()