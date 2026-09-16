#!/usr/bin/env python3
# marks_elapsed_compare.py
"""
Compare RPi vs LFP mark patterns by *per-stream* elapsed seconds.
Each stream is zeroed at its own first timestamp, so clock offsets are removed.
This helps you see if the *pattern* of marks matches even if clocks are misaligned (e.g., LFP 4 min behind).

Usage examples:
  python marks_elapsed_compare.py \
    --lfp R037_MorningMarks_LFP.csv --lfp-col time_abs \
    --rpi ObsReward_B_03_17_2025_11_15_RNS_RPi_unified.csv --rpi-col RPi_Time_verb \
    --out elapsed_compare.png --export elapsed_values.csv --plot-iei --show
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", infer_datetime_format=True)


def detect_rpi_time_column(df: pd.DataFrame, prefer: Optional[str] = None) -> Optional[str]:
    if prefer and prefer in df.columns:
        return prefer
    candidates: Sequence[str] = (
        "RPi_Time_unified", "RPi_Time_verb", "ML_Time_verb",
        "RPi_Time_simple", "Mono_Time_verb", "Mono_Time_Raw_verb",
        "time_abs", "timestamp", "time", "datetime"
    )
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        if df[c].dtype == object and any(k in c.lower() for k in ("time", "timestamp", "date", "datetime")):
            return c
    return None


def elapsed_from_first(dt: pd.Series) -> np.ndarray:
    if dt.empty:
        return np.array([], dtype=float)
    t0 = dt.min()
    return (dt - t0).dt.total_seconds().to_numpy()


def main() -> None:
    ap = argparse.ArgumentParser(description="Plot per-stream elapsed-seconds timelines for LFP and RPi marks.")
    ap.add_argument("--lfp", type=Path, required=True, help="LFP CSV path.")
    ap.add_argument("--lfp-col", type=str, default="time_abs", help="LFP datetime column name (default: time_abs).")
    ap.add_argument("--rpi", type=Path, required=True, help="RPi CSV path.")
    ap.add_argument("--rpi-col", type=str, default=None, help="RPi datetime column name (auto-detected if omitted).")
    ap.add_argument("--out", type=Path, default=Path("./elapsed_compare.png"), help="Output PNG path for timeline plot.")
    ap.add_argument("--export", type=Path, help="Optional CSV to export elapsed seconds for both series.")
    ap.add_argument("--plot-iei", action="store_true", help="Also save a second figure plotting inter-event intervals (IEI).")
    ap.add_argument("--show", action="store_true", help="Show plots interactively.")
    ap.add_argument("--dpi", type=int, default=150, help="DPI for saved figures.")
    args = ap.parse_args()

    # Load data
    lfp_df = pd.read_csv(args.lfp)
    if args.lfp_col not in lfp_df.columns:
        raise RuntimeError(f"LFP column '{args.lfp_col}' not found. Available: {list(lfp_df.columns)}")
    lfp_dt = to_datetime(lfp_df[args.lfp_col]).dropna()

    rpi_df = pd.read_csv(args.rpi)
    rpi_col = args.rpi_col or detect_rpi_time_column(rpi_df, None)
    if not rpi_col:
        raise RuntimeError("Could not detect RPi time column; specify --rpi-col.")
    rpi_dt = to_datetime(rpi_df[rpi_col]).dropna()

    # Compute per-stream elapsed seconds (t=0 at each stream's first mark)
    lfp_sec = elapsed_from_first(lfp_dt)
    rpi_sec = elapsed_from_first(rpi_dt)

    # Basic summary
    print(f"[INFO] LFP marks: {len(lfp_sec)}, first={lfp_dt.min()}, last={lfp_dt.max()}")
    print(f"[INFO] RPi marks: {len(rpi_sec)}, first={rpi_dt.min()}, last={rpi_dt.max()}")

    # --- Plot elapsed timeline ---
    plt.figure(figsize=(11, 3.8))
    # RPi at y=0, LFP at y=1 (no explicit colors per platform policy)
    plt.stem(rpi_sec, np.zeros_like(rpi_sec), basefmt=" ")
    plt.stem(lfp_sec, np.ones_like(lfp_sec), basefmt=" ")
    plt.yticks([0, 1], ["RPi (elapsed)", "LFP (elapsed)"])
    plt.xlabel("Elapsed seconds from first mark (per stream)")
    plt.title("Per-stream Elapsed Timelines (RPi vs LFP)")
    plt.tight_layout()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.out, dpi=args.dpi, bbox_inches="tight")
    if args.show:
        plt.show()
    plt.close()
    print(f"[OK] Saved timeline → {args.out}")

    # --- Optional: plot IEI (inter-event intervals) to compare patterns ---
    if args.plot_iei:
        def iei(x: np.ndarray) -> np.ndarray:
            return np.diff(x) if x.size >= 2 else np.array([], dtype=float)

        lfp_iei = iei(lfp_sec)
        rpi_iei = iei(rpi_sec)

        plt.figure(figsize=(11, 3.8))
        # Plot as scatter vs event index so you can compare sequences visually
        plt.plot(np.arange(lfp_iei.size), lfp_iei, marker="o", linestyle="none", label="LFP IEI")
        plt.plot(np.arange(rpi_iei.size), rpi_iei, marker="x", linestyle="none", label="RPi IEI")
        plt.xlabel("Interval index")
        plt.ylabel("Inter-event interval (s)")
        plt.title("Inter-event Intervals (LFP vs RPi)")
        plt.legend()
        plt.tight_layout()

        iei_out = args.out.with_name(args.out.stem + "_iei" + args.out.suffix)
        plt.savefig(iei_out, dpi=args.dpi, bbox_inches="tight")
        if args.show:
            plt.show()
        plt.close()
        print(f"[OK] Saved IEI plot → {iei_out}")

    # --- Optional export of elapsed values ---
    if args.export:
        # pad the shorter series with NaN so they can sit side-by-side
        maxlen = max(len(lfp_sec), len(rpi_sec))
        lcol = np.full(maxlen, np.nan)
        rcol = np.full(maxlen, np.nan)
        lcol[:len(lfp_sec)] = lfp_sec
        rcol[:len(rpi_sec)] = rpi_sec
        out_df = pd.DataFrame({"lfp_elapsed_sec": lcol, "rpi_elapsed_sec": rcol})
        args.export.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(args.export, index=False)
        print(f"[OK] Exported elapsed values → {args.export}")


if __name__ == "__main__":
    main()
