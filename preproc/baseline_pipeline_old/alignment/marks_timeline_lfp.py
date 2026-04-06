#!/usr/bin/env python3
# marks_timeline_lfp.py
"""
Overlay Raspberry Pi marks (blue) against LFP marks (black) on a single datetime axis.
LFP CSV must contain a column named 'time_abs' (absolute datetime strings).
RPi CSV may have any absolute datetime column; we auto-detect or you can specify via --rpi-col.

Examples:
  python marks_timeline_lfp.py \
    --lfp  /path/to/R037_MorningMarks_LFP.csv \
    --rpi  /path/to/ObsReward_B_03_17_2025_11_15_RNS_RPi_unified.csv \
    --out  ./lfp_rpi_timeline.png --dpi 150 --show

  # explicitly choose the RPi time column
  python marks_timeline_lfp.py --lfp *_LFP.csv --rpi *_RNS_RPi_unified.csv --rpi-col RPi_Time_verb
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# ------------------- helpers -------------------
def detect_rpi_time_column(df: pd.DataFrame, prefer: Optional[str] = None) -> Optional[str]:
    """Pick a reasonable datetime-like column in the RPi table."""
    if prefer and prefer in df.columns:
        return prefer
    # common absolute-datetime columns seen in user data
    candidates: Sequence[str] = (
        "RPi_Time_unified", "RPi_Time_verb", "ML_Time_verb",
        "RPi_Time_simple", "Mono_Time_verb", "Mono_Time_Raw_verb",
        "time_abs", "timestamp", "time", "datetime"
    )
    for c in candidates:
        if c in df.columns:
            return c
    # last resort: any object column containing time-ish words
    for c in df.columns:
        if df[c].dtype == object and any(k in c.lower() for k in ("time", "timestamp", "date", "datetime")):
            return c
    return None


# ------------------- plotting -------------------
def render_timeline(
    lfp_times: pd.Series,
    rpi_times: pd.Series,
    out_path: Path,
    dpi: int,
    show: bool,
) -> None:
    # coerce to datetime
    lfp_dt = pd.to_datetime(lfp_times, errors="coerce", infer_datetime_format=True).dropna()
    rpi_dt = pd.to_datetime(rpi_times, errors="coerce", infer_datetime_format=True).dropna()

    if lfp_dt.empty:
        raise RuntimeError("No valid datetimes found in LFP 'time_abs'.")
    if rpi_dt.empty:
        raise RuntimeError("No valid datetimes found in RPi column.")

    t0 = min(lfp_dt.min(), rpi_dt.min())
    t1 = max(lfp_dt.max(), rpi_dt.max())

    plt.figure(figsize=(11, 4.0))

    # background span
    plt.axvspan(t0, t1, alpha=0.05)

    # RPi marks at y=0 (blue), LFP marks at y=1 (black)
    plt.stem(rpi_dt.values, np.zeros(len(rpi_dt)), linefmt='b-', markerfmt='bo', basefmt=' ')
    plt.stem(lfp_dt.values, np.ones(len(lfp_dt)), linefmt='k-', markerfmt='ko', basefmt=' ')

    plt.yticks([0, 1], ["RPi Mark (blue)", "LFP Mark (black)"])
    plt.xlabel("Time (absolute)")
    plt.title("RPi vs LFP Marks — Absolute Timeline")
    plt.tight_layout()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=dpi, bbox_inches="tight")
    if show:
        plt.show()
    plt.close()


# ------------------- CLI -------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="Overlay RPi marks vs LFP marks on one absolute-time axis.")
    ap.add_argument("--lfp", required=True, type=Path, help="LFP CSV path (must have 'time_abs' column).")
    ap.add_argument("--rpi", required=True, type=Path, help="RPi CSV path (must have an absolute datetime column).")
    ap.add_argument("--rpi-col", type=str, default=None, help="Explicit RPi datetime column (overrides auto-detect).")
    ap.add_argument("--out", type=Path, default=Path("./lfp_rpi_timeline.png"), help="Output PNG path.")
    ap.add_argument("--dpi", type=int, default=150, help="PNG DPI.")
    ap.add_argument("--show", action="store_true", help="Show the plot window.")
    args = ap.parse_args()

    # read LFP
    lfp = pd.read_csv(args.lfp)
    if "time_abs" not in lfp.columns:
        raise RuntimeError("LFP CSV must contain a 'time_abs' column.")
    lfp_times = lfp["time_abs"]

    # read RPi and pick its time column
    rpi = pd.read_csv(args.rpi)
    rpi_col = detect_rpi_time_column(rpi, args.rpi_col)
    if not rpi_col:
        raise RuntimeError("Could not find an absolute datetime column in the RPi CSV. Try --rpi-col.")
    rpi_times = rpi[rpi_col]

    print(f"[INFO] LFP rows: {len(lfp_times)} | RPi rows: {len(rpi_times)} | RPi time column: {rpi_col}")
    render_timeline(lfp_times=lfp_times, rpi_times=rpi_times, out_path=args.out, dpi=args.dpi, show=args.show)
    print(f"[OK] Saved timeline → {args.out}")


if __name__ == "__main__":
    main()
