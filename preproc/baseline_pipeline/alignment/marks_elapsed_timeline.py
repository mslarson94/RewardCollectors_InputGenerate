#!/usr/bin/env python3
# marks_elapsed_timeline.py
"""
Plot LFP and/or RPi marks on an elapsed-seconds axis where the first timestamp is set to 0.

By default, the origin (t=0) is the earliest timestamp across all provided series ("global").
You can force the origin to the first LFP mark or the first RPi mark with --origin lfp|rpi.

Inputs:
  - LFP CSV with a datetime column (default: 'time_abs')
  - RPi CSV with a datetime column (auto-detected or specified via --rpi-col)

Examples:
  python marks_elapsed_timeline.py \
    --lfp R037_MorningMarks_LFP.csv \
    --rpi ObsReward_B_03_17_2025_11_15_RNS_RPi_unified.csv \
    --rpi-col RPi_Time_verb \
    --out elapsed_timeline.png --dpi 150 --show

  # Only LFP:
  python marks_elapsed_timeline.py --lfp *_LFP.csv --out lfp_elapsed.png --show

"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Sequence, Tuple

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


def compute_elapsed_seconds(
    lfp_dt: Optional[pd.Series],
    rpi_dt: Optional[pd.Series],
    origin: str
) -> Tuple[Optional[np.ndarray], Optional[np.ndarray]]:
    """Return elapsed seconds arrays for lfp_dt and rpi_dt per chosen origin."""
    has_lfp = lfp_dt is not None and not lfp_dt.empty
    has_rpi = rpi_dt is not None and not rpi_dt.empty

    if not has_lfp and not has_rpi:
        raise RuntimeError("No valid timestamps provided.")

    if origin == "lfp":
        if not has_lfp:
            raise RuntimeError("Requested origin 'lfp' but LFP times are empty.")
        t0 = lfp_dt.min()
    elif origin == "rpi":
        if not has_rpi:
            raise RuntimeError("Requested origin 'rpi' but RPi times are empty.")
        t0 = rpi_dt.min()
    else:  # global
        candidates = []
        if has_lfp: candidates.append(lfp_dt.min())
        if has_rpi: candidates.append(rpi_dt.min())
        t0 = min(candidates)

    lfp_sec = None
    rpi_sec = None
    if has_lfp:
        lfp_sec = (lfp_dt - t0).dt.total_seconds().to_numpy()
    if has_rpi:
        rpi_sec = (rpi_dt - t0).dt.total_seconds().to_numpy()
    return lfp_sec, rpi_sec


def main() -> None:
    ap = argparse.ArgumentParser(description="Plot LFP and/or RPi marks vs elapsed seconds (t0=0).")
    ap.add_argument("--lfp", type=Path, help="LFP CSV path (default LFP column: 'time_abs').")
    ap.add_argument("--lfp-col", type=str, default="time_abs", help="LFP datetime column name (default: time_abs).")
    ap.add_argument("--rpi", type=Path, help="RPi CSV path.")
    ap.add_argument("--rpi-col", type=str, default=None, help="RPi datetime column name (auto-detected if omitted).")
    ap.add_argument("--origin", type=str, choices=("global", "lfp", "rpi"), default="global",
                    help="Where t=0 is set: earliest across both (global), LFP first, or RPi first.")
    ap.add_argument("--out", type=Path, default=Path("./elapsed_timeline.png"), help="Output PNG path.")
    ap.add_argument("--dpi", type=int, default=150, help="PNG DPI.")
    ap.add_argument("--show", action="store_true", help="Show the plot window.")
    args = ap.parse_args()

    if not args.lfp and not args.rpi:
        raise SystemExit("Provide at least one CSV via --lfp and/or --rpi")

    lfp_dt = None
    rpi_dt = None

    if args.lfp:
        lfp = pd.read_csv(args.lfp)
        if args.lfp_col not in lfp.columns:
            raise RuntimeError(f"LFP column '{args.lfp_col}' not found. Available: {list(lfp.columns)}")
        lfp_dt = to_datetime(lfp[args.lfp_col]).dropna()

    if args.rpi:
        rpi = pd.read_csv(args.rpi)
        rpi_col = args.rpi_col or detect_rpi_time_column(rpi, None)
        if not rpi_col:
            raise RuntimeError("Could not detect an RPi time column; provide --rpi-col.")
        rpi_dt = to_datetime(rpi[rpi_col]).dropna()

    lfp_sec, rpi_sec = compute_elapsed_seconds(lfp_dt, rpi_dt, args.origin)

    # Plot
    plt.figure(figsize=(11, 3.8))
    ylabels = []
    yticks = []

    if rpi_sec is not None:
        plt.stem(rpi_sec, np.zeros_like(rpi_sec), basefmt=" ")
        ylabels.append("RPi")
        yticks.append(0)

    if lfp_sec is not None:
        plt.stem(lfp_sec, np.ones_like(lfp_sec), basefmt=" ")
        ylabels.append("LFP")
        yticks.append(1)

    plt.yticks(yticks, ylabels)
    plt.xlabel("Elapsed seconds (t0 = 0)")
    plt.title(f"Elapsed Timeline (origin: {args.origin})")
    plt.tight_layout()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.out, dpi=args.dpi, bbox_inches="tight")
    if args.show:
        plt.show()
    plt.close()

    print(f"[OK] Saved plot → {args.out}")


if __name__ == "__main__":
    main()
