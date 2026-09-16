from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv


def main():
    ap = argparse.ArgumentParser(description="Produce a compact clock-drift report.")
    ap.add_argument("--drift-summary", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    df = read_csv(args.drift_summary)
    cols = [
        "scope", "status", "n_used", "reference_ml_time", "reference_offset_s",
        "clock_skew_ppm", "drift_s_per_hour", "drift_ms_per_min",
        "duration_s", "raw_offset_start_s", "raw_offset_end_s",
        "observed_offset_change_s", "fitted_offset_change_s",
        "median_residual_s", "median_abs_residual_s", "mad_residual_s",
        "rmse_s", "p95_abs_residual_s", "max_abs_residual_s",
    ]
    keep = [c for c in cols if c in df.columns]
    out = df[keep].copy()
    write_csv(out, args.out)
    print(f"[ok] compact drift report -> {args.out}")


if __name__ == "__main__":
    main()
