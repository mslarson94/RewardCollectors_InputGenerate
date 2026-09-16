from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from temporal_alignment.common.io import read_csv, write_csv, write_json
from temporal_alignment.common.model_io import require_columns, MATCH_REQUIRED, fit_mask
from temporal_alignment.common.affine import fit_affine
from temporal_alignment.common.stats import summarize_residuals


def main():
    ap = argparse.ArgumentParser(description="Characterize offset and clock drift without altering timestamps.")
    ap.add_argument("--matched-marks", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--robust", action=argparse.BooleanOptionalAction, default=True)
    args = ap.parse_args()

    df = read_csv(args.matched_marks)
    require_columns(df, MATCH_REQUIRED, "matched marks")
    mask = fit_mask(df)
    fitdf = df.loc[mask].copy()
    ml = pd.to_datetime(fitdf["corrected_ml_time"])
    rp = pd.to_datetime(fitdf["rpi_time"])
    ml_unix = ml.astype("int64").to_numpy(float) / 1e9
    rp_unix = rp.astype("int64").to_numpy(float) / 1e9

    model = fit_affine(ml_unix, rp_unix, robust=args.robust)
    pred = model.predict_unix_s(ml_unix)
    resid = rp_unix - pred
    elapsed = ml_unix - ml_unix.min()
    raw = rp_unix - ml_unix

    od = Path(args.out_dir)
    out = fitdf[["match_mode","match_id","ml_mark_id","rpi_mark_id","corrected_ml_time","rpi_time","raw_offset_s"]].copy()
    out["elapsed_s"] = elapsed
    out["fitted_offset_s"] = pred - ml_unix
    out["drift_residual_s"] = resid
    write_csv(out, od / "clock_drift_points.csv")

    stats = {
        "match_mode": str(fitdf["match_mode"].iloc[0]) if len(fitdf) else "",
        "n_used": model.n_used,
        "affine_slope": model.slope,
        "reference_offset_s": model.intercept_s,
        "clock_skew_ppm": model.skew_ppm,
        "drift_s_per_hour": (model.slope - 1.0) * 3600.0,
        "drift_ms_per_min": (model.slope - 1.0) * 60_000.0,
        **summarize_residuals(resid),
    }
    write_json(stats, od / "clock_drift_summary.json")

    fig, ax = plt.subplots(figsize=(10,4))
    ax.scatter(elapsed/60.0, raw, s=20)
    ax.plot(elapsed/60.0, pred-ml_unix)
    ax.set_xlabel("Elapsed ML time (min)")
    ax.set_ylabel("RPi - ML offset (s)")
    ax.set_title("Raw clock difference and fitted offset/drift")
    ax.grid(True, alpha=0.3)
    od.mkdir(parents=True, exist_ok=True)
    fig.savefig(od / "clock_drift.png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"[ok] drift characterization -> {od}")


if __name__ == "__main__":
    main()
