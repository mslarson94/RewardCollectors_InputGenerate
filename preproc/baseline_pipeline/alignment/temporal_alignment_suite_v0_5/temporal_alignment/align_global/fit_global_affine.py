from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv, write_json
from temporal_alignment.common.model_io import require_columns, MATCH_REQUIRED, fit_mask
from temporal_alignment.common.affine import fit_affine
from temporal_alignment.common.stats import summarize_residuals


def main():
    ap = argparse.ArgumentParser(description="Cluster-blind global affine alignment.")
    ap.add_argument("--matched-marks", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--robust", action=argparse.BooleanOptionalAction, default=True)
    args = ap.parse_args()

    df = read_csv(args.matched_marks)
    require_columns(df, MATCH_REQUIRED, "matched marks")
    mask = fit_mask(df)
    work = df.loc[mask].copy()
    ml = pd.to_datetime(work["corrected_ml_time"])
    rp = pd.to_datetime(work["rpi_time"])
    x = ml.astype("int64").to_numpy(float)/1e9
    y = rp.astype("int64").to_numpy(float)/1e9
    model = fit_affine(x, y, robust=args.robust)
    pred = model.predict_unix_s(x)
    resid = y - pred

    result = work.copy()
    result["model_name"] = "global_affine"
    result["predicted_rpi_time"] = pd.to_datetime(pred, unit="s")
    result["aligned_time"] = result["predicted_rpi_time"]
    result["predicted_offset_s"] = pred - x
    result["residual_s"] = resid
    result["used_for_fit"] = True
    result["held_out"] = False
    result["segment_id"] = "global"

    od = Path(args.out_dir)
    write_csv(result, od/"alignment_global_affine.csv")
    write_json({
        "model_name":"global_affine",
        "affine_slope":model.slope,
        "affine_intercept_s":model.intercept_s,
        "clock_skew_ppm":model.skew_ppm,
        **summarize_residuals(resid),
    }, od/"alignment_global_affine_summary.json")
    print(f"[ok] global affine -> {od}")


if __name__ == "__main__":
    main()
