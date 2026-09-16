from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv, write_json
from temporal_alignment.common.model_io import require_columns, MATCH_REQUIRED, fit_mask
from temporal_alignment.common.affine import fit_affine
from temporal_alignment.common.apply import to_unix_s
from temporal_alignment.common.stats import summarize_residuals


def main():
    ap = argparse.ArgumentParser(description="Fit one cluster-blind affine model over the full session.")
    ap.add_argument("--matched-marks", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--robust", action=argparse.BooleanOptionalAction, default=True)
    args = ap.parse_args()

    df = read_csv(args.matched_marks)
    require_columns(df, MATCH_REQUIRED, "matched marks")
    work = df.loc[fit_mask(df)].sort_values("corrected_ml_time").reset_index(drop=True)
    if len(work) < 2:
        raise ValueError("Need at least two usable matches")

    x = to_unix_s(work["corrected_ml_time"])
    y = to_unix_s(work["rpi_time"])
    model = fit_affine(x, y, robust=args.robust)
    pred = model.predict_unix_s(x)
    resid = y - pred

    diag = work.copy()
    diag["model_name"] = "global_affine"
    diag["predicted_rpi_time"] = pd.to_datetime(pred, unit="s")
    diag["predicted_offset_s"] = pred - x
    diag["residual_s"] = resid
    diag["used_for_fit"] = True

    payload = {
        "model_type": "global_affine",
        "version": 1,
        "robust": bool(args.robust),
        "slope": model.slope,
        "intercept_s": model.intercept_s,
        "origin_unix_s": model.origin_unix_s,
        "clock_skew_ppm": model.skew_ppm,
        "n_used": model.n_used,
        "fit_time_start": str(pd.to_datetime(work["corrected_ml_time"]).min()),
        "fit_time_end": str(pd.to_datetime(work["corrected_ml_time"]).max()),
        "match_mode": str(work["match_mode"].iloc[0]) if len(work) else "",
        "fit_metrics": summarize_residuals(resid),
    }

    od = Path(args.out_dir)
    write_json(payload, od / "global_affine_model.json")
    write_csv(diag, od / "global_affine_mark_diagnostics.csv")
    print(f"[ok] fitted global affine model -> {od}")


if __name__ == "__main__":
    main()
