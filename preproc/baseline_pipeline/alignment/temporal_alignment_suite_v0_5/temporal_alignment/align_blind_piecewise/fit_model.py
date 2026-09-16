from __future__ import annotations
import argparse, math
from pathlib import Path
import numpy as np
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv, write_json
from temporal_alignment.common.model_io import require_columns, MATCH_REQUIRED, fit_mask
from temporal_alignment.common.affine import fit_affine
from temporal_alignment.common.apply import to_unix_s
from temporal_alignment.common.stats import summarize_residuals


def segment_cost(x, y, i, j):
    if j - i < 2:
        return math.inf
    xx, yy = x[i:j], y[i:j]
    p = np.polyfit(xx - xx[0], yy - xx[0], 1)
    pred = xx[0] + p[1] + p[0]*(xx - xx[0])
    return float(np.sum((yy - pred)**2))


def find_segments(x, y, min_points, penalty):
    n = len(x)
    dp = [math.inf]*(n+1)
    prev = [-1]*(n+1)
    dp[0] = -penalty
    for j in range(min_points, n+1):
        for i in range(0, j-min_points+1):
            if i > 0 and not math.isfinite(dp[i]):
                continue
            c = segment_cost(x, y, i, j) + penalty
            val = dp[i] + c
            if val < dp[j]:
                dp[j], prev[j] = val, i
    if not math.isfinite(dp[n]):
        return [(0,n)]
    segs, j = [], n
    while j > 0:
        i = prev[j]
        if i < 0:
            return [(0,n)]
        segs.append((i,j))
        j=i
    return list(reversed(segs))


def main():
    ap = argparse.ArgumentParser(description="Fit cluster-blind piecewise affine model.")
    ap.add_argument("--matched-marks", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--min-points", type=int, default=4)
    ap.add_argument("--penalty", type=float, default=0.0025)
    ap.add_argument("--robust", action=argparse.BooleanOptionalAction, default=True)
    args = ap.parse_args()

    df = read_csv(args.matched_marks)
    require_columns(df, MATCH_REQUIRED, "matched marks")
    work = df.loc[fit_mask(df)].sort_values("corrected_ml_time").reset_index(drop=True)
    x = to_unix_s(work["corrected_ml_time"])
    y = to_unix_s(work["rpi_time"])
    if len(x) < max(2, args.min_points):
        raise ValueError("Not enough usable matches")

    segs = find_segments(x, y, args.min_points, args.penalty)
    segment_models, diags = [], []

    for k,(i,j) in enumerate(segs, start=1):
        model = fit_affine(x[i:j], y[i:j], robust=args.robust)
        pred = model.predict_unix_s(x[i:j])
        resid = y[i:j] - pred
        start_x, end_x = float(x[i]), float(x[j-1])

        segment_models.append({
            "segment_id": f"segment_{k}",
            "start_ml_unix_s": start_x,
            "end_ml_unix_s": end_x,
            "start_ml_time": str(pd.to_datetime(start_x, unit="s")),
            "end_ml_time": str(pd.to_datetime(end_x, unit="s")),
            "slope": model.slope,
            "intercept_s": model.intercept_s,
            "origin_unix_s": model.origin_unix_s,
            "clock_skew_ppm": model.skew_ppm,
            "n_used": j-i,
            "fit_metrics": summarize_residuals(resid),
        })

        d = work.iloc[i:j].copy()
        d["model_name"] = "blind_piecewise_affine"
        d["segment_id"] = f"segment_{k}"
        d["predicted_rpi_time"] = pd.to_datetime(pred, unit="s")
        d["predicted_offset_s"] = pred - x[i:j]
        d["residual_s"] = resid
        d["used_for_fit"] = True
        diags.append(d)

    # Application boundaries are midpoints between adjacent fitted segment endpoints.
    for k, seg in enumerate(segment_models):
        if k == 0:
            seg["apply_start_ml_unix_s"] = None
        else:
            seg["apply_start_ml_unix_s"] = (
                segment_models[k-1]["end_ml_unix_s"] + seg["start_ml_unix_s"]
            ) / 2.0
        if k == len(segment_models)-1:
            seg["apply_end_ml_unix_s"] = None
        else:
            seg["apply_end_ml_unix_s"] = (
                seg["end_ml_unix_s"] + segment_models[k+1]["start_ml_unix_s"]
            ) / 2.0

    payload = {
        "model_type": "blind_piecewise_affine",
        "version": 1,
        "robust": bool(args.robust),
        "min_points": args.min_points,
        "penalty": args.penalty,
        "n_segments": len(segment_models),
        "match_mode": str(work["match_mode"].iloc[0]) if len(work) else "",
        "segments": segment_models,
    }

    od = Path(args.out_dir)
    write_json(payload, od/"blind_piecewise_model.json")
    write_csv(pd.concat(diags, ignore_index=True), od/"blind_piecewise_mark_diagnostics.csv")
    print(f"[ok] fitted blind piecewise model -> {od}")


if __name__ == "__main__":
    main()
