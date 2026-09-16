from __future__ import annotations
import argparse, math
from pathlib import Path
import numpy as np
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv, write_json
from temporal_alignment.common.model_io import require_columns, MATCH_REQUIRED, fit_mask
from temporal_alignment.common.affine import fit_affine
from temporal_alignment.common.stats import summarize_residuals


def segment_cost(x, y, i, j):
    if j - i < 2:
        return math.inf
    xx, yy = x[i:j], y[i:j]
    p = np.polyfit(xx - xx[0], yy - xx[0], 1)
    pred = xx[0] + p[1] + p[0]*(xx-xx[0])
    return float(np.sum((yy-pred)**2))


def find_segments(x, y, min_points, penalty):
    n = len(x)
    dp = [math.inf]*(n+1)
    prev = [-1]*(n+1)
    dp[0] = -penalty
    for j in range(min_points, n+1):
        for i in range(0, j-min_points+1):
            if i != 0 and i < min_points:
                continue
            if i > 0 and not math.isfinite(dp[i]):
                continue
            c = segment_cost(x, y, i, j) + penalty
            val = dp[i] + c
            if val < dp[j]:
                dp[j], prev[j] = val, i
    if not math.isfinite(dp[n]):
        return [(0,n)]
    segs=[]
    j=n
    while j>0:
        i=prev[j]
        if i<0:
            return [(0,n)]
        segs.append((i,j))
        j=i
    return list(reversed(segs))


def main():
    ap=argparse.ArgumentParser(description="Cluster-blind data-driven piecewise affine alignment.")
    ap.add_argument("--matched-marks", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--min-points", type=int, default=4)
    ap.add_argument("--penalty", type=float, default=0.0025,
                    help="SSE penalty per additional segment, in squared seconds.")
    ap.add_argument("--robust", action=argparse.BooleanOptionalAction, default=True)
    args=ap.parse_args()

    df=read_csv(args.matched_marks)
    require_columns(df, MATCH_REQUIRED, "matched marks")
    work=df.loc[fit_mask(df)].sort_values("corrected_ml_time").reset_index(drop=True)
    ml=pd.to_datetime(work["corrected_ml_time"])
    rp=pd.to_datetime(work["rpi_time"])
    x=ml.astype("int64").to_numpy(float)/1e9
    y=rp.astype("int64").to_numpy(float)/1e9
    if len(x)<max(2,args.min_points):
        raise ValueError("Not enough marks for piecewise fitting")

    segs=find_segments(x,y,args.min_points,args.penalty)
    pieces=[]
    summaries=[]
    for k,(i,j) in enumerate(segs, start=1):
        model=fit_affine(x[i:j],y[i:j],robust=args.robust)
        pred=model.predict_unix_s(x[i:j])
        r=y[i:j]-pred
        part=work.iloc[i:j].copy()
        part["model_name"]="blind_piecewise_affine"
        part["segment_id"]=f"segment_{k}"
        part["predicted_rpi_time"]=pd.to_datetime(pred,unit="s")
        part["aligned_time"]=part["predicted_rpi_time"]
        part["predicted_offset_s"]=pred-x[i:j]
        part["residual_s"]=r
        part["used_for_fit"]=True
        part["held_out"]=False
        pieces.append(part)
        summaries.append({
            "segment_id":f"segment_{k}","start_index":i,"end_index_exclusive":j,
            "n":j-i,"affine_slope":model.slope,"affine_intercept_s":model.intercept_s,
            "clock_skew_ppm":model.skew_ppm,**summarize_residuals(r)
        })
    result=pd.concat(pieces,ignore_index=True)
    od=Path(args.out_dir)
    write_csv(result,od/"alignment_blind_piecewise.csv")
    write_csv(pd.DataFrame(summaries),od/"alignment_blind_piecewise_segments.csv")
    write_json({
        "model_name":"blind_piecewise_affine",
        "n_segments":len(segs),"min_points":args.min_points,"penalty":args.penalty,
        **summarize_residuals(result["residual_s"].to_numpy(float))
    },od/"alignment_blind_piecewise_summary.json")
    print(f"[ok] blind piecewise affine -> {od}")


if __name__=="__main__":
    main()
