from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv, write_json
from temporal_alignment.common.model_io import require_columns, MATCH_REQUIRED, fit_mask
from temporal_alignment.common.apply import to_unix_s
from temporal_alignment.common.stats import summarize_residuals


def med_offset(df):
    x = df["raw_offset_s"].astype(float).to_numpy()
    x = x[np.isfinite(x)]
    if len(x) == 0:
        raise ValueError("Boundary has no usable offsets")
    return float(np.median(x))


def main():
    ap=argparse.ArgumentParser(description="Fit reviewed chunk-aware affine offset interpolation models.")
    ap.add_argument("--matched-marks",required=True)
    ap.add_argument("--out-dir",required=True)
    args=ap.parse_args()

    df=read_csv(args.matched_marks)
    require_columns(df,MATCH_REQUIRED,"matched marks")
    require_columns(df,["chunk_id","chunk_role","cluster_id"],"cluster-aware matched marks")
    usable=df.loc[fit_mask(df)].copy()

    models=[]
    diags=[]
    for chunk_id,ch in usable.loc[usable["chunk_id"].notna()].groupby("chunk_id",sort=False):
        start=ch.loc[ch["chunk_role"].eq("start")]
        end=ch.loc[ch["chunk_role"].eq("end")]
        if start.empty or end.empty:
            continue

        ds,de=med_offset(start),med_offset(end)
        ts=to_unix_s(start["corrected_ml_time"])
        te=to_unix_s(end["corrected_ml_time"])
        T0,T1=float(np.median(ts)),float(np.median(te))
        if T1<=T0:
            continue
        drift_rate=(de-ds)/(T1-T0)

        all_chunk=df.loc[df["chunk_id"].eq(chunk_id) & df["matched"].fillna(False).astype(bool)].copy()
        t=to_unix_s(all_chunk["corrected_ml_time"])
        frac=(t-T0)/(T1-T0)
        off=ds+(de-ds)*frac
        pred=t+off
        obs=to_unix_s(all_chunk["rpi_time"])
        resid=obs-pred

        models.append({
            "chunk_id":str(chunk_id),
            "start_cluster_ids":sorted(start["cluster_id"].dropna().astype(str).unique().tolist()),
            "end_cluster_ids":sorted(end["cluster_id"].dropna().astype(str).unique().tolist()),
            "start_boundary_ml_unix_s":T0,
            "end_boundary_ml_unix_s":T1,
            "start_boundary_ml_time":str(pd.to_datetime(T0,unit="s")),
            "end_boundary_ml_time":str(pd.to_datetime(T1,unit="s")),
            "start_offset_s":ds,
            "end_offset_s":de,
            "drift_rate_s_per_s":drift_rate,
            "clock_skew_ppm":drift_rate*1e6,
            "n_start_marks_used":len(start),
            "n_end_marks_used":len(end),
            "fit_metrics":summarize_residuals(resid),
        })

        d=all_chunk.copy()
        d["model_name"]="cluster_aware_affine"
        d["segment_id"]=str(chunk_id)
        d["predicted_rpi_time"]=pd.to_datetime(pred,unit="s")
        d["predicted_offset_s"]=off
        d["residual_s"]=resid
        d["used_for_fit"]=d["chunk_role"].isin(["start","end"]) & ~d["effective_excluded"].fillna(False).astype(bool)
        diags.append(d)

    if not models:
        raise ValueError("No complete chunks with usable start/end boundaries")

    # Time-based application windows use midpoints between adjacent chunks.
    for i,m in enumerate(models):
        if i==0:
            m["apply_start_ml_unix_s"]=None
        else:
            m["apply_start_ml_unix_s"]=(models[i-1]["end_boundary_ml_unix_s"]+m["start_boundary_ml_unix_s"])/2
        if i==len(models)-1:
            m["apply_end_ml_unix_s"]=None
        else:
            m["apply_end_ml_unix_s"]=(m["end_boundary_ml_unix_s"]+models[i+1]["start_boundary_ml_unix_s"])/2

    payload={
        "model_type":"cluster_aware_affine",
        "version":1,
        "boundary_estimator":"median_raw_offset",
        "match_mode":str(usable["match_mode"].iloc[0]) if len(usable) else "",
        "chunks":models,
    }
    od=Path(args.out_dir)
    write_json(payload,od/"cluster_aware_model.json")
    write_csv(pd.concat(diags,ignore_index=True),od/"cluster_aware_mark_diagnostics.csv")
    print(f"[ok] fitted cluster-aware model -> {od}")


if __name__=="__main__":
    main()
