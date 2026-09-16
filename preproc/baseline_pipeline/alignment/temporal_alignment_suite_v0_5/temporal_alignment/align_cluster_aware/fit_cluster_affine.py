from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv, write_json
from temporal_alignment.common.model_io import require_columns, MATCH_REQUIRED, fit_mask
from temporal_alignment.common.stats import summarize_residuals


def boundary_estimate(df):
    x = df["raw_offset_s"].astype(float).to_numpy()
    x = x[np.isfinite(x)]
    if len(x) == 0:
        return np.nan
    return float(np.median(x))


def main():
    ap=argparse.ArgumentParser(description="Reviewed-cluster-aware chunk affine alignment.")
    ap.add_argument("--matched-marks",required=True)
    ap.add_argument("--out-dir",required=True)
    args=ap.parse_args()

    df=read_csv(args.matched_marks)
    require_columns(df,MATCH_REQUIRED,"matched marks")
    for c in ["chunk_id","chunk_role","cluster_id"]:
        if c not in df:
            raise KeyError(f"Cluster-aware alignment requires {c!r} in master/matched marks")
    usable=df.loc[fit_mask(df)].copy()
    pieces=[]
    summaries=[]

    for chunk_id, ch in usable.loc[usable["chunk_id"].notna()].groupby("chunk_id", sort=False):
        start=ch.loc[ch["chunk_role"]=="start"]
        end=ch.loc[ch["chunk_role"]=="end"]
        if start.empty or end.empty:
            summaries.append({"chunk_id":chunk_id,"status":"skipped_missing_boundary"})
            continue

        ds=boundary_estimate(start)
        de=boundary_estimate(end)
        ts=pd.to_datetime(start["corrected_ml_time"]).astype("int64").to_numpy(float)/1e9
        te=pd.to_datetime(end["corrected_ml_time"]).astype("int64").to_numpy(float)/1e9
        # Median corrected ML time is the temporal representative of each boundary cluster.
        T0=float(np.median(ts)); T1=float(np.median(te))
        if T1<=T0:
            summaries.append({"chunk_id":chunk_id,"status":"skipped_nonpositive_duration"})
            continue

        chunk_rows=df.loc[df["chunk_id"].eq(chunk_id) & df["matched"].fillna(False).astype(bool)].copy()
        t=pd.to_datetime(chunk_rows["corrected_ml_time"]).astype("int64").to_numpy(float)/1e9
        frac=(t-T0)/(T1-T0)
        fitted_offset=ds+(de-ds)*frac
        pred=t+fitted_offset
        obs=pd.to_datetime(chunk_rows["rpi_time"]).astype("int64").to_numpy(float)/1e9
        resid=obs-pred

        chunk_rows["model_name"]="cluster_aware_affine"
        chunk_rows["segment_id"]=str(chunk_id)
        chunk_rows["predicted_rpi_time"]=pd.to_datetime(pred,unit="s")
        chunk_rows["aligned_time"]=chunk_rows["predicted_rpi_time"]
        chunk_rows["predicted_offset_s"]=fitted_offset
        chunk_rows["residual_s"]=resid
        chunk_rows["used_for_fit"]=chunk_rows["chunk_role"].isin(["start","end"]) & ~chunk_rows["effective_excluded"].fillna(False).astype(bool)
        chunk_rows["held_out"]=False
        pieces.append(chunk_rows)

        drift_rate=(de-ds)/(T1-T0)
        summaries.append({
            "chunk_id":chunk_id,"status":"ok",
            "start_cluster_id":"|".join(sorted(start["cluster_id"].dropna().astype(str).unique())),
            "end_cluster_id":"|".join(sorted(end["cluster_id"].dropna().astype(str).unique())),
            "n_start_marks_used":len(start),"n_end_marks_used":len(end),
            "start_offset_s":ds,"end_offset_s":de,
            "boundary_duration_s":T1-T0,
            "drift_s_per_hour":drift_rate*3600,
            "drift_ms_per_min":drift_rate*60000,
            "clock_skew_ppm":drift_rate*1e6,
            **summarize_residuals(resid),
        })

    if not pieces:
        raise ValueError("No complete chunks with usable start/end boundaries")
    result=pd.concat(pieces,ignore_index=True)
    od=Path(args.out_dir)
    write_csv(result,od/"alignment_cluster_aware.csv")
    write_csv(pd.DataFrame(summaries),od/"alignment_cluster_aware_chunks.csv")
    write_json({
        "model_name":"cluster_aware_affine",
        "n_complete_chunks":int(sum(x.get("status")=="ok" for x in summaries)),
        **summarize_residuals(result["residual_s"].to_numpy(float))
    },od/"alignment_cluster_aware_summary.json")
    print(f"[ok] cluster-aware affine -> {od}")


if __name__=="__main__":
    main()
