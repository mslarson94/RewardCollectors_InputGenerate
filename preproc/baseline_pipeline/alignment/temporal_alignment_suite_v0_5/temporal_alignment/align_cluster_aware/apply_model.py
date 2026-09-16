from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv
from temporal_alignment.common.apply import to_unix_s


def choose_by_time(u, chunks):
    for ch in chunks:
        lo=ch.get("apply_start_ml_unix_s")
        hi=ch.get("apply_end_ml_unix_s")
        if (lo is None or u>=lo) and (hi is None or u<hi):
            return ch
    return None


def apply_chunk(u, ch):
    T0=ch["start_boundary_ml_unix_s"]
    T1=ch["end_boundary_ml_unix_s"]
    ds=ch["start_offset_s"]
    de=ch["end_offset_s"]
    frac=(u-T0)/(T1-T0)
    off=ds+(de-ds)*frac
    return u+off, off


def main():
    ap=argparse.ArgumentParser(description="Apply reviewed chunk-aware affine model to full event data.")
    ap.add_argument("--events-csv",required=True)
    ap.add_argument("--model-json",required=True)
    ap.add_argument("--time-col",required=True)
    ap.add_argument("--out",required=True)
    ap.add_argument("--chunk-col",default="",
                    help="Optional chunk ID column. If omitted, model chooses chunk by corrected ML time.")
    ap.add_argument("--aligned-col",default="aligned_time")
    ap.add_argument("--outside-policy",choices=["nearest","nan"],default="nearest")
    args=ap.parse_args()

    df=read_csv(args.events_csv)
    if args.time_col not in df:
        raise KeyError(args.time_col)
    model=json.loads(Path(args.model_json).read_text())
    if model.get("model_type")!="cluster_aware_affine":
        raise ValueError("Wrong model type")
    chunks=model["chunks"]
    by_id={str(c["chunk_id"]):c for c in chunks}

    times=pd.to_datetime(df[args.time_col],errors="coerce")
    df[args.aligned_col]=pd.NaT
    df["predicted_offset_s"]=pd.NA
    df["alignment_chunk_id"]=""
    df["alignment_model"]="cluster_aware_affine"

    valid_idx=df.index[times.notna()]
    unix=to_unix_s(times.loc[valid_idx])

    for idx,u in zip(valid_idx,unix):
        ch=None
        if args.chunk_col and args.chunk_col in df and pd.notna(df.at[idx,args.chunk_col]):
            ch=by_id.get(str(df.at[idx,args.chunk_col]))
        if ch is None:
            ch=choose_by_time(float(u),chunks)
        if ch is None and args.outside_policy=="nearest":
            ch=min(chunks,key=lambda x:min(abs(u-x["start_boundary_ml_unix_s"]),abs(u-x["end_boundary_ml_unix_s"])))
        if ch is None:
            continue
        pred,off=apply_chunk(float(u),ch)
        df.at[idx,args.aligned_col]=pd.to_datetime(pred,unit="s")
        df.at[idx,"predicted_offset_s"]=off
        df.at[idx,"alignment_chunk_id"]=str(ch["chunk_id"])

    write_csv(df,args.out)
    print(f"[ok] applied cluster-aware model -> {args.out}")


if __name__=="__main__":
    main()
