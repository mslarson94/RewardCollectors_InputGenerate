from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--model-json",required=True)
    ap.add_argument("--diagnostics-csv",required=True)
    ap.add_argument("--out-dir",required=True)
    args=ap.parse_args()

    model=json.loads(Path(args.model_json).read_text())
    d=pd.read_csv(args.diagnostics_csv)
    od=Path(args.out_dir)
    od.mkdir(parents=True,exist_ok=True)

    rows=[]
    for ch in model["chunks"]:
        rows.append({
            "chunk_id":ch["chunk_id"],
            "start_boundary_ml_time":ch["start_boundary_ml_time"],
            "end_boundary_ml_time":ch["end_boundary_ml_time"],
            "start_offset_s":ch["start_offset_s"],
            "end_offset_s":ch["end_offset_s"],
            "clock_skew_ppm":ch["clock_skew_ppm"],
            "n_start_marks_used":ch["n_start_marks_used"],
            "n_end_marks_used":ch["n_end_marks_used"],
            **ch["fit_metrics"],
        })
    pd.DataFrame(rows).to_csv(od/"cluster_aware_summary.csv",index=False)

    fig,ax=plt.subplots(figsize=(10,4))
    t=pd.to_datetime(d["corrected_ml_time"])
    t0=t.min()
    for cid,g in d.groupby("segment_id"):
        tt=pd.to_datetime(g["corrected_ml_time"])
        x=(tt-t0).dt.total_seconds()/60
        ax.scatter(x,g["raw_offset_s"].astype(float),s=18,label=f"{cid} raw")
        ax.plot(x,g["predicted_offset_s"].astype(float))
    ax.set_xlabel("Elapsed corrected ML time (min)")
    ax.set_ylabel("Offset (s)")
    ax.set_title("Cluster-aware affine alignment")
    ax.grid(True,alpha=.3)
    ax.legend(fontsize=8)
    fig.savefig(od/"cluster_aware_fit.png",dpi=160,bbox_inches="tight")
    plt.close(fig)
    print(f"[ok] summarized cluster-aware -> {od}")


if __name__=="__main__":
    main()
