from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from temporal_alignment.common.io import read_csv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-json", required=True)
    ap.add_argument("--diagnostics-csv", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    model = json.loads(Path(args.model_json).read_text())
    d = read_csv(args.diagnostics_csv)
    od = Path(args.out_dir)
    od.mkdir(parents=True, exist_ok=True)

    rows=[]
    for seg in model["segments"]:
        rows.append({
            "segment_id": seg["segment_id"],
            "start_ml_time": seg["start_ml_time"],
            "end_ml_time": seg["end_ml_time"],
            "slope": seg["slope"],
            "intercept_s": seg["intercept_s"],
            "clock_skew_ppm": seg["clock_skew_ppm"],
            "n_used": seg["n_used"],
            **seg["fit_metrics"],
        })
    pd.DataFrame(rows).to_csv(od/"blind_piecewise_summary.csv", index=False)

    fig,ax=plt.subplots(figsize=(10,4))
    t=pd.to_datetime(d["corrected_ml_time"])
    x=(t-t.min()).dt.total_seconds()/60
    for sid,g in d.groupby("segment_id"):
        tt=pd.to_datetime(g["corrected_ml_time"])
        xx=(tt-t.min()).dt.total_seconds()/60
        ax.scatter(xx,g["raw_offset_s"].astype(float),s=18,label=f"{sid} raw")
        ax.plot(xx,g["predicted_offset_s"].astype(float))
    ax.set_xlabel("Elapsed corrected ML time (min)")
    ax.set_ylabel("Offset (s)")
    ax.set_title("Blind piecewise affine alignment")
    ax.grid(True,alpha=.3)
    ax.legend(ncol=2,fontsize=8)
    fig.savefig(od/"blind_piecewise_fit.png",dpi=160,bbox_inches="tight")
    plt.close(fig)
    print(f"[ok] summarized blind piecewise -> {od}")


if __name__=="__main__":
    main()
