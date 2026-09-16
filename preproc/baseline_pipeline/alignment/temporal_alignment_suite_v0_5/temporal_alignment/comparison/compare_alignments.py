from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

from temporal_alignment.common.io import read_csv, write_csv
from temporal_alignment.common.stats import summarize_residuals


def main():
    ap=argparse.ArgumentParser(description="Compare standardized alignment outputs.")
    ap.add_argument("--alignment", action="append", required=True,
                    help="Repeat for each alignment CSV to compare.")
    ap.add_argument("--out-dir", required=True)
    args=ap.parse_args()

    frames=[]
    metrics=[]
    for path in args.alignment:
        df=read_csv(path)
        if "model_name" not in df or "residual_s" not in df:
            raise KeyError(f"{path} lacks model_name/residual_s")
        frames.append(df)
        for model,g in df.groupby("model_name"):
            metrics.append({"model_name":model,"source_file":str(path),**summarize_residuals(g["residual_s"].to_numpy(float))})

    od=Path(args.out_dir)
    summary=pd.DataFrame(metrics).sort_values(["median_abs_residual_s","rmse_s"])
    write_csv(summary,od/"alignment_model_comparison.csv")

    combined=pd.concat(frames,ignore_index=True)
    fig,ax=plt.subplots(figsize=(10,4))
    for model,g in combined.groupby("model_name"):
        times=pd.to_datetime(g["corrected_ml_time"])
        t0=times.min()
        x=(times-t0).dt.total_seconds()/60
        ax.scatter(x,g["residual_s"].astype(float),s=18,label=model)
    ax.axhline(0,linewidth=1)
    ax.set_xlabel("Elapsed time within model data (min)")
    ax.set_ylabel("Residual (s)")
    ax.set_title("Alignment residual comparison")
    ax.grid(True,alpha=.3)
    ax.legend()
    od.mkdir(parents=True,exist_ok=True)
    fig.savefig(od/"alignment_residual_comparison.png",dpi=160,bbox_inches="tight")
    plt.close(fig)
    print(f"[ok] comparison -> {od}")


if __name__=="__main__":
    main()
