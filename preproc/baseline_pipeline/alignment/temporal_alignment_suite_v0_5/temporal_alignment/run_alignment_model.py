from __future__ import annotations
import argparse, subprocess, sys
from pathlib import Path


MODULES = {
    "global": (
        "temporal_alignment.align_global.fit_model",
        "temporal_alignment.align_global.apply_model",
        "temporal_alignment.align_global.summarize_model",
        "global_affine_model.json",
        "global_affine_mark_diagnostics.csv",
    ),
    "piecewise": (
        "temporal_alignment.align_blind_piecewise.fit_model",
        "temporal_alignment.align_blind_piecewise.apply_model",
        "temporal_alignment.align_blind_piecewise.summarize_model",
        "blind_piecewise_model.json",
        "blind_piecewise_mark_diagnostics.csv",
    ),
    "cluster-aware": (
        "temporal_alignment.align_cluster_aware.fit_model",
        "temporal_alignment.align_cluster_aware.apply_model",
        "temporal_alignment.align_cluster_aware.summarize_model",
        "cluster_aware_model.json",
        "cluster_aware_mark_diagnostics.csv",
    ),
}


def run(cmd):
    print("[cmd]", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main():
    ap=argparse.ArgumentParser(description="Fit, apply, and summarize one alignment model.")
    ap.add_argument("--model",required=True,choices=MODULES)
    ap.add_argument("--matched-marks",required=True)
    ap.add_argument("--events-csv",required=True)
    ap.add_argument("--time-col",required=True)
    ap.add_argument("--out-dir",required=True)
    ap.add_argument("--min-points",type=int,default=4)
    ap.add_argument("--penalty",type=float,default=0.0025)
    args=ap.parse_args()

    fit_mod,apply_mod,sum_mod,model_name,diag_name=MODULES[args.model]
    od=Path(args.out_dir)
    fit_dir=od/"fit"
    apply_dir=od/"applied"
    summary_dir=od/"summary"
    fit_dir.mkdir(parents=True,exist_ok=True)
    apply_dir.mkdir(parents=True,exist_ok=True)
    summary_dir.mkdir(parents=True,exist_ok=True)

    fit_cmd=[sys.executable,"-m",fit_mod,
             "--matched-marks",args.matched_marks,
             "--out-dir",str(fit_dir)]
    if args.model=="piecewise":
        fit_cmd += ["--min-points",str(args.min_points),"--penalty",str(args.penalty)]
    run(fit_cmd)

    model_json=fit_dir/model_name
    diag_csv=fit_dir/diag_name
    aligned_csv=apply_dir/f"events_{args.model}_aligned.csv"

    run([sys.executable,"-m",apply_mod,
         "--events-csv",args.events_csv,
         "--model-json",str(model_json),
         "--time-col",args.time_col,
         "--out",str(aligned_csv)])

    run([sys.executable,"-m",sum_mod,
         "--model-json",str(model_json),
         "--diagnostics-csv",str(diag_csv),
         "--out-dir",str(summary_dir)])

    print(f"[ok] complete {args.model} suite -> {od}")


if __name__=="__main__":
    main()
