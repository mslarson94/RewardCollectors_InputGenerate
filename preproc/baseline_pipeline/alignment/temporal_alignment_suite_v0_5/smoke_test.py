from pathlib import Path
import subprocess, sys, tempfile
import pandas as pd
import numpy as np

root = Path(__file__).resolve().parent
tmp = root / "_smoke"
tmp.mkdir(exist_ok=True)

n=12
base=pd.Timestamp("2025-01-01 12:00:00")
ml=pd.DataFrame({
    "mark_id":[f"events_{i:04d}" for i in range(n)],
    "ordinal":range(n),
    "corrected_ml_time":[base+pd.Timedelta(seconds=30*i) for i in range(n)],
    "source_row_index":range(100,100+n),
})
rpi=pd.DataFrame({
    "mark_id":[f"rpi_{i:04d}" for i in range(n)],
    "ordinal":range(n),
    "mark_time":[base+pd.Timedelta(seconds=30*i+0.4+0.001*i) for i in range(n)],
})
ml.to_csv(tmp/"ml.csv",index=False)
rpi.to_csv(tmp/"rpi.csv",index=False)

env={"PYTHONPATH":str(root)}
def run(args):
    subprocess.run([sys.executable,"-m",*args],check=True,cwd=root,env={**__import__("os").environ,**env})

run(["temporal_alignment.master_marks.build_master_inventory",
     "--corrected-ml-marks",str(tmp/"ml.csv"),"--rpi-marks",str(tmp/"rpi.csv"),
     "--session-id","synthetic","--label","RNS","--out-dir",str(tmp/"master")])
run(["temporal_alignment.mark_matching.build_matched_marks",
     "--master-dir",str(tmp/"master"),"--mode","automatic","--max-match-gap-s","2",
     "--out-dir",str(tmp/"match")])
run(["temporal_alignment.drift.calculate_clock_drift",
     "--matched-marks",str(tmp/"match/matched_marks_automatic.csv"),
     "--out-dir",str(tmp/"drift")])
run(["temporal_alignment.align_global.fit_global_affine",
     "--matched-marks",str(tmp/"match/matched_marks_automatic.csv"),
     "--out-dir",str(tmp/"global")])
run(["temporal_alignment.align_blind_piecewise.fit_piecewise_affine",
     "--matched-marks",str(tmp/"match/matched_marks_automatic.csv"),
     "--min-points","4","--penalty","0.01","--out-dir",str(tmp/"piecewise")])
print("SMOKE TEST PASSED")
