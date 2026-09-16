from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv
from temporal_alignment.common.apply import to_unix_s, apply_affine_unix


def choose_segment(t, segments):
    for seg in segments:
        lo = seg.get("apply_start_ml_unix_s")
        hi = seg.get("apply_end_ml_unix_s")
        if (lo is None or t >= lo) and (hi is None or t < hi):
            return seg
    return segments[-1]


def main():
    ap = argparse.ArgumentParser(description="Apply blind piecewise affine model.")
    ap.add_argument("--events-csv", required=True)
    ap.add_argument("--model-json", required=True)
    ap.add_argument("--time-col", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--aligned-col", default="aligned_time")
    args = ap.parse_args()

    df = read_csv(args.events_csv)
    if args.time_col not in df:
        raise KeyError(args.time_col)
    model = json.loads(Path(args.model_json).read_text())
    if model.get("model_type") != "blind_piecewise_affine":
        raise ValueError("Wrong model type")

    t = pd.to_datetime(df[args.time_col], errors="coerce")
    df[args.aligned_col] = pd.NaT
    df["predicted_offset_s"] = pd.NA
    df["alignment_segment_id"] = ""
    df["alignment_model"] = "blind_piecewise_affine"

    valid_idx = df.index[t.notna()]
    unix = to_unix_s(t.loc[valid_idx])
    for idx, u in zip(valid_idx, unix):
        seg = choose_segment(float(u), model["segments"])
        pred = apply_affine_unix(
            [u], seg["slope"], seg["intercept_s"], seg["origin_unix_s"]
        )[0]
        df.at[idx, args.aligned_col] = pd.to_datetime(pred, unit="s")
        df.at[idx, "predicted_offset_s"] = pred - u
        df.at[idx, "alignment_segment_id"] = seg["segment_id"]

    write_csv(df, args.out)
    print(f"[ok] applied blind piecewise model -> {args.out}")


if __name__ == "__main__":
    main()
