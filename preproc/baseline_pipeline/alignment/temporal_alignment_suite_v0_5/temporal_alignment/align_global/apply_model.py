from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv
from temporal_alignment.common.apply import to_unix_s, apply_affine_unix


def main():
    ap = argparse.ArgumentParser(description="Apply a fitted global affine model to an event CSV.")
    ap.add_argument("--events-csv", required=True)
    ap.add_argument("--model-json", required=True)
    ap.add_argument("--time-col", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--aligned-col", default="aligned_time")
    ap.add_argument("--predicted-offset-col", default="predicted_offset_s")
    args = ap.parse_args()

    df = read_csv(args.events_csv)
    if args.time_col not in df:
        raise KeyError(f"Missing time column {args.time_col!r}")
    model = json.loads(Path(args.model_json).read_text())
    if model.get("model_type") != "global_affine":
        raise ValueError("Model JSON is not global_affine")

    t = pd.to_datetime(df[args.time_col], errors="coerce")
    valid = t.notna()
    df[args.aligned_col] = pd.NaT
    df[args.predicted_offset_col] = pd.NA

    x = to_unix_s(t.loc[valid])
    pred = apply_affine_unix(x, model["slope"], model["intercept_s"], model["origin_unix_s"])
    df.loc[valid, args.aligned_col] = pd.to_datetime(pred, unit="s").values
    df.loc[valid, args.predicted_offset_col] = pred - x
    df["alignment_model"] = "global_affine"
    write_csv(df, args.out)
    print(f"[ok] applied global affine -> {args.out}")


if __name__ == "__main__":
    main()
