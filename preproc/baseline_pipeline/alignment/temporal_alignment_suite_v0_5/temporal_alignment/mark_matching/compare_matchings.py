from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
from temporal_alignment.common.io import read_csv, write_csv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manual", required=True)
    ap.add_argument("--automatic", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    m = read_csv(args.manual)[["ml_mark_id","rpi_mark_id","raw_offset_s"]].rename(
        columns={"rpi_mark_id":"manual_rpi_mark_id","raw_offset_s":"manual_raw_offset_s"})
    a = read_csv(args.automatic)[["ml_mark_id","rpi_mark_id","raw_offset_s"]].rename(
        columns={"rpi_mark_id":"automatic_rpi_mark_id","raw_offset_s":"automatic_raw_offset_s"})
    out = m.merge(a, on="ml_mark_id", how="outer")
    out["same_pair"] = out["manual_rpi_mark_id"].fillna("") == out["automatic_rpi_mark_id"].fillna("")
    out["offset_difference_s"] = out["automatic_raw_offset_s"] - out["manual_raw_offset_s"]
    write_csv(out, args.out)
    print(f"[ok] matching comparison -> {args.out}")


if __name__ == "__main__":
    main()
