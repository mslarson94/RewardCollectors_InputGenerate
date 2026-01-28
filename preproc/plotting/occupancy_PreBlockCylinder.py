#!/usr/bin/env python3
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--startprop", required=True, help="Path to *_startPosPropagated.csv")
    ap.add_argument("--reprocessed", required=True, help="Path to *_reprocessed.csv")
    ap.add_argument("--event", default="PreBlock_CylinderWalk_segment")
    ap.add_argument("--xcol", default="HeadPosAnchored_x")
    ap.add_argument("--ycol", default="HeadPosAnchored_z")
    ap.add_argument("--out-prefix", dest="out_prefix", default=None)
    ap.add_argument("--outDir", required=True)
    args = ap.parse_args()

    out_prefix = args.out_prefix or args.event

    outdir = Path(args.outDir)
    outdir.mkdir(parents=True, exist_ok=True)

    start_df = pd.read_csv(args.startprop)
    re_df = pd.read_csv(args.reprocessed)

    for c in ["lo_eventType", "origRow_start", "origRow_end"]:
        if c not in start_df.columns:
            raise SystemExit(f"Missing column in startPosPropagated: {c}")

    for c in [args.xcol, args.ycol]:
        if c not in re_df.columns:
            raise SystemExit(f"Missing column in reprocessed: {c}")

    seg_meta = start_df.loc[
        start_df["lo_eventType"].astype(str) == args.event,
        ["origRow_start", "origRow_end"]
    ].copy()

    seg_meta["origRow_start"] = pd.to_numeric(seg_meta["origRow_start"], errors="coerce")
    seg_meta["origRow_end"]   = pd.to_numeric(seg_meta["origRow_end"], errors="coerce")
    seg_meta = seg_meta.dropna(subset=["origRow_start", "origRow_end"])

    seg_meta["origRow_start"] = seg_meta["origRow_start"].astype(int)
    seg_meta["origRow_end"]   = seg_meta["origRow_end"].astype(int)

    nrows = len(re_df)
    seg_meta["start"] = seg_meta[["origRow_start", "origRow_end"]].min(axis=1).clip(0, nrows - 1)
    seg_meta["end"]   = seg_meta[["origRow_start", "origRow_end"]].max(axis=1).clip(0, nrows - 1)

    seg_meta = (
        seg_meta[["start", "end"]]
        .drop_duplicates()
        .sort_values(["start", "end"])
        .reset_index(drop=True)
    )
    seg_meta.insert(0, "segment_id", np.arange(1, len(seg_meta) + 1))
    seg_meta["n_points"] = seg_meta["end"] - seg_meta["start"] + 1

    if seg_meta.empty:
        raise SystemExit(f"No segments found for eventType='{args.event}'")

    segments = []
    for _, r in seg_meta.iterrows():
        s, e = int(r["start"]), int(r["end"])
        seg = re_df.iloc[s:e + 1][[args.xcol, args.ycol]].copy()
        seg["segment_id"] = int(r["segment_id"])
        seg["row_idx"] = np.arange(s, e + 1)
        segments.append(seg)

    pts = pd.concat(segments, ignore_index=True)
    pts_nonan = pts.dropna(subset=[args.xcol, args.ycol]).copy()

    intervals_csv = outdir / f"{out_prefix}__intervals.csv"
    points_csv    = outdir / f"{out_prefix}__extracted_points.csv"
    plot_png      = outdir / f"{out_prefix}__x_vs_z.png"

    seg_meta.to_csv(intervals_csv, index=False)
    pts.to_csv(points_csv, index=False)

    plt.figure(figsize=(7, 7))
    for sid, g in pts_nonan.groupby("segment_id", sort=True):
        plt.plot(g[args.xcol].to_numpy(), g[args.ycol].to_numpy(), linewidth=1, alpha=0.8)
        plt.scatter(g[args.xcol].to_numpy(), g[args.ycol].to_numpy(), s=6, alpha=0.6)

    plt.xlabel(args.xcol)
    plt.ylabel(args.ycol)
    plt.title(f"{args.event}: {len(seg_meta)} segments, {len(pts_nonan)} points")
    plt.axis("equal")
    plt.tight_layout()
    plt.savefig(plot_png, dpi=200)
    plt.close()

    print(f"Found {len(seg_meta)} segments for '{args.event}'.")
    print(f"Wrote: {intervals_csv}")
    print(f"Wrote: {points_csv}")
    print(f"Wrote: {plot_png}")


if __name__ == "__main__":
    main()
