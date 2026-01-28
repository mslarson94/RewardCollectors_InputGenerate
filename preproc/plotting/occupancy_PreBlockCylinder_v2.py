#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def _parse_time_seconds(series: pd.Series, unit: str) -> np.ndarray:
    """
    Convert a timestamp-like series to float seconds (monotonic-ish).

    unit:
      - "auto": numeric heuristic; otherwise tries datetime parsing
      - "s", "ms", "us", "ns": treat numeric values as that unit
      - "datetime": parse as datetime strings
    """
    # If explicitly datetime, do that.
    if unit == "datetime":
        dt = pd.to_datetime(series, errors="coerce", utc=True)
        t = (dt.view("int64") / 1e9).to_numpy(dtype=float)  # seconds
        return t

    # Try numeric conversion
    num = pd.to_numeric(series, errors="coerce")
    if unit in {"s", "ms", "us", "ns"}:
        scale = {"s": 1.0, "ms": 1e-3, "us": 1e-6, "ns": 1e-9}[unit]
        return (num.to_numpy(dtype=float) * scale)

    # auto mode
    if num.notna().mean() > 0.95:
        arr = num.to_numpy(dtype=float)
        finite = arr[np.isfinite(arr)]
        if finite.size == 0:
            return arr

        # Heuristic based on magnitude (common cases):
        # - seconds since start/epoch: ~1e0..1e9
        # - ms since epoch: ~1e12..1e13
        # - us since epoch: ~1e15..1e16
        # - ns since epoch: ~1e18..1e19
        med = float(np.median(finite))
        if med > 1e17:
            return arr * 1e-9  # ns -> s
        if med > 1e14:
            return arr * 1e-6  # us -> s
        if med > 1e11:
            return arr * 1e-3  # ms -> s
        return arr  # already seconds (or relative seconds)
    else:
        # fallback: datetime parse
        dt = pd.to_datetime(series, errors="coerce", utc=True)
        return (dt.view("int64") / 1e9).to_numpy(dtype=float)


def _safe_stem(s: str) -> str:
    out = []
    for ch in str(s):
        if ch.isalnum() or ch in ("-", "_", "."):
            out.append(ch)
        elif ch.isspace():
            out.append("_")
        else:
            out.append("_")
    return "".join(out).strip("_")[:180]


def _extract_intervals(start_df: pd.DataFrame, event: str) -> pd.DataFrame:
    seg_meta = start_df.loc[
        start_df["lo_eventType"].astype(str) == event,
        ["origRow_start", "origRow_end"],
    ].copy()

    seg_meta["origRow_start"] = pd.to_numeric(seg_meta["origRow_start"], errors="coerce")
    seg_meta["origRow_end"] = pd.to_numeric(seg_meta["origRow_end"], errors="coerce")
    seg_meta = seg_meta.dropna(subset=["origRow_start", "origRow_end"])

    seg_meta["origRow_start"] = seg_meta["origRow_start"].astype(int)
    seg_meta["origRow_end"] = seg_meta["origRow_end"].astype(int)
    return seg_meta


def _make_histogram_counts(
    x: np.ndarray,
    z: np.ndarray,
    x_edges: np.ndarray,
    z_edges: np.ndarray,
) -> np.ndarray:
    # histogram2d expects x then y; we treat z as "y"
    H, _, _ = np.histogram2d(x, z, bins=[x_edges, z_edges])
    return H


def _make_histogram_time(
    x: np.ndarray,
    z: np.ndarray,
    t: np.ndarray,
    x_edges: np.ndarray,
    z_edges: np.ndarray,
    max_gap_sec: float,
) -> np.ndarray:
    """
    Assign each delta-t to the bin of the *current* sample.
    dt[i] = t[i+1] - t[i]
    We accumulate dt[i] into bin at (x[i], z[i]).
    """
    # dt aligned to current sample
    dt = np.diff(t)
    # clean up dt: drop negatives/NaN; clip large gaps
    dt = np.where(np.isfinite(dt), dt, 0.0)
    dt = np.where(dt < 0, 0.0, dt)
    if max_gap_sec is not None:
        dt = np.minimum(dt, max_gap_sec)

    x0 = x[:-1]
    z0 = z[:-1]

    # digitize to bin indices
    xi = np.digitize(x0, x_edges) - 1
    zi = np.digitize(z0, z_edges) - 1

    H = np.zeros((len(x_edges) - 1, len(z_edges) - 1), dtype=float)

    valid = (
        (xi >= 0) & (xi < H.shape[0]) &
        (zi >= 0) & (zi < H.shape[1]) &
        np.isfinite(dt) & (dt > 0)
    )

    np.add.at(H, (xi[valid], zi[valid]), dt[valid])
    return H


def _save_tidy_csv(out_csv: Path, H: np.ndarray, x_edges: np.ndarray, z_edges: np.ndarray) -> None:
    x_centers = (x_edges[:-1] + x_edges[1:]) / 2.0
    z_centers = (z_edges[:-1] + z_edges[1:]) / 2.0
    Xc, Zc = np.meshgrid(x_centers, z_centers, indexing="ij")

    tidy = pd.DataFrame({
        "bin_center_x": Xc.ravel(),
        "bin_center_z": Zc.ravel(),
        "value": H.ravel(),
    })
    tidy.to_csv(out_csv, index=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--startprop", required=True, help="Path to *_startPosPropagated.csv")
    ap.add_argument("--reprocessed", required=True, help="Path to *_reprocessed.csv")
    ap.add_argument("--event", default="PreBlock_CylinderWalk_segment")
    ap.add_argument("--xcol", default="HeadPosAnchored_x")
    ap.add_argument("--ycol", default="HeadPosAnchored_z")  # ycol is z in your naming
    ap.add_argument("--tcol", default="eMLT_orig")
    ap.add_argument("--mode", choices=["counts", "time"], default="time")
    ap.add_argument("--timestamp-unit", choices=["auto", "s", "ms", "us", "ns", "datetime"], default="auto")
    ap.add_argument("--max-gap-sec", type=float, default=0.5, help="Clip dt to this value in time mode.")
    ap.add_argument("--bin-size", type=float, default=0.10, help="Bin size in meters.")
    ap.add_argument("--xmin", type=float, default=-10.0)
    ap.add_argument("--xmax", type=float, default=10.0)
    ap.add_argument("--ymin", type=float, default=-10.0)  # z-min
    ap.add_argument("--ymax", type=float, default=10.0)   # z-max
    ap.add_argument("--out-prefix", dest="out_prefix", default=None)
    ap.add_argument("--outDir", required=True)
    args = ap.parse_args()

    outdir = Path(args.outDir)
    outdir.mkdir(parents=True, exist_ok=True)

    out_prefix = _safe_stem(args.out_prefix or args.event)

    start_df = pd.read_csv(args.startprop)
    re_df = pd.read_csv(args.reprocessed)

    # Validate columns
    for c in ["lo_eventType", "origRow_start", "origRow_end"]:
        if c not in start_df.columns:
            raise SystemExit(f"Missing column in startPosPropagated: {c}")

    for c in [args.xcol, args.ycol, args.tcol]:
        if c not in re_df.columns:
            raise SystemExit(f"Missing column in reprocessed: {c}")

    seg_meta = _extract_intervals(start_df, args.event)

    if seg_meta.empty:
        raise SystemExit(f"No segments found for eventType='{args.event}'")

    # Normalize start/end + clamp bounds
    nrows = len(re_df)
    seg_meta["start"] = seg_meta[["origRow_start", "origRow_end"]].min(axis=1).clip(0, nrows - 1)
    seg_meta["end"] = seg_meta[["origRow_start", "origRow_end"]].max(axis=1).clip(0, nrows - 1)

    seg_meta = (
        seg_meta[["start", "end"]]
        .drop_duplicates()
        .sort_values(["start", "end"])
        .reset_index(drop=True)
    )
    seg_meta.insert(0, "segment_id", np.arange(1, len(seg_meta) + 1))
    seg_meta["n_points"] = seg_meta["end"] - seg_meta["start"] + 1

    # Extract points
    parts = []
    for _, r in seg_meta.iterrows():
        s, e = int(r["start"]), int(r["end"])
        seg = re_df.iloc[s:e + 1][[args.xcol, args.ycol, args.tcol]].copy()
        seg["segment_id"] = int(r["segment_id"])
        seg["row_idx"] = np.arange(s, e + 1)
        parts.append(seg)

    pts = pd.concat(parts, ignore_index=True)

    # Coerce numeric positions
    pts[args.xcol] = pd.to_numeric(pts[args.xcol], errors="coerce")
    pts[args.ycol] = pd.to_numeric(pts[args.ycol], errors="coerce")

    # Save extracted artifacts
    intervals_csv = outdir / f"{out_prefix}__intervals.csv"
    extracted_csv = outdir / f"{out_prefix}__extracted_points.csv"
    seg_meta.to_csv(intervals_csv, index=False)
    pts.to_csv(extracted_csv, index=False)

    # Spatial bin edges
    if args.bin_size <= 0:
        raise SystemExit("--bin-size must be > 0")

    x_edges = np.arange(args.xmin, args.xmax + args.bin_size, args.bin_size)
    y_edges = np.arange(args.ymin, args.ymax + args.bin_size, args.bin_size)

    # Make heatmap
    if args.mode == "counts":
        valid = pts[[args.xcol, args.ycol]].dropna()
        x = valid[args.xcol].to_numpy(dtype=float)
        z = valid[args.ycol].to_numpy(dtype=float)
        H = _make_histogram_counts(x, z, x_edges, y_edges)
        units_label = "samples / bin"
    else:
        # time mode: do it per segment to avoid dt spanning across segment boundaries
        H = np.zeros((len(x_edges) - 1, len(y_edges) - 1), dtype=float)

        for sid, g in pts.groupby("segment_id", sort=True):
            g = g.dropna(subset=[args.xcol, args.ycol, args.tcol]).copy()
            if len(g) < 2:
                continue
            # Preserve row order (important for dt)
            g = g.sort_values("row_idx")

            x = g[args.xcol].to_numpy(dtype=float)
            z = g[args.ycol].to_numpy(dtype=float)
            t = _parse_time_seconds(g[args.tcol], args.timestamp_unit)

            # Drop non-finite
            ok = np.isfinite(x) & np.isfinite(z) & np.isfinite(t)
            x = x[ok]
            z = z[ok]
            t = t[ok]
            if len(t) < 2:
                continue

            H += _make_histogram_time(x, z, t, x_edges, y_edges, max_gap_sec=args.max_gap_sec)
        units_label = "seconds / bin"

    # Save tidy heatmap CSV
    heat_csv = outdir / f"{out_prefix}__heatmap_{args.mode}.csv"
    _save_tidy_csv(heat_csv, H, x_edges, y_edges)

    # Plot heatmap
    plt.figure(figsize=(7.5, 7.0))
    # transpose so x is horizontal and z is vertical? Actually:
    # H is indexed [xbin, zbin]. imshow expects [row, col] -> [y, x].
    # We'll display with x on horizontal, z on vertical by plotting H.T
    im = plt.imshow(
        H.T,
        origin="lower",
        extent=[args.xmin, args.xmax, args.ymin, args.ymax],
        aspect="equal",
        interpolation="nearest",
    )
    plt.xlabel(args.xcol)
    plt.ylabel(args.ycol)
    plt.title(f"Occupancy heatmap ({args.mode}) — {args.event}\nBins: {args.bin_size:.3f}m, Range: [{args.xmin},{args.xmax}]x[{args.ymin},{args.ymax}]")
    cbar = plt.colorbar(im)
    cbar.set_label(units_label)
    plt.tight_layout()

    heat_png = outdir / f"{out_prefix}__heatmap_{args.mode}.png"
    plt.savefig(heat_png, dpi=200)
    plt.close()

    print(f"Found {len(seg_meta)} segments for '{args.event}'.")
    print(f"Wrote: {intervals_csv}")
    print(f"Wrote: {extracted_csv}")
    print(f"Wrote: {heat_csv}")
    print(f"Wrote: {heat_png}")


if __name__ == "__main__":
    main()
