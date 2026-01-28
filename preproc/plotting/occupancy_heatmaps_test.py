#!/usr/bin/env python3
"""
occupancy_heatmaps_test.py

Generate occupancy heat maps from time-series occupancy data.

Expected CSV columns (minimum):
  - timestamp : ISO-8601 string or anything pandas can parse
  - location  : room/zone identifier (string)
  - occupancy : numeric (count or 0/1 presence)

Example CSV:
timestamp,location,occupancy
2026-01-01T08:00:00,Room A,3
2026-01-01T08:15:00,Room A,4
2026-01-01T08:00:00,Room B,0

Outputs:
  1) "weekly" heatmap: day-of-week x time-of-day (aggregated across all dates)
  2) "calendar" heatmap: date x time-of-day (for a chosen location)

Dependencies:
  pip install pandas numpy matplotlib

Usage examples:
  python occupancy_heatmaps.py data.csv --outdir out
  python occupancy_heatmaps.py data.csv --location "Room A" --bin-minutes 15 --agg mean --outdir out
  python occupancy_heatmaps.py data.csv --tz "America/Los_Angeles" --start 2026-01-01 --end 2026-01-31 --outdir out
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


DOW_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


@dataclass(frozen=True)
class Config:
    input_csv: str
    outdir: str
    timestamp_col: str
    location_col: str
    occupancy_col: str
    location: Optional[str]
    tz: Optional[str]
    start: Optional[str]
    end: Optional[str]
    bin_minutes: int
    agg: str  # mean, max, sum
    clip_min: Optional[float]
    clip_max: Optional[float]
    vmin: Optional[float]
    vmax: Optional[float]
    cmap: str
    dpi: int


def parse_args() -> Config:
    p = argparse.ArgumentParser(description="Generate occupancy heat maps from CSV data.")
    p.add_argument("input_csv", help="Path to CSV containing timestamp/location/occupancy columns.")
    p.add_argument("--outdir", default="out_heatmaps", help="Output directory for PNGs.")
    p.add_argument("--timestamp-col", default="timestamp", help="Timestamp column name.")
    p.add_argument("--location-col", default="location", help="Location column name.")
    p.add_argument("--occupancy-col", default="occupancy", help="Occupancy column name.")
    p.add_argument("--location", default=None, help="Filter to a single location/room/zone.")
    p.add_argument("--tz", default=None, help="Timezone name (e.g., America/Los_Angeles).")
    p.add_argument("--start", default=None, help="Inclusive start date/time filter (e.g., 2026-01-01).")
    p.add_argument("--end", default=None, help="Exclusive end date/time filter (e.g., 2026-02-01).")
    p.add_argument("--bin-minutes", type=int, default=15, help="Time bin size in minutes (e.g., 5/10/15/30/60).")
    p.add_argument("--agg", choices=["mean", "max", "sum"], default="mean",
                   help="Aggregation within each time bin.")
    p.add_argument("--clip-min", type=float, default=None, help="Clip occupancy below this value (after load).")
    p.add_argument("--clip-max", type=float, default=None, help="Clip occupancy above this value (after load).")
    p.add_argument("--vmin", type=float, default=None, help="Color scale min (optional).")
    p.add_argument("--vmax", type=float, default=None, help="Color scale max (optional).")
    p.add_argument("--cmap", default="viridis", help="Matplotlib colormap name.")
    p.add_argument("--dpi", type=int, default=200, help="PNG DPI.")
    a = p.parse_args()

    return Config(
        input_csv=a.input_csv,
        outdir=a.outdir,
        timestamp_col=a.timestamp_col,
        location_col=a.location_col,
        occupancy_col=a.occupancy_col,
        location=a.location,
        tz=a.tz,
        start=a.start,
        end=a.end,
        bin_minutes=a.bin_minutes,
        agg=a.agg,
        clip_min=a.clip_min,
        clip_max=a.clip_max,
        vmin=a.vmin,
        vmax=a.vmax,
        cmap=a.cmap,
        dpi=a.dpi,
    )


def ensure_outdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def load_data(cfg: Config) -> pd.DataFrame:
    df = pd.read_csv(cfg.input_csv)

    missing = [c for c in [cfg.timestamp_col, cfg.location_col, cfg.occupancy_col] if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns in CSV: {missing}. Found: {list(df.columns)}")

    # Parse timestamps
    ts = pd.to_datetime(df[cfg.timestamp_col], errors="coerce", utc=True)
    bad = ts.isna().sum()
    if bad:
        raise SystemExit(f"Failed to parse {bad} timestamps. Fix input or format.")

    df = df.copy()
    df[cfg.timestamp_col] = ts

    # Convert from UTC to local tz if requested
    if cfg.tz:
        df[cfg.timestamp_col] = df[cfg.timestamp_col].dt.tz_convert(cfg.tz)

    # Filter by location if requested
    if cfg.location is not None:
        df = df[df[cfg.location_col].astype(str) == str(cfg.location)]

    # Filter by time range
    if cfg.start:
        start_ts = pd.to_datetime(cfg.start)
        if start_ts.tzinfo is None and cfg.tz:
            start_ts = start_ts.tz_localize(cfg.tz)
        df = df[df[cfg.timestamp_col] >= start_ts]

    if cfg.end:
        end_ts = pd.to_datetime(cfg.end)
        if end_ts.tzinfo is None and cfg.tz:
            end_ts = end_ts.tz_localize(cfg.tz)
        df = df[df[cfg.timestamp_col] < end_ts]

    # Coerce occupancy to numeric
    df[cfg.occupancy_col] = pd.to_numeric(df[cfg.occupancy_col], errors="coerce")
    df = df.dropna(subset=[cfg.occupancy_col])

    if cfg.clip_min is not None or cfg.clip_max is not None:
        df[cfg.occupancy_col] = df[cfg.occupancy_col].clip(lower=cfg.clip_min, upper=cfg.clip_max)

    if df.empty:
        raise SystemExit("No data after filters. Adjust --location/--start/--end.")

    return df


def bin_timeseries(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """
    Returns a dataframe indexed by (location, time_bin) with aggregated occupancy in each bin.
    """
    ts_col = cfg.timestamp_col
    loc_col = cfg.location_col
    occ_col = cfg.occupancy_col

    # Floor timestamps into fixed bins
    freq = f"{cfg.bin_minutes}min"
    df = df.copy()
    df["time_bin"] = df[ts_col].dt.floor(freq)

    agg_map = {"mean": "mean", "max": "max", "sum": "sum"}[cfg.agg]
    grouped = (
        df.groupby([loc_col, "time_bin"], as_index=False)[occ_col]
          .agg(agg_map)
          .rename(columns={occ_col: "occ"})
    )

    # Keep timezone-aware bins if input was timezone-aware
    grouped["time_bin"] = pd.to_datetime(grouped["time_bin"])
    if hasattr(df[ts_col].dtype, "tz") and df[ts_col].dt.tz is not None:
        grouped["time_bin"] = grouped["time_bin"].dt.tz_localize(df[ts_col].dt.tz, ambiguous="NaT", nonexistent="shift_forward")

    return grouped


def make_weekly_heatmap(grouped: pd.DataFrame, cfg: Config, location: str) -> str:
    """
    day-of-week (rows) x time-of-day (cols), aggregated across all dates for that location.
    """
    df = grouped[grouped[cfg.location_col].astype(str) == str(location)].copy()
    if df.empty:
        raise SystemExit(f"No binned data for location: {location}")

    tb = pd.to_datetime(df["time_bin"])
    df["dow"] = tb.dt.day_name().str.slice(0, 3)  # Mon/Tue/...
    df["tod"] = tb.dt.hour * 60 + tb.dt.minute  # minutes since midnight

    # Determine columns (time-of-day bins)
    step = cfg.bin_minutes
    tod_bins = np.arange(0, 24 * 60, step, dtype=int)

    # Pivot: rows=dow, cols=tod
    pivot = (
        df.pivot_table(index="dow", columns="tod", values="occ", aggfunc="mean")
          .reindex(DOW_ORDER)
          .reindex(columns=tod_bins)
    )

    data = pivot.to_numpy(dtype=float)

    fig = plt.figure(figsize=(14, 4.5))
    ax = fig.add_subplot(111)

    im = ax.imshow(
        data,
        aspect="auto",
        origin="upper",
        vmin=cfg.vmin,
        vmax=cfg.vmax,
        cmap=cfg.cmap,
        interpolation="nearest",
    )

    # X ticks: show every 2 hours (or nearest)
    tick_minutes = np.arange(0, 24 * 60 + 1, 120)
    tick_positions = (tick_minutes // step).astype(int)
    tick_positions = tick_positions[tick_positions < data.shape[1]]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels([f"{m//60:02d}:00" for m in tick_minutes[: len(tick_positions)]], rotation=0)

    ax.set_yticks(np.arange(len(DOW_ORDER)))
    ax.set_yticklabels(DOW_ORDER)

    title_loc = location if location is not None else "ALL"
    ax.set_title(f"Weekly Occupancy Heatmap — {title_loc} ({cfg.agg} per {cfg.bin_minutes} min)")
    ax.set_xlabel("Time of day")
    ax.set_ylabel("Day of week")

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Occupancy")

    fig.tight_layout()

    outpath = os.path.join(cfg.outdir, f"weekly_heatmap__{safe_filename(location)}.png")
    fig.savefig(outpath, dpi=cfg.dpi)
    plt.close(fig)
    return outpath


def make_calendar_heatmap(grouped: pd.DataFrame, cfg: Config, location: str) -> str:
    """
    date (rows) x time-of-day (cols), showing day-by-day patterns.
    """
    df = grouped[grouped[cfg.location_col].astype(str) == str(location)].copy()
    if df.empty:
        raise SystemExit(f"No binned data for location: {location}")

    tb = pd.to_datetime(df["time_bin"])
    df["date"] = tb.dt.date
    df["tod"] = tb.dt.hour * 60 + tb.dt.minute

    step = cfg.bin_minutes
    tod_bins = np.arange(0, 24 * 60, step, dtype=int)

    pivot = (
        df.pivot_table(index="date", columns="tod", values="occ", aggfunc="mean")
          .sort_index()
          .reindex(columns=tod_bins)
    )

    data = pivot.to_numpy(dtype=float)

    fig = plt.figure(figsize=(14, 6.5))
    ax = fig.add_subplot(111)

    im = ax.imshow(
        data,
        aspect="auto",
        origin="upper",
        vmin=cfg.vmin,
        vmax=cfg.vmax,
        cmap=cfg.cmap,
        interpolation="nearest",
    )

    # X ticks: show every 2 hours (or nearest)
    tick_minutes = np.arange(0, 24 * 60 + 1, 120)
    tick_positions = (tick_minutes // step).astype(int)
    tick_positions = tick_positions[tick_positions < data.shape[1]]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels([f"{m//60:02d}:00" for m in tick_minutes[: len(tick_positions)]], rotation=0)

    # Y ticks: show up to ~12 labels (depending on number of days)
    n_days = data.shape[0]
    if n_days <= 15:
        y_positions = np.arange(n_days)
    else:
        y_positions = np.linspace(0, n_days - 1, num=12, dtype=int)
        y_positions = np.unique(y_positions)

    ax.set_yticks(y_positions)
    ax.set_yticklabels([str(pivot.index[i]) for i in y_positions])

    ax.set_title(f"Calendar Occupancy Heatmap — {location} ({cfg.agg} per {cfg.bin_minutes} min)")
    ax.set_xlabel("Time of day")
    ax.set_ylabel("Date")

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Occupancy")

    fig.tight_layout()

    outpath = os.path.join(cfg.outdir, f"calendar_heatmap__{safe_filename(location)}.png")
    fig.savefig(outpath, dpi=cfg.dpi)
    plt.close(fig)
    return outpath


def safe_filename(s: str) -> str:
    keep = []
    for ch in str(s):
        if ch.isalnum() or ch in ("-", "_", "."):
            keep.append(ch)
        elif ch.isspace():
            keep.append("_")
        else:
            keep.append("_")
    out = "".join(keep).strip("_")
    return out[:180] if len(out) > 180 else out


def pick_locations(df: pd.DataFrame, cfg: Config) -> List[str]:
    locs = df[cfg.location_col].astype(str).unique().tolist()
    locs.sort()
    if cfg.location is not None:
        return [str(cfg.location)]
    return locs


def main() -> None:
    cfg = parse_args()
    ensure_outdir(cfg.outdir)

    raw = load_data(cfg)
    grouped = bin_timeseries(raw, cfg)

    locations = pick_locations(raw, cfg)

    written = []
    for loc in locations:
        weekly_path = make_weekly_heatmap(grouped, cfg, loc)
        cal_path = make_calendar_heatmap(grouped, cfg, loc)
        written.extend([weekly_path, cal_path])

    print("Wrote:")
    for p in written:
        print("  ", p)


if __name__ == "__main__":
    main()
