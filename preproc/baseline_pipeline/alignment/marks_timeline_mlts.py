#!/usr/bin/env python3
# marks_timeline_mlts.py
"""
Overlay Raspberry Pi marks (blue) and Events marks (red) on a single datetime axis,
using the Events file's mLTimestamp directly. Blocks and rounds come from Events.

Example:
  python scripts/marks_timeline_mlts.py \
    --events /path/to/ObsReward_B_03_17_2025_15_50_events_final.csv \
    --rpi    /path/to/ObsReward_B_03_17_2025_15_50_RNS_RPi_unified.csv \
    --block All \
    --out   ./marks_timeline.png \
    --dpi   150 --show
"""

from __future__ import annotations
import argparse
from pathlib import Path
from typing import Optional, Dict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ---------- column detection ----------
def detect_events_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    cols = {c.lower(): c for c in df.columns}
    ts_col = cols.get("mltimestamp")  # required for this script
    etype = cols.get("lo_eventtype") or cols.get("eventtype") or cols.get("event_type") or cols.get("type")
    block = cols.get("blocknum") or cols.get("block") or cols.get("blockid") or cols.get("block_id") or cols.get("lo_block")
    rnd   = cols.get("roundnum") or cols.get("round") or cols.get("roundid") or cols.get("round_id") or cols.get("lo_round")
    return {"ts": ts_col, "etype": etype, "block": block, "round": rnd}


def detect_rpi_time_column(df: pd.DataFrame) -> Optional[str]:
    # Prefer full datetimes
    for name in ("RPi_Time_unified", "ML_Time_verb", "RPi_Time_simple", "Mono_Time_verb", "Mono_Time_Raw_verb"):
        if name in df.columns:
            return name
    # last resort: any object column containing "time" or "timestamp"
    for c in df.columns:
        if df[c].dtype == object and any(k in c.lower() for k in ("time", "timestamp", "date")):
            return c
    return None


# ---------- helpers ----------
def build_blocks(events: pd.DataFrame, ts_col: str, etype_col: str, block_col: Optional[str]) -> pd.DataFrame:
    e = events.sort_values(ts_col).copy()
    et = e[etype_col].astype(str).str.lower()

    starts = e[et.eq("blockstart")]
    ends   = e[et.eq("blockend")]

    if not starts.empty and not ends.empty and block_col and block_col in e.columns:
        blocks = starts[[ts_col, block_col]].merge(
            ends[[ts_col, block_col]], on=block_col, how="left", suffixes=("_start", "_end")
        ).rename(columns={block_col: "block", f"{ts_col}_start": "start", f"{ts_col}_end": "end"})
    else:
        if block_col and block_col in e.columns:
            grp = e.groupby(block_col)[ts_col]
            blocks = pd.DataFrame({"block": grp.apply(lambda s: s.name).index})
            blocks["start"] = grp.min().values
            blocks["end"] = grp.max().values
        else:
            # Single session if we have no block id at all
            blocks = pd.DataFrame({"block": [1], "start": [e[ts_col].min()], "end": [e[ts_col].max()]})

    # If some ends are missing, cap at next start or last event timestamp
    blocks = blocks.sort_values("start").reset_index(drop=True)
    next_starts = blocks["start"].shift(-1)
    blocks["end"] = np.where(blocks["end"].isna(), next_starts, blocks["end"])
    blocks["end"] = blocks["end"].fillna(e[ts_col].max())

    return blocks.sort_values("block").reset_index(drop=True)


def assign_block(t: pd.Series, blocks: pd.DataFrame) -> pd.Series:
    b = blocks.sort_values("start")
    starts = pd.to_datetime(b["start"]).values.astype("datetime64[ns]")
    ends   = pd.to_datetime(b["end"]).values.astype("datetime64[ns]")
    ids    = b["block"].values

    tv = pd.to_datetime(t).values.astype("datetime64[ns]")
    idxs = np.searchsorted(starts, tv, side="right") - 1
    idxs = np.clip(idxs, 0, len(b) - 1)
    in_range = (tv >= starts[idxs]) & (tv <= ends[idxs])
    out = np.where(in_range, ids[idxs], np.nan)
    return pd.Series(out, index=t.index, dtype="float")


# ---------- plotting ----------
def render_plot(
    blocks: pd.DataFrame,
    events_marks: pd.DataFrame,
    rpi_marks: pd.DataFrame,
    ts_col: str,
    block_sel: str | int,
    out_path: Path,
    dpi: int,
    show: bool,
) -> None:
    plt.figure(figsize=(11, 4.8))

    if str(block_sel).lower() == "all":
        t0 = min(blocks["start"].min(), events_marks[ts_col].min() if not events_marks.empty else blocks["start"].min())
        t1 = max(blocks["end"].max(),   events_marks[ts_col].max() if not events_marks.empty else blocks["end"].max())
        sub_evt = events_marks
        sub_rpi = rpi_marks
        title_suffix = "All"
    else:
        b = int(block_sel)
        row = blocks.loc[blocks["block"] == b]
        if row.empty:
            raise ValueError(f"Block {b} not found. Available: {sorted(blocks['block'].astype(int).unique())}")
        r0 = row.iloc[0]
        t0, t1 = r0["start"], r0["end"]
        sub_evt = events_marks[events_marks["block"] == b]
        sub_rpi = rpi_marks[rpi_marks["block"] == b]
        title_suffix = str(b)

    # Background region
    plt.axvspan(pd.to_datetime(t0), pd.to_datetime(t1), alpha=0.05)

    # RPi marks in blue (y=0)
    if not sub_rpi.empty:
        plt.stem(pd.to_datetime(sub_rpi[ts_col]).values, np.zeros(len(sub_rpi)),
                 linefmt='b-', markerfmt='bo', basefmt=' ')

    # Events marks in red (y=1)
    if not sub_evt.empty:
        plt.stem(pd.to_datetime(sub_evt[ts_col]).values, np.ones(len(sub_evt)),
                 linefmt='r-', markerfmt='ro', basefmt=' ')

    # Block boundaries
    for _, r in blocks.iterrows():
        if (r["end"] >= t0) and (r["start"] <= t1):
            plt.axvline(pd.to_datetime(r["start"]), linestyle="--", alpha=0.6)
            plt.text(pd.to_datetime(r["start"]), 1.05, f"B{int(r['block'])} start", rotation=90, va="bottom", ha="right")
            plt.axvline(pd.to_datetime(r["end"]), linestyle="--", alpha=0.6)
            plt.text(pd.to_datetime(r["end"]), 1.05, f"B{int(r['block'])} end", rotation=90, va="bottom", ha="left")

    plt.yticks([0, 1], ["RPi Mark (blue)", "Events Mark (red)"])
    plt.xlabel("Time (mLTimestamp)")
    plt.title(f"Marks Timeline — Block: {title_suffix}")
    plt.tight_layout()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=dpi, bbox_inches="tight")
    if show:
        plt.show()
    plt.close()


# ---------- CLI pipeline ----------
def main() -> None:
    ap = argparse.ArgumentParser(description="Overlay RPi vs Events marks using Events.mLTimestamp (datetime).")
    ap.add_argument("--events", required=True, type=Path, help="Events CSV path (must have mLTimestamp).")
    ap.add_argument("--rpi", required=True, type=Path, help="RPi marks CSV path (with absolute datetime column).")
    ap.add_argument("--block", default="All", help='Block number or "All"')
    ap.add_argument("--out", type=Path, default=Path("./marks_timeline.png"), help="Output PNG path")
    ap.add_argument("--dpi", type=int, default=150, help="Output image DPI")
    ap.add_argument("--show", action="store_true", help="Show the plot window")
    args = ap.parse_args()

    # Load events
    events = pd.read_csv(args.events)
    ev = detect_events_columns(events)
    ts_col = ev["ts"]
    et_col = ev["etype"]
    blk_col = ev["block"]

    if not ts_col or not et_col:
        raise RuntimeError("Events file must contain 'mLTimestamp' and an event-type column (e.g., 'lo_eventType').")

    # Parse event timestamps (datetime)
    events[ts_col] = pd.to_datetime(events[ts_col], errors="coerce", infer_datetime_format=True)
    events = events.dropna(subset=[ts_col])

    # Keep relevant event types
    keep_types = {"blockstart", "blockend", "roundstart", "roundend", "mark"}
    events = events[events[et_col].astype(str).str.lower().isin(keep_types)].copy()
    events = events.sort_values(ts_col)

    blocks = build_blocks(events, ts_col, et_col, blk_col)
    if blocks.empty:
        raise RuntimeError("No blocks could be constructed from Events.")

    # Event marks (datetime)
    events_marks = events[events[et_col].astype(str).str.lower() == "mark"].copy()
    events_marks["block"] = assign_block(events_marks[ts_col], blocks)

    # Load RPi and parse absolute datetime
    rpi = pd.read_csv(args.rpi)
    rpi_time_col = detect_rpi_time_column(rpi)
    if not rpi_time_col:
        raise RuntimeError("Could not find a suitable datetime column in RPi CSV (e.g., 'RPi_Time_unified').")

    rpi[ts_col] = pd.to_datetime(rpi[rpi_time_col], errors="coerce", infer_datetime_format=True)
    rpi = rpi.dropna(subset=[ts_col]).sort_values(ts_col).copy()

    # Assign RPi marks to blocks in the same datetime space
    rpi["block"] = assign_block(rpi[ts_col], blocks)

    print(f"[INFO] Detected blocks: {sorted(blocks['block'].dropna().astype(int).unique().tolist())}")
    print(f"[INFO] Events marks: {len(events_marks)} | RPi marks: {len(rpi)}")
    print(f"[INFO] Selected block: {args.block}")

    render_plot(
        blocks=blocks,
        events_marks=events_marks,
        rpi_marks=rpi,
        ts_col=ts_col,
        block_sel=args.block,
        out_path=args.out,
        dpi=args.dpi,
        show=args.show,
    )
    print(f"[OK] Saved timeline → {args.out}")


if __name__ == "__main__":
    main()
