#!/usr/bin/env python3
from __future__ import annotations
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Tuple, List, Optional, Iterable

import numpy as np
import pandas as pd

# Columns to drop from final augmented outputs (dropped safely if present)
# DROP_OPTIONAL_COLS = [
#     "mLTs_AN",
#     "AppTime",
#     "start_AppTime",
#     "end_AppTime",
# ]
DROP_OPTIONAL_COLS = [
    "mLTs_AN"
]
def _drop_optional_columns(df: pd.DataFrame, names: list[str] = DROP_OPTIONAL_COLS) -> pd.DataFrame:
    return df.drop(columns=[c for c in names if c in df.columns], errors="ignore")

# ----------------------------
# calcElapsedTime -> function
# ----------------------------
@dataclass
class Interval:
    start: pd.Timestamp
    end: pd.Timestamp
    duration_s: float
    blocknum: int
    blockinstance: int
    roundnum: Optional[int] = None
    index_in_block: Optional[int] = None


def calc_elapsed_time(
    events_csv: Path | str,
    out_dir: Path | str | None = None,
    prefix: Optional[str] = None,
    write_outputs: bool = True,
) -> dict[str, pd.DataFrame]:
    events_csv = Path(events_csv)

    # Load and preserve ALL original columns/rows via a stable row id
    df_orig = pd.read_csv(events_csv).reset_index(drop=True)
    df_orig["__rowid__"] = np.arange(len(df_orig), dtype=np.int64)

    # Mark events that occur before the first TrueContentStart in the file
    if "lo_eventType" in df_orig.columns:
        first_true_idx = df_orig.index[df_orig["lo_eventType"] == "TrueContentStart"]
        if len(first_true_idx) > 0:
            cutoff = int(first_true_idx[0])
            df_orig["begOfFile"] = (df_orig.index < cutoff)
        else:
            # No TrueContentStart present: treat entire file as beginning-of-file
            df_orig["begOfFile"] = True
        df_orig["begOfFile"] = df_orig["begOfFile"].astype(bool)
    else:
        df_orig["begOfFile"] = True  # conservative default if column missing

    # Work copy for computing timers on rows that have block context
    ev = df_orig.copy()
    ev["mLTimestamp"] = pd.to_datetime(ev["mLTimestamp"], errors="coerce")

    has_block = ev["BlockNum"].notna() & ev["BlockInstance"].notna()
    sub = ev.loc[has_block].copy()

    for col in ("BlockNum", "BlockInstance", "RoundNum"):
        if col in sub.columns:
            sub[col] = sub[col].astype("Int64")

    sub = sub.sort_values(["BlockNum", "BlockInstance", "mLTimestamp"]).reset_index(drop=True)

    for c in ("block_elapsed_s", "round_elapsed_s", "truecontent_elapsed_s"):
        sub[c] = np.nan

    block_intervals: List[Interval] = []
    round_intervals: List[Interval] = []
    true_intervals: List[Interval] = []

    open_start: Dict[Tuple[int, int, str], pd.Timestamp] = {}
    round_idx: Dict[Tuple[int, int], int] = {}
    true_idx: Dict[Tuple[int, int], int] = {}

    def close_interval(kind: str, key: Tuple[int, int], end_ts: pd.Timestamp, roundnum):
        start_ts = open_start.pop((key[0], key[1], kind), None)
        if start_ts is None or pd.isna(start_ts):
            return
        dur = (end_ts - start_ts).total_seconds()
        if kind == "Block":
            block_intervals.append(Interval(start_ts, end_ts, dur, key[0], key[1]))
        elif kind == "Round":
            round_intervals.append(
                Interval(
                    start_ts,
                    end_ts,
                    dur,
                    key[0],
                    key[1],
                    int(roundnum) if pd.notna(roundnum) else None,
                    round_idx[key],
                )
            )
        elif kind == "TrueContent":
            true_intervals.append(
                Interval(
                    start_ts,
                    end_ts,
                    dur,
                    key[0],
                    key[1],
                    int(roundnum) if pd.notna(roundnum) else None,
                    true_idx[key],
                )
            )

    for (bn, bi), g in sub.groupby(["BlockNum", "BlockInstance"], sort=False):
        bn = int(bn)
        bi = int(bi)
        round_idx[(bn, bi)] = 0
        true_idx[(bn, bi)] = 0
        for i, row in g.iterrows():
            ts = row["mLTimestamp"]
            et = row["lo_eventType"]
            key = (bn, bi)

            for kind, col in (("Block", "block_elapsed_s"), ("Round", "round_elapsed_s"), ("TrueContent", "truecontent_elapsed_s")):
                st = open_start.get((bn, bi, kind))
                if st is not None and pd.notna(st):
                    sub.at[i, col] = (ts - st).total_seconds()

            if et == "BlockStart":
                open_start[(bn, bi, "Block")] = ts
                sub.at[i, "block_elapsed_s"] = 0.0
            elif et == "BlockEnd":
                if (bn, bi, "Block") in open_start:
                    sub.at[i, "block_elapsed_s"] = (ts - open_start[(bn, bi, "Block")]).total_seconds()
                close_interval("Block", key, ts, roundnum=None)

            elif et == "RoundStart":
                open_start[(bn, bi, "Round")] = ts
                round_idx[(bn, bi)] += 1
                sub.at[i, "round_elapsed_s"] = 0.0
            elif et == "RoundEnd":
                if (bn, bi, "Round") in open_start:
                    sub.at[i, "round_elapsed_s"] = (ts - open_start[(bn, bi, "Round")]).total_seconds()
                close_interval("Round", key, ts, roundnum=row.get("RoundNum", pd.NA))

            elif et == "TrueContentStart":
                open_start[(bn, bi, "TrueContent")] = ts
                true_idx[(bn, bi)] += 1
                sub.at[i, "truecontent_elapsed_s"] = 0.0
            elif et == "TrueContentEnd":
                if (bn, bi, "TrueContent") in open_start:
                    sub.at[i, "truecontent_elapsed_s"] = (ts - open_start[(bn, bi, "TrueContent")]).total_seconds()
                close_interval("TrueContent", key, ts, roundnum=row.get("RoundNum", pd.NA))

    # Merge computed timers back to the FULL original dataframe (preserve all columns & order)
    timers = sub[["__rowid__", "block_elapsed_s", "round_elapsed_s", "truecontent_elapsed_s"]]
    events_with_timers = df_orig.merge(timers, on="__rowid__", how="left")

    blocks_df = pd.DataFrame([vars(x) for x in block_intervals]).sort_values(["blocknum", "blockinstance", "start"]) if block_intervals else pd.DataFrame(columns=[f.name for f in Interval.__dataclass_fields__.values()])
    rounds_df = pd.DataFrame([vars(x) for x in round_intervals]).sort_values(["blocknum", "blockinstance", "start"]) if round_intervals else pd.DataFrame(columns=[f.name for f in Interval.__dataclass_fields__.values()])
    true_df = pd.DataFrame([vars(x) for x in true_intervals]).sort_values(["blocknum", "blockinstance", "start"]) if true_intervals else pd.DataFrame(columns=[f.name for f in Interval.__dataclass_fields__.values()])

    if write_outputs and out_dir is not None:
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        base = prefix or Path(events_csv).name.removesuffix("_events_flat.csv").removesuffix(".csv")
        events_with_timers.drop(columns=["__rowid__"]).to_csv(out_dir / f"{base}_events_with_timers.csv", index=False)
        blocks_df.to_csv(out_dir / f"{base}_block_intervals.csv", index=False)
        rounds_df.to_csv(out_dir / f"{base}_round_intervals.csv", index=False)
        true_df.to_csv(out_dir / f"{base}_truecontent_intervals.csv", index=False)

    return {"events": events_with_timers, "blocks": blocks_df, "rounds": rounds_df, "truecontent": true_df}


# -------------------------
# getPositions -> function
# -------------------------

def get_positions(
    events_csv: Path | str,
    processed_csv: Path | str,
    out_csv: Path | str | None = None,
    write_output: bool = True,
) -> pd.DataFrame:
    events_csv = Path(events_csv)
    processed_csv = Path(processed_csv)

    ev = pd.read_csv(events_csv).reset_index(drop=True)
    ev["__rowid__"] = np.arange(len(ev), dtype=np.int64)

    # Mark events before the first TrueContentStart
    if "lo_eventType" in ev.columns:
        first_true_idx = ev.index[ev["lo_eventType"] == "TrueContentStart"]
        if len(first_true_idx) > 0:
            cutoff = int(first_true_idx[0])
            ev["begOfFile"] = (ev.index < cutoff)
        else:
            ev["begOfFile"] = True
        ev["begOfFile"] = ev["begOfFile"].astype(bool)
    else:
        ev["begOfFile"] = True

    proc = pd.read_csv(processed_csv)

    headpos_anch_cols = [c for c in proc.columns if c.startswith("HeadPosAnchored_")]
    headforth_anch_cols = [c for c in proc.columns if c.startswith("HeadForthAnchored_")]
    anchor_cols = headpos_anch_cols + headforth_anch_cols

    proc_sorted = proc.sort_values("origRow").set_index("origRow", drop=True)

    rows_start = ev["origRow_start"].dropna().astype(int).tolist() if "origRow_start" in ev.columns else []
    rows_end = ev["origRow_end"].dropna().astype(int).tolist() if "origRow_end" in ev.columns else []
    target_idx = sorted(set(proc_sorted.index.tolist()) | set(rows_start) | set(rows_end))

    pad = proc_sorted[anchor_cols].reindex(target_idx).ffill()

    def extract_at(rows: pd.Series) -> pd.DataFrame:
        out = pad.loc[rows.fillna(-10).astype(int).tolist()].copy()
        out.loc[rows.isna().values] = np.nan
        return out

    start_vals = extract_at(ev["origRow_start"]).add_suffix("_at_start").reset_index(drop=True) if "origRow_start" in ev.columns else pd.DataFrame(index=range(len(ev)))
    end_vals = extract_at(ev["origRow_end"]).add_suffix("_at_end").reset_index(drop=True) if "origRow_end" in ev.columns else pd.DataFrame(index=range(len(ev)))

    ev_out = pd.concat([ev.reset_index(drop=True), start_vals, end_vals], axis=1)

    if write_output and out_csv is not None:
        out_csv = Path(out_csv)
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        ev_out.drop(columns=["__rowid__"]).to_csv(out_csv, index=False)

    return ev_out

def get_positions(
    events_csv: Path | str,
    processed_csv: Path | str,
    out_csv: Path | str | None = None,
    write_output: bool = True,
) -> pd.DataFrame:
    events_csv = Path(events_csv)
    processed_csv = Path(processed_csv)

    ev = pd.read_csv(events_csv)
    proc = pd.read_csv(processed_csv)

    headpos_anch_cols = [c for c in proc.columns if c.startswith("HeadPosAnchored_")]
    headforth_anch_cols = [c for c in proc.columns if c.startswith("HeadForthAnchored_")]
    anchor_cols = headpos_anch_cols + headforth_anch_cols

    proc_sorted = proc.sort_values("origRow").set_index("origRow", drop=True)

    rows_start = ev["origRow_start"].dropna().astype(int).tolist() if "origRow_start" in ev.columns else []
    rows_end = ev["origRow_end"].dropna().astype(int).tolist() if "origRow_end" in ev.columns else []
    target_idx = sorted(set(proc_sorted.index.tolist()) | set(rows_start) | set(rows_end))

    pad = proc_sorted[anchor_cols].reindex(target_idx).ffill()

    def extract_at(rows: pd.Series) -> pd.DataFrame:
        out = pad.loc[rows.fillna(-10).astype(int).tolist()].copy()
        out.loc[rows.isna().values] = np.nan
        return out

    start_vals = extract_at(ev["origRow_start"]).add_suffix("_at_start").reset_index(drop=True) if "origRow_start" in ev.columns else pd.DataFrame(index=range(len(ev)))
    end_vals = extract_at(ev["origRow_end"]).add_suffix("_at_end").reset_index(drop=True) if "origRow_end" in ev.columns else pd.DataFrame(index=range(len(ev)))

    ev_out = pd.concat([ev.reset_index(drop=True), start_vals, end_vals], axis=1)

    if write_output and out_csv is not None:
        out_csv = Path(out_csv)
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        ev_out.to_csv(out_csv, index=False)

    return ev_out


# -------------------------
# Directory wrapper
# -------------------------

def _iter_event_files(events_dir: Path, pattern: str) -> Iterable[Path]:
    return sorted(Path(events_dir).glob(pattern))


def augment_events(
    events_csv: Path | str,
    processed_csv: Path | str,
    out_csv: Path | str | None = None,
    write_output: bool = True,
) -> pd.DataFrame:
    """Augment the original events dataframe with BOTH timers and positions.

    Preserves every original column and row order, adds:
      - block_elapsed_s, round_elapsed_s, truecontent_elapsed_s
      - *_at_start / *_at_end anchored position columns
    """
    # timers
    timers = calc_elapsed_time(events_csv, write_outputs=False)["events"].copy()
    # positions
    pos = get_positions(events_csv, processed_csv, write_output=False).copy()

    # Ensure both have a join key; if not, create one based on row order
    if "__rowid__" not in timers.columns:
        timers["__rowid__"] = np.arange(len(timers), dtype=np.int64)
    if "__rowid__" not in pos.columns:
        pos["__rowid__"] = np.arange(len(pos), dtype=np.int64)

    # Keep only new position columns to avoid duplicating original columns
    pos_new_cols = [c for c in pos.columns if c not in timers.columns or c == "__rowid__"]

    # Prefer key-based merge; if it fails for any reason, fall back to index concat
    try:
        merged = timers.merge(pos[pos_new_cols], on="__rowid__", how="left", validate="one_to_one")
    except Exception as e:
        # Fallback: align by row order (lengths must match)
        if len(timers) != len(pos):
            raise RuntimeError(
                f"augment_events: cannot align by index; different lengths timers={len(timers)} pos={len(pos)}"
            ) from e
        only_pos_cols = [c for c in pos_new_cols if c != "__rowid__"]
        merged = pd.concat([timers.reset_index(drop=True), pos[only_pos_cols].reset_index(drop=True)], axis=1)

    # Drop optional columns safely across heterogeneous files
    merged_clean = _drop_optional_columns(merged)

    if write_output and out_csv is not None:
        Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
        merged_clean.drop(columns=["__rowid__"], errors="ignore").to_csv(out_csv, index=False)

    return merged_clean.drop(columns=["__rowid__"], errors="ignore")


def process_directory(
    events_dir: Path | str,
    processed_dir: Path | str,
    out_root: Path | str,
    pattern: str = "*_events_flat.csv",
    write_outputs: bool = True,
) -> None:
    events_dir = Path(events_dir)
    processed_dir = Path(processed_dir)
    out_root = Path(out_root)

    elapsed_out = out_root / "elapsedTime"
    positions_out = out_root / "eventsPositions"
    combined_out = out_root / "augmented"
    elapsed_out.mkdir(parents=True, exist_ok=True)
    positions_out.mkdir(parents=True, exist_ok=True)
    combined_out.mkdir(parents=True, exist_ok=True)

    for ev_path in _iter_event_files(events_dir, pattern):
        name = ev_path.name
        if name.endswith('.csv'):
            name = name[:-4]                 # drop extension
        base = name.rsplit('_events', 1)[0]  # drop only the final "_events_<anything>"

        proc_candidate = processed_dir / f"{base}_processed.csv"
        if not proc_candidate.exists():
            print(f"[SKIP] Missing processed CSV for {ev_path.name}: {proc_candidate}")
            continue

        print(f"[RUN] {base}")
        # Write separate outputs (back-compat)
        calc_elapsed_time(
            events_csv=ev_path,
            out_dir=elapsed_out,
            prefix=base,
            write_outputs=write_outputs,
        )
        get_positions(
            events_csv=ev_path,
            processed_csv=proc_candidate,
            out_csv=(positions_out / f"{base}_processed_events_pos.csv") if write_outputs else None,
            write_output=write_outputs,
        )
        # Write combined augmented events
        if write_outputs:
            augment_events(
                events_csv=ev_path,
                processed_csv=proc_candidate,
                out_csv=combined_out / f"{base}_events_final.csv",
                write_output=True,
            )


# -------------------------
# CLI entry
# -------------------------
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Batch RC processing: elapsed time + positions")
    p.add_argument("--events-dir", required=True, type=Path, help="Directory containing *_processed_events.csv files")
    p.add_argument("--processed-dir", required=True, type=Path, help="Directory containing corresponding *_processed.csv files")
    p.add_argument("--out-dir", required=True, type=Path, help="Output root directory (will create subfolders)")
    p.add_argument("--pattern", default="*_events_flat.csv", help="Glob for event files (default: %(default)s)")
    p.add_argument("--no-write", action="store_true", help="Do not write outputs; just run functions")

    args = p.parse_args()

    process_directory(
        events_dir=args.events_dir,
        processed_dir=args.processed_dir,
        out_root=args.out_dir,
        pattern=args.pattern,
        write_outputs=(not args.no_write),
    )
