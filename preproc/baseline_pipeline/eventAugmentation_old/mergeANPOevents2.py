#!/usr/bin/env python3
"""
Merge AN & PO event logs for a participant pair + session, align events, and quantify timing delays.

Design goals
------------
* Robust outer-merge on (lo_eventType, BlockInstance, BlockNum, RoundNum, evt_idx).
  - evt_idx is a per-(BlockInstance, BlockNum, RoundNum, lo_eventType) sequence to prevent many-to-many explosions
    when the same lo_eventType appears multiple times in a round.
* Tag *all* participant-specific (and most other) fields with role suffixes (_AN, _PO).
  - Only the join keys remain unsuffixed.
* Compute per-event delay (seconds) using an absolute timestamp derived from testingDate + mLTimestamp_raw.
* Provide per-round and per-block delay summaries, plus an overall summary.
* Optionally estimate PO times for AN-only events using median delays at the round/block/overall level.

Outputs (written next to input files by default):
  - merged_events.csv                      # full outer-merged, role-suffixed table + delay_s
  - delay_summary_by_round.csv             # delay stats per (BlockInstance, BlockNum, RoundNum)
  - delay_summary_by_block.csv             # delay stats per (BlockInstance, BlockNum)
  - delay_summary_overall.json             # overall delay stats

Usage
-----
python mergeANPOevents2.py \
  --an "/path/to/ObsReward_A_..._events_final.csv" \
  --po "/path/to/ObsReward_B_..._events_final.csv" \
  --outdir "/path/to/output_dir" \
  [--estimate-missing-po]  # add estimated PO timestamps for AN-only rows

Notes
-----
* This script treats file name "A" as AN and "B" as PO. If your roles differ, pass --role-of-a / --role-of-b.
* If some lo_eventType names differ across roles (e.g., "CoinCollectMoment_PinDrop" vs "CoinCollect_Moment_PinDrop"),
  we normalize known cases. Extend NORMALIZE_LO_EVENTTYPE if needed.
* The variable ptIsAorB is NOT used for alignment.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# -----------------------------
# Configurable normalization
# -----------------------------
NORMALIZE_LO_EVENTTYPE = {
    # Known AN/PO naming discrepancy (PO missing underscore before Moment)
    "CoinCollectMoment_PinDrop": "CoinCollect_Moment_PinDrop",
    # Add more one-offs here if you discover them.
}

JOIN_KEYS = ["BlockInstance", "BlockNum", "RoundNum", "lo_eventType", "evt_idx"]


# -----------------------------
# Utilities
# -----------------------------

def normalize_lo_event_type(value: object) -> object:
    if pd.isna(value):
        return value
    s = str(value).strip()
    return NORMALIZE_LO_EVENTTYPE.get(s, s)


def parse_testing_date(s: object) -> Optional[datetime]:
    """Parse testingDate formatted like MM_DD_YYYY into a date (time midnight)."""
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return None
    try:
        dt = datetime.strptime(str(s), "%m_%d_%Y")
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    except Exception:
        return None


def parse_mlt_raw_to_datetime(testing_date: object, mlt_raw: object) -> Optional[datetime]:
    """Combine testingDate (MM_DD_YYYY) with mLTimestamp_raw formatted HH:MM:SS:ms -> absolute datetime.

    Returns None if parsing fails.
    """
    base = parse_testing_date(testing_date)
    if base is None or mlt_raw is None or (isinstance(mlt_raw, float) and math.isnan(mlt_raw)):
        return None
    m = re.fullmatch(r"(\d{1,2}):(\d{2}):(\d{2}):(\d{1,3})", str(mlt_raw))
    if not m:
        return None
    hh, mm, ss, ms = map(int, m.groups())
    return base.replace(hour=hh, minute=mm, second=ss, microsecond=ms * 1000)


def add_evt_index(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize lo_eventType, build absolute mlt_datetime, and assign per-event sequence index (evt_idx)."""
    out = df.copy()
    out["lo_eventType"] = out["lo_eventType"].map(normalize_lo_event_type)
    out["mlt_datetime"] = [
        parse_mlt_raw_to_datetime(td, raw) for td, raw in zip(out.get("testingDate"), out.get("mLTimestamp_raw"))
    ]
    # Sorting to ensure stable evt_idx within each group
    sort_cols = [c for c in ["BlockInstance", "BlockNum", "RoundNum", "lo_eventType", "mlt_datetime"] if c in out.columns]
    out = out.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
    out["evt_idx"] = out.groupby(["BlockInstance", "BlockNum", "RoundNum", "lo_eventType"], dropna=False).cumcount()
    return out


def suffix_all_but_keys(df: pd.DataFrame, role: str, keys: Sequence[str]) -> pd.DataFrame:
    """Suffix every column name with _{role}, except the provided join keys."""
    df2 = df.copy()
    for k in keys:
        if k not in df2.columns:
            df2[k] = np.nan
    rename = {c: f"{c}_{role}" for c in df2.columns if c not in keys}
    return df2.rename(columns=rename)


# -----------------------------
# Core merge & delay computation
# -----------------------------

def merge_an_po(
    an_csv: str,
    po_csv: str,
    role_of_a: str = "AN",
    role_of_b: str = "PO",
    estimate_missing_po: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, float]]:
    """
    Merge the two logs and compute delays.

    Returns: (merged_df, delay_by_round, delay_by_block, delay_overall_dict)
    """
    # Load
    an = pd.read_csv(an_csv)
    po = pd.read_csv(po_csv)

    # Establish roles
    if role_of_a == role_of_b:
        raise ValueError("role_of_a and role_of_b must differ")

    # Preprocess each
    an_prep = add_evt_index(an)
    po_prep = add_evt_index(po)

    # Suffix all non-key columns by role
    an_s = suffix_all_but_keys(an_prep, role_of_a, JOIN_KEYS)
    po_s = suffix_all_but_keys(po_prep, role_of_b, JOIN_KEYS)

    # Outer merge on keys
    merged = pd.merge(an_s, po_s, on=JOIN_KEYS, how="outer", validate="m:m")

    # Compute per-event delay in seconds if both sides have absolute timestamps
    mlt_an = f"mlt_datetime_{role_of_a}"
    mlt_po = f"mlt_datetime_{role_of_b}"
    if mlt_an in merged.columns and mlt_po in merged.columns:
        merged["delay_s"] = (merged[mlt_po] - merged[mlt_an]).dt.total_seconds()

    # Delay summaries
    by_round, by_block, overall = summarize_delays(merged)

    # Optional: estimate PO times for AN-only events using median delay
    if estimate_missing_po:
        merged = add_estimated_po_times(merged, by_round, by_block, overall, role_of_a, role_of_b)

    return merged, by_round, by_block, overall


def summarize_delays(merged: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, float]]:
    df = merged.copy()
    df = df[df["delay_s"].notna()]

    def _agg(s: pd.Series) -> Dict[str, float]:
        arr = s.dropna().to_numpy()
        if arr.size == 0:
            return dict(n=0)
        return dict(
            n=float(arr.size),
            median_s=float(np.median(arr)),
            mean_s=float(np.mean(arr)),
            std_s=float(np.std(arr, ddof=0)) if arr.size > 1 else 0.0,
            min_s=float(np.min(arr)),
            p10_s=float(np.percentile(arr, 10)),
            p90_s=float(np.percentile(arr, 90)),
            max_s=float(np.max(arr)),
        )

    # Per-round
    round_keys = ["BlockInstance", "BlockNum", "RoundNum"]
    by_round = (
        df.groupby(round_keys, dropna=False)
        .agg(delay_s=("delay_s", list))
        .reset_index()
    )
    by_round = pd.concat(
        [by_round[round_keys], by_round["delay_s"].apply(lambda lst: pd.Series(_agg(pd.Series(lst))))], axis=1
    )

    # Per-block
    block_keys = ["BlockInstance", "BlockNum"]
    by_block = (
        df.groupby(block_keys, dropna=False)
        .agg(delay_s=("delay_s", list))
        .reset_index()
    )
    by_block = pd.concat(
        [by_block[block_keys], by_block["delay_s"].apply(lambda lst: pd.Series(_agg(pd.Series(lst))))], axis=1
    )

    overall = _agg(df["delay_s"])  # type: ignore[arg-type]
    return by_round, by_block, overall


def add_estimated_po_times(
    merged: pd.DataFrame,
    by_round: pd.DataFrame,
    by_block: pd.DataFrame,
    overall: Dict[str, float],
    role_of_a: str,
    role_of_b: str,
) -> pd.DataFrame:
    out = merged.copy()
    dt_an = f"mlt_datetime_{role_of_a}"
    dt_po = f"mlt_datetime_{role_of_b}"

    # Build lookup dicts of median delay
    rkey = ["BlockInstance", "BlockNum", "RoundNum"]
    round_median = by_round.set_index(rkey)["median_s"].to_dict() if "median_s" in by_round.columns else {}
    bkey = ["BlockInstance", "BlockNum"]
    block_median = by_block.set_index(bkey)["median_s"].to_dict() if "median_s" in by_block.columns else {}
    overall_median = float(overall.get("median_s", np.nan))

    est_vals: List[Optional[pd.Timestamp]] = []
    est_srcs: List[Optional[str]] = []

    for _, row in out.iterrows():
        a_dt = row.get(dt_an)
        p_dt = row.get(dt_po)
        if pd.notna(a_dt) and pd.isna(p_dt):
            rk = (row.get("BlockInstance"), row.get("BlockNum"), row.get("RoundNum"))
            bk = (row.get("BlockInstance"), row.get("BlockNum"))
            if rk in round_median and pd.notna(round_median[rk]):
                est_vals.append(a_dt + timedelta(seconds=float(round_median[rk])))
                est_srcs.append("round_median")
            elif bk in block_median and pd.notna(block_median[bk]):
                est_vals.append(a_dt + timedelta(seconds=float(block_median[bk])))
                est_srcs.append("block_median")
            elif pd.notna(overall_median):
                est_vals.append(a_dt + timedelta(seconds=float(overall_median)))
                est_srcs.append("overall_median")
            else:
                est_vals.append(pd.NaT)
                est_srcs.append(None)
        else:
            est_vals.append(pd.NaT)
            est_srcs.append(None)

    out[f"est_mlt_datetime_{role_of_b}"] = est_vals
    out["est_delay_source"] = est_srcs
    return out


# -----------------------------
# CLI
# -----------------------------

def infer_roles_from_filenames(an_csv: str, po_csv: str) -> Tuple[str, str]:
    """Infer A->AN and B->PO from filenames; falls back to provided defaults."""
    an_base = os.path.basename(an_csv)
    po_base = os.path.basename(po_csv)
    role_of_a = "AN" if re.search(r"_A_", an_base) else "AN"
    role_of_b = "PO" if re.search(r"_B_", po_base) else "PO"
    return role_of_a, role_of_b


def main(argv: Optional[Sequence[str]] = None) -> None:
    p = argparse.ArgumentParser(description="Merge AN & PO event logs and quantify timing differences.")
    p.add_argument("--an", required=True, help="Path to A-file (typically AN)")
    p.add_argument("--po", required=True, help="Path to B-file (typically PO)")
    p.add_argument("--outdir", default=None, help="Directory to write outputs (default: alongside --an)")
    p.add_argument("--role-of-a", default=None, choices=["AN", "PO"], help="Explicit role for the A file")
    p.add_argument("--role-of-b", default=None, choices=["AN", "PO"], help="Explicit role for the B file")
    p.add_argument("--estimate-missing-po", action="store_true", help="Estimate PO timestamps for AN-only events")

    args = p.parse_args(argv)

    role_of_a, role_of_b = infer_roles_from_filenames(args.an, args.po)
    if args.role_of_a:
        role_of_a = args.role_of_a
    if args.role_of_b:
        role_of_b = args.role_of_b
    if role_of_a == role_of_b:
        raise SystemExit("--role-of-a and --role-of-b must differ")

    outdir = args.outdir or os.path.dirname(os.path.abspath(args.an))
    os.makedirs(outdir, exist_ok=True)

    merged, by_round, by_block, overall = merge_an_po(
        args.an,
        args.po,
        role_of_a=role_of_a,
        role_of_b=role_of_b,
        estimate_missing_po=args.estimate_missing_po,
    )

    merged_csv = os.path.join(outdir, "merged_events.csv")
    by_round_csv = os.path.join(outdir, "delay_summary_by_round.csv")
    by_block_csv = os.path.join(outdir, "delay_summary_by_block.csv")
    overall_json = os.path.join(outdir, "delay_summary_overall.json")

    merged.to_csv(merged_csv, index=False)
    by_round.to_csv(by_round_csv, index=False)
    by_block.to_csv(by_block_csv, index=False)
    with open(overall_json, "w") as f:
        json.dump(overall, f, indent=2)

    print(f"Wrote: {merged_csv}")
    print(f"Wrote: {by_round_csv}")
    print(f"Wrote: {by_block_csv}")
    print(f"Wrote: {overall_json}")


if __name__ == "__main__":
    main()
