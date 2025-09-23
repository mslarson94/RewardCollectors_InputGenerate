#!/usr/bin/env python3
"""
extract_pin_drops.py

Collect only PinDrop_Moment rows across ALL participants and (optionally) split by coin type.
Designed to run on the coin-labeled event CSVs you already produce.

Targets columns created upstream:
- coinLabel, actualClosestCoinLabel, actualClosestCoinDist,
  distToPin_LV, distToPin_NV, distToPin_HV

Upstream context: mergeEventsV3.py merges/exports event CSVs; this script reads the flat per-file CSVs instead.  :contentReference[oaicite:0]{index=0}
run_overnight_modular3.sh shows the Events_CoinsLabeled and Events_Final_* output locations.  :contentReference[oaicite:1]{index=1}
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd


PIN_EVENT_DEFAULT = "PinDrop_Moment"
EVENT_COL_CANDIDATES = [
    "EventName", "eventName", "Event", "event", "EventType", "eventType", "Event_Label"
]


def _find_event_col(df: pd.DataFrame, pin_event: str) -> Optional[str]:
    for c in EVENT_COL_CANDIDATES:
        if c in df.columns:
            return c
    # Fallback: try to sniff a column that contains the pin_event value (case-insensitive)
    pin_lower = pin_event.lower()
    obj_cols = [c for c in df.columns if df[c].dtype == "object" or pd.api.types.is_string_dtype(df[c])]
    for c in obj_cols:
        vals = df[c].dropna().astype(str)
        if any(pin_lower == v.strip().lower() for v in vals.head(200).unique()):
            return c
    return None


def _coerce_float_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _ensure_cols(df: pd.DataFrame, cols: List[str]) -> None:
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan


def _dist_for_label_row(row: pd.Series, label_col: str) -> float:
    lab = str(row.get(label_col, "") or "").strip().upper()
    if lab == "LV":
        return float(row.get("distToPin_LV", np.nan))
    if lab == "NV":
        return float(row.get("distToPin_NV", np.nan))
    if lab == "HV":
        return float(row.get("distToPin_HV", np.nan))
    return float("nan")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Extract PinDrop_Moment rows across all participants and write combined & per-coin CSVs."
    )
    ap.add_argument("--root-dir", required=True, type=Path, help="Project root (e.g., '/Users/you/RC_TestingNotes').")
    ap.add_argument("--proc-dir", required=True, type=Path, help="Dataset subdir under --root-dir (e.g., 'FreshStart').")
    ap.add_argument("--input-dir-name", default="Events_CoinsLabeled",
                    help="Folder under <root/proc/full> that contains source CSVs.")
    ap.add_argument("--pattern", default="*_events_coinLabel.csv",
                    help="Glob for source CSVs inside input-dir-name.")
    ap.add_argument("--pin-event", default=PIN_EVENT_DEFAULT, help="Event name to filter (default: PinDrop_Moment).")
    ap.add_argument("--out-dir-name", default="PinDrops_All", help="Output folder under <root/proc/full>.")
    ap.add_argument("--split-on", choices=["coinLabel", "actualClosestCoinLabel"], default="coinLabel",
                    help="Which label column defines coin type for splits.")
    args = ap.parse_args()

    proc_dir = (args.proc_dir if args.proc_dir.is_absolute() else (args.root_dir / args.proc_dir)) / "full"
    in_dir = proc_dir / args.input_dir_name / "augmented"
    print(in_dir)
    out_dir = proc_dir / args.out_dir_name
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(in_dir.glob(args.pattern))
    if not files:
        print(f"[warn] No files matched {in_dir}/{args.pattern}")
        return

    # keep_cols = [
    #     # identifiers (keep if present)
    #     "participantID", "pairID", "testingDate", "currentRole", "ptIsAorB",
    #     "coinSet", "sessionType", "device", "main_RR",
    #     # coin label & QA cols
    #     "coinLabel", "actualClosestCoinLabel", "actualClosestCoinDist",
    #     "distToPin_LV", "distToPin_NV", "distToPin_HV",
    #     # optionally useful
    #     "mLTimestamp", "AppTime"
    # ]

    keep_cols = [
      "mLTimestamp","AppTime","mLTimestamp_raw","start_AppTime","end_AppTime","start_mLT","end_mLT","lo_eventType",
      "med_eventType","hi_eventType","hiMeta_eventType","source","BlockInstance","BlockNum","RoundNum","CoinSetID",
      "BlockStatus","BlockType","chestPin_num","origRow_start","origRow_end","participantID","pairID","testingDate",
      "sessionType","ptIsAorB","coinSet","device","main_RR","currentRole","source_file","relative_path",
      "pinLocal_x","pinLocal_y","pinLocal_z","coinPos_x","coinPos_y","coinPos_z",
      "dropDist","dropQual","coinValue","currRoundNum","curmLerfRoundNum","runningBlockTotal","currGrandTotal","SwapVote",
      "SwapVoteScore","mark","coinLabel","actualClosestCoinLabel","actualClosestCoinDist","distToPin_LV","distToPin_NV","distToPin_HV",
      "distFromCoinPos_LV","distFromCoinPos_NV","distFromCoinPos_HV","coinStemUsed","coinSetUsed","begOfFile","block_elapsed_s","round_elapsed_s",
      "truecontent_elapsed_s","HeadPosAnchored_x_at_start","HeadPosAnchored_y_at_start","HeadPosAnchored_z_at_start","HeadForthAnchored_yaw_at_start","HeadForthAnchored_pitch_at_start","HeadForthAnchored_roll_at_start",
      "HeadPosAnchored_x_at_end","HeadPosAnchored_y_at_end","HeadPosAnchored_z_at_end","HeadForthAnchored_yaw_at_end","HeadForthAnchored_pitch_at_end","HeadForthAnchored_roll_at_end"
    ]



    all_rows: List[pd.DataFrame] = []
    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception as e:
            print(f"[skip] {f.name}: {e}")
            continue

        event_col = _find_event_col(df, args.pin_event)
        if not event_col:
            print(f"[skip] {f.name}: could not find event column")
            continue

        # Filter to PinDrop rows
        mask = df[event_col].astype(str).str.strip().str.lower() == args.pin_event.lower()
        pin_df = df.loc[mask].copy()
        if pin_df.empty:
            continue

        # Ensure expected columns exist & numeric
        _ensure_cols(pin_df, ["coinLabel", "actualClosestCoinLabel",
                              "actualClosestCoinDist", "distToPin_LV", "distToPin_NV", "distToPin_HV"])
        for c in ["actualClosestCoinDist", "distToPin_LV", "distToPin_NV", "distToPin_HV", "truecontent_elapsed_s"]:
            pin_df[c] = _coerce_float_series(pin_df[c])

        # Compute per-row chosen distances
        pin_df["pinDropDist_by_actualClosest"] = pin_df["actualClosestCoinDist"]
        pin_df["pinDropDist_by_coinLabel"] = pin_df.apply(_dist_for_label_row, axis=1, label_col="coinLabel")

        # Add provenance
        pin_df["sourceFile"] = f.name

        # Subset to tidy, preserving any keep columns that exist
        cols_present = [c for c in keep_cols if c in pin_df.columns]
        cols_present += ["pinDropDist_by_actualClosest", "pinDropDist_by_coinLabel", "sourceFile"]
        pin_df = pin_df[cols_present]

        all_rows.append(pin_df)

    if not all_rows:
        print("[warn] No PinDrop_Moment rows found.")
        return

    all_df = pd.concat(all_rows, ignore_index=True)

    # Write combined
    all_path = out_dir / "PinDrops_ALL.csv"
    all_df.to_csv(all_path, index=False)
    print(f"[ok] Wrote {all_path}")

    # # Split by coin type
    # split_col = args.split_on if args.split_on in all_df.columns else "coinLabel"
    # for lab in ("LV", "NV", "HV"):
    #     part = all_df[(all_df[split_col].astype(str).str.upper() == lab)]
    #     if part.empty:
    #         continue
    #     p = out_dir / f"PinDrops_{lab}.csv"
    #     part.to_csv(p, index=False)
    #     print(f"[ok] Wrote {p}")

    # Split by coin type (LV/NV/HV) and then by coin set (A/B/C/D/Ax/Bx/...)
    split_col_label = args.split_on if args.split_on in all_df.columns else "coinLabel"

    # Prefer 'coinSet', else fall back to 'coinSetUsed'; if neither exists, only split by label.
    split_col_coinset = (
        "coinSet" if "coinSet" in all_df.columns
        else ("coinSetUsed" if "coinSetUsed" in all_df.columns else None)
    )

    for lab in ("LV", "NV", "HV"):
        part = all_df[all_df[split_col_label].astype(str).str.strip().str.upper() == lab]
        if part.empty:
            continue

        # write the per-label file (like your original working block)
        p_label = out_dir / f"PinDrops_{lab}.csv"
        part.to_csv(p_label, index=False)
        print(f"[ok] Wrote {p_label}")

        # further split by coin set if we have that column
        if split_col_coinset:
            # discover coin sets present instead of hardcoding
            coin_sets = sorted({str(x) for x in part[split_col_coinset].dropna().unique()})
            for cs in coin_sets:
                mask = part[split_col_coinset].astype(str).str.strip().str.upper() == cs.upper()
                part2 = part[mask]
                if part2.empty:
                    continue
                p_cs = out_dir / f"PinDrops_{lab}_{cs}.csv"
                part2.to_csv(p_cs, index=False)
                print(f"[ok] Wrote {p_cs}")


if __name__ == "__main__":
    main()
