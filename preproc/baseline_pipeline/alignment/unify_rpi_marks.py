# file: unify_rpi_marks.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Unify simple and verbose RPi marks.

Join keys (outer join):
  ['Device', 'DeviceIP', 'RPi_Source', 'markNumber']

Outputs columns:
  - Keys
  - RPi_Time_simple
  - RPi_Time_verb
  - RPi_Time_unified   (verb if present else simple)
  - ML_Time_verb, RPi_Time_verb_str, Monotonic_Time_verb, Monotonic_Time_Adj_verb
  - RPi_Timestamp_Source
  - LogFile_simple, LogFile_verb, LogLineText_simple, LogLineText_verb
  - orphaned_from  ("simple" or "verb" when missing counterpart; "" when both)
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

KEYS = ["Device", "DeviceIP", "RPi_Source", "markNumber"]

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Unify simple + verbose RPi marks into one file.")
    p.add_argument("--simple-marks", required=True, help="Path to <…>_RPiMarks.csv from extractor")
    p.add_argument("--verb-marks", required=True, help="Path to <…>_RPi_verb.csv from verb_to_rpi_marks.py")
    p.add_argument("--out-csv", required=True, help="Output unified CSV path")
    p.add_argument("--label", required=True, help="BioPac or RNS")
    p.add_argument("--ml-csv-file", required=True)
    p.add_argument("--strip-ml-suffixes", default="_events_final,_processed")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--marks-timestamp-col", default="RPi_Time_verb")
    return p.parse_args()

def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        # minimal frame with join keys present so outer merge works
        return pd.DataFrame(columns=KEYS, dtype="string")
    return pd.read_csv(path, dtype="string")

def main() -> None:
    args = parse_args()
    print('starting the unify script')
    #marksDir = Path(args.base_dir, args.proc_dir, 'RPi_preproc', args.label)
    #simple_path = marksDir / args.simple_marks
    #verb_path   = marksDir / args.verb_marks
    #out_path    = marksDir / "RPi_unified" / args.out_csv
    #out_path.parent.mkdir(parents=True, exist_ok=True)
    
    df_s = _read_csv(Path(args.simple_marks))
    df_v = _read_csv(Path(args.verb_marks))
    print('was not able to read the csv files')
    # Ensure keys exist
    for k in KEYS:
        if k not in df_s.columns: df_s[k] = pd.Series(dtype="string")
        if k not in df_v.columns: df_v[k] = pd.Series(dtype="string")

    # Normalizations:
    # Simple extractor emits RPi_Timestamp (datetime) + LogPairIndex (0-based). Add/derive markNumber if missing.
    if "RPi_Time_simple" not in df_s.columns and "RPi_Timestamp" in df_s.columns:
        df_s.rename(columns={"RPi_Timestamp": "RPi_Time_simple"}, inplace=True)
    if "markNumber" not in df_s.columns:
        if "LogPairIndex" in df_s.columns:
            # markNumber should be 1-based
            try:
                df_s["markNumber"] = (pd.to_numeric(df_s["LogPairIndex"], errors="coerce") + 1).astype("Int64").astype("string")
            except Exception:
                df_s["markNumber"] = pd.Series(dtype="string")
        else:
            df_s["markNumber"] = pd.Series(dtype="string")

    # Verb file already has RPi_Time_verb and markNumber
    if "RPi_Time_verb" not in df_v.columns and "RPi_Timestamp" in df_v.columns:
        df_v.rename(columns={"RPi_Timestamp": "RPi_Time_verb"}, inplace=True)

    # Outer join on keys
    merged = df_s.merge(
        df_v,
        on=KEYS,
        how="outer",
        suffixes=("_simple", "_verb"),
        sort=True,
        copy=False,
    )

    # Canonical unified time: prefer verb if present
    merged["RPi_Time_unified"] = merged["RPi_Time_verb"].where(
        merged.get("RPi_Time_verb").notna() & (merged.get("RPi_Time_verb") != ""),
        merged.get("RPi_Time_simple"),
    )

    # Orphans
    only_simple = merged.get("RPi_Time_simple").notna() & (merged.get("RPi_Time_simple") != "") & (
        merged.get("RPi_Time_verb").isna() | (merged.get("RPi_Time_verb") == "")
    )
    only_verb = merged.get("RPi_Time_verb").notna() & (merged.get("RPi_Time_verb") != "") & (
        merged.get("RPi_Time_simple").isna() | (merged.get("RPi_Time_simple") == "")
    )

    merged["orphaned_from"] = ""
    merged.loc[only_simple, "orphaned_from"] = "verb"
    merged.loc[only_verb,   "orphaned_from"] = "simple"

    if bool(only_simple.sum()):
        print(f"[warn] {int(only_simple.sum())} simple-only marks (no verbose counterpart)")
    if bool(only_verb.sum()):
        print(f"[warn] {int(only_verb.sum())} verbose-only marks (no simple counterpart)")

    # Select output columns; guard existence
    keep_cols = KEYS + [
        "RPi_Time_simple",
        "RPi_Time_verb",
        "RPi_Time_unified",
        "ML_Time_verb",
        "RPi_Time_verb_str",
        "Mono_Time_Raw_verb",
        "Mono_Time_verb",
        "RPi_Timestamp_Source",
        "LogFile_simple",
        "LogFile_verb",
        "LogLineText_simple",
        "LogLineText_verb",
        "orphaned_from",
    ]
    for c in keep_cols:
        if c not in merged.columns:
            merged[c] = pd.Series(dtype="string")

    out_df = merged.loc[:, keep_cols].copy()
    out_df = out_df.fillna("unknown")
    out_df.replace({"": "unknown"}, inplace=True)
    timestamp_col = str(args.marks_timestamp_col)
    out_df.loc[:, timestamp_col] = pd.to_datetime(out_df[timestamp_col], errors="coerce")
    final_out_df = out_df.sort_values(timestamp_col, ascending=True, na_position="last")
    final_out_df.dropna(how="all", inplace=True)
    final_out_df.to_csv(args.out_csv, index=False)
    print(f"[ok] wrote unified marks → {args.out_csv}")

if __name__ == "__main__":
    main()
