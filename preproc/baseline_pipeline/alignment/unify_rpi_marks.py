#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

KEYS = ["Device", "DeviceIP", "RPi_Source", "markNumber"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Unify simple + verbose RPi marks into one file.")
    p.add_argument("--simple-marks", required=True, help="Path to simple marks CSV")
    p.add_argument("--verb-marks", required=True, help="Path to verb marks CSV")
    p.add_argument("--out-csv", required=True, help="Output unified CSV path")
    p.add_argument("--label", required=True, help="BioPac or RNS")
    p.add_argument("--ml-csv-file", required=True)
    p.add_argument("--strip-ml-suffixes", default="_events_final,_processed")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--marks-timestamp-col", default="RPi_Time_verb")
    return p.parse_args()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=KEYS, dtype="string")
    return pd.read_csv(path, dtype="string")


def _ensure_col(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        df[col] = pd.Series(dtype="string")


def main() -> None:
    args = parse_args()

    simple_path = Path(args.simple_marks)
    verb_path = Path(args.verb_marks)
    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df_s = _read_csv(simple_path)
    df_v = _read_csv(verb_path)

    for k in KEYS:
        _ensure_col(df_s, k)
        _ensure_col(df_v, k)

    if "RPi_Time_simple" not in df_s.columns and "RPi_Timestamp" in df_s.columns:
        df_s = df_s.rename(columns={"RPi_Timestamp": "RPi_Time_simple"})
    _ensure_col(df_s, "RPi_Time_simple")

    if "markNumber" not in df_s.columns:
        if "LogPairIndex" in df_s.columns:
            try:
                df_s["markNumber"] = (
                    pd.to_numeric(df_s["LogPairIndex"], errors="coerce") + 1
                ).astype("Int64").astype("string")
            except Exception:
                df_s["markNumber"] = pd.Series(dtype="string")
        else:
            df_s["markNumber"] = pd.Series(dtype="string")

    if "RPi_Time_verb" not in df_v.columns and "RPi_Timestamp" in df_v.columns:
        df_v = df_v.rename(columns={"RPi_Timestamp": "RPi_Time_verb"})
    _ensure_col(df_v, "RPi_Time_verb")

    merged = df_s.merge(
        df_v,
        on=KEYS,
        how="outer",
        suffixes=("_simple", "_verb"),
        sort=True,
        copy=False,
    )

    _ensure_col(merged, "RPi_Time_simple")
    _ensure_col(merged, "RPi_Time_verb")

    verb_time = merged.get("RPi_Time_verb")
    simple_time = merged.get("RPi_Time_simple")

    has_simple = simple_time.notna() & (simple_time != "")
    has_verb = verb_time.notna() & (verb_time != "")

    if not bool((has_simple | has_verb).any()):
        print(f"[skip] no usable simple or verb marks for {args.label}; not writing {out_path.name}")
        return

    merged["RPi_Time_unified"] = verb_time.where(has_verb, simple_time)

    only_simple = has_simple & ~has_verb
    only_verb = has_verb & ~has_simple

    merged["orphaned_from"] = ""
    merged.loc[only_simple, "orphaned_from"] = "verb"
    merged.loc[only_verb, "orphaned_from"] = "simple"

    if bool(only_simple.sum()):
        print(f"[warn] {int(only_simple.sum())} simple-only marks (no verbose counterpart)")
    if bool(only_verb.sum()):
        print(f"[warn] {int(only_verb.sum())} verbose-only marks (no simple counterpart)")

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
        _ensure_col(merged, c)

    out_df = merged.loc[:, keep_cols].copy()
    out_df = out_df.fillna("unknown")
    out_df.replace({"": "unknown"}, inplace=True)

    timestamp_col = str(args.marks_timestamp_col)
    if timestamp_col not in out_df.columns:
        out_df[timestamp_col] = pd.Series(dtype="string")

    out_df[timestamp_col] = pd.to_datetime(out_df[timestamp_col], errors="coerce")
    out_df = out_df.sort_values(timestamp_col, ascending=True, na_position="last")

    out_df.to_csv(out_path, index=False)
    print(f"[ok] wrote unified marks → {out_path}")


if __name__ == "__main__":
    main()