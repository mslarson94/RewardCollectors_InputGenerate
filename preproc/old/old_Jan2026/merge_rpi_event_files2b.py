# ========================= merge_rpi_event_files.py =========================
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Merge the per-source events CSVs (BioPac and/or RNS) into a single combined events file.

Rules:
- If BOTH are provided and align 1:1, keep one copy of the base ML columns and append
  all label-specific columns from each source (BioPac_*, RNS_*).
- If ONLY one exists, write a combined file anyway (it will just contain that source).
- If the two files differ in length or in mLTimestamp sequence, preserve ALL rows by
  aligning by positional index (max length) and coalescing overlapping columns.

Output:
  <base>_<device>_BioPacRNS_events.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from batchAlignHelpers import _is_label_col, _parse_base_and_device


def _is_base_col(col: str) -> bool:
    # Not a label col, and not explicitly namespaced
    return (not _is_label_col(col)) and (not col.startswith("BioPac_")) and (not col.startswith("RNS_"))


def _coalesce_into(target: pd.DataFrame, src: pd.DataFrame, cols: list[str]) -> None:
    for col in cols:
        if col in target.columns:
            # Fill NaNs in target from src; leave existing target values in place
            target[col] = target[col].combine_first(src.get(col))
        else:
            target[col] = src.get(col)


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge BioPac and RNS events CSVs into a single combined file")
    ap.add_argument("--biopac_events_csv", default="", help="Path to <base>_<device>_BioPac_events.csv (optional)")
    ap.add_argument("--rns_events_csv", default="", help="Path to <base>_<device>_RNS_events.csv (optional)")
    ap.add_argument("--out_dir", default="", help="Directory for output (defaults to folder of first provided file)")
    args = ap.parse_args()

    bio_path = Path(args.biopac_events_csv) if args.biopac_events_csv else None
    rns_path = Path(args.rns_events_csv) if args.rns_events_csv else None

    if not bio_path and not rns_path:
        raise SystemExit("no input provided: supply --biopac_events_csv and/or --rns_events_csv")

    df_bio = pd.read_csv(bio_path) if (bio_path and bio_path.exists()) else None
    df_rns = pd.read_csv(rns_path) if (rns_path and rns_path.exists()) else None

    if df_bio is None and df_rns is None:
        raise SystemExit("neither input file exists on disk")

    # Determine base/device from whichever file we have
    src_path = bio_path or rns_path
    base, device, _ = _parse_base_and_device(src_path.stem)

    # Choose out_dir
    out_dir = Path(args.out_dir) if args.out_dir else src_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"{base}_{device}_BioPacRNS_events.csv"

    # If only one file, just save it under the combined name
    if df_bio is None or df_rns is None:
        (df_bio or df_rns).to_csv(out_csv, index=False)
        print(out_csv)
        return

    # Normalize indices and decide whether we must preserve extra rows
    same_len = len(df_bio) == len(df_rns)
    same_ts = False
    if same_len and ("mLTimestamp" in df_bio.columns and "mLTimestamp" in df_rns.columns):
        same_ts = pd.Series(df_bio["mLTimestamp"].astype(str).values).equals(
            pd.Series(df_rns["mLTimestamp"].astype(str).values)
        )

    if not same_len or not same_ts:
        print("[warn] BioPac/RNS events differ in length or timestamps; preserving all rows by index.")
        max_len = max(len(df_bio), len(df_rns))
        df_bio = df_bio.reindex(range(max_len)).reset_index(drop=True)
        df_rns = df_rns.reindex(range(max_len)).reset_index(drop=True)
    else:
        df_bio = df_bio.reset_index(drop=True)
        df_rns = df_rns.reset_index(drop=True)

    # Start from BioPac copy
    out = df_bio.copy()

    # 1) Base ML columns: coalesce (fill NaNs in BioPac from RNS)
    base_cols_bio = [c for c in df_bio.columns if _is_base_col(c)]
    base_cols_rns = [c for c in df_rns.columns if _is_base_col(c)]
    # ensure all base cols present
    _coalesce_into(out, df_rns, sorted(set(base_cols_bio) | set(base_cols_rns)))

    # 2) Label-specific columns: add/merge per namespace
    biopac_label_cols = [c for c in df_bio.columns if (c.startswith("BioPac_") or _is_label_col(c))]
    rns_label_cols = [c for c in df_rns.columns if (c.startswith("RNS_") or _is_label_col(c))]

    # BioPac label cols: prefer existing, fill if missing (already in out via copy); just ensure presence
    _coalesce_into(out, df_bio, biopac_label_cols)

    # RNS label cols: add; if any name collides, coalesce rather than overwrite
    _coalesce_into(out, df_rns, rns_label_cols)

    out.to_csv(out_csv, index=False)
    print(out_csv)


if __name__ == "__main__":
    main()
