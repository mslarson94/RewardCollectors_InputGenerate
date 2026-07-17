# file: alignment/merge_rpi_event_files2.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from batchAlignHelpers import _is_label_col


def _parse_aligned_stem(stem: str) -> Tuple[str, str, str]:
    """
    Parse stems like:
      <base>_BioPac_<device>_aligned_with_RPi
      <base>_RNS_<device>_aligned_with_RPi

    Returns:
      (base, label, device)
    """
    suffix = "_aligned_with_RPi"
    if not stem.endswith(suffix):
        raise ValueError(f"cannot parse aligned output stem '{stem}'")

    core = stem[: -len(suffix)]
    if "_BioPac_" in core:
        base, device = core.rsplit("_BioPac_", 1)
        return base, "BioPac", device
    if "_RNS_" in core:
        base, device = core.rsplit("_RNS_", 1)
        return base, "RNS", device

    raise ValueError(f"cannot parse base/label/device from aligned output stem '{stem}'")


def _load_if_exists(path: Optional[Path]) -> Optional[pd.DataFrame]:
    if path and path.exists():
        return pd.read_csv(path)
    return None


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Merge BioPac and/or RNS aligned ML CSVs into a single combined events file"
    )
    ap.add_argument(
        "--biopac_events_csv",
        default="",
        help="Path to <base>_BioPac_<device>_aligned_with_RPi.csv (optional)",
    )
    ap.add_argument(
        "--rns_events_csv",
        default="",
        help="Path to <base>_RNS_<device>_aligned_with_RPi.csv (optional)",
    )
    ap.add_argument(
        "--out_dir",
        default="",
        help="Directory for output (defaults to folder of first provided file)",
    )
    args = ap.parse_args()

    bio_path = Path(args.biopac_events_csv) if args.biopac_events_csv else None
    rns_path = Path(args.rns_events_csv) if args.rns_events_csv else None

    if not bio_path and not rns_path:
        raise SystemExit("no input provided: supply --biopac_events_csv and/or --rns_events_csv")

    df_bio = _load_if_exists(bio_path)
    df_rns = _load_if_exists(rns_path)

    if df_bio is None and df_rns is None:
        raise SystemExit("neither input file exists on disk")

    src_path = bio_path if df_bio is not None else rns_path
    if src_path is None:
        raise SystemExit("could not determine source path")

    base, _, device = _parse_aligned_stem(src_path.stem)

    out_root = Path(args.out_dir) if args.out_dir else src_path.parent
    out_dir = out_root / "BioPacRNS"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"{base}_{device}_BioPacRNS_events.csv"

    if df_bio is None or df_rns is None:
        single_df = df_bio if df_bio is not None else df_rns
        if single_df is None:
            raise SystemExit("no readable input dataframe found")
        single_df.to_csv(out_csv, index=False)
        print(f"[ok] wrote combined CSV -> {out_csv}")
        return

    same_len = len(df_bio) == len(df_rns)
    same_ts = False
    if same_len and ("eMLT_orig" in df_bio.columns and "eMLT_orig" in df_rns.columns):
        same_ts = pd.Series(df_bio["eMLT_orig"].astype(str).values).equals(
            pd.Series(df_rns["eMLT_orig"].astype(str).values)
        )

    if not same_len or not same_ts:
        print("[warn] BioPac/RNS aligned files differ in length or eMLT_orig; merging by index.")

    out = df_bio.copy()

    for col in df_rns.columns:
        if _is_label_col(col) or col.startswith("RNS_"):
            out[col] = df_rns[col]
        elif col not in out.columns:
            out[col] = df_rns[col]
        else:
            if not out[col].equals(df_rns[col]):
                print(f"[warn] column differs between files, keeping BioPac values: {col}")

    out.to_csv(out_csv, index=False)
    print(f"[ok] wrote combined CSV -> {out_csv}")


if __name__ == "__main__":
    main()