#!/usr/bin/env python3
# add_coin_labels_by_filestem_qol.py
"""
Enhancements:
- Always key by the CURRENT FILE'S STEM against compiledCoinLocations' source-file list.
- Adds LV/NV/HV distances to the pin point:  distToPin_LV/NV/HV
- Adds LV/NV/HV distances to the coinPos point: distFromCoinPos_LV/NV/HV
- Adds coinStemUsed (the normalized stem that was matched)
- --fail-on-miss: exit non-zero if any input file had no stem match
- --debug: print diagnostics

Usage:
  python add_coin_labels_by_filestem_qol.py \
    --compiled "/path/to/compiledCoinLocations.csv" \
    --events-dir "/path/to/events_dir" \
    --out-dir "/path/to/output_dir" \
    --pattern "*_events_flat.csv" \
    [--fail-on-miss] [--debug]
"""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

LABELS: Tuple[str, str, str] = ("LV", "NV", "HV")

# ----------------------------- helpers -----------------------------

def _root_key(s: str) -> str:
    """Lowercased stem with ALL extensions removed and common suffixes stripped."""
    name = Path(str(s).strip()).name
    prev = None
    # remove all extensions (.csv, .gz, etc.)
    while prev != name:
        prev = name
        name = Path(name).stem
    root = name
    for suf in (
        "_events_coinLabel",
        "_events_flat",
        "_processed_events",
        "_events",
        "_main_meta",
        "_meta",
        "_main",
    ):
        if root.lower().endswith(suf):
            root = root[: -len(suf)]
    return root.lower()

def _to_float(v) -> Optional[float]:
    try:
        f = float(v)
        if np.isnan(f):
            return None
        return f
    except Exception:
        return None

@dataclass(frozen=True)
class CoinTriplet:
    LV: Tuple[Optional[float], Optional[float], Optional[float]]
    NV: Tuple[Optional[float], Optional[float], Optional[float]]
    HV: Tuple[Optional[float], Optional[float], Optional[float]]

    def as_dict(self) -> Dict[str, Tuple[Optional[float], Optional[float], Optional[float]]]:
        return {"LV": self.LV, "NV": self.NV, "HV": self.HV}

def _euclid(a: Tuple[Optional[float], Optional[float], Optional[float]],
            b: Tuple[Optional[float], Optional[float], Optional[float]]) -> float:
    ax, ay, az = a
    bx, by, bz = b
    if any(v is None for v in (ax, ay, az, bx, by, bz)):
        return float("nan")
    return math.sqrt((ax - bx) ** 2 + (ay - by) ** 2 + (az - bz) ** 2)

def _nearest_label(
    point: Tuple[Optional[float], Optional[float], Optional[float]],
    triplet: CoinTriplet,
) -> Tuple[str, float]:
    best_label = ""
    best_dist = float("inf")
    for lab, pos in triplet.as_dict().items():
        d = _euclid(point, pos)
        if not math.isnan(d) and d < best_dist:
            best_label, best_dist = lab, d
    if best_label == "":
        return ("", float("nan"))
    return (best_label, best_dist)

# Accept common coordinate column variants.
COIN_POS_CANDIDATES = [
    ("coinPos_x", "coinPos_y", "coinPos_z"),
    ("coin_pos_x", "coin_pos_y", "coin_pos_z"),
    ("coinX", "coinY", "coinZ"),
    ("coin_x", "coin_y", "coin_z"),
]
PIN_LOCAL_CANDIDATES = [
    ("pinLocal_x", "pinLocal_y", "pinLocal_z"),
    ("pin_local_x", "pin_local_y", "pin_local_z"),
    ("pinX", "pinY", "pinZ"),
    ("pin_x", "pin_y", "pin_z"),
]

def _pick_xyz(df: pd.DataFrame, candidates: List[Tuple[str, str, str]]) -> Tuple[str, str, str]:
    for trip in candidates:
        if all(col in df.columns for col in trip):
            return trip
    # Create canonical columns so downstream logic works.
    first = candidates[0]
    for col in first:
        if col not in df.columns:
            df[col] = np.nan
    return first

# ------------------------ compiled map building ------------------------

def _choose_source_field(columns: List[str]) -> Optional[str]:
    # Prefer exact lower-case 'source_file' if present; otherwise accept 'SourceFiles'
    lc = {c.lower(): c for c in columns}
    if "source_file" in lc:
        return lc["source_file"]
    if "sourcefiles" in lc:
        return lc["sourcefiles"]
    return None

def _split_sources(s: str) -> List[str]:
    # Accept semicolon or comma separated lists
    s = (s or "").strip()
    if not s:
        return []
    parts: List[str] = []
    for chunk in s.replace(",", ";").split(";"):
        chunk = chunk.strip()
        if chunk:
            parts.append(chunk)
    return parts

def load_compiled_index_by_stem(compiled_csv: Path, debug: bool = False) -> Dict[str, CoinTriplet]:
    """
    Returns a mapping: file STEM (normalized via _root_key) -> CoinTriplet.
    Built strictly from the compiled CSV's source-file list column
    ('source_file' or 'SourceFiles').
    """
    df = pd.read_csv(compiled_csv, dtype="string")

    # Ensure numeric coin columns exist
    for col in [f"{lab}_{axis}" for lab in LABELS for axis in ("x", "y", "z")]:
        df[col] = pd.to_numeric(df[col], errors="coerce") if col in df.columns else np.nan

    src_field = _choose_source_field(list(df.columns))
    if src_field is None:
        print("[error] compiled CSV lacks 'source_file' or 'SourceFiles' column", file=sys.stderr)
        sys.exit(1)

    stem_map: Dict[str, CoinTriplet] = {}
    duplicates_reported: set[str] = set()

    for _, r in df.iterrows():
        trip = CoinTriplet(
            LV=(_to_float(r.get("LV_x")), _to_float(r.get("LV_y")), _to_float(r.get("LV_z"))),
            NV=(_to_float(r.get("NV_x")), _to_float(r.get("NV_y")), _to_float(r.get("NV_z"))),
            HV=(_to_float(r.get("HV_x")), _to_float(r.get("HV_y")), _to_float(r.get("HV_z"))),
        )
        for entry in _split_sources(r.get(src_field, "")):
            stem = _root_key(entry)
            if not stem:
                continue
            if stem in stem_map and debug and stem not in duplicates_reported:
                print(f"[debug] duplicate stem in compiled mapping: '{stem}' -> keeping first", file=sys.stderr)
                duplicates_reported.add(stem)
            stem_map.setdefault(stem, trip)

    if debug:
        print(f"[debug] compiled index size (unique stems): {len(stem_map)}")
    return stem_map

# ---------------------------- labeling core ----------------------------

def label_file_with_stem(df: pd.DataFrame, trip: Optional[CoinTriplet], coin_stem_used: str) -> pd.DataFrame:
    coin_x, coin_y, coin_z = _pick_xyz(df, COIN_POS_CANDIDATES)
    pin_x, pin_y, pin_z = _pick_xyz(df, PIN_LOCAL_CANDIDATES)

    # Ensure numeric
    for c in (coin_x, coin_y, coin_z, pin_x, pin_y, pin_z):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    coin_labels: List[str] = []
    closest_labels: List[str] = []
    closest_dists: List[float] = []

    # QA distances (to pinLocal)
    dist_pin_LV: List[float] = []
    dist_pin_NV: List[float] = []
    dist_pin_HV: List[float] = []

    # QA distances (to coinPos)
    dist_coin_LV: List[float] = []
    dist_coin_NV: List[float] = []
    dist_coin_HV: List[float] = []

    for _, row in df.iterrows():
        if trip is None:
            coin_labels.append("")
            closest_labels.append("")
            closest_dists.append(float("nan"))
            dist_pin_LV.append(float("nan")); dist_pin_NV.append(float("nan")); dist_pin_HV.append(float("nan"))
            dist_coin_LV.append(float("nan")); dist_coin_NV.append(float("nan")); dist_coin_HV.append(float("nan"))
            continue

        pin_pt = (row[pin_x], row[pin_y], row[pin_z])
        coin_pt = (row[coin_x], row[coin_y], row[coin_z])

        # distances to pin
        dplv = _euclid(pin_pt, trip.LV); dpnv = _euclid(pin_pt, trip.NV); dphv = _euclid(pin_pt, trip.HV)
        dist_pin_LV.append(dplv); dist_pin_NV.append(dpnv); dist_pin_HV.append(dphv)

        # distances to coinPos
        dclv = _euclid(coin_pt, trip.LV); dcnv = _euclid(coin_pt, trip.NV); dchv = _euclid(coin_pt, trip.HV)
        dist_coin_LV.append(dclv); dist_coin_NV.append(dcnv); dist_coin_HV.append(dchv)

        # labels
        clab, _ = _nearest_label(coin_pt, trip)
        alab, adist = _nearest_label(pin_pt, trip)

        coin_labels.append(clab)
        closest_labels.append(alab)
        closest_dists.append(adist)

    out = df.copy()
    out["coinLabel"] = pd.Series(coin_labels, index=df.index, dtype="string")
    out["actualClosestCoinLabel"] = pd.Series(closest_labels, index=df.index, dtype="string")
    out["actualClosestCoinDist"] = pd.Series(closest_dists, index=df.index)

    # QA columns (pin distances)
    out["distToPin_LV"] = pd.Series(dist_pin_LV, index=df.index)
    out["distToPin_NV"] = pd.Series(dist_pin_NV, index=df.index)
    out["distToPin_HV"] = pd.Series(dist_pin_HV, index=df.index)

    # QA columns (coinPos distances)
    out["distFromCoinPos_LV"] = pd.Series(dist_coin_LV, index=df.index)
    out["distFromCoinPos_NV"] = pd.Series(dist_coin_NV, index=df.index)
    out["distFromCoinPos_HV"] = pd.Series(dist_coin_HV, index=df.index)

    # Traceability
    out["coinStemUsed"] = coin_stem_used
    return out

def out_name_for(infile: Path) -> str:
    base = infile.name
    if base.endswith("_processed_events.csv"):
        return base[:-len("_processed_events.csv")] + "_events_coinLabel.csv"
    if base.endswith("_events_flat.csv"):
        return base[:-len("_events_flat.csv")] + "_events_coinLabel.csv"
    return infile.stem + "_events_coinLabel.csv"

# ----------------------------- pipeline -----------------------------

def process_dir(events_dir: Path, out_dir: Path, compiled_csv: Path, pattern: str,
                fail_on_miss: bool, debug: bool) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    stem_index = load_compiled_index_by_stem(compiled_csv, debug=debug)

    files = sorted(events_dir.glob(pattern))
    if not files:
        print(f"[warn] No files matched pattern '{pattern}' in {events_dir}", file=sys.stderr)

    unmatched: List[str] = []

    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception as e:
            print(f"[warn] Skipping {f.name}: failed to read CSV: {e}", file=sys.stderr)
            continue

        stem = _root_key(f.name)
        trip = stem_index.get(stem)
        if debug:
            print(f"[debug] input: {f.name}  -> stem: '{stem}'  -> match: {'yes' if trip else 'no'}")
        if trip is None:
            unmatched.append(stem)

        labeled = label_file_with_stem(df, trip, coin_stem_used=(stem if trip else ""))

        out_path = out_dir / out_name_for(f)
        try:
            labeled.to_csv(out_path, index=False)
            print(f"[ok] Wrote {out_path}")
        except Exception as e:
            print(f"[warn] Failed to write {out_path}: {e}", file=sys.stderr)

    if unmatched:
        print(f"[warn] No compiled mapping for {len(unmatched)} file stem(s): {sorted(set(unmatched))}", file=sys.stderr)
        return 2 if fail_on_miss else 0
    return 0

# ----------------------------- cli -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Add coin labels/distances using file-stem match to compiledCoinLocations source-file list.")
    ap.add_argument("--compiled", required=True, help="Path to compiledCoinLocations.csv (must contain 'source_file' or 'SourceFiles')")
    ap.add_argument("--events-dir", required=True, help="Directory of input events CSVs")
    ap.add_argument("--out-dir", required=True, help="Directory to write labeled CSVs")
    ap.add_argument("--pattern", default="*_events_flat.csv", help="Glob pattern for input files (default: '*_events_flat.csv')")
    ap.add_argument("--fail-on-miss", action="store_true", help="Exit non-zero if any input file stem has no compiled mapping")
    ap.add_argument("--debug", action="store_true", help="Print diagnostics (matched stems, duplicates, sizes)")
    args = ap.parse_args()

    compiled_csv = Path(args.compiled)
    events_dir = Path(args.events_dir)
    out_dir = Path(args.out_dir)

    if not compiled_csv.exists():
        print(f"[error] compiled CSV not found: {compiled_csv}", file=sys.stderr)
        sys.exit(1)
    if not events_dir.exists():
        print(f"[error] events dir not found: {events_dir}", file=sys.stderr)
        sys.exit(1)

    code = process_dir(events_dir, out_dir, compiled_csv, args.pattern, args.fail_on_miss, args.debug)
    if args.fail_on_miss and code != 0:
        sys.exit(code)

if __name__ == "__main__":
    main()
