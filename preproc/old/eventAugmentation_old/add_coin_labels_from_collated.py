#!/usr/bin/env python3
"""
add_coin_labels_from_collated.py

Purpose
-------
Label each events CSV with the nearest coin label (LV/NV/HV) based on a *known*
coin set for that file, using two simplified inputs you already maintain:
  1) collatedData.xlsx  -> which file has which coinSet
  2) CoinSets.csv       -> LV/NV/HV coordinates for each coinSet

This skips all "guessing" or tuple-based heuristics. We only match by the current
input file's normalized stem against stems we index from the collated file.

Output columns (added to the input events CSV):
  - coinLabel
  - actualClosestCoinLabel
  - actualClosestCoinDist
  - distToPin_LV / _NV / _HV
  - distFromCoinPos_LV / _NV / _HV
  - coinStemUsed        (the stem we matched on)
  - coinSetUsed         (the coinSet applied)

Usage
-----
python add_coin_labels_from_collated.py \
  --collated /path/to/collatedData.xlsx \
  --coin-sets /path/to/CoinSets.csv \
  --events-dir /path/to/events \
  --out-dir /path/to/out \
  --pattern "*_events_flat.csv" \
  [--sheet "MagicLeapFiles"] [--fail-on-miss] [--debug]

Notes
-----
- We index stems from a few filename columns in the collated sheet:
    MagicLeapFiles, cleanedFile, unalignedFile (if present).
  Any of those may be used as the match-key for a given events file.
- If CoinSets.csv omits *_y columns, we assume y = 0 for all coin coordinates.
- Distance is Euclidean in (x, y, z). If any coordinate is missing, the distance
  will be NaN and the corresponding label will be empty.
"""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

LABELS: Tuple[str, str, str] = ("LV", "NV", "HV")

# ----------------------------- helpers -----------------------------

def _root_keyV1(s: str) -> str:
    """Lowercased stem with *all* extensions removed and common suffixes stripped.
    Examples:
      'foo_events_flat.csv'         -> 'foo'
      'foo_processed_events.csv.gz' -> 'foo'
      'foo_main_meta.json'          -> 'foo'
      'foo_events_coinLabel.csv'    -> 'foo'
    """
    name = Path(str(s).strip()).name
    prev = None
    # remove all extensions (.csv, .gz, .json, etc.)
    while prev != name:
        prev = name
        name = Path(name).stem
    root = name
    for suf in (
        "_events_coinLabel",
        "_eventsFlat",
        "_processed_events",
        "_events_flat",
        "_events",
        "_processed",
        "_main_meta",
        "_meta",
        "_main",
    ):
        if root.lower().endswith(suf):
            root = root[: -len(suf)]
    return root.lower()

def _root_key(s: str, pattern: str) -> str:
    """Lowercased stem with *all* extensions removed and common suffixes stripped.
    Examples:
      'foo_events_flat.csv'         -> 'foo'
      'foo_processed_events.csv.gz' -> 'foo'
      'foo_main_meta.json'          -> 'foo'
      'foo_events_coinLabel.csv'    -> 'foo'
    """
    name = Path(str(s).strip()).name
    prev = None
    # remove all extensions (.csv, .gz, .json, etc.)
    while prev != name:
        prev = name
        name = Path(name).stem
    root = name
    root = root.replace(pattern, '')
    return root.lower()

def _to_float(v) -> Optional[float]:
    try:
        f = float(v)
        if np.isnan(f):
            return None
        return f
    except Exception:
        return None

def _num_or_zero(v) -> Optional[float]:
    """Try float; if missing/NaN -> 0.0 (used to synthesize *_y when absent)."""
    try:
        f = float(v)
        if np.isnan(f):
            return 0.0
        return f
    except Exception:
        return 0.0

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
    # print('_nearest_label')
    # print('point, Tuple[Optional[float], Optional[float], Optional[float]]')
    # print(point)
    # print('triplet, CoinTriplet')
    # print(triplet)

    best_label = ""
    best_dist = float("inf")
    for lab, pos in triplet.as_dict().items():
        d = _euclid(point, pos)
        if not math.isnan(d) and d < best_dist:
            best_label, best_dist = lab, d
    if best_label == "":
        return ("", float("nan"))
    # print('best_label', best_label)
    # print('best_dist', best_dist)
    return (best_label, best_dist)

# Accept common coordinate column variants from the events files.
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
    #print('_pick_xyz')
    #print('first', first)
    return first

# ------------------------ load inputs ------------------------

def _coin_triplet_from_row(r: pd.Series, synth_y0: bool) -> CoinTriplet:
    def get_axis(prefix: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        # canonical underscore names
        x = r.get(f"{prefix}_x", r.get(f"{prefix}x"))
        y = r.get(f"{prefix}_y", r.get(f"{prefix}y"))
        z = r.get(f"{prefix}_z", r.get(f"{prefix}z"))
        if synth_y0 and y is None:
            y = 0.0
        #print('_coin_triplet_from_row, x/y/z')
        #print(x, y, z)
        return (_to_float(x), _to_float(y) if not synth_y0 else _num_or_zero(y), _to_float(z))
    #print('_coin_triplet_from_row')
    #print(CoinTriplet(LV=get_axis("LV"), NV=get_axis("NV"), HV=get_axis("HV")))
    return CoinTriplet(LV=get_axis("LV"), NV=get_axis("NV"), HV=get_axis("HV"))

def load_coinsets(coinsets_csv: Path) -> Dict[str, CoinTriplet]:
    """Load CoinSets.csv into {CoinSetName -> CoinTriplet}."""
    df = pd.read_csv(coinsets_csv, dtype="string")
    # detect whether *_y columns are present; if not, synthesize y=0
    has_y = any(c.lower().endswith("_y") for c in df.columns)
    synth_y0 = not has_y

    # Normalize column names to underscores if needed (we access with r.get).
    df.columns = [str(c).strip() for c in df.columns]
    out: Dict[str, CoinTriplet] = {}
    for _, r in df.iterrows():
        name = str(r.get("CoinSet") or r.get("coinSet") or "").strip()
        if not name:
            continue
        out[name] = _coin_triplet_from_row(r, synth_y0=synth_y0)
    #print('load_coinsets')
    #print('out', out)
    return out

def _get_str(v) -> str:
    try:
        import pandas as _pd
        if v is None or (isinstance(v, float) and math.isnan(v)) or _pd.isna(v):
            return ""
    except Exception:
        if v is None:
            return ""
    return str(v)

def load_stem_to_coinsetV1(collated_xlsx: Path, sheet: str, pattern: str, debug: bool=False) -> Dict[str, str]:
    """
    Build {stem -> coinSet} from collatedData.xlsx.
    We index stems from these columns if present: MagicLeapFiles, cleanedFile, unalignedFile.
    """
    try:
        df = pd.read_excel(collated_xlsx, sheet_name=sheet, dtype="string")
    except ValueError as e:
        print(f"[error] sheet '{sheet}' not found in {collated_xlsx}: {e}", file=sys.stderr)
        sys.exit(1)

    for c in ("MagicLeapFiles", "cleanedFile", "coinSet"):
        if c not in df.columns:
            if c == "coinSet":
                print(f"[error] collated sheet missing required 'coinSet' column", file=sys.stderr)
                sys.exit(1)
            # create optional filename columns if absent
            df[c] = pd.Series([None] * len(df), dtype="string")

    stem_map: Dict[str, str] = {}
    dups: set[str] = set()
    for _, r in df.iterrows():
        cset = _get_str(r["coinSet"]).strip()
        # print('load_stem_to_coinset')
        # print('load stem cset', cset)
        if not cset or cset.lower() == "none":
            continue
        for fname_col in ("MagicLeapFiles", "cleanedFile"):
            raw = _get_str(r[fname_col]).strip()
            raw = raw.replace('_processsed', '')
            raw = raw.replace('.csv', '')
            # raw1 = _get_str(r[fname_col]).strip()
            # raw = raw1.replace(pattern, '')
            # print('raw',raw)
            # print('raw.lower()', raw.lower())
            if not raw or raw.lower() == "none":
                continue
            stem = _root_key(raw, pattern)
            # print('stem', stem)
            if not stem:
                continue
            if stem in stem_map and debug and stem not in dups:
                print(f"[debug] duplicate stem in collated mapping: '{stem}' -> keeping first", file=sys.stderr)
                dups.add(stem)
            stem_map[stem] = cset
            rows_used += 1
    if debug:
        print(f"[debug] collated index size (unique stems): {len(stem_map)}")
    #print('stem_map', stem_map)
    return stem_map



def load_stem_to_coinset(collated_xlsx: Path, sheet: str, pattern: str, debug: bool=False) -> Dict[str, str]:
    """
    Build {stem -> coinSet} from collatedData.xlsx.
    We index stems from these columns if present: MagicLeapFiles, cleanedFile, unalignedFile.
    """
    try:
        df = pd.read_excel(collated_xlsx, sheet_name=sheet, dtype="string")
    except ValueError as e:
        print(f"[error] sheet '{sheet}' not found in {collated_xlsx}: {e}", file=sys.stderr)
        sys.exit(1)

    for c in ("MagicLeapFiles", "cleanedFile", "coinSet"):
        if c not in df.columns:
            if c == "coinSet":
                print(f"[error] collated sheet missing required 'coinSet' column", file=sys.stderr)
                sys.exit(1)
            # create optional filename columns if absent
            df[c] = pd.Series([None] * len(df), dtype="string")

    stem_map: Dict[str, str] = {}
    dups: set[str] = set()
    rows_seen = 0
    rows_used = 0
    for _, r in df.iterrows():
        cset = _get_str(r["coinSet"]).strip()
        # print('load_stem_to_coinset')
        # print('load stem cset', cset)
        rows_seen += 1
        if not cset or cset.lower() == "none":
            continue
        for fname_col in ("MagicLeapFiles", "cleanedFile"):
            raw = _get_str(r[fname_col]).strip()
            raw = raw.replace('_processed.csv', '')
            #print('raw', raw)
            # raw1 = _get_str(r[fname_col]).strip()
            # raw = raw1.replace(pattern, '')
            # print('raw',raw)
            # print('raw.lower()', raw.lower())
            if not raw or raw.lower() == "none":
                continue
            stem = _root_key(raw, pattern)
            #print('stem', stem)
            if not stem:
                continue
            if stem in stem_map and debug and stem not in dups:
                print(f"[debug] duplicate stem in collated mapping: '{stem}' -> keeping first", file=sys.stderr)
                dups.add(stem)
            stem_map[stem] = cset
            rows_used += 1
    if debug:
        print(f"[debug] collated index size (unique stems): {len(stem_map)}")
    #print('stem_map', stem_map)
    return stem_map
# ---------------------------- labeling core ----------------------------

def label_file(df: pd.DataFrame, trip: Optional[CoinTriplet], coin_stem_used: str, coin_set_used: str) -> pd.DataFrame:
    coin_x, coin_y, coin_z = _pick_xyz(df, COIN_POS_CANDIDATES)
    pin_x, pin_y, pin_z = _pick_xyz(df, PIN_LOCAL_CANDIDATES)
    #print('label_file')
    #print('trip', trip)
    print('coin_stem_used:', coin_stem_used,'coin_set_used:', coin_set_used)
    #print('coin_set_used', coin_set_used)
    #print('coin position', coin_x, coin_y, coin_z)
    #print('pin location', pin_x, pin_y, pin_z)
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
    #print('df', df)
    for _, row in df.iterrows():
        # print('actual pin location',row[pin_x], row[pin_y], row[pin_z])
        # print('actual coin location',row[coin_x], row[coin_y], row[coin_z])
        # print('coinSet', row['coinSet'])
        if trip is None:
            coin_labels.append("")
            closest_labels.append("")
            closest_dists.append(float("nan"))
            dist_pin_LV.append(float("nan")); dist_pin_NV.append(float("nan")); dist_pin_HV.append(float("nan"))
            dist_coin_LV.append(float("nan")); dist_coin_NV.append(float("nan")); dist_coin_HV.append(float("nan"))
            continue

        pin_pt = (row[pin_x], row[pin_y], row[pin_z])
        # print('pin_pt', pin_pt)
        coin_pt = (row[coin_x], row[coin_y], row[coin_z])
        # print('coin_pt', coin_pt)
        # distances to pin
        dplv = _euclid(pin_pt, trip.LV); dpnv = _euclid(pin_pt, trip.NV); dphv = _euclid(pin_pt, trip.HV)
        dist_pin_LV.append(dplv); dist_pin_NV.append(dpnv); dist_pin_HV.append(dphv)
        # print('dist_pin_LV', dist_pin_LV)
        # print('dist_pin_NV', dist_pin_NV)
        # print('dist_pin_HV', dist_pin_HV)

        # distances to coinPos
        dclv = _euclid(coin_pt, trip.LV); dcnv = _euclid(coin_pt, trip.NV); dchv = _euclid(coin_pt, trip.HV)
        dist_coin_LV.append(dclv); dist_coin_NV.append(dcnv); dist_coin_HV.append(dchv)

        # labels
        clab, _ = _nearest_label(coin_pt, trip)
        # print('clab', clab)
        alab, adist = _nearest_label(pin_pt, trip)

        coin_labels.append(clab)
        closest_labels.append(alab)
        closest_dists.append(adist)
        # print('coin_labels', coin_labels)

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
    out["coinSetUsed"] = coin_set_used
    return out

def out_name_for(infile: Path) -> str:
    base = infile.name
    if base.endswith("_processed_events.csv"):
        return base[:-len("_processed_events.csv")] + "_events_coinLabel.csv"
    if base.endswith("_eventsFlat.csv"):
        return base[:-len("_eventsFlat.csv")] + "_events_coinLabel.csv"
    return infile.stem + "_events_coinLabel.csv"

# ----------------------------- pipeline -----------------------------

def process_dir(events_dir: Path, out_dir: Path, collated_xlsx: Path, coinsets_csv: Path,
                pattern: str, sheet: str, fail_on_miss: bool, debug: bool) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)

    stem_to_coinset = load_stem_to_coinset(collated_xlsx, sheet=sheet, debug=debug, pattern=pattern)
    coinsets = load_coinsets(coinsets_csv)

    unmatched: List[str] = []
    files = sorted(events_dir.glob(f"*_{pattern}.csv"))
    if not files:
        print(f"[warn] No files matched pattern '*_{pattern}.csv' in {events_dir}", file=sys.stderr)

    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception as e:
            print(f"[warn] Skipping {f.name}: failed to read CSV: {e}", file=sys.stderr)
            continue

        #stem = _root_key(f.name, pattern)
        stem = f.name
        stem = stem.replace(f"_{pattern}.csv", "")
        #print('process_dir')
        #print('stem', stem)
        cset = stem_to_coinset.get(stem.lower(), "")
        #print('cset', cset)
        #print(coinsets.get(cset) if cset else 'empty cset')
        trip = coinsets.get(cset) if cset else None

        if debug:
            print(f"[debug] input: {f.name}  -> stem: '{stem}'  -> coinSet: '{cset or 'none'}'  -> match: {'yes' if trip else 'no'}")

        if trip is None:
            unmatched.append(stem)

        labeled = label_file(df, trip, coin_stem_used=(stem if trip else ""), coin_set_used=(cset if trip else ""))

        out_path = out_dir / out_name_for(f)
        try:
            labeled.to_csv(out_path, index=False)
            print(f"[ok] Wrote {out_path}")
        except Exception as e:
            print(f"[warn] Failed to write {out_path}: {e}", file=sys.stderr)

    if unmatched:
        print(f"[warn] No coinSet mapping for {len(unmatched)} file stem(s) based on collated sheet '{sheet}': {sorted(set(unmatched))}", file=sys.stderr)
        return 2 if fail_on_miss else 0
    return 0

# ----------------------------- cli -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Add coin labels/distances using (collatedData.xlsx + CoinSets.csv). No guessing—file-stem match only.")
    ap.add_argument("--collated", required=True, help="Path to collatedData.xlsx")
    ap.add_argument("--coin-sets", required=True, help="Path to CoinSets.csv")
    ap.add_argument("--events-dir", required=True, help="Directory of input events CSVs")
    ap.add_argument("--out-dir", required=True, help="Directory to write labeled CSVs")
    ap.add_argument("--pattern", default="eventsFlat", help="Glob pattern for input files (default: 'events_flat')")
    ap.add_argument("--sheet", default="MagicLeapFiles", help="Sheet in collatedData.xlsx to read (default: MagicLeapFiles)")
    ap.add_argument("--fail-on-miss", action="store_true", help="Exit non-zero if any input file stem has no mapping in collatedData.xlsx")
    ap.add_argument("--debug", action="store_true", help="Print diagnostics (matched stems, sizes)")
    args = ap.parse_args()

    collated_xlsx = Path(args.collated)
    coinsets_csv = Path(args.coin_sets)
    events_dir = Path(args.events_dir)
    out_dir = Path(args.out_dir)

    if not collated_xlsx.exists():
        print(f"[error] collatedData.xlsx not found: {collated_xlsx}", file=sys.stderr)
        sys.exit(1)
    if not coinsets_csv.exists():
        print(f"[error] CoinSets.csv not found: {coinsets_csv}", file=sys.stderr)
        sys.exit(1)
    if not events_dir.exists():
        print(f"[error] events dir not found: {events_dir}", file=sys.stderr)
        sys.exit(1)

    code = process_dir(events_dir, out_dir, collated_xlsx, coinsets_csv,
                       args.pattern, args.sheet, args.fail_on_miss, args.debug)
    if args.fail_on_miss and code != 0:
        sys.exit(code)

if __name__ == "__main__":
    main()
