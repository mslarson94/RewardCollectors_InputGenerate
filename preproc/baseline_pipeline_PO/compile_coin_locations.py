#!/usr/bin/env python3
# add_coin_labels.py
"""
For each events CSV in --events-dir (pattern via --pattern):
- Look up LV/NV/HV coin positions from compiledCoinLocations.csv.
- Populate:
    coinLabel                  := nearest of LV/NV/HV to (coinPos_x, coinPos_y, coinPos_z)
    actualClosestCoinLabel     := nearest of LV/NV/HV to (pinLocal_x, pinLocal_y, pinLocal_z)
    actualClosestCoinDist      := corresponding Euclidean distance
- Write to <basename>_events_coinLabel.csv in --out-dir.

Keying strategy (robust; supports your "stem-only" case):
1) Use the row's source-file column (case/variant-insensitive: source_file, sourceFile, src, etc.).
   Match by BOTH normalized basename and normalized stem (extension removed, common suffixes stripped).
2) If no per-row source file or no match, also try the current input CSV file's own basename & stem.
3) Fallback tuple match (participantID,currentRole,CoinSet/mainRR) if present.
Indexing compiledCoinLocations:
- For each row, index triplet by: fileName basename & stem, and each entry of SourceFiles by basename & stem.

Usage:
  python add_coin_labels.py \
    --compiled "/path/to/compiledCoinLocations.csv" \
    --events-dir "/path/to/events" \
    --out-dir "/path/to/output" \
    --pattern "*_events_flat.csv"
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

def _basename_key(s: str) -> str:
    """lowercased basename (keeps suffixes like _events_flat)"""
    return Path(str(s).strip()).name.lower()

def _root_key(s: str) -> str:
    """lowercased stem with common suffixes removed: _events_flat, _processed_events, _events, _main_meta, _meta, _main"""
    name = Path(str(s).strip()).name
    # Strip all extensions (handles .tar.gz etc.)
    prev = None
    while prev != name:
        prev = name
        name = Path(name).stem
    root = name
    for suf in ("_events_flat", "_processed_events", "_events", "_main_meta", "_meta", "_main"):
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

def nearest_label(point: Tuple[Optional[float], Optional[float], Optional[float]],
                  triplet: CoinTriplet) -> Tuple[str, float]:
    px, py, pz = point
    if any(v is None for v in (px, py, pz)):
        return ("", float("nan"))
    best_label = ""
    best_dist = float("inf")
    for lab, (cx, cy, cz) in triplet.as_dict().items():
        if any(v is None for v in (cx, cy, cz)):
            continue
        d = math.sqrt((px - cx) ** 2 + (py - cy) ** 2 + (pz - cz) ** 2)
        if d < best_dist:
            best_dist = d
            best_label = lab
    if best_label == "":
        return ("", float("nan"))
    return (best_label, best_dist)

# Candidate column names we will accept
SRC_COL_CANDIDATES = ("source_file", "sourceFile", "src", "source", "sourcefile", "fileStem", "file_stem")

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

def _pick_source_col(df: pd.DataFrame) -> Optional[str]:
    for c in SRC_COL_CANDIDATES:
        if c in df.columns:
            return c
    return None

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

@dataclass
class CompiledIndex:
    by_basename: Dict[str, CoinTriplet]
    by_root: Dict[str, CoinTriplet]
    by_tuple: Dict[Tuple[str, str, str, str], CoinTriplet]

def load_compiled_index(compiled_csv: Path) -> CompiledIndex:
    df = pd.read_csv(compiled_csv, dtype="string")

    # Ensure numeric coin columns
    for col in [f"{lab}_{axis}" for lab in LABELS for axis in ("x", "y", "z")]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = np.nan

    # Normalize string key columns
    for k in ("participantID", "currentRole", "CoinSet", "mainRR", "fileName", "SourceFiles"):
        if k not in df.columns:
            df[k] = pd.Series([None] * len(df), dtype="string")
        else:
            df[k] = df[k].astype("string")

    def triplet_from_row(r) -> CoinTriplet:
        return CoinTriplet(
            LV=(_to_float(r["LV_x"]), _to_float(r["LV_y"]), _to_float(r["LV_z"])),
            NV=(_to_float(r["NV_x"]), _to_float(r["NV_y"]), _to_float(r["NV_z"])),
            HV=(_to_float(r["HV_x"]), _to_float(r["HV_y"]), _to_float(r["HV_z"])),
        )

    by_basename: Dict[str, CoinTriplet] = {}
    by_root: Dict[str, CoinTriplet] = {}

    # Index via fileName (meta json name)
    for _, r in df.iterrows():
        trip = triplet_from_row(r)
        fname = (r["fileName"] or "").strip()
        if fname:
            by_basename.setdefault(_basename_key(fname), trip)
            by_root.setdefault(_root_key(fname), trip)

    # Index via SourceFiles (semicolon-separated)
    for _, r in df.iterrows():
        trip = triplet_from_row(r)
        srcs = (r["SourceFiles"] or "").strip()
        if not srcs:
            continue
        for part in srcs.split(";"):
            part = part.strip()
            if not part:
                continue
            by_basename.setdefault(_basename_key(part), trip)
            by_root.setdefault(_root_key(part), trip)

    # Tuple index (participantID,currentRole,CoinSet,mainRR)
    by_tuple: Dict[Tuple[str, str, str, str], CoinTriplet] = {}
    for _, r in df.iterrows():
        pid = (r["participantID"] or "").strip().lower()
        role = (r["currentRole"] or "").strip().upper()
        cset = (r["CoinSet"] or "").strip().upper()
        rr = (r["mainRR"] or "").strip().lower()
        if pid and role and cset and rr:
            by_tuple.setdefault((pid, role, cset, rr), triplet_from_row(r))

    return CompiledIndex(by_basename=by_basename, by_root=by_root, by_tuple=by_tuple)

# ---------------------------- labeling core ----------------------------

def _resolve_triplet(row: pd.Series, src_col: Optional[str], compiled: CompiledIndex,
                     file_context_name: Optional[str]) -> Optional[CoinTriplet]:
    # 1) per-row source column
    if src_col is not None:
        src = row.get(src_col)
        if isinstance(src, str) and src.strip():
            k1 = _basename_key(src)
            k2 = _root_key(src)
            trip = compiled.by_basename.get(k1) or compiled.by_root.get(k2)
            if trip is not None:
                return trip

    # 2) file context (the input CSV's own name)
    if file_context_name:
        k1 = _basename_key(file_context_name)
        k2 = _root_key(file_context_name)
        trip = compiled.by_basename.get(k1) or compiled.by_root.get(k2)
        if trip is not None:
            return trip

    # 3) tuple fallback
    pid = str(row.get("participantID") or "").strip().lower()
    role = str(row.get("currentRole") or "").strip().upper()
    cset = str(row.get("coinSet") or row.get("CoinSet") or "").strip().upper()
    rr = str(row.get("main_RR") or row.get("mainRR") or "").strip().lower()
    if pid and role and cset and rr:
        return compiled.by_tuple.get((pid, role, cset, rr))

    return None

def label_dataframe(df: pd.DataFrame, compiled: CompiledIndex, file_context_name: Optional[str]) -> pd.DataFrame:
    src_col = _pick_source_col(df)
    coin_x, coin_y, coin_z = _pick_xyz(df, COIN_POS_CANDIDATES)
    pin_x, pin_y, pin_z = _pick_xyz(df, PIN_LOCAL_CANDIDATES)

    # Ensure numeric
    for c in (coin_x, coin_y, coin_z, pin_x, pin_y, pin_z):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    coin_labels: List[str] = []
    closest_labels: List[str] = []
    closest_dists: List[float] = []

    for _, row in df.iterrows():
        trip = _resolve_triplet(row, src_col, compiled, file_context_name)

        if trip is None:
            coin_labels.append("")
            closest_labels.append("")
            closest_dists.append(float("nan"))
            continue

        clab, _ = nearest_label((row[coin_x], row[coin_y], row[coin_z]), trip)
        alab, adist = nearest_label((row[pin_x], row[pin_y], row[pin_z]), trip)

        coin_labels.append(clab)
        closest_labels.append(alab)
        closest_dists.append(adist)

    out = df.copy()
    out["coinLabel"] = pd.Series(coin_labels, index=df.index, dtype="string")
    out["actualClosestCoinLabel"] = pd.Series(closest_labels, index=df.index, dtype="string")
    out["actualClosestCoinDist"] = pd.Series(closest_dists, index=df.index)
    return out

def write_with_suffix(infile: Path, out_dir: Path) -> Path:
    base = infile.name
    if base.endswith("_processed_events.csv"):
        name = base[:-len("_processed_events.csv")] + "_events_coinLabel.csv"
    elif base.endswith("_events_flat.csv"):
        name = base[:-len("_events_flat.csv")] + "_events_coinLabel.csv"
    else:
        name = infile.stem + "_events_coinLabel.csv"
    return out_dir / name

# ----------------------------- pipeline -----------------------------

def process_events_dir(events_dir: Path, out_dir: Path, compiled_csv: Path, pattern: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    compiled = load_compiled_index(compiled_csv)

    files = sorted(events_dir.glob(pattern))
    if not files:
        print(f"[warn] No files matched pattern '{pattern}' in {events_dir}", file=sys.stderr)

    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception as e:
            print(f"[warn] Skipping {f.name}: failed to read CSV: {e}", file=sys.stderr)
            continue

        # Ensure tuple keys exist for fallback
        for col in ("participantID", "currentRole"):
            if col not in df.columns:
                df[col] = pd.NA
        if "coinSet" not in df.columns and "CoinSet" not in df.columns:
            df["coinSet"] = pd.NA
        if "main_RR" not in df.columns and "mainRR" not in df.columns:
            df["main_RR"] = pd.NA

        labeled = label_dataframe(df, compiled, file_context_name=f.name)

        out_path = write_with_suffix(f, out_dir)
        try:
            labeled.to_csv(out_path, index=False)
            print(f"[ok] Wrote {out_path}")
        except Exception as e:
            print(f"[warn] Failed to write {out_path}: {e}", file=sys.stderr)

# ----------------------------- cli -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Add coin labels and nearest-coin metrics to events CSVs.")
    ap.add_argument("--compiled", required=True, help="Path to compiledCoinLocations.csv")
    ap.add_argument("--events-dir", required=True, help="Directory containing input events CSVs")
    ap.add_argument("--out-dir", required=True, help="Directory to write labeled CSVs")
    ap.add_argument("--pattern", default="*_processed_events.csv", help="Glob pattern for input files (e.g., '*_events_flat.csv')")
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

    process_events_dir(events_dir, out_dir, compiled_csv, args.pattern)

if __name__ == "__main__":
    main()
