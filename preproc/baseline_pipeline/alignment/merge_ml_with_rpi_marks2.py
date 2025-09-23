# ========================= merge_ml_with_rpi_marks.py =========================
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
merge_ml_with_rpi_marks.py — Align ML 'Mark' events to a precomputed RPi marks CSV
and merge alignment columns back into the ML CSV.

New columns (filled only on Mark rows):
  <Label>_RPi_Timestamp
  <Label>_RPi_Timestamp_drift   # seconds (RPi - ML)
  <Label>_RPi_Matched           # 1/0
  <Label>_RPi_MatchReason       # "" or explanation for no-match

Output filename:
  <ml_base>_<device>_<label>_events.csv

Example:
  python merge_ml_with_rpi_marks.py \
    --ml_csv_file \
      "/.../augmented/ObsReward_A_02_17_2025_15_11_events_final.csv" \
    --rpi_marks_csv \
      "/.../ObsReward_A_02_17_2025_15_11_events_final_ML2A_BioPac_RPiMarks.csv" \
    --csv_timestamp_column mLTimestamp \
    --event_type_column lo_eventType \
    --event_type_values Mark \
    --label BioPac \
    --device ML2A \
    --timezone_offset_hours auto \
    --max_match_gap_s 1.0
"""

from __future__ import annotations

import argparse
import os
import numpy as np
import pandas as pd
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import List, Optional, Sequence, Tuple


def _select_mark_rows(df: pd.DataFrame, col: str, values: Sequence[str]) -> pd.DataFrame:
    vals = {str(v).strip().lower() for v in values}
    mask = df[col].astype(str).str.strip().str.lower().isin(vals)
    out = df.loc[mask].copy()
    if out.empty:
        raise ValueError(f"no rows where {col} in {sorted(vals)}")
    return out


def _auto_offset_hours(ml_times: pd.Series, rpi_times: pd.Series) -> float:
    n = int(min(10, len(ml_times), len(rpi_times)))
    diffs = (ml_times.iloc[:n].reset_index(drop=True) - rpi_times.iloc[:n].reset_index(drop=True)).dt.total_seconds()
    return float(np.median(diffs) / 3600.0)


def _nearest_unique_alignment(ml_times: pd.Series, rpi_times: pd.Series, max_gap: Optional[float]) -> Tuple[np.ndarray, np.ndarray, List[str]]:
    ml = ml_times.reset_index(drop=True).to_numpy(dtype="datetime64[ns]")
    rpi = rpi_times.reset_index(drop=True).to_numpy(dtype="datetime64[ns]")
    n, m = len(ml), len(rpi)
    match_idx = np.full(n, -1, dtype=int)
    deltas = np.full(n, np.nan, dtype=float)
    reasons: List[str] = ["" for _ in range(n)]
    last_j = -1
    for i in range(n):
        if last_j + 1 >= m:
            for k in range(i, n):
                reasons[k] = "exhausted log"
            break
        # search candidates >= last_j+1
        diffs = np.abs((rpi[last_j+1:] - ml[i]).astype("timedelta64[ns]").astype("int64")) / 1e9
        j_rel = int(np.argmin(diffs))
        j = last_j + 1 + j_rel
        if (max_gap is not None) and (diffs[j_rel] > max_gap):
            reasons[i] = f"no log within ≤{max_gap:.3f}s"
            continue
        match_idx[i] = j
        deltas[i] = ((rpi[j] - ml[i]).astype("timedelta64[ns]").astype("int64")) / 1e9
        last_j = j
    return match_idx, deltas, reasons


# # -------------------------------
# # Helpers (self-contained)
# # -------------------------------
# import re
# def _parse_device_ip_map(path: Path) -> Dict[str, str]:
#     text = path.read_text(encoding="utf-8")
#     out: Dict[str, str] = {}
#     for line in text.splitlines():
#         line = line.strip()
#         if not line or line.startswith("#"):
#             continue
#         m = re.match(r"'([^']+)'\s*:\s*'([^']+)'", line)
#         if m:
#             out[m.group(1).strip()] = m.group(2).strip()
#     return out


# def _to_session_date(s: str) -> str:
#     """Convert testingDate like '02_17_2025' or '02-17-2025' to '2025-02-17'."""
#     s = s.replace("/", "_").replace("-", "_")
#     mm, dd, yy = s.split("_")
#     return f"{yy}-{int(mm):02d}-{int(dd):02d}"


# def _normalize_ml_stem(stem: str, suffixes: Sequence[str]) -> str:
#     """Strip any of the given suffixes (if stem ends with them), repeatedly.
#     Then trim trailing underscores/hyphens."""
#     base = stem
#     changed = True
#     while changed:
#         changed = False
#         for s in suffixes:
#             if s and base.endswith(s):
#                 base = base[: -len(s)]
#                 changed = True
#     return re.sub(r"[_-]+$", "", base)


# def _missing_like(x: object) -> bool:
#     s = str(x).strip().lower()
#     return s in {"", "none", "na", "n/a", "nan", "<na>", "null", "-"}


# def _resolve_ml_csv(ml_root: Path, cleaned_value: str, suffixes: Sequence[str]) -> Path:
#     """Resolve ML CSV from a 'cleanedFile' value that may include variant suffixes."""
#     p = Path(cleaned_value)
#     stem = _normalize_ml_stem(p.stem, suffixes)
#     ext = p.suffix or ".csv"

#     candidates = [
#         ml_root / p.name,  # as provided
#         ml_root / f"{stem}_events_final{ext}",
#         ml_root / f"{stem}_processed{ext}",
#         ml_root / f"{stem}{ext}",
#     ]
#     tried = []
#     for c in candidates:
#         tried.append(str(c))
#         if c.exists():
#             return c
#     raise FileNotFoundError("could not resolve ML CSV for '" + cleaned_value + "'. Tried: " + ", ".join(tried))



def main() -> None:
    ap = argparse.ArgumentParser(description="Merge ML Mark events with RPi marks CSV and write merged ML CSV")
    ap.add_argument("--ml_csv_file", required=True)
    ap.add_argument("--rpi_marks_csv", required=True)
    ap.add_argument("--csv_timestamp_column", default="mLTimestamp")
    ap.add_argument("--event_type_column", default="lo_eventType")
    ap.add_argument("--event_type_values", default="Mark")
    ap.add_argument("--label", required=True, help="BioPac or RNS (used for column prefix)")
    ap.add_argument("--device", required=True)
    ap.add_argument("--timezone_offset_hours", default="auto", help="number or 'auto'")
    ap.add_argument("--max_match_gap_s", type=float, default=1.0)
    ap.add_argument("--out_dir", default="", help="Optional directory for output file (defaults to ML CSV directory)")
    ap.add_argument("--strip-ml-suffixes", default="_events_final,_processed", help="Comma-separated suffixes to strip from ML stem for output naming")
    args = ap.parse_args()

    ml_df = pd.read_csv(args.ml_csv_file)
    ev_vals = [v.strip() for v in args.event_type_values.split(",") if v.strip()]

    marks_df = _select_mark_rows(ml_df, args.event_type_column, ev_vals)
    marks_idx = marks_df.index
    ml_times = pd.to_datetime(marks_df[args.csv_timestamp_column], errors="coerce")
    if ml_times.isna().any():
        raise ValueError("some ML timestamps failed to parse")

    rpi_df = pd.read_csv(args.rpi_marks_csv)
    if "RPi_Timestamp" not in rpi_df.columns:
        raise KeyError("RPi marks CSV missing 'RPi_Timestamp'")
    rpi_times = pd.to_datetime(rpi_df["RPi_Timestamp"], errors="coerce")

    # offset
    if args.timezone_offset_hours.strip().lower() == "auto":
        est = _auto_offset_hours(ml_times, rpi_times)
        tz_offset = timedelta(hours=est)
    else:
        tz_offset = timedelta(hours=float(args.timezone_offset_hours))

    rpi_aligned = rpi_times + pd.to_timedelta(tz_offset)

    match_idx, deltas, reasons = _nearest_unique_alignment(ml_times, rpi_aligned, args.max_match_gap_s if args.max_match_gap_s > 0 else None)
    matched_mask = match_idx >= 0
    chosen_rpi = pd.Series([pd.NaT] * len(ml_times), dtype="datetime64[ns]")
    chosen_rpi.loc[matched_mask] = rpi_aligned.iloc[match_idx[matched_mask]].values

    label = args.label.strip()
    col_ts = f"{label}_RPi_Timestamp"
    col_drift = f"{label}_RPi_Timestamp_drift"
    col_match = f"{label}_RPi_Matched"
    col_reason = f"{label}_RPi_MatchReason"

    # Create columns on full ML CSV
    ml_df[col_ts] = pd.NaT
    ml_df[col_drift] = np.nan
    ml_df[col_match] = 0
    ml_df[col_reason] = ""

    # Fill only at mark rows
    ml_df.loc[marks_idx, col_ts] = chosen_rpi.values
    ml_df.loc[marks_idx, col_drift] = deltas
    ml_df.loc[marks_idx, col_match] = matched_mask.astype(int)
    ml_df.loc[marks_idx, col_reason] = reasons

    suffixes = [s for s in (args.strip_ml_suffixes or "").split(",") if s]
    ml_root = _normalize_ml_stem(Path(args.ml_csv_file).stem, suffixes)
    out_dir = Path(args.out_dir) if args.out_dir else Path(args.ml_csv_file).parent
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"{ml_root}_{args.device}_{label}_events.csv"
    ml_df.to_csv(out_csv, index=False)
    print(out_csv)


if __name__ == "__main__":
    main()