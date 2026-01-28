#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
merge_ml_with_rpi_marks3.py — Align ML 'Mark' events with RPi marks (unified format)
and optionally synthesize ML rows for unmatched RPi marks using a template CSV.

Unified behavior:
- Resolve the RPi timestamp per-row via `RPi_Timestamp_Source`.
- If source invalid/blank, fall back: RPi_Time_unified → RPi_Time_verb → RPi_Time_simple → RPi_Timestamp.
- Apply timezone offset ONLY to rows whose resolved source is *_simple (no offset to *_verb).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from batchAlignHelpers import (  # type: ignore
    _select_mark_rows,
    _auto_offset_hours,
    _nearest_unique_alignment,
    _normalize_ml_stem,
)

# ----------------------------- unified time resolver -----------------------------


def _choose_unified_rpi_times_and_sources(rpi_df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """
    Return:
      (resolved_times, resolved_sources)

    resolved_times: pandas datetime64[ns] Series of chosen RPi timestamps.
    resolved_sources: pandas string Series, the column name used per row
                      (e.g., 'RPi_Time_verb', 'RPi_Time_simple', 'RPi_Time_unified', 'RPi_Timestamp', or '').
    """
    cols = set(map(str, rpi_df.columns))

    # Normalize Mono_* → Monotonic_* (for consistency only)
    rename_map = {}
    if "Mono_Time_Raw_verb" in cols and "Monotonic_Time_Raw_verb" not in cols:
        rename_map["Mono_Time_Raw_verb"] = "Monotonic_Time_Raw_verb"
    if "Mono_Time_verb" in cols and "Monotonic_Time_verb" not in cols:
        rename_map["Mono_Time_verb"] = "Monotonic_Time_verb"
    if rename_map:
        rpi_df.rename(columns=rename_map, inplace=True)
        cols = set(map(str, rpi_df.columns))

    def _pick(row: pd.Series):
        src = str(row.get("RPi_Timestamp_Source", "") or "").strip()
        if src and src in row.index:
            val = row[src]
            if pd.notna(val) and str(val).strip():
                return val, src
        for cand in ("RPi_Time_unified", "RPi_Time_verb", "RPi_Time_simple", "RPi_Timestamp"):
            if cand in row.index:
                val = row[cand]
                if pd.notna(val) and str(val).strip():
                    return val, cand
        return np.nan, ""

    picked_vals, picked_srcs = zip(*[ _pick(r) for _, r in rpi_df.iterrows() ]) if len(rpi_df) else ([], [])
    times = pd.to_datetime(pd.Series(picked_vals, index=rpi_df.index), errors="coerce")
    sources = pd.Series(list(picked_srcs), index=rpi_df.index, dtype="string")
    return times, sources


# ------------------------------------ CLI ------------------------------------


@dataclass
class Args:
    ml_csv_file: str
    rpi_marks_csv: str
    blankRowTemplate: Optional[str]
    csv_timestamp_column: str
    event_type_column: str
    event_type_values: str
    label: str
    device: str
    timezone_offset_hours: str
    max_match_gap_s: float
    out_dir: str
    strip_ml_suffixes: str


def _parse_args() -> Args:
    ap = argparse.ArgumentParser(description="Align ML marks to unified RPi marks and synthesize unmatched rows.")
    ap.add_argument("--ml_csv_file", required=True)
    ap.add_argument("--rpi_marks_csv", required=True)
    ap.add_argument("--blankRowTemplate", help="Template CSV for synthetic rows (e.g. NewRowInfo.csv)")
    ap.add_argument("--csv_timestamp_column", default="mLTimestamp")
    ap.add_argument("--event_type_column", default="lo_eventType")
    ap.add_argument("--event_type_values", default="Mark", help="Comma-separated values, e.g. 'Mark,TrialStart'")
    ap.add_argument("--label", required=True, help="BioPac or RNS (used as column prefix)")
    ap.add_argument("--device", required=True)
    ap.add_argument("--timezone_offset_hours", default="auto", help="number or 'auto'")
    ap.add_argument("--max_match_gap_s", type=float, default=1.0)
    ap.add_argument("--out_dir", default="", help="Optional output directory")
    ap.add_argument(
        "--strip-ml-suffixes",
        default="_events_final,_events,_final",
        help="Comma-separated suffixes to strip from ML stem for output naming",
    )
    ns = ap.parse_args()
    return Args(
        ml_csv_file=ns.ml_csv_file,
        rpi_marks_csv=ns.rpi_marks_csv,
        blankRowTemplate=ns.blankRowTemplate,
        csv_timestamp_column=ns.csv_timestamp_column,
        event_type_column=ns.event_type_column,
        event_type_values=ns.event_type_values,
        label=ns.label,
        device=ns.device,
        timezone_offset_hours=ns.timezone_offset_hours,
        max_match_gap_s=ns.max_match_gap_s,
        out_dir=ns.out_dir,
        strip_ml_suffixes=ns.strip_ml_suffixes,
    )


# ----------------------------------- main ------------------------------------


def main():
    args = _parse_args()

    ml_path = Path(args.ml_csv_file)
    rpi_path = Path(args.rpi_marks_csv)
    if not ml_path.exists():
        raise FileNotFoundError(f"Missing ML CSV: {ml_path}")
    if not rpi_path.exists():
        raise FileNotFoundError(f"Missing RPi marks CSV: {rpi_path}")

    # --- Load ML and select requested event rows -----------------------------
    ml_df_all = pd.read_csv(ml_path)
    ts_col = args.csv_timestamp_column
    type_col = args.event_type_column
    type_vals = [v.strip() for v in args.event_type_values.split(",") if v.strip()]

    if ts_col not in ml_df_all.columns:
        raise KeyError(f"ML CSV missing timestamp column '{ts_col}'")
    if type_col not in ml_df_all.columns:
        raise KeyError(f"ML CSV missing event type column '{type_col}'")

    ml_df = _select_mark_rows(ml_df_all, type_col, type_vals)
    ml_times = pd.to_datetime(ml_df[ts_col], errors="coerce")

    # --- Load unified RPi marks & resolve per-row timestamps -----------------
    rpi_df = pd.read_csv(rpi_path)
    rpi_times, rpi_sources = _choose_unified_rpi_times_and_sources(rpi_df)
    if rpi_times.isna().all():
        raise KeyError(
            "Could not resolve RPi timestamps. Expected 'RPi_Timestamp_Source' → "
            "'RPi_Time_verb'/'RPi_Time_simple'/'RPi_Time_unified', or legacy 'RPi_Timestamp'."
        )

    # --- Estimate or apply clock offset (RPi→ML) -----------------------------
    tz_arg = str(args.timezone_offset_hours).strip().lower()
    if tz_arg == "auto":
        est_hours = _auto_offset_hours(ml_times, rpi_times)
    else:
        est_hours = float(args.timezone_offset_hours)

    offset_td = timedelta(hours=est_hours)

    # Apply offset only to *_simple rows; *_verb rows remain unshifted.
    # For other sources (e.g., RPi_Time_unified/RPi_Timestamp), keep legacy behavior and apply the offset.
    src_lower = rpi_sources.fillna("").str.lower()
    is_simple = src_lower.str.contains("_simple")
    is_verb = src_lower.str.contains("_verb")
    apply_offset_mask = (~rpi_times.isna()) & (~is_verb)  # offset applied to simple + other, not to verb

    rpi_times_effective = rpi_times.copy()
    if apply_offset_mask.any() and offset_td != timedelta(0):
        rpi_times_effective.loc[apply_offset_mask] = rpi_times_effective.loc[apply_offset_mask] + offset_td

    # --- Greedy unique nearest matching --------------------------------------
    match_idx, deltas_s, reasons = _nearest_unique_alignment(
        ml_times, rpi_times_effective, max_gap=float(args.max_match_gap_s)
    )
    ml_matched_mask = match_idx >= 0
    ml_match_i = np.flatnonzero(ml_matched_mask)
    rpi_match_j = match_idx[ml_matched_mask].astype(int)

    # --- Build matched block --------------------------------------------------
    out = ml_df.copy()
    label = args.label
    ts_col_label = f"{label}_RPi_Timestamp"
    drift_col_label = f"{label}_RPi_Timestamp_drift"
    matched_col_label = f"{label}_RPi_Matched"
    reason_col_label = f"{label}_RPi_MatchReason"

    out[matched_col_label] = False
    out[reason_col_label] = ""
    out[ts_col_label] = pd.NaT
    out[drift_col_label] = np.nan

    out.loc[ml_match_i, matched_col_label] = True
    out.loc[ml_match_i, reason_col_label] = "matched"
    out.loc[ml_match_i, ts_col_label] = rpi_times_effective.iloc[rpi_match_j].values
    out.loc[ml_match_i, drift_col_label] = (
        (rpi_times_effective.iloc[rpi_match_j].reset_index(drop=True) - ml_times.iloc[ml_match_i].reset_index(drop=True))
        .dt.total_seconds()
        .values
    )

    unmatched_i = np.flatnonzero(~ml_matched_mask)
    if len(unmatched_i):
        for i in unmatched_i:
            out.at[out.index[i], reason_col_label] = reasons[i] if reasons[i] else "unmatched"

    # Attach provenance columns (prefixed)
    attach_cols: Sequence[str] = [
        "markNumber",
        "DeviceIP",
        "Device",
        "RPi_Source",
        "RPi_Timestamp_Source",
        "RPi_Time_unified",
        "RPi_Time_verb",
        "RPi_Time_simple",
    ]
    attach_df = rpi_df.iloc[rpi_match_j].reset_index(drop=True).copy()
    attach_df = attach_df[[c for c in attach_cols if c in attach_df.columns]]
    # Also attach which source was used and whether offset was applied on that row
    used_src_matched = rpi_sources.iloc[rpi_match_j].reset_index(drop=True).rename("RPi_Resolved_Source")
    offset_applied_matched = apply_offset_mask.iloc[rpi_match_j].reset_index(drop=True).rename("RPi_Offset_Applied")

    attach_df = pd.concat([attach_df, used_src_matched, offset_applied_matched], axis=1)
    attach_df = attach_df.add_prefix(f"{label}_RPi__")

    matched_block = out.iloc[ml_match_i].reset_index(drop=True)
    matched_block = pd.concat([matched_block, attach_df], axis=1)

    # --- Synthesize ML rows for unmatched RPi marks (optional) ---------------
    rpi_all_idx = set(range(len(rpi_df)))
    rpi_used_idx = set(rpi_match_j.tolist())
    rpi_unmatched_idx = sorted(rpi_all_idx - rpi_used_idx)

    synth_rows: List[pd.Series] = []
    template_row: Optional[pd.Series] = None
    if args.blankRowTemplate:
        tmpl_path = Path(args.blankRowTemplate)
        if tmpl_path.exists():
            tmpl_df = pd.read_csv(tmpl_path)
            if not tmpl_df.empty:
                template_row = tmpl_df.iloc[0]

    if rpi_unmatched_idx and template_row is not None:
        for j in rpi_unmatched_idx:
            rp_row = rpi_df.iloc[j]
            new_row = template_row.copy()

            if "device" in new_row.index:
                new_row["device"] = args.device
            if "label" in new_row.index:
                new_row["label"] = args.label

            # For synthetic rows, use the per-row effective time (offset applied only if not *_verb)
            eff_time = rpi_times_effective.iloc[j]
            if ts_col in new_row.index:
                new_row[ts_col] = eff_time

            # provenance fields
            for k in (
                "markNumber",
                "DeviceIP",
                "Device",
                "RPi_Source",
                "RPi_Timestamp_Source",
                "RPi_Time_unified",
                "RPi_Time_verb",
                "RPi_Time_simple",
            ):
                if k in rp_row.index:
                    new_row[f"RPi_{k}"] = rp_row[k]
            new_row["RPi_Resolved_Source"] = rpi_sources.iloc[j]
            new_row["RPi_Offset_Applied"] = bool(apply_offset_mask.iloc[j])

            new_row["_synthetic_from_rpi"] = True
            synth_rows.append(new_row)

    synth_df = pd.DataFrame(synth_rows) if synth_rows else pd.DataFrame(columns=out.columns)

    # --- Final assembly -------------------------------------------------------
    out_matched_full = matched_block
    out_unmatched_ml = out.iloc[unmatched_i].copy()
    final_df = pd.concat([out_matched_full, out_unmatched_ml, synth_df], axis=0, ignore_index=True)

    # --- Output paths ---------------------------------------------------------
    out_dir = Path(args.out_dir) if args.out_dir else ml_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    stem = _normalize_ml_stem(ml_path.stem, [s.strip() for s in args.strip_ml_suffixes.split(",") if s.strip()])
    out_csv = out_dir / f"{stem}_{args.label}_{args.device}_aligned_with_RPi.csv"
    summary_csv = out_dir / f"{stem}_{args.label}_{args.device}_alignment_summary.csv"

    # --- Summary --------------------------------------------------------------
    n_ml_marks = len(ml_df)
    n_rpi_marks = len(rpi_df)
    n_matched = len(ml_match_i)
    n_synth = len(synth_df)

    summary = pd.DataFrame(
        [
            {
                "ml_file": ml_path.name,
                "rpi_file": rpi_path.name,
                "n_ml_marks": n_ml_marks,
                "n_rpi_marks": n_rpi_marks,
                "n_matched": n_matched,
                "n_unmatched_ml": int(len(unmatched_i)),
                "n_synthetic_from_rpi": n_synth,
                "timezone_offset_hours_estimated": est_hours,
                "max_match_gap_s": float(args.max_match_gap_s),
                "offset_applied_rows": int(apply_offset_mask.sum()),
                "offset_skipped_rows": int(is_verb.sum()),
            }
        ]
    )

    # --- Write ----------------------------------------------------------------
    final_df.to_csv(out_csv, index=False)
    summary.to_csv(summary_csv, index=False)

    print(f"[ok] wrote aligned CSV   → {out_csv}")
    print(f"[ok] wrote summary CSV   → {summary_csv}")


if __name__ == "__main__":
    main()