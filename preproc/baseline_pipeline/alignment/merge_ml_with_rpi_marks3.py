# file: alignment/merge_ml_with_rpi_marks3.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from batchAlignHelpers import (
    _auto_offset_hours,
    _nearest_unique_alignment,
    _normalize_ml_stem,
    _select_mark_rows,
)


def _choose_unified_rpi_times_and_sources(rpi_df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    cols = set(map(str, rpi_df.columns))

    rename_map = {}
    if "Mono_Time_Raw_verb" in cols and "Monotonic_Time_Raw_verb" not in cols:
        rename_map["Mono_Time_Raw_verb"] = "Monotonic_Time_Raw_verb"
    if "Mono_Time_verb" in cols and "Monotonic_Time_verb" not in cols:
        rename_map["Mono_Time_verb"] = "Monotonic_Time_verb"
    if rename_map:
        rpi_df = rpi_df.rename(columns=rename_map)

    def _pick(row: pd.Series) -> tuple[object, str]:
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

    if len(rpi_df) == 0:
        return (
            pd.Series(dtype="datetime64[ns]"),
            pd.Series(dtype="string"),
        )

    picked_vals, picked_srcs = zip(*[_pick(r) for _, r in rpi_df.iterrows()])
    times = pd.to_datetime(pd.Series(picked_vals, index=rpi_df.index), errors="coerce")
    sources = pd.Series(list(picked_srcs), index=rpi_df.index, dtype="string")
    return times, sources


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
    ap = argparse.ArgumentParser(
        description="Align ML marks to unified RPi marks and synthesize unmatched rows."
    )
    ap.add_argument("--ml_csv_file", required=True)
    ap.add_argument("--rpi_marks_csv", required=True)
    ap.add_argument("--blankRowTemplate", help="Template CSV for synthetic rows")
    ap.add_argument("--csv_timestamp_column", default="eMLT_orig")
    ap.add_argument("--event_type_column", default="lo_eventType")
    ap.add_argument("--event_type_values", default="Mark")
    ap.add_argument("--label", required=True)
    ap.add_argument("--device", required=True)
    ap.add_argument("--timezone_offset_hours", default="auto")
    ap.add_argument("--max_match_gap_s", type=float, default=1.0)
    ap.add_argument("--out_dir", default="")
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


def main() -> None:
    args = _parse_args()

    ml_path = Path(args.ml_csv_file)
    rpi_path = Path(args.rpi_marks_csv)

    if not ml_path.exists():
        raise FileNotFoundError(f"Missing ML CSV: {ml_path}")
    if not rpi_path.exists():
        raise FileNotFoundError(f"Missing RPi marks CSV: {rpi_path}")

    ml_df_all = pd.read_csv(ml_path)
    ts_col = args.csv_timestamp_column
    type_col = args.event_type_column
    type_vals = [v.strip() for v in args.event_type_values.split(",") if v.strip()]

    if ts_col not in ml_df_all.columns:
        raise KeyError(f"ML CSV missing timestamp column '{ts_col}'")
    if type_col not in ml_df_all.columns:
        raise KeyError(f"ML CSV missing event type column '{type_col}'")

    ml_df = _select_mark_rows(ml_df_all, type_col, type_vals).reset_index(drop=True)
    if ml_df.empty:
        raise ValueError(
            f"No ML rows matched {type_col} in {type_vals} for file: {ml_path.name}"
        )

    ml_times = pd.to_datetime(ml_df[ts_col], errors="coerce")

    rpi_df = pd.read_csv(rpi_path)
    rpi_times, rpi_sources = _choose_unified_rpi_times_and_sources(rpi_df)

    if rpi_times.isna().all():
        raise KeyError(
            "Could not resolve RPi timestamps. Expected 'RPi_Timestamp_Source' -> "
            "'RPi_Time_unified'/'RPi_Time_verb'/'RPi_Time_simple', or legacy 'RPi_Timestamp'."
        )

    tz_arg = str(args.timezone_offset_hours).strip().lower()
    if tz_arg == "auto":
        est_hours = _auto_offset_hours(ml_times, rpi_times)
    else:
        est_hours = float(args.timezone_offset_hours)

    offset_td = timedelta(hours=est_hours)

    src_lower = rpi_sources.fillna("").str.lower()
    is_verb = src_lower.str.contains("_verb")
    apply_offset_mask = (~rpi_times.isna()) & (~is_verb)

    rpi_times_effective = rpi_times.copy()
    if apply_offset_mask.any() and offset_td != timedelta(0):
        rpi_times_effective.loc[apply_offset_mask] = (
            rpi_times_effective.loc[apply_offset_mask] + offset_td
        )

    match_idx, deltas_s, reasons = _nearest_unique_alignment(
        ml_times, rpi_times_effective, max_gap=float(args.max_match_gap_s)
    )

    ml_matched_mask = match_idx >= 0
    ml_match_i = np.flatnonzero(ml_matched_mask)
    rpi_match_j = match_idx[ml_matched_mask].astype(int)

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

    if len(ml_match_i):
        out.loc[ml_match_i, matched_col_label] = True
        out.loc[ml_match_i, reason_col_label] = "matched"
        out.loc[ml_match_i, ts_col_label] = rpi_times_effective.iloc[rpi_match_j].values
        out.loc[ml_match_i, drift_col_label] = (
            (
                rpi_times_effective.iloc[rpi_match_j].reset_index(drop=True)
                - ml_times.iloc[ml_match_i].reset_index(drop=True)
            )
            .dt.total_seconds()
            .values
        )

    unmatched_i = np.flatnonzero(~ml_matched_mask)
    for i in unmatched_i:
        out.at[i, reason_col_label] = reasons[i] if reasons[i] else "unmatched"

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

    if len(rpi_match_j):
        attach_df = rpi_df.iloc[rpi_match_j].reset_index(drop=True).copy()
        attach_df = attach_df[[c for c in attach_cols if c in attach_df.columns]]

        used_src_matched = (
            rpi_sources.iloc[rpi_match_j].reset_index(drop=True).rename("RPi_Resolved_Source")
        )
        offset_applied_matched = (
            apply_offset_mask.iloc[rpi_match_j].reset_index(drop=True).rename("RPi_Offset_Applied")
        )

        attach_df = pd.concat([attach_df, used_src_matched, offset_applied_matched], axis=1)
        attach_df = attach_df.add_prefix(f"{label}_RPi__")

        matched_block = out.iloc[ml_match_i].reset_index(drop=True)
        matched_block = pd.concat([matched_block, attach_df], axis=1)
    else:
        matched_block = pd.DataFrame(columns=out.columns)

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
            if ts_col in new_row.index:
                new_row[ts_col] = rpi_times_effective.iloc[j]

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

    out_unmatched_ml = out.iloc[unmatched_i].copy()
    final_df = pd.concat([matched_block, out_unmatched_ml, synth_df], axis=0, ignore_index=True)

    out_dir = Path(args.out_dir) if args.out_dir else ml_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    stem = _normalize_ml_stem(
        ml_path.stem,
        [s.strip() for s in args.strip_ml_suffixes.split(",") if s.strip()],
    )
    out_csv = out_dir / f"{stem}_{args.label}_{args.device}_aligned_with_RPi.csv"
    summary_csv = out_dir / f"{stem}_{args.label}_{args.device}_alignment_summary.csv"

    summary = pd.DataFrame(
        [
            {
                "ml_file": ml_path.name,
                "rpi_file": rpi_path.name,
                "n_ml_marks": len(ml_df),
                "n_rpi_marks": len(rpi_df),
                "n_matched": len(ml_match_i),
                "n_unmatched_ml": int(len(unmatched_i)),
                "n_synthetic_from_rpi": len(synth_df),
                "timezone_offset_hours_estimated": est_hours,
                "max_match_gap_s": float(args.max_match_gap_s),
                "offset_applied_rows": int(apply_offset_mask.sum()),
                "offset_skipped_rows": int(is_verb.sum()),
            }
        ]
    )

    final_df.to_csv(out_csv, index=False)
    summary.to_csv(summary_csv, index=False)

    print(f"[ok] wrote aligned CSV   → {out_csv}")
    print(f"[ok] wrote summary CSV   → {summary_csv}")


if __name__ == "__main__":
    main()


