#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from RC_utilities.alignHelpers.batchAlignHelpers import (
    _normalize_ml_stem,
    _select_mark_rows,
)


MATCH_REQUIRED_COLUMNS = {
    "pair_id",
    "events_mark_id",
    "rpi_mark_id",
    "events_time",
    "rpi_time",
    "exclude",
    "reason",
}

SINGLE_REQUIRED_COLUMNS = {
    "stream",
    "mark_id",
    "ordinal",
    "mark_time",
    "matched_pair_id",
    "exclude",
    "reason",
}


@dataclass
class Args:
    ml_csv_file: str
    rpi_marks_csv: str
    manual_matches_csv: str
    manual_singles_csv: str
    csv_timestamp_column: str
    event_type_column: str
    event_type_values: str
    rpi_time_type: str
    label: str
    device: str
    manual_time_tolerance_s: float
    burst_gap_s: float
    out_dir: str
    strip_ml_suffixes: str


def _parse_args() -> Args:
    ap = argparse.ArgumentParser(
        description=(
            "Convert manually reviewed ML↔RPi mark assignments into the canonical frozen matched-mark table used by downstream affine fitting. "
            "This script never automatically rematches marks."
        )
    )

    ap.add_argument("--ml_csv_file", required=True)
    ap.add_argument("--rpi_marks_csv", required=True)
    ap.add_argument("--manual_matches_csv", required=True)
    ap.add_argument("--manual_singles_csv", required=True)

    ap.add_argument("--csv_timestamp_column", default="mLT_orig")
    ap.add_argument("--event_type_column", default="lo_eventType")
    ap.add_argument("--event_type_values", default="Mark")
    ap.add_argument( "--rpi_time_type", required=True, help="Exact preprocessed RPi timestamp column represented by the manual review.")
    ap.add_argument("--label", required=True)
    ap.add_argument("--device", required=True)

    ap.add_argument("--manual_time_tolerance_s", type=float, default=0.005,
        help="Maximum allowed absolute timestamp difference, in seconds, between manual annotations and the current raw candidate at the same ordinal.")
    ap.add_argument("--burst_gap_s", type=float, default=30.0, help="Gap in seconds between consecutive accepted manual ML matches that starts a new burst.")

    ap.add_argument("--out_dir", default="")
    ap.add_argument("--strip_ml_suffixes", default="_events_final,_processed")

    ns = ap.parse_args()

    return Args(
        ml_csv_file=ns.ml_csv_file,
        rpi_marks_csv=ns.rpi_marks_csv,
        manual_matches_csv=ns.manual_matches_csv,
        manual_singles_csv=ns.manual_singles_csv,
        csv_timestamp_column=ns.csv_timestamp_column,
        event_type_column=ns.event_type_column,
        event_type_values=ns.event_type_values,
        rpi_time_type=ns.rpi_time_type,
        label=ns.label,
        device=ns.device,
        manual_time_tolerance_s=ns.manual_time_tolerance_s,
        burst_gap_s=ns.burst_gap_s,
        out_dir=ns.out_dir,
        strip_ml_suffixes=ns.strip_ml_suffixes,
    )


def _as_bool(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)

    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .isin({"1", "true", "t", "yes", "y"})
    )


def _safe_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _require_columns(df: pd.DataFrame, required: set[str], name: str) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise KeyError(f"{name} missing required columns: {missing}")


def _validate_label_column(df: pd.DataFrame, label: str, name: str) -> None:
    if "label" not in df.columns:
        return

    values = {
        str(value).strip()
        for value in df["label"].dropna().tolist()
        if str(value).strip()
    }

    bad = sorted(value for value in values if value != label)
    if bad:
        raise ValueError(
            f"{name} contains label value(s) inconsistent with --label {label!r}: {bad}"
        )


def _prepare_single_stream(
    singles: pd.DataFrame,
    *,
    stream: str,
    candidate_times: pd.Series,
    tolerance_s: float,
) -> tuple[pd.DataFrame, list[dict]]:
    rows = singles.loc[singles["stream"].astype(str).str.strip().str.lower().eq(stream.lower())].copy()

    if rows.empty:
        raise ValueError(f"manual singles CSV contains no stream={stream!r} rows")

    rows["ordinal"] = pd.to_numeric(rows["ordinal"], errors="raise").astype(int)
    rows["manual_mark_time"] = pd.to_datetime(rows["mark_time"], errors="coerce")
    rows["manual_exclude"] = _as_bool(rows["exclude"])
    rows["mark_id"] = rows["mark_id"].astype(str).str.strip()
    rows["matched_pair_id"] = rows["matched_pair_id"].fillna("").astype(str).str.strip()
    rows["reason"] = rows["reason"].fillna("").astype(str)

    duplicate_ord = rows["ordinal"].duplicated(keep=False)
    if duplicate_ord.any():
        values = sorted(rows.loc[duplicate_ord, "ordinal"].unique().tolist())
        raise ValueError(f"duplicate {stream} ordinal(s) in manual singles CSV: {values}")

    duplicate_id = rows["mark_id"].duplicated(keep=False)
    if duplicate_id.any():
        values = sorted(rows.loc[duplicate_id, "mark_id"].unique().tolist())
        raise ValueError(f"duplicate {stream} mark_id value(s) in manual singles CSV: {values}")

    if (rows["mark_id"] == "").any():
        raise ValueError(f"manual singles CSV contains blank mark_id for stream={stream!r}")

    candidate_times = pd.to_datetime(candidate_times, errors="coerce").reset_index(drop=True)

    if candidate_times.isna().any():
        bad = candidate_times.index[candidate_times.isna()].tolist()
        raise ValueError(f"current {stream} candidate timestamps contain unparseable value(s) at ordinal(s): {bad}")

    expected_ordinals = set(range(len(candidate_times)))
    observed_ordinals = set(rows["ordinal"].tolist())

    missing_ordinals = sorted(expected_ordinals - observed_ordinals)
    extra_ordinals = sorted(observed_ordinals - expected_ordinals)

    if missing_ordinals or extra_ordinals:
        raise ValueError(f"manual singles/current {stream} candidate count mismatch; missing ordinal(s)={missing_ordinals}, extra ordinal(s)={extra_ordinals}")

    audit_rows: list[dict] = []

    rows = rows.sort_values("ordinal").reset_index(drop=True)

    for idx, row in rows.iterrows():
        ordinal = int(row["ordinal"])
        manual_time = row["manual_mark_time"]
        raw_time = candidate_times.iloc[ordinal]

        delta_s = np.nan
        time_ok = False

        if pd.notna(manual_time):
            delta_s = abs((raw_time - manual_time).total_seconds())
            time_ok = delta_s <= tolerance_s

        audit_rows.append(
            {
                "audit_type": "single",
                "stream": stream,
                "mark_id": row["mark_id"],
                "ordinal": ordinal,
                "pair_id": row["matched_pair_id"],
                "manual_time": manual_time,
                "raw_time": raw_time,
                "time_difference_s": delta_s,
                "time_check_ok": bool(time_ok),
                "manual_exclude": bool(row["manual_exclude"]),
                "manual_reason": row["reason"],
                "accepted_pair": pd.NA,
                "validation_status": "ok" if time_ok else "timestamp_mismatch",
            }
        )

        if not time_ok:
            raise ValueError(f"{stream} ordinal {ordinal} ({row['mark_id']}) manual timestamp does not match current candidate within {tolerance_s:.6f}s; difference={delta_s!r}s")

        rows.at[idx, "raw_time"] = raw_time

    rows["raw_time"] = candidate_times.to_numpy()

    return rows, audit_rows


def _build_lookup(rows: pd.DataFrame, stream: str) -> dict[str, pd.Series]:
    lookup: dict[str, pd.Series] = {}

    for _, row in rows.iterrows():
        mark_id = str(row["mark_id"])
        if mark_id in lookup:
            raise ValueError(f"duplicate {stream} mark_id: {mark_id}")
        lookup[mark_id] = row

    return lookup


def _validate_manual_pairs(
    matches: pd.DataFrame,
    *,
    events_lookup: dict[str, pd.Series],
    rpi_lookup: dict[str, pd.Series],
    tolerance_s: float,
) -> tuple[
    dict[int, dict],
    set[int],
    set[int],
    list[dict],
]:
    work = matches.copy()

    work["pair_id"] = work["pair_id"].fillna("").astype(str).str.strip()
    work["events_mark_id"] = work["events_mark_id"].fillna("").astype(str).str.strip()
    work["rpi_mark_id"] = work["rpi_mark_id"].fillna("").astype(str).str.strip()
    work["manual_pair_exclude"] = _as_bool(work["exclude"])
    work["manual_events_time"] = pd.to_datetime(work["events_time"], errors="coerce")
    work["manual_rpi_time"] = pd.to_datetime(work["rpi_time"], errors="coerce")
    work["reason"] = work["reason"].fillna("").astype(str)

    duplicate_pair_id = work["pair_id"].duplicated(keep=False)
    if duplicate_pair_id.any():
        values = sorted(work.loc[duplicate_pair_id, "pair_id"].unique().tolist())
        raise ValueError(f"duplicate pair_id value(s) in manual matches CSV: {values}")

    if (work["pair_id"] == "").any():
        raise ValueError("manual matches CSV contains blank pair_id")

    accepted_by_ml_ordinal: dict[int, dict] = {}
    used_ml_ordinals: set[int] = set()
    used_rpi_ordinals: set[int] = set()
    audit_rows: list[dict] = []

    for _, pair in work.iterrows():
        pair_id = pair["pair_id"]
        event_id = pair["events_mark_id"]
        rpi_id = pair["rpi_mark_id"]

        if event_id not in events_lookup:
            raise ValueError(f"manual pair {pair_id!r} references unknown events mark_id {event_id!r}")

        if rpi_id not in rpi_lookup:
            raise ValueError(f"manual pair {pair_id!r} references unknown RPi mark_id {rpi_id!r}")

        event_single = events_lookup[event_id]
        rpi_single = rpi_lookup[rpi_id]

        ml_ordinal = int(event_single["ordinal"])
        rpi_ordinal = int(rpi_single["ordinal"])

        current_ml_time = pd.to_datetime(event_single["raw_time"])
        current_rpi_time = pd.to_datetime(rpi_single["raw_time"])

        pair_event_time = pair["manual_events_time"]
        pair_rpi_time = pair["manual_rpi_time"]

        event_delta_s = (
            abs((current_ml_time - pair_event_time).total_seconds())
            if pd.notna(pair_event_time)
            else np.nan
        )
        rpi_delta_s = (
            abs((current_rpi_time - pair_rpi_time).total_seconds())
            if pd.notna(pair_rpi_time)
            else np.nan
        )

        event_time_ok = (
            np.isfinite(event_delta_s)
            and event_delta_s <= tolerance_s
        )
        rpi_time_ok = (
            np.isfinite(rpi_delta_s)
            and rpi_delta_s <= tolerance_s
        )

        if not event_time_ok:
            raise ValueError(f"manual pair {pair_id!r}: events timestamp mismatch for {event_id!r}; difference={event_delta_s!r}s, tolerance={tolerance_s:.6f}s")

        if not rpi_time_ok:
            raise ValueError(f"manual pair {pair_id!r}: RPi timestamp mismatch for {rpi_id!r}; difference={rpi_delta_s!r}s, tolerance={tolerance_s:.6f}s")

        event_pair_ref = _safe_text(event_single["matched_pair_id"])
        rpi_pair_ref = _safe_text(rpi_single["matched_pair_id"])

        event_pair_ref_ok = (not event_pair_ref) or event_pair_ref == pair_id
        rpi_pair_ref_ok = (not rpi_pair_ref) or rpi_pair_ref == pair_id

        if not event_pair_ref_ok:
            raise ValueError(f"manual pair {pair_id!r}: events single {event_id!r} points to matched_pair_id={event_pair_ref!r}")

        if not rpi_pair_ref_ok:
            raise ValueError(f"manual pair {pair_id!r}: RPi single {rpi_id!r} points to matched_pair_id={rpi_pair_ref!r}")

        pair_excluded = bool(pair["manual_pair_exclude"])
        event_excluded = bool(event_single["manual_exclude"])
        rpi_excluded = bool(rpi_single["manual_exclude"])

        accepted = not (pair_excluded or event_excluded or rpi_excluded)

        if accepted:
            if ml_ordinal in used_ml_ordinals:
                raise ValueError(f"accepted manual pairs reuse ML ordinal {ml_ordinal}")

            if rpi_ordinal in used_rpi_ordinals:
                raise ValueError(f"accepted manual pairs reuse RPi ordinal {rpi_ordinal}")

            used_ml_ordinals.add(ml_ordinal)
            used_rpi_ordinals.add(rpi_ordinal)

            accepted_by_ml_ordinal[ml_ordinal] = {
                "pair_id": pair_id,
                "rpi_ordinal": rpi_ordinal,
                "pair_reason": pair["reason"],
                "pair_reviewed_at": _safe_text(pair.get("reviewed_at", "")),
                "pair_block": _safe_text(pair.get("block", "")),
            }

        if pair_excluded:
            status = "pair_excluded"
        elif event_excluded and rpi_excluded:
            status = "both_singles_excluded"
        elif event_excluded:
            status = "events_single_excluded"
        elif rpi_excluded:
            status = "rpi_single_excluded"
        else:
            status = "accepted"

        audit_rows.append(
            {
                "audit_type": "pair",
                "stream": "pair",
                "mark_id": f"{event_id}|{rpi_id}",
                "ordinal": f"{ml_ordinal}|{rpi_ordinal}",
                "pair_id": pair_id,
                "manual_time": f"{pair_event_time}|{pair_rpi_time}",
                "raw_time": f"{current_ml_time}|{current_rpi_time}",
                "time_difference_s": f"{event_delta_s}|{rpi_delta_s}",
                "time_check_ok": bool(event_time_ok and rpi_time_ok),
                "manual_exclude": bool(pair_excluded),
                "manual_reason": pair["reason"],
                "accepted_pair": bool(accepted),
                "validation_status": status,
            }
        )

    return (
        accepted_by_ml_ordinal,
        used_ml_ordinals,
        used_rpi_ordinals,
        audit_rows,
    )


def _compute_burst_metadata(
    ml_times: pd.Series,
    matched_ml_ordinals: set[int],
    burst_gap_s: float,
) -> tuple[np.ndarray, np.ndarray, int]:
    burst_ids = np.full(len(ml_times), np.nan, dtype=float)
    burst_positions = np.full(len(ml_times), np.nan, dtype=float)

    ordered = sorted(matched_ml_ordinals)

    current_burst = 0
    current_position = 0
    previous_time = None

    for ordinal in ordered:
        mark_time = ml_times.iloc[ordinal]

        if pd.isna(mark_time):
            continue

        if previous_time is None:
            current_burst = 1
            current_position = 1
        else:
            gap_s = (mark_time - previous_time).total_seconds()

            if gap_s > burst_gap_s:
                current_burst += 1
                current_position = 1
            else:
                current_position += 1

        burst_ids[ordinal] = current_burst
        burst_positions[ordinal] = current_position
        previous_time = mark_time

    return burst_ids, burst_positions, current_burst


def main() -> None:
    args = _parse_args()

    ml_path = Path(args.ml_csv_file)
    rpi_path = Path(args.rpi_marks_csv)
    manual_matches_path = Path(args.manual_matches_csv)
    manual_singles_path = Path(args.manual_singles_csv)

    for path, name in (
        (ml_path, "ML CSV"),
        (rpi_path, "RPi marks CSV"),
        (manual_matches_path, "manual matches CSV"),
        (manual_singles_path, "manual singles CSV"),
    ):
        if not path.exists():
            raise FileNotFoundError(f"Missing {name}: {path}")

    ml_df = pd.read_csv(ml_path)

    if args.csv_timestamp_column not in ml_df.columns:
        raise KeyError(f"ML CSV missing timestamp column {args.csv_timestamp_column!r}")

    if args.event_type_column not in ml_df.columns:
        raise KeyError(f"ML CSV missing event type column {args.event_type_column!r}")

    event_type_values = [
        value.strip()
        for value in args.event_type_values.split(",")
        if value.strip()
    ]

    ml_marks = _select_mark_rows(
        ml_df,
        args.event_type_column,
        event_type_values,
    ).copy()

    ml_marks["_ml_source_index"] = ml_marks.index
    ml_marks = ml_marks.reset_index(drop=True)

    ml_times = pd.to_datetime(
        ml_marks[args.csv_timestamp_column],
        errors="coerce",
    ).reset_index(drop=True)

    if ml_times.isna().any():
        bad = ml_times.index[ml_times.isna()].tolist()
        raise ValueError(f"ML mark timestamps contain unparseable value(s) at ordinal(s): {bad}")

    source_indices = ml_marks["_ml_source_index"].to_numpy(dtype=int)

    rpi_df = pd.read_csv(rpi_path).reset_index(drop=True)

    if args.rpi_time_type not in rpi_df.columns:
        raise KeyError(f"RPi timestamp column not found: {args.rpi_time_type!r}")

    rpi_times = pd.to_datetime(rpi_df[args.rpi_time_type], errors="coerce").reset_index(drop=True)

    if rpi_times.isna().any():
        bad = rpi_times.index[rpi_times.isna()].tolist()
        raise ValueError(f"RPi timestamps contain unparseable value(s) at ordinal(s): {bad}")

    manual_matches = pd.read_csv(manual_matches_path)
    manual_singles = pd.read_csv(manual_singles_path)

    _require_columns(manual_matches, MATCH_REQUIRED_COLUMNS, "manual matches CSV")
    _require_columns(manual_singles, SINGLE_REQUIRED_COLUMNS, "manual singles CSV")

    _validate_label_column(manual_matches, args.label, "manual matches CSV")
    _validate_label_column(manual_singles, args.label, "manual singles CSV")

    events_singles, events_audit = _prepare_single_stream(
        manual_singles,
        stream="events",
        candidate_times=ml_times,
        tolerance_s=args.manual_time_tolerance_s,
    )

    rpi_singles, rpi_audit = _prepare_single_stream(
        manual_singles,
        stream="rpi",
        candidate_times=rpi_times,
        tolerance_s=args.manual_time_tolerance_s,
    )

    events_lookup = _build_lookup(events_singles, "events")
    rpi_lookup = _build_lookup(rpi_singles, "rpi")

    
        accepted_by_ml_ordinal, used_ml_ordinals, used_rpi_ordinals, pair_audit = _validate_manual_pairs(
        manual_matches,
        events_lookup=events_lookup,
        rpi_lookup=rpi_lookup,
        tolerance_s=args.manual_time_tolerance_s,
    )

    burst_ids, burst_positions, n_matched_bursts = _compute_burst_metadata(
        ml_times,
        used_ml_ordinals,
        args.burst_gap_s,
    )

    events_by_ordinal = {
        int(row["ordinal"]): row
        for _, row in events_singles.iterrows()
    }
    rpi_by_ordinal = {
        int(row["ordinal"]): row
        for _, row in rpi_singles.iterrows()
    }

    rejected_pair_ml_ordinals: set[int] = set()
    rejected_pair_rpi_ordinals: set[int] = set()

    for audit in pair_audit:
        if audit["accepted_pair"] is True:
            continue

        event_id, rpi_id = str(audit["mark_id"]).split("|", 1)

        if event_id in events_lookup:
            rejected_pair_ml_ordinals.add(
                int(events_lookup[event_id]["ordinal"])
            )

        if rpi_id in rpi_lookup:
            rejected_pair_rpi_ordinals.add(
                int(rpi_lookup[rpi_id]["ordinal"])
            )

    rows: list[dict] = []

    for ml_ordinal in range(len(ml_marks)):
        single = events_by_ordinal[ml_ordinal]
        accepted_pair = accepted_by_ml_ordinal.get(ml_ordinal)

        single_excluded = bool(single["manual_exclude"])
        exclusion_reason = (
            _safe_text(single["reason"])
            if single_excluded
            else ""
        )

        burst_id = (
            int(burst_ids[ml_ordinal])
            if np.isfinite(burst_ids[ml_ordinal])
            else pd.NA
        )
        burst_position = (
            int(burst_positions[ml_ordinal])
            if np.isfinite(burst_positions[ml_ordinal])
            else pd.NA
        )
        is_first_in_burst = (
            bool(burst_position == 1)
            if pd.notna(burst_position)
            else pd.NA
        )

        if accepted_pair is not None:
            rpi_ordinal = int(accepted_pair["rpi_ordinal"])

            rows.append(
                {
                    "record_type": "ml_mark",
                    "matching_method": "manual",
                    "ml_mark_index": int(ml_ordinal),
                    "ml_source_index": int(source_indices[ml_ordinal]),
                    "ml_time": ml_times.iloc[ml_ordinal],
                    "rpi_index": rpi_ordinal,
                    "rpi_time": rpi_times.iloc[rpi_ordinal],
                    "rpi_timestamp_source": args.rpi_time_type,
                    "matched": True,
                    "excluded": False,
                    "exclusion_reason": "",
                    "matcher_reason_internal": "manual_direct_pair",
                    "raw_offset_s": (rpi_times.iloc[rpi_ordinal] - ml_times.iloc[ml_ordinal]).total_seconds(),
                    "matcher_temp_affine_inlier": False,
                    "matcher_temp_affine_residual_s": np.nan,
                    "matcher_initial_match_gap_s": np.nan,
                    "matcher_final_match_gap_s": np.nan,
                    "matcher_coarse_search_window_s": np.nan,
                    "matcher_sigma_clip": np.nan,
                    "matched_burst_id": burst_id,
                    "matched_burst_position": burst_position,
                    "is_first_in_burst": is_first_in_burst,
                    "matcher_burst_gap_s": args.burst_gap_s,
                    "manual_pair_id": accepted_pair["pair_id"],
                    "manual_pair_reason": accepted_pair["pair_reason"],
                    "manual_pair_reviewed_at": accepted_pair["pair_reviewed_at"],
                    "manual_pair_block": accepted_pair["pair_block"],
                }
            )
            continue

        if single_excluded:
            reason_internal = "manual_single_excluded"
        elif ml_ordinal in rejected_pair_ml_ordinals:
            reason_internal = "manual_pair_rejected"
        else:
            reason_internal = "manual_unmatched"

        rows.append(
            {
                "record_type": "ml_mark",
                "matching_method": "manual",
                "ml_mark_index": int(ml_ordinal),
                "ml_source_index": int(source_indices[ml_ordinal]),
                "ml_time": ml_times.iloc[ml_ordinal],
                "rpi_index": pd.NA,
                "rpi_time": pd.NaT,
                "rpi_timestamp_source": "",
                "matched": False,
                "excluded": bool(single_excluded),
                "exclusion_reason": exclusion_reason,
                "matcher_reason_internal": reason_internal,
                "raw_offset_s": np.nan,
                "matcher_temp_affine_inlier": False,
                "matcher_temp_affine_residual_s": np.nan,
                "matcher_initial_match_gap_s": np.nan,
                "matcher_final_match_gap_s": np.nan,
                "matcher_coarse_search_window_s": np.nan,
                "matcher_sigma_clip": np.nan,
                "matched_burst_id": pd.NA,
                "matched_burst_position": pd.NA,
                "is_first_in_burst": pd.NA,
                "matcher_burst_gap_s": args.burst_gap_s,
                "manual_pair_id": "",
                "manual_pair_reason": "",
                "manual_pair_reviewed_at": "",
                "manual_pair_block": _safe_text(single.get("block", "")),
            }
        )

    for rpi_ordinal in range(len(rpi_df)):
        if rpi_ordinal in used_rpi_ordinals:
            continue

        single = rpi_by_ordinal[rpi_ordinal]
        single_excluded = bool(single["manual_exclude"])

        if single_excluded:
            reason_internal = "manual_single_excluded"
        elif rpi_ordinal in rejected_pair_rpi_ordinals:
            reason_internal = "manual_pair_rejected"
        else:
            reason_internal = "manual_unmatched_rpi"

        rows.append(
            {
                "record_type": "rpi_only",
                "matching_method": "manual",
                "ml_mark_index": pd.NA,
                "ml_source_index": pd.NA,
                "ml_time": pd.NaT,
                "rpi_index": int(rpi_ordinal),
                "rpi_time": rpi_times.iloc[rpi_ordinal],
                "rpi_timestamp_source": args.rpi_time_type,
                "matched": False,
                "excluded": bool(single_excluded),
                "exclusion_reason": (
                    _safe_text(single["reason"])
                    if single_excluded
                    else ""
                ),
                "matcher_reason_internal": reason_internal,
                "raw_offset_s": np.nan,
                "matcher_temp_affine_inlier": False,
                "matcher_temp_affine_residual_s": np.nan,
                "matcher_initial_match_gap_s": np.nan,
                "matcher_final_match_gap_s": np.nan,
                "matcher_coarse_search_window_s": np.nan,
                "matcher_sigma_clip": np.nan,
                "matched_burst_id": pd.NA,
                "matched_burst_position": pd.NA,
                "is_first_in_burst": pd.NA,
                "matcher_burst_gap_s": args.burst_gap_s,
                "manual_pair_id": "",
                "manual_pair_reason": "",
                "manual_pair_reviewed_at": "",
                "manual_pair_block": _safe_text(single.get("block", "")),
            }
        )

    frozen = pd.DataFrame(rows)

    audit = pd.DataFrame(
        events_audit + rpi_audit + pair_audit
    )

    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else ml_path.parent
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    suffixes = [
        suffix.strip()
        for suffix in args.strip_ml_suffixes.split(",")
        if suffix.strip()
    ]

    stem = _normalize_ml_stem(
        ml_path.stem,
        suffixes,
    )

    matched_csv = out_dir / f"{stem}_{args.label}_{args.device}_matched_marks.csv"
    
    summary_csv =  out_dir / f"{stem}_{args.label}_{args.device}_matching_summary.csv"
    
    audit_csv = out_dir / f"{stem}_{args.label}_{args.device}_manual_validation_audit.csv"
    

    n_pairs_total = len(manual_matches)
    n_pairs_excluded = int(_as_bool(manual_matches["exclude"]).sum())
    n_pairs_accepted = len(accepted_by_ml_ordinal)

    n_ml_excluded = int(events_singles["manual_exclude"].sum())
    n_rpi_excluded = int(rpi_singles["manual_exclude"].sum())

    accepted_ordinals = sorted(used_ml_ordinals)
    accepted_times = ml_times.iloc[accepted_ordinals] if accepted_ordinals else pd.Series(dtype="datetime64[ns]")

    first_burst_start_time = pd.NaT
    first_burst_end_time = pd.NaT
    last_burst_start_time = pd.NaT
    last_burst_end_time = pd.NaT
    max_interburst_gap_s = np.nan

    if n_matched_bursts > 0:
        burst_table = pd.DataFrame(
            {
                "burst_id": [
                    int(burst_ids[i])
                    for i in accepted_ordinals
                ],
                "ml_time": accepted_times.to_numpy(),
            }
        )

        burst_summary = burst_table.groupby("burst_id", sort=True)["ml_time"].agg(burst_start="min", burst_end="max").reset_index()
        

        first_burst_start_time = burst_summary.iloc[0]["burst_start"]
        first_burst_end_time = burst_summary.iloc[0]["burst_end"]
        last_burst_start_time = burst_summary.iloc[-1]["burst_start"]
        last_burst_end_time = burst_summary.iloc[-1]["burst_end"]

        if len(burst_summary) >= 2:
            gaps = (
                burst_summary["burst_start"].iloc[1:].reset_index(drop=True)
                - burst_summary["burst_end"].iloc[:-1].reset_index(drop=True)
            ).dt.total_seconds()

            if len(gaps):
                max_interburst_gap_s = float(gaps.max())

    summary = pd.DataFrame(
        [
            {
                "ml_file": ml_path.name,
                "rpi_file": rpi_path.name,
                "manual_matches_file": manual_matches_path.name,
                "manual_singles_file": manual_singles_path.name,
                "label": args.label,
                "device": args.device,
                "matching_method": "manual",
                "rpi_timestamp_column": args.rpi_time_type,
                "manual_time_tolerance_s": args.manual_time_tolerance_s,
                "n_ml_marks": len(ml_marks),
                "n_rpi_marks": len(rpi_df),
                "n_manual_pairs_total": n_pairs_total,
                "n_manual_pairs_pair_excluded": n_pairs_excluded,
                "n_manual_pairs_accepted": n_pairs_accepted,
                "n_manual_pairs_not_accepted": int(n_pairs_total - n_pairs_accepted),
                "n_manual_ml_single_exclusions": n_ml_excluded,
                "n_manual_rpi_single_exclusions": n_rpi_excluded,
                "n_unmatched_ml_marks": int(len(ml_marks) - n_pairs_accepted),
                "n_unmatched_rpi_marks": int(len(rpi_df) - n_pairs_accepted),
                "burst_gap_s": args.burst_gap_s,
                "n_matched_bursts": n_matched_bursts,
                "single_burst_only": bool(n_matched_bursts == 1),
                "first_burst_start_time": first_burst_start_time,
                "first_burst_end_time": first_burst_end_time,
                "last_burst_start_time": last_burst_start_time,
                "last_burst_end_time": last_burst_end_time,
                "max_interburst_gap_s": max_interburst_gap_s,
            }
        ]
    )

    frozen.to_csv(matched_csv, index=False)
    summary.to_csv(summary_csv, index=False)
    audit.to_csv(audit_csv, index=False)

    print(f"[ok] wrote frozen manual matches -> {matched_csv}")
    print(f"[ok] wrote manual summary        -> {summary_csv}")
    print(f"[ok] wrote validation audit      -> {audit_csv}")
    print(
        "[manual] "
        f"accepted_pairs={n_pairs_accepted}/{n_pairs_total}; "
        f"ML exclusions={n_ml_excluded}; "
        f"RPi exclusions={n_rpi_excluded}; "
        f"bursts={n_matched_bursts}; "
        f"timestamp={args.rpi_time_type}"
    )


if __name__ == "__main__":
    main()
