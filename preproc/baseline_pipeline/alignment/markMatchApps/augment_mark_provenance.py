#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
augment_mark_provenance.py

Augment existing mark_matches.csv / mark_singles.csv files with authoritative
RPi timestamps from the corresponding *_RPi_unified.csv file.

IMPORTANT:
- This script DOES NOT rematch events and RPi marks.
- Existing pair_id / events_mark_id / rpi_mark_id relationships are preserved.
- Existing event/RPi timestamps and delta_seconds are preserved as *_orig.
- Authoritative RPi timestamps are pulled from the unified CSV using each
  RPi single's ordinal.
- Three new deltas are calculated:
    delta_seconds_rpi_time_simple
    delta_seconds_rpi_time_verb
    delta_seconds_rpi_time_unified

Delta convention:
    RPi timestamp - event timestamp

Expected RPi unified columns:
    RPi_Time_simple
    RPi_Time_verb
    RPi_Time_unified

Recommended unified mapping:
    mark_singles ordinal == zero-based row position in RPi_unified
    markNumber            == ordinal + 1   (validated when markNumber exists)
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


RPI_TIME_COLS = [
    "RPi_Time_simple",
    "RPi_Time_verb",
    "RPi_Time_unified",
]

MATCH_REQUIRED = [
    "pair_id",
    "events_mark_id",
    "rpi_mark_id",
    "events_time",
    "rpi_time",
    "delta_seconds",
]

SINGLES_REQUIRED = [
    "stream",
    "mark_id",
    "ordinal",
    "mark_time",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=("Add simple/verb/unified RPi timestamp provenance and per-source delta_seconds columns to existing mark match files without rematching."))

    p.add_argument("--mark-matches", required=True, help="Existing *_mark_matches.csv.")
    p.add_argument("--mark-singles", required=True, help="Existing *_mark_singles.csv.")
    p.add_argument("--rpi-unified", default="", help="Authoritative *_RPi_unified.csv. If omitted, use the rpi_file stored in mark_matches.csv.")
    p.add_argument("--out-dir", required=True, help="Output directory.")
    p.add_argument("--suffix", default="_provenance", help="Suffix added before .csv. Default: _provenance. Use an empty string only if you intentionally want the original names.")
    p.add_argument("--allow-marknumber-mismatch", action="store_true", help="Do not fail when unified markNumber != singles ordinal + 1. Not recommended unless you have independently validated the mapping.")
    return p.parse_args()


def require_columns(df: pd.DataFrame, columns: list[str], source_name: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(f"{source_name} is missing required column(s): {missing}")


def parse_datetime_series(series: pd.Series, column_name: str, source_name: str, *, require_all: bool = False) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    if require_all:
        bad_mask = series.notna() & series.astype(str).str.strip().ne("") & parsed.isna()
        if bad_mask.any():
            examples = series.loc[bad_mask].astype(str).head(10).tolist()
            raise ValueError(f"Could not parse {int(bad_mask.sum())} value(s) in {source_name}:{column_name}. Examples: {examples}")
    return parsed


def fmt_timestamp(value: object) -> str:
    """Write a stable, full-precision timestamp string. Empty/NaT -> empty string."""
    if pd.isna(value):
        return ""

    ts = pd.Timestamp(value)
    return ts.strftime("%Y-%m-%d %H:%M:%S.%f")


def delta_seconds(rpi_time: object, event_time: object) -> float:
    if pd.isna(rpi_time) or pd.isna(event_time):
        return np.nan
    return (pd.Timestamp(rpi_time) - pd.Timestamp(event_time)).total_seconds()


def resolve_rpi_unified(args: argparse.Namespace, matches: pd.DataFrame) -> Path:
    if args.rpi_unified:
        path = Path(args.rpi_unified).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"Specified --rpi-unified does not exist: {path}")
        return path

    if "rpi_file" not in matches.columns:
        raise KeyError("--rpi-unified was not provided and mark_matches.csv does not contain an rpi_file column.")

    candidates = matches["rpi_file"].dropna().astype(str).str.strip()
    candidates = candidates[candidates.ne("")].unique().tolist()

    if len(candidates) != 1:
        raise ValueError(f"Could not infer exactly one RPi unified file from rpi_file. Found {len(candidates)} unique values: {candidates}")

    path = Path(candidates[0]).expanduser()

    if not path.exists():
        raise FileNotFoundError(f"The rpi_file stored in mark_matches.csv does not exist on this machine:\n{path}\nPass the correct file explicitly with --rpi-unified.")
    return path


def build_rpi_lookup(singles: pd.DataFrame, unified: pd.DataFrame, *, allow_marknumber_mismatch: bool) -> pd.DataFrame:
    """
    Build one row per RPi mark_id containing authoritative RPi timestamps.

    Mapping:
        mark_singles[stream == 'rpi'].ordinal
            -> zero-based row position in unified CSV

    If unified has markNumber, also validate:
        markNumber == ordinal + 1
    """

    rpi_singles = singles.loc[singles["stream"].astype(str).str.strip().str.lower().eq("rpi")].copy()

    if rpi_singles.empty:
        raise ValueError("mark_singles.csv contains no rows where stream == 'rpi'.")

    rpi_singles["ordinal_numeric"] = pd.to_numeric(rpi_singles["ordinal"], errors="coerce")

    if rpi_singles["ordinal_numeric"].isna().any():
        bad = rpi_singles.loc[rpi_singles["ordinal_numeric"].isna(), ["mark_id", "ordinal"]]
        raise ValueError("Some RPi singles have non-numeric ordinals:\n" + bad.to_string(index=False))

    rpi_singles["ordinal_numeric"] = rpi_singles["ordinal_numeric"].astype(int)

    if rpi_singles["mark_id"].duplicated().any():
        dupes = rpi_singles.loc[
            rpi_singles["mark_id"].duplicated(keep=False), ["mark_id", "ordinal"]]
        raise ValueError("Duplicate RPi mark_id values found in singles:\n" + dupes.to_string(index=False))

    if rpi_singles["ordinal_numeric"].duplicated().any():
        dupes = rpi_singles.loc[
            rpi_singles["ordinal_numeric"].duplicated(keep=False),
            ["mark_id", "ordinal"],
        ]
        raise ValueError("Duplicate RPi ordinal values found in singles:\n" + dupes.to_string(index=False))

    min_ord = int(rpi_singles["ordinal_numeric"].min())
    max_ord = int(rpi_singles["ordinal_numeric"].max())

    if min_ord < 0 or max_ord >= len(unified):
        raise IndexError(f"RPi singles ordinal is outside the unified CSV row range. Singles ordinals span {min_ord}..{max_ord}; unified has {len(unified)} rows.")

    lookup_rows: list[dict[str, object]] = []

    for _, single_row in rpi_singles.iterrows():
        ordinal = int(single_row["ordinal_numeric"])
        unified_row = unified.iloc[ordinal]

        if ("markNumber" in unified.columns and pd.notna(unified_row["markNumber"])):
            mark_number = pd.to_numeric(pd.Series([unified_row["markNumber"]]), errors="coerce").iloc[0]

            if pd.notna(mark_number):
                expected = ordinal + 1

                if int(mark_number) != expected:
                    msg = f"Mapping validation failed for {single_row['mark_id']}: singles ordinal={ordinal}, expected unified markNumber={expected}, but found markNumber={mark_number}."

                    if not allow_marknumber_mismatch:
                        raise ValueError(msg + "\nIf this mapping is nevertheless known to be correct, rerun with --allow-marknumber-mismatch.")
                    else:
                        print(f"[warn] {msg}")

        row: dict[str, object] = {
            "rpi_mark_id": str(single_row["mark_id"]),
            "rpi_ordinal": ordinal,
        }

        if "markNumber" in unified.columns:
            row["rpi_markNumber"] = unified_row.get("markNumber", pd.NA)

        for col in RPI_TIME_COLS:
            row[f"{col}_parsed"] = pd.to_datetime(unified_row[col], errors="coerce")
            row[f"{col}_raw"] = unified_row[col]

        lookup_rows.append(row)

    lookup = pd.DataFrame(lookup_rows)

    return lookup


def augment_matches(matches: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    """
    Preserve existing pairing and metadata.

    Rename old timestamps/delta:
        events_time   -> events_time_orig
        rpi_time      -> rpi_time_orig
        delta_seconds -> delta_seconds_orig

    Add authoritative RPi timestamps:
        rpi_time_simple_orig
        rpi_time_verb_orig
        rpi_time_unified_orig

    Add deltas against events_time_orig.
    """

    out = matches.copy()

    out = out.rename(
        columns={
            "events_time": "events_time",
            "rpi_time": "rpi_time_orig",
            "delta_seconds": "delta_seconds_orig",
        }
    )

    if out["rpi_mark_id"].duplicated().any():
        # Multiple matches to the same RPi mark would violate the expected
        # one-to-one review structure.
        dupes = out.loc[out["rpi_mark_id"].duplicated(keep=False), ["pair_id", "rpi_mark_id"]]
        raise ValueError("mark_matches.csv contains duplicate rpi_mark_id assignments:\n" + dupes.to_string(index=False))

    out = out.merge(lookup, how="left", on="rpi_mark_id", validate="many_to_one")

    missing_lookup = out["rpi_ordinal"].isna()

    if missing_lookup.any():
        bad = out.loc[missing_lookup, ["pair_id", "rpi_mark_id"]]
        raise ValueError("Some matched rpi_mark_id values were not found in mark_singles.csv:\n" + bad.to_string(index=False))

    event_dt = parse_datetime_series(out["events_time"], "events_time", "mark_matches.csv", require_all=True)

    out["RPi_Time_simple"] = out["RPi_Time_simple_parsed"].map(fmt_timestamp)
    out["RPi_Time_verb"] = out["RPi_Time_verb_parsed"].map(fmt_timestamp)
    out["RPi_Time_unified"] = out["RPi_Time_unified_parsed"].map(fmt_timestamp)
    out["events_time"] = event_dt.map(fmt_timestamp)

    # Preserve the original rpi_time value as faithfully as possible.
    # For valid datetimes, normalize to full precision.
    old_rpi_dt = pd.to_datetime(out["rpi_time_orig"], errors="coerce")

    valid_old_rpi = old_rpi_dt.notna()

    out.loc[valid_old_rpi, "rpi_time_orig"] = old_rpi_dt.loc[valid_old_rpi].map(fmt_timestamp)

    out["delta_seconds_RPi_Time_simple"] = [delta_seconds(rpi, event) for rpi, event in zip(out["RPi_Time_simple_parsed"], event_dt)]

    out["delta_seconds_RPi_Time_verb"] = [delta_seconds(rpi, event) for rpi, event in zip(out["RPi_Time_verb_parsed"], event_dt)]

    out["delta_seconds_RPi_Time_unified"] = [delta_seconds(rpi, event) for rpi, event in zip(out["RPi_Time_unified_parsed"], event_dt)]

    # Temporary/helper columns are not part of final file.
    drop_cols = [
        "RPi_Time_simple_parsed",
        "RPi_Time_verb_parsed",
        "RPi_Time_unified_parsed",
        "RPi_Time_simple_raw",
        "RPi_Time_verb_raw",
        "RPi_Time_unified_raw",
    ]
    out = out.drop(columns=[c for c in drop_cols if c in out.columns])

    # Put provenance columns near the original timestamps.
    preferred_order = [
        "pair_id",
        "events_file",
        "rpi_file",
        "label",
        "block",
        "events_mark_id",
        "rpi_mark_id",
        "rpi_ordinal",
        "rpi_markNumber",
        "events_time",
        "rpi_time_orig",
        "delta_seconds_orig",
        "RPi_Time_simple",
        "RPi_Time_verb",
        "RPi_Time_unified",
        "delta_seconds_RPi_Time_simple",
        "delta_seconds_RPi_Time_verb",
        "delta_seconds_RPi_Time_unified",
        "exclude",
        "reason",
        "reviewed_at",
    ]

    ordered = [
        c for c in preferred_order
        if c in out.columns
    ] + [
        c for c in out.columns
        if c not in preferred_order
    ]

    return out.loc[:, ordered]


def augment_singles(singles: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    """
    Preserve all singles rows.

    mark_time -> mark_time_orig

    For stream == rpi:
        attach authoritative simple / verb / unified times.

    For stream == events:
        RPi provenance fields remain blank.
    """

    out = singles.copy()

    out = out.rename(columns={"mark_time": "mark_time_orig"})

    rpi_lookup = lookup.rename(columns={"rpi_mark_id": "mark_id"})

    out = out.merge(rpi_lookup, how="left", on="mark_id", validate="many_to_one")

    is_rpi = out["stream"].astype(str).str.strip().str.lower().eq("rpi")

    missing_rpi = is_rpi & out["rpi_ordinal"].isna()
    

    if missing_rpi.any():
        bad = out.loc[missing_rpi, ["stream", "mark_id", "ordinal"]]
        raise ValueError("Some RPi singles could not be linked to the unified CSV:\n" + bad.to_string(index=False))

    # Preserve event mark timestamps as-is except normalize valid complete
    # datetime strings to full precision.
    old_mark_dt = pd.to_datetime(out["mark_time_orig"], errors="coerce",)
    valid_mark = old_mark_dt.notna()

    out.loc[valid_mark, "mark_time_orig"] = old_mark_dt.loc[valid_mark].map(fmt_timestamp)

    out["RPi_Time_simple"] = ""
    out["RPi_Time_verb"] = ""
    out["RPi_Time_unified"] = ""

    out.loc[is_rpi, "RPi_Time_simple"] = out.loc[is_rpi,"RPi_Time_simple_parsed"].map(fmt_timestamp)

    out.loc[is_rpi, "RPi_Time_verb"] = out.loc[is_rpi, "RPi_Time_verb_parsed"].map(fmt_timestamp)

    out.loc[is_rpi, "RPi_Time_unified"] = out.loc[is_rpi, "RPi_Time_unified_parsed",].map(fmt_timestamp)

    drop_cols = [
        "RPi_Time_simple_parsed",
        "RPi_Time_verb_parsed",
        "RPi_Time_unified_parsed",
        "RPi_Time_simple_raw",
        "RPi_Time_verb_raw",
        "RPi_Time_unified_raw",
    ]

    out = out.drop(columns=[c for c in drop_cols if c in out.columns])

    preferred_order = [
        "events_file",
        "rpi_file",
        "label",
        "stream",
        "mark_id",
        "ordinal",
        "rpi_ordinal",
        "rpi_markNumber",
        "mark_time_orig",
        "RPi_Time_simple",
        "RPi_Time_verb",
        "RPi_Time_unified",
        "block",
        "matched_pair_id",
        "exclude",
        "reason",
        "reviewed_at",
    ]

    ordered = [
        c for c in preferred_order
        if c in out.columns
    ] + [
        c for c in out.columns
        if c not in preferred_order
    ]

    return out.loc[:, ordered]


def output_path(original: Path, out_dir: Path, suffix: str) -> Path:
    return out_dir / f"{original.stem}{suffix}{original.suffix}"


def main() -> None:
    args = parse_args()

    matches_path = Path(args.mark_matches).expanduser()
    singles_path = Path(args.mark_singles).expanduser()
    out_dir = Path(args.out_dir).expanduser()

    if not matches_path.exists():
        raise FileNotFoundError(matches_path)

    if not singles_path.exists():
        raise FileNotFoundError(singles_path)

    out_dir.mkdir(parents=True, exist_ok=True)

    matches = pd.read_csv(matches_path, dtype="string")

    singles = pd.read_csv(singles_path, dtype="string")

    require_columns(matches, MATCH_REQUIRED, matches_path.name)
    require_columns(singles, SINGLES_REQUIRED, singles_path.name)

    unified_path = resolve_rpi_unified(args, matches)

    unified = pd.read_csv(unified_path, dtype="string")

    require_columns(unified, RPI_TIME_COLS, unified_path.name)

    lookup = build_rpi_lookup(singles, unified, allow_marknumber_mismatch=args.allow_marknumber_mismatch,)

    augmented_matches = augment_matches(matches, lookup)
    augmented_singles = augment_singles(singles, lookup)

    matches_out = output_path(matches_path, out_dir, args.suffix)
    singles_out = output_path(singles_path, out_dir, args.suffix)

    augmented_matches.to_csv(matches_out, index=False)
    augmented_singles.to_csv(singles_out, index=False)

    print("\n=== Mark provenance augmentation ===")
    print(f"Original matches : {matches_path}")
    print(f"Original singles : {singles_path}")
    print(f"RPi source       : {unified_path}")
    print(f"Unified rows     : {len(unified)}")
    print(f"RPi singles      : {len(lookup)}")
    print(f"Matched pairs    : {len(augmented_matches)}")

    print("\nDelta convention: RPi timestamp - event timestamp")

    delta_cols = ["delta_seconds_RPi_Time_simple", "delta_seconds_RPi_Time_verb", "delta_seconds_RPi_Time_unified"]

    for col in ["delta_seconds_RPi_Time_simple", "delta_seconds_RPi_Time_verb", "delta_seconds_RPi_Time_unified"]:
        vals = pd.to_numeric(augmented_matches[col], errors="coerce").dropna()

        if vals.empty:
            print(f"{col}: no valid values")
        else:
            print(
                f"{col}: "
                f"n={len(vals)}, "
                f"mean={vals.mean():.6f}s, "
                f"median={vals.median():.6f}s, "
                f"max_abs={vals.abs().max():.6f}s"
            )

    print(f"\n[ok] matches -> {matches_out}")
    print(f"[ok] singles -> {singles_out}")


if __name__ == "__main__":
    main()
