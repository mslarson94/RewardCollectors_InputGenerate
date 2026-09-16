#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
translate_verb_log.py

Translate a *_verb.log into a flat CSV for one target device/IP.

Raw verbose-log triplets are interpreted as:

    ML time, RPi time, Mono time

The RPi clock is treated as the reference wall-clock frame.

Processing:
1. Parse the verbose log.
2. Keep only rows belonging to --ip.
3. Examine the FIRST remaining row for that device.
4. Compute whole-hour corrections independently for:
       ML_Time_Raw_verb   -> ML_Time_verb
       Mono_Time_Raw_verb -> Mono_Time_verb
   using RPi_Time_verb as the reference.
5. Leave RPi_Time_verb unchanged.
6. Apply the same whole-hour corrections to all rows for that device.

Output columns:
    ipAddress
    markNumber
    ML_Time_Raw_verb
    ML_Time_verb
    RPi_Time_verb
    Mono_Time_Raw_verb
    Mono_Time_verb
    ML_Time_Offset_Hours
    Mono_Time_Offset_Hours
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


HEADER_RE = re.compile(
    r"^\[(\d{1,3}(?:\.\d{1,3}){3})\]\s*$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Translate *_verb.log into CSV rows for one target IP, using RPi_Time_verb as the wall-clock reference.")

    parser.add_argument("--verb-log", required=True, help="Path to input *_verb.log")
    parser.add_argument("--out-csv", default="", help="Optional output CSV path; defaults to <verb-log>_verb_full.csv")
    parser.add_argument("--ip", required=True, help="Target device IP, with or without brackets, e.g. 192.168.50.156")
    parser.add_argument("--debug", action="store_true", help="Print parsing and correction diagnostics")

    # Retained so the existing preprocessing wrapper does not break.
    # Corrections are now always computed independently for ML and Mono
    # relative to RPi, so this flag is no longer needed operationally.
    #parser.add_argument("--force-if-ml-rpi-mismatch", action="store_true", help="Retained for backward CLI compatibility. ML and Mono are now independently aligned to RPi.")

    return parser.parse_args()


def tokenize_triplet(line: str) -> Tuple[str, str, str] | None:
    """Parse ML, RPi, Mono time strings from a data line."""

    tokens = [
        token
        for token in re.split(
            r"[,\s]+",
            line.strip(),
        )
        if token
    ]

    if len(tokens) < 3:
        return None

    return (
        tokens[0],
        tokens[1],
        tokens[2],
    )


def _time_of_day_seconds(time_string: str) -> float | None:
    match = re.match(
        r"^(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d+))?$",
        str(time_string).strip(),
    )

    if not match:
        return None

    hour = int(match.group(1))
    minute = int(match.group(2))
    second = int(match.group(3))
    fraction = match.group(4)

    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
        return None

    fractional_seconds = (
        float(f"0.{fraction}")
        if fraction
        else 0.0
    )

    return (
        hour * 3600
        + minute * 60
        + second
        + fractional_seconds
    )


def _whole_hour_offset(reference_time: str, source_time: str) -> int:
    """
    Whole-hour correction needed to place source_time
    in the same wall-clock frame as reference_time.
    """

    reference_s = _time_of_day_seconds(reference_time)
    source_s = _time_of_day_seconds(source_time)

    if reference_s is None or source_s is None:
        raise ValueError(f"Could not determine whole-hour correction from reference={reference_time!r}, source={source_time!r}")

    delta_s = reference_s - source_s

    # Wrap to the nearest equivalent difference within +/- 12 hours.
    delta_s = ((delta_s + 12 * 3600) % (24 * 3600)) - 12 * 3600

    return int(round(delta_s / 3600.0))

def _shift_hours(time_string: str, delta_hours: int) -> str:
    """
    Shift a time-of-day string by whole hours modulo 24.

    Minutes, seconds, and fractional seconds are preserved.
    """

    match = re.match(
        r"^(\d{1,2}):(\d{2}:\d{2}(?:\.\d+)?)$",
        str(time_string).strip(),
    )

    if not match:
        return time_string

    hour = int(match.group(1))
    shifted_hour = (hour + delta_hours) % 24

    return (
        f"{shifted_hour:02d}:"
        f"{match.group(2)}"
    )


def _wrap_hour_diff(difference: int) -> int:
    """
    Normalize a whole-hour difference into [-12, +11].
    """

    return ((difference + 12) % 24) - 12


def _parse_verbose_log(path: Path, debug: bool = False) -> pd.DataFrame:
    """
    Parse the complete verbose log without performing corrections.
    """

    rows: List[Tuple[str,int,str,str,str,]] = []

    mark_counters: Dict[str, int] = {}

    current_ip: str | None = None

    with path.open("r", encoding="utf-8", errors="replace") as file_handle:

        for line_number, raw_line in enumerate(file_handle, start=1):
            line = raw_line.rstrip("\r\n")

            # ---------------------------------------------
            # Header: [IP]
            # ---------------------------------------------

            header_match = HEADER_RE.match(line.strip())

            if header_match:
                ip_literal = f"[{header_match.group(1)}]"

                current_ip = ip_literal

                mark_counters[ip_literal] = (mark_counters.get(ip_literal, 0,) + 1)

                if debug:
                    print(f"[{line_number}] NEW MARK ip={ip_literal} markNumber={mark_counters[ip_literal]}")

                continue

            # ---------------------------------------------
            # Blank line ends current bundle.
            # ---------------------------------------------

            if not line.strip():

                if debug and current_ip is not None:
                    print(f"[{line_number}] END MARK ip={current_ip}")

                current_ip = None
                continue

            # ---------------------------------------------
            # Ignore data outside an IP block.
            # ---------------------------------------------

            if current_ip is None:

                if debug:
                    print(f"[{line_number}] WARN: data outside a mark; skipping: {line!r}")
                continue

            triplet = tokenize_triplet(line)

            if triplet is None:

                if debug:
                    print(f"[{line_number}] WARN: malformed triplet; skipping: {line!r}")
                continue

            ml_time, rpi_time, mono_time = triplet

            rows.append([current_ip, mark_counters[current_ip], ml_time, rpi_time, mono_time])

            if debug:
                print(f"[{line_number}] row -> ip={current_ip} mark={mark_counters[current_ip]} ML={ml_time} RPi={rpi_time} Mono={mono_time}")

    return pd.DataFrame(rows, columns=["ipAddress", "markNumber", "ML_Time_Raw_verb", "RPi_Time_verb", "Mono_Time_Raw_verb"])


def _filter_target_ip(df: pd.DataFrame, target_ip: str,) -> pd.DataFrame:
    """
    Keep only rows for the requested device IP.
    """

    raw_ip = str(target_ip).strip().strip("[]")
    bracketed_ip = f"[{raw_ip}]"
    filtered = df.loc[df["ipAddress"].isin([raw_ip, bracketed_ip])].copy()
    filtered = filtered.reset_index(drop=True)

    if filtered.empty:
        raise ValueError(f"No verbose rows found for IP={raw_ip}")

    return filtered

def _apply_clock_corrections(df: pd.DataFrame, debug: bool = False) -> pd.DataFrame:

    if df.empty:
        raise ValueError("Cannot correct an empty verbose dataframe.")

    out = df.copy()
    first = out.iloc[0]

    ml_offset_hours = _whole_hour_offset(first["RPi_Time_verb"], first["ML_Time_Raw_verb"])

    mono_offset_hours = _whole_hour_offset(first["RPi_Time_verb"], first["Mono_Time_Raw_verb"])

    out["ML_Time_verb"] = out["ML_Time_Raw_verb"].map(lambda value: _shift_hours(value, ml_offset_hours))
    out["Mono_Time_verb"] = out["Mono_Time_Raw_verb"].map(lambda value: _shift_hours(value, mono_offset_hours))
    out["ML_Time_Offset_Hours"] = ml_offset_hours
    out["Mono_Time_Offset_Hours"] = mono_offset_hours
    

    if debug:
        print("[clock correction]")
        print(f"  first ML raw       = {first['ML_Time_Raw_verb']}")
        print(f"  first RPi          = {first['RPi_Time_verb']}")
        print(f"  first Mono raw     = {first['Mono_Time_Raw_verb']}")
        print(f"  ML offset          = {ml_offset_hours:+d} h")
        print(f"  Mono offset        = {mono_offset_hours:+d} h")
        print(f"  first ML corrected = {out.iloc[0]['ML_Time_verb']}")
        print(f"  first Mono corrected = {out.iloc[0]['Mono_Time_verb']}")

    return out

def translate_verb_log(path: Path, target_ip: str, debug: bool = False) -> pd.DataFrame:
    """
    Parse, filter, and normalize one verbose log for one device.
    """

    df = _parse_verbose_log(path, debug=debug)

    if df.empty:
        raise ValueError(f"No parseable verbose rows found in {path}")

    df = _filter_target_ip(df, target_ip)

    if debug:
        print(f"[filter] target IP={target_ip}; remaining rows={len(df)}")

    df = _apply_clock_corrections(df, debug=debug)

    return df

def main() -> None:
    args = parse_args()

    in_path = Path(args.verb_log)

    if not in_path.exists():
        raise FileNotFoundError(in_path)

    if args.out_csv:
        out_path = Path(args.out_csv)
    else:
        out_path = in_path.with_suffix(in_path.suffix + "_verb_full.csv")

    df = translate_verb_log(in_path, target_ip=args.ip, debug=args.debug)

    out_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(out_path, index=False)

    print(f"[ok] wrote {len(df)} rows for IP={args.ip} -> {out_path}")


if __name__ == "__main__":
    main()