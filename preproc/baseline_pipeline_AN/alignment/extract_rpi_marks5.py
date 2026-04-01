#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
extract_rpi_marks4_from_csv.py — Extract RPi mark timestamps from pre-parsed CSVs
and apply timezone offset adjustment.

This version works directly on CSV files produced by read_rpi_logs_to_csv_per_file.py
instead of scanning the raw .log files.

Inputs (per CSV row):
    RPi_Time_Raw_simple  - time-of-day string, e.g. "08:50:01.148287"
    DeviceIP             - e.g. "192.168.50.128"
    EnclosingFolder      - folder name containing the log (for info)
    LogFileName          - original .log file name
    markNumber           - per-IP mark index (1-based) within that log

Outputs:
    - Per-IP CSV filtered to --device_ip
    - All-IP CSV containing all IPs in the provided CSV(s)

Both outputs include:
    RPi_Time_Raw_simple  - datetime on the given --session_date (with rollover if enabled)
    RPi_Time_simple      - timezone-adjusted datetime
    Timezone_Offset_Hours
    Device, DeviceIP, RPi_Source, LogFile, LogLineText, LogPairIndex, markNumber
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from batchAlignHelpers import (
    _to_datetime_on_date,
    _auto_offset_hours,
    _normalize_ml_stem,
    _round_offset_hours,
)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Extract RPi marks from pre-parsed CSVs and apply timezone offset.")

    ap.add_argument(
        "--rpi_csv_file", required=True, action="append", help="RPi CSV file(s) produced by read_rpi_logs_to_csv_per_file.py; pass multiple times to combine.",)
    ap.add_argument(
        "--session_date", required=True, help="Session date as YYYY-MM-DD (used to attach a date to the RPi times).",)
    ap.add_argument(
        "--device", required=True,
        help="Device name (e.g., ML2G).",)
    ap.add_argument(
        "--device_ip", required=True, help="Device IPv4 to extract a single-IP CSV for (e.g., 192.168.50.109).",)
    ap.add_argument(
        "--label", default="RPi", help="RPi source label (BioPac|RNS|RPi).",)
    ap.add_argument(
        "--ml_csv_file", default="", help="Optional ML CSV (for auto timezone offset).",)
    ap.add_argument(
        "--timezone_offset_hours", default="auto", help="Timezone offset in hours, or 'auto' to estimate from ML CSV.",)
    ap.add_argument(
        "--out_dir", default="", help="Directory to write the marks CSV(s) (default: alongside ML/CSV input).",)
    ap.add_argument(
        "--strip-ml-suffixes", default="_events_final,_processed", help="Comma-separated suffixes to strip from ML stem for output naming.",)
    ap.add_argument(
        "--allow_day_rollover", action="store_true", help="If times go backward across files, add 1 day to subsequent times.",)
    ap.add_argument(
        "--dedupe-sec", type=float, default=0.0, help="Drop marks closer than this many seconds (0 disables, global across IPs).",)

    args = ap.parse_args()

    csv_paths = [Path(p) for p in args.rpi_csv_file]
    print("🐦‍⬛ starting CSV-based extract script")
    print("CSV paths:", csv_paths)

    for p in csv_paths:
        if not p.exists():
            raise FileNotFoundError(p)

    combined_rows: List[Tuple[str, datetime, str, str, int]] = []
    # row structure: (DeviceIP, dt, raw_time_str, log_file_name, markNumber)

    last_dt: datetime | None = None
    day_offset = timedelta(0)

    for p in csv_paths:
        df_in = pd.read_csv(p)

        # Basic column sanity check
        for col in ("RPi_Time_Raw_simple", "DeviceIP", "EnclosingFolder", "LogFileName", "markNumber"):
            if col not in df_in.columns:
                raise ValueError(f"Expected column '{col}' in {p}")

        times_str = df_in["RPi_Time_Raw_simple"].astype(str).tolist()
        dts0 = _to_datetime_on_date(times_str, args.session_date)  # attach date

        ips = df_in["DeviceIP"].astype(str).tolist()
        log_names = df_in["LogFileName"].astype(str).tolist()
        mark_nums = df_in["markNumber"].astype(int).tolist()

        for raw_time_str, dt0, ip, log_name, mark_num in zip(
            times_str, dts0, ips, log_names, mark_nums
        ):
            dt = dt0 + day_offset

            # Handle day rollover across files if enabled
            if args.allow_day_rollover and last_dt is not None and dt < last_dt:
                while dt <= last_dt:
                    dt += timedelta(days=1)
                advance = dt - dt0
                day_offset += advance

            combined_rows.append((ip, dt, raw_time_str, log_name, mark_num))
            last_dt = dt

    if not combined_rows:
        raise SystemExit("No rows found in provided RPi CSV file(s).")

    # Sort globally by datetime
    combined_rows.sort(key=lambda r: r[1])

    # Optional dedupe (global across IPs) on datetime
    if args.dedupe_sec and args.dedupe_sec > 0:
        deduped: List[Tuple[str, datetime, str, str, int]] = []
        prev_dt: datetime | None = None
        thresh = timedelta(seconds=float(args.dedupe_sec))

        for row in combined_rows:
            dt = row[1]
            if prev_dt is None or (dt - prev_dt) > thresh:
                deduped.append(row)
                prev_dt = dt

        combined_rows = deduped

    # Build base DataFrame
    df = pd.DataFrame(
        {
            "RPi_Time_Raw_simple": [r[1] for r in combined_rows],  # datetime
            "Raw_Time_String": [r[2] for r in combined_rows],      # original HH:MM:SS.ffffff
            "Device": args.device,
            "DeviceIP": [r[0] for r in combined_rows],
            "RPi_Source": args.label,
            "LogFile": [r[3] for r in combined_rows],
            # We no longer have the original log line, so keep this empty for compatibility
            "LogLineText": ["" for _ in combined_rows],
            # MarkNumber comes from CSV; LogPairIndex is 0-based version of it
            "LogPairIndex": [int(r[4]) - 1 for r in combined_rows],
            "markNumber": [int(r[4]) for r in combined_rows],
        }
    )

    # --- Timezone offset handling ---
    tz_offset_hours = 0.0
    if args.timezone_offset_hours.strip().lower() == "auto":
        if not args.ml_csv_file or not Path(args.ml_csv_file).exists():
            raise ValueError("auto timezone offset requires --ml_csv_file")
        ml_df = pd.read_csv(args.ml_csv_file)
        if "mLTimestamp" not in ml_df.columns:
            raise ValueError("ML CSV must contain 'mLTimestamp' column for auto offset")
        ml_times = pd.to_datetime(ml_df["mLTimestamp"], errors="coerce").dropna()
        if ml_times.empty:
            raise ValueError("No valid timestamps found in ML CSV for auto offset")
        est = _auto_offset_hours(
            ml_times,
            pd.to_datetime(df["RPi_Time_Raw_simple"], errors="coerce"),
        )
        raw_offset = float(est)
    else:
        raw_offset = float(args.timezone_offset_hours)

    tz_offset_hours = _round_offset_hours(raw_offset, step=1.0)  # nearest whole hour
    tz_offset = timedelta(hours=tz_offset_hours)
    df["RPi_Time_simple"] = df["RPi_Time_Raw_simple"] + pd.to_timedelta(tz_offset)
    df["Timezone_Offset_Hours"] = tz_offset_hours

    # --- Naming and save ---
    suffixes = [s for s in (args.strip_ml_suffixes or "").split(",") if s]
    if args.ml_csv_file:
        ml_stem = Path(args.ml_csv_file).stem
        base = _normalize_ml_stem(ml_stem, suffixes)
        out_dir = Path(args.out_dir) if args.out_dir else Path(args.ml_csv_file).parent
    else:
        base = Path(csv_paths[0]).stem
        out_dir = Path(args.out_dir) if args.out_dir else Path(csv_paths[0]).parent

    out_dir.mkdir(parents=True, exist_ok=True)

    # Per-IP CSV (for the requested --device_ip)
    individual_ip_df = df[df["DeviceIP"] == args.device_ip].copy()
    out_csv_ip = out_dir / f"{base}_{args.label}_RPi_simple.csv"
    individual_ip_df.to_csv(out_csv_ip, index=False)

    # All-IP CSV
    out_csv_all = out_dir / f"{base}_{args.label}_allIP_RPi_simple.csv"
    df.to_csv(out_csv_all, index=False)

    print(f"[info] Wrote {len(individual_ip_df)} rows for DeviceIP={args.device_ip} → {out_csv_ip}")
    print(f"[info] Wrote {len(df)} rows (all IPs) → {out_csv_all}")
    print(f"[info] Timezone offset applied: {tz_offset_hours:.2f} hours")


if __name__ == "__main__":
    main()
