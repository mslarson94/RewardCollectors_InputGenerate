#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
extract_rpi_marks2.py — Extract per-IP mark timestamps and apply timezone offset adjustment.
Now includes both raw and adjusted timestamps in the output CSV.
"""

from __future__ import annotations
import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Sequence, Tuple
import pandas as pd

from batchAlignHelpers import (
    _read_logfile,
    _scan_ip_pairs,
    _normalize_ml_stem,
    _to_datetime_on_date,
    _auto_offset_hours,
    _scan_all_ip_pairs,
)

def main() -> None:
    ap = argparse.ArgumentParser(description="Extract per-IP mark timestamps from RPi logs and apply timezone offset")
    ap.add_argument("--log_file", required=True, action="append", help="RPi log file(s); pass multiple times to combine")
    ap.add_argument("--session_date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--device", required=True, help="Device name (e.g., ML2G)")
    ap.add_argument("--device_ip", required=True, help="Device IPv4 (e.g., 192.168.50.109)")
    ap.add_argument("--label", default="RPi", help="RPi source label (BioPac|RNS|RPi)")
    ap.add_argument("--ml_csv_file", default="", help="Optional ML CSV (for auto timezone offset)")
    ap.add_argument("--timezone_offset_hours", default="auto", help="number or 'auto'")
    ap.add_argument("--out_dir", default="", help="Directory to write the marks CSV (default: alongside ML/LOG)")
    ap.add_argument("--strip-ml-suffixes", default="_events_final,_processed", help="Comma-separated suffixes to strip from ML stem for output naming")
    ap.add_argument("--allow_day_rollover", action="store_true", help="If times go backward across files, add 1 day to subsequent times")
    ap.add_argument("--dedupe-sec", type=float, default=0.0, help="Drop marks closer than this many seconds (0 disables)")
    args = ap.parse_args()

    log_paths = [Path(p) for p in args.log_file]
    print('🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛')
    print('starting the extract script')
    print('log paths: ', log_paths)
    for p in log_paths:
        if not p.exists():
            raise FileNotFoundError(p)

    search_ip = f"[{args.device_ip}]"

    combined_rows: List[Tuple[datetime, str, str, int]] = []  # (dt, header, src_name, local_idx)
    last_dt: datetime | None = None
    day_offset = timedelta(0)

    for p in log_paths:
        lines = _read_logfile(p)
        #matches = _scan_ip_pairs(lines, search_ip)  # (pair_idx, header, ts)
        matches = _scan_all_ip_pairs(lines)
        if not matches:
            continue
        dts = _to_datetime_on_date([ts for _, _, _, ts in matches], args.session_date)
        for (ip, k, hdr, _), dt0 in zip(matches, dts):
            dt = dt0 + day_offset
            if args.allow_day_rollover and last_dt is not None and dt < last_dt:
                while dt <= last_dt:
                    dt += timedelta(days=1)
                advance = dt - dt0
                day_offset += advance
            combined_rows.append((ip, dt, hdr, p.name, k))
            last_dt = dt

    if not combined_rows:
        raise SystemExit(f"no lines with {search_ip} followed by a time in any provided log(s)")

    # sort globally (across files)
    combined_rows.sort(key=lambda r: r[-1])

    # optional dedupe window
    if args.dedupe_sec and args.dedupe_sec > 0:
        deduped: List[Tuple[datetime, str, str, int]] = []
        prev_dt: datetime | None = None
        thresh = timedelta(seconds=float(args.dedupe_sec))
        for row in combined_rows:
            if prev_dt is None or (row[1] - prev_dt) > thresh:
                deduped.append(row)
                prev_dt = row[1]
        combined_rows = deduped

    # Build base DataFrame
    df = pd.DataFrame(
        {
            "RPi_Time_Raw_simple": [r[1] for r in combined_rows],
            "Device": args.device,
            "DeviceIP": [r[0] for r in combined_rows],
            "RPi_Source": args.label,
            "LogFile": [r[3] for r in combined_rows],
            "LogLineText": [r[2] for r in combined_rows],
            "LogPairIndex": [r[4] for r in combined_rows],
            "markNumber": [(r[4] +1) for r in combined_rows], # Check this: Is the operation r[3] + 1 okay to do? I just want to add 1 to the value of each logPairIndex number to generate the actual markNumber value
        }
    )

    # --- Timezone offset handling ---
    tz_offset_hours = 0.0
    if args.timezone_offset_hours.strip().lower() == "auto":
        if not args.ml_csv_file or not Path(args.ml_csv_file).exists():
            raise ValueError("auto timezone offset requires --ml_csv_file")
        ml_df = pd.read_csv(args.ml_csv_file)
        ml_times = pd.to_datetime(ml_df["mLTimestamp"], errors="coerce").dropna()
        if ml_times.empty:
            raise ValueError("No valid timestamps found in ML CSV for auto offset")
        est = _auto_offset_hours(ml_times, pd.to_datetime(df["RPi_Time_Raw_simple"], errors="coerce"))
        tz_offset_hours = float(est)
    else:
        tz_offset_hours = float(args.timezone_offset_hours)

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
        base = Path(log_paths[0]).stem
        out_dir = Path(args.out_dir) if args.out_dir else Path(log_paths[0]).parent

    #out_dir = out_dir / f"{args.label}"
    #out_dir.mkdir(parents=True, exist_ok=True)

    indivdual_ip_df = df[df["DeviceIP"] == args.device_ip]
    out_csv = out_dir / f"{base}_{args.label}_RPi_simple12.csv"
    indivdual_ip_df.to_csv(out_csv, index=False)
    out_csv_all = out_dir / f"{base}_{args.label}_allIP_RPi_simple.csv"
    df.to_csv(out_csv_all, index=False)
    print(f"[info] Wrote {len(df)} RPi marks → {out_csv}")
    print(f"[info] Timezone offset applied: {tz_offset_hours:.2f} hours")


if __name__ == "__main__":
    main()
