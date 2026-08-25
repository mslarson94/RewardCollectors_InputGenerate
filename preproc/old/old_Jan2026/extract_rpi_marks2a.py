#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
extract_rpi_marks2.py — Robust multi-file extractor for per-IP mark timestamps.

Key features vs. v1:
  • Accepts multiple --log_file values (pass flag repeatedly)
  • Robust scan: finds lines containing "[<device_ip>]" and uses the next
    non-empty line as the timestamp (no global even/odd pairing assumption)
  • Tolerates banners, restarts, extra lines, mixed segments
  • Optional midnight rollover handling (--allow_day_rollover)
  • Optional deduplication window (--dedupe-sec) to collapse near-duplicates
  • Output rows include source filename and local pair indices

Output CSV columns:
  RPi_Timestamp          (datetime64[ns])  — built on --session_date (+rollover)
  Device                 (string)
  DeviceIP               (string)
  RPi_Source             (string)
  LogFile                (basename of source log)
  LogLineText            (the matching header line containing the IP)
  LogPairIndex           (0-based index among matches within that file)

Filename pattern (recommended):
  <ml_root or log_base>_<device>_<label>_RPiMarks.csv

Examples:
  python extract_rpi_marks2.py \
    --log_file /.../RNS_RPi/2025-03-17_14_13_24_417382511.log \
    --log_file /.../RNS_RPi/2025-03-17_14_41_04_514886047.log \
    --log_file /.../RNS_RPi/2025-03-17_14_45_44_099056293.log \
    --session_date 2025-03-17 \
    --device ML2G \
    --device_ip 192.168.50.128 \
    --label RNS \
    --ml_csv_file /.../augmented/ObsReward_A_03_17_2025_14_16_events_final.csv \
    --allow_day_rollover --dedupe-sec 0.05
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Sequence, Tuple

import pandas as pd

# -------------------------------
# Utilities
# -------------------------------

def _read_logfile(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.rstrip("\n") for ln in f]
    # Keep all lines; downstream filters will ignore junk safely
    return lines


def _fix_fraction_colon(ts: str) -> str:
    m = re.fullmatch(r"(\d{2}:\d{2}:\d{2}):(\d+)", str(ts).strip())
    return f"{m.group(1)}.{m.group(2)}" if m else str(ts).strip()


def _to_datetime_on_date(times: Sequence[str], session_date: str) -> List[datetime]:
    out: List[datetime] = []
    for t in times:
        t = _fix_fraction_colon(t)
        parsed = None
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(f"{session_date} {t}", fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            raise ValueError(f"Unparseable time: '{t}' (session_date={session_date})")
        out.append(parsed)
    return out


def _normalize_ml_stem(stem: str, suffixes: Sequence[str]) -> str:
    base = stem
    changed = True
    while changed:
        changed = False
        for s in suffixes:
            if s and base.endswith(s):
                base = base[: -len(s)]
                changed = True
    return re.sub(r"[_-]+$", "", base)


# -------------------------------
# Core scanning
# -------------------------------

def _scan_ip_pairs(lines: List[str], search_ip: str) -> List[Tuple[int, str, str]]:
    """Return list of (pair_index, header_line, ts_line) for the given IP.
    We consider any line containing the exact substring `search_ip` (e.g., "[192.168.50.128]")
    as a header; the very next non-empty line must be a time.
    """
    time_re = re.compile(r"^\d{2}:\d{2}:\d{2}(?::\d+|\.\d+)?$")
    matches: List[Tuple[int, str, str]] = []
    pair_idx = 0
    i = 0
    N = len(lines)
    while i < N:
        header = lines[i]
        if search_ip in header:
            # find next non-empty line
            j = i + 1
            while j < N and (lines[j] is None or lines[j].strip() == ""):
                j += 1
            if j < N:
                ts_line = _fix_fraction_colon(lines[j].strip())
                if time_re.match(ts_line):
                    matches.append((pair_idx, header.strip(), ts_line))
                    pair_idx += 1
                    i = j + 1
                    continue
        i += 1
    return matches


# -------------------------------
# Main
# -------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Extract per-IP mark timestamps from one or more RPi logs")
    ap.add_argument("--log_file", required=True, action="append", help="RPi log file(s); pass multiple times to combine")
    ap.add_argument("--session_date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--device", required=True, help="Device name (e.g., ML2G)")
    ap.add_argument("--device_ip", required=True, help="Device IPv4 (e.g., 192.168.50.128)")
    ap.add_argument("--label", default="RPi", help="RPi source label (BioPac|RNS|RPi)")
    ap.add_argument("--ml_csv_file", default="", help="Optional ML CSV to borrow basename for output naming")
    ap.add_argument("--out_dir", default="", help="Directory to write the marks CSV (default: alongside ML/LOG)")
    ap.add_argument("--strip-ml-suffixes", default="_events_final,_processed", help="Comma-separated suffixes to strip from ML stem for output naming")
    ap.add_argument("--allow_day_rollover", action="store_true", help="If times go backward across files, add 1 day to subsequent times")
    ap.add_argument("--dedupe-sec", type=float, default=0.0, help="Drop marks closer than this many seconds (0 disables)")

    args = ap.parse_args()

    log_paths = [Path(p) for p in args.log_file]
    for p in log_paths:
        if not p.exists():
            raise FileNotFoundError(p)

    search_ip = f"[{args.device_ip}]"

    # Scan each file in the provided order
    combined_rows: List[Tuple[datetime, str, str, int]] = []  # (dt, header, src_name, local_idx)
    last_dt: datetime | None = None
    day_offset = timedelta(0)

    for p in log_paths:
        lines = _read_logfile(p)
        matches = _scan_ip_pairs(lines, search_ip)  # (pair_idx, header, ts)
        if not matches:
            # continue; we simply skip files with no matches for this IP
            continue
        # Parse times for this file
        dts = _to_datetime_on_date([ts for _, _, ts in matches], args.session_date)
        for (k, hdr, _), dt0 in zip(matches, dts):
            dt = dt0 + day_offset
            if args.allow_day_rollover and last_dt is not None and dt < last_dt:
                # bump a day (or more, though usually 1)
                while dt <= last_dt:
                    dt += timedelta(days=1)
                # also advance the running offset for the rest of this file and afterwards
                advance = dt - dt0
                day_offset += advance
            combined_rows.append((dt, hdr, p.name, k))
            last_dt = dt

    if not combined_rows:
        raise SystemExit(f"no lines with {search_ip} followed by a time in any provided log(s)")

    # Sort by timestamp to ensure monotonic sequence
    combined_rows.sort(key=lambda r: r[0])

    # Optional de-duplication within a short window
    if args.dedupe_sec and args.dedupe_sec > 0:
        deduped: List[Tuple[datetime, str, str, int]] = []
        prev_dt: datetime | None = None
        thresh = timedelta(seconds=float(args.dedupe_sec))
        for row in combined_rows:
            if prev_dt is None or (row[0] - prev_dt) > thresh:
                deduped.append(row)
                prev_dt = row[0]
        combined_rows = deduped

    # Build DataFrame
    df = pd.DataFrame(
        {
            "RPi_Timestamp": [r[0] for r in combined_rows],
            "Device": args.device,
            "DeviceIP": args.device_ip,
            "RPi_Source": args.label,
            "LogFile": [r[2] for r in combined_rows],
            "LogLineText": [r[1] for r in combined_rows],
            "LogPairIndex": [r[3] for r in combined_rows],
        }
    )

    # Naming
    suffixes = [s for s in (args.strip_ml_suffixes or "").split(",") if s]
    if args.ml_csv_file:
        ml_stem = Path(args.ml_csv_file).stem
        base = _normalize_ml_stem(ml_stem, suffixes)
        out_dir = Path(args.out_dir) if args.out_dir else Path(args.ml_csv_file).parent
    else:
        # derive from first log
        base = Path(log_paths[0]).stem
        out_dir = Path(args.out_dir) if args.out_dir else Path(log_paths[0]).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    out_csv = out_dir / f"{base}_{args.device}_{args.label}_RPiMarks.csv"
    df.to_csv(out_csv, index=False)
    print(out_csv)


if __name__ == "__main__":
    main()
