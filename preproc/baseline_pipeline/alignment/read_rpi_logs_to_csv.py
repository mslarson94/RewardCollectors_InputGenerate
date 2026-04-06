#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
read_rpi_logs_to_csv_per_file.py

Read all *.log files in a directory, each of the form:

    [192.168.50.128]
    08:50:01.148287

    [192.168.50.128]
    08:50:02.318015
    ...

and for EACH .log file write a separate CSV with the same base name:

    <log_stem>.csv

Columns match logFileSample.csv:

    RPi_Time_Raw_simple,DeviceIP,EnclosingFolder,LogFileName,markNumber

Notes:
- Iterates over *all* files in the given directory that end with .log (non-recursive).
- markNumber is counted per IP address *within each log file* (resets for each new file).
"""

from __future__ import annotations
import argparse
import re
from pathlib import Path
from typing import List, Tuple, Dict

import pandas as pd


IP_RE = re.compile(r"\[(\d{1,3}(?:\.\d{1,3}){3})\]")


def parse_log_file(path: Path) -> List[Tuple[str, str, str, str, int]]:
    """
    Parse a single log file and return rows:

        (time_str, ip, enclosing_folder, log_file_name, mark_number)

    markNumber is per-IP and resets for each file.
    """
    rows: List[Tuple[str, str, str, str, int]] = []
    ip_counters: Dict[str, int] = {}

    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    n = len(lines)

    enclosing_folder = path.parent.name if path.parent.name else ""
    log_file_name = path.name

    i = 0
    while i < n:
        line = lines[i]
        if line is None:
            i += 1
            continue

        m = IP_RE.search(line)
        if m:
            ip = m.group(1)

            # find next non-empty line as the timestamp
            j = i + 1
            while j < n and (lines[j] is None or lines[j].strip() == ""):
                j += 1

            if j < n:
                ts_line = lines[j].strip()

                # increment per-IP counter
                mark_num = ip_counters.get(ip, 0) + 1
                ip_counters[ip] = mark_num

                rows.append((ts_line, ip, enclosing_folder, log_file_name, mark_num))

                # jump past the timestamp line
                i = j + 1
                continue

        i += 1

    return rows


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Parse all .log files in a directory into per-log CSVs like logFileSample.csv"
    )
    ap.add_argument(
        "--log_dir",
        required=True,
        help="Directory containing .log files (non-recursive).",
    )
    ap.add_argument(
        "--out_dir",
        required=False,
        default="",
        help="Output directory for CSVs (default: same as log_dir).",
    )
    args = ap.parse_args()

    log_dir = Path(args.log_dir)
    if not log_dir.exists() or not log_dir.is_dir():
        raise NotADirectoryError(log_dir)

    out_dir = Path(args.out_dir) if args.out_dir else log_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    # All *.log files in the directory (non-recursive)
    log_paths = sorted(p for p in log_dir.glob("*.log") if p.is_file())
    if not log_paths:
        raise SystemExit(f"No .log files found in directory: {log_dir}")

    total_rows = 0
    for p in log_paths:
        rows = parse_log_file(p)
        if not rows:
            print(f"[warn] No [IP]/timestamp pairs found in {p.name}, skipping CSV.")
            continue

        df = pd.DataFrame(
            rows,
            columns=["RPi_Time_Raw_simple", "DeviceIP", "EnclosingFolder", "LogFileName", "markNumber"],
        )

        out_csv = out_dir / f"{p.stem}.csv"
        df.to_csv(out_csv, index=False)
        total_rows += len(df)

        print(f"[info] {p.name}: wrote {len(df)} rows → {out_csv}")

    print(f"[info] Processed {len(log_paths)} .log file(s) from {log_dir}")
    print(f"[info] Total rows across all CSVs: {total_rows}")


if __name__ == "__main__":
    main()
