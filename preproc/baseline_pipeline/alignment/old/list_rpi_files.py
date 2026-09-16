#!/usr/bin/env python3
"""
Make a CSV like SampleRPiOutput.csv for a single pairID.

It expects directories like:
  {root}/{pairID}/{testingDate}/{sessionType}/RPi/{RPi_Type}/**/*.log
where:
  - pairID dir looks like "pair_200"
  - testingDate dir looks like "MM_DD_YYYY" (e.g., 03_17_2025)
  - sessionType is any dir name (e.g., Morning/Afternoon/Evening)
  - RPi_Type dir may look like "RNS_RPi" (we emit "RNS" to the CSV)

From each .log, it extracts:
  - FirstTimestamp: the timestamp on the line *right after* the first [IP] line
  - FirstIPAddress, SecondIPAddress: the first two unique IPs seen in [...] (order of first appearance)

Output columns (exactly as in SampleRPiOutput.csv):
  pairID,testingDate,sessionType,RPi_Type,RPi_Name,FirstTimestamp,FirstIPAddress,SecondIPAddress

USAGE EXAMPLES
  # Minimal (search current directory as root)
  python3 make_rpi_csv.py pair_200 -o out.csv

  # Specify a root directory that contains pair_200/
  python3 make_rpi_csv.py pair_200 --root /data/study -o SampleRPiOutput.csv
"""

from __future__ import annotations
import argparse
import csv
import re
from pathlib import Path
from typing import Iterable, Tuple

DATE_DIR_RE = re.compile(r"^\d{2}_\d{2}_\d{4}$")
IP_RE = re.compile(r"\[(\d{1,3}(?:\.\d{1,3}){3})\]")
TS_RE = re.compile(r"^\s*(\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s*$")

def extract_pair_numeric(pair_dir_name: str) -> str:
    """
    Convert 'pair_200' -> '200'. If it doesn't match, return as-is.
    """
    m = re.fullmatch(r"pair_(\d+)", pair_dir_name, flags=re.IGNORECASE)
    return m.group(1) if m else pair_dir_name

def normalize_rpi_type(name: str) -> str:
    """
    Convert 'RNS_RPi' -> 'RNS' (drop optional '_RPi' or 'RPi' suffix, case-insensitive).
    """
    # Remove trailing _RPi or RPi
    n = re.sub(r"(_?RPi)$", "", name, flags=re.IGNORECASE)
    return n

def parse_log_firsts(log_path: Path) -> Tuple[str, str, str]:
    """
    Return (first_timestamp, ip1, ip2) for a given log file.
    - ip1/ip2: first two unique IPs encountered in [x.x.x.x] lines (order preserved).
    - first_timestamp: the timestamp on the line immediately after the FIRST [IP] line.
    If something is missing, return empty strings for the missing fields.
    """
    ips: list[str] = []
    first_ts: str | None = None
    prev_was_ip = False

    with log_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = IP_RE.search(line)
            if m:
                ip = m.group(1)
                if ip not in ips:
                    ips.append(ip)
                if first_ts is None:
                    prev_was_ip = True
                else:
                    prev_was_ip = False
                # continue to next line
                continue

            if prev_was_ip and first_ts is None:
                tm = TS_RE.match(line)
                if tm:
                    first_ts = tm.group(1)
                prev_was_ip = False

            if first_ts is not None and len(ips) >= 2:
                break

    ip1 = ips[0] if len(ips) >= 1 else ""
    ip2 = ips[1] if len(ips) >= 2 else ""
    return (first_ts or "", ip1, ip2)

def gather_rows(pair_dir: Path) -> list[tuple[str, str, str, str, str, str, str, str]]:
    """
    Walk:
      pair_dir/<date>/<session>/RPi/<rpi_type>/**/*.log
    and build rows matching SampleRPiOutput.csv column order.
    """
    rows: list[tuple[str, str, str, str, str, str, str, str]] = []
    pair_numeric = extract_pair_numeric(pair_dir.name)

    # Iterate date directories (MM_DD_YYYY)
    for date_dir in sorted([d for d in pair_dir.iterdir() if d.is_dir() and DATE_DIR_RE.match(d.name)]):
        date_str = date_dir.name

        # sessionType directories
        for session_dir in sorted([s for s in date_dir.iterdir() if s.is_dir()]):
            session_type = session_dir.name

            rpi_root = session_dir / "RPi"
            if not rpi_root.is_dir():
                continue

            # RPi_Type directories
            for rpi_type_dir in sorted([t for t in rpi_root.iterdir() if t.is_dir()]):
                rpi_type_csv = normalize_rpi_type(rpi_type_dir.name)

                # Recurse for .log files only
                for log_path in sorted(rpi_type_dir.rglob("*.log")):
                    first_ts, ip1, ip2 = parse_log_firsts(log_path)
                    rows.append((
                        pair_numeric,          # pairID
                        date_str,              # testingDate
                        session_type,          # sessionType
                        rpi_type_csv,          # RPi_Type
                        log_path.name,         # RPi_Name
                        first_ts,              # FirstTimestamp
                        ip1,                   # FirstIPAddress
                        ip2                    # SecondIPAddress
                    ))
    return rows

def write_csv(rows: Iterable[tuple[str, str, str, str, str, str, str, str]], out_path: Path | None) -> None:
    header = [
        "pairID",
        "testingDate",
        "sessionType",
        "RPi_Type",
        "RPi_Name",
        "FirstTimestamp",
        "FirstIPAddress",
        "SecondIPAddress",
    ]

    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        f = out_path.open("w", newline="", encoding="utf-8")
        close = True
    else:
        import sys as _sys
        f = _sys.stdout
        close = False

    try:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)
    finally:
        if close:
            f.close()

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair_id", help='pair directory name (e.g., "pair_200")')
    ap.add_argument("--root", default=".", help="root directory that contains the pair directory (default: current dir)")
    ap.add_argument("-o", "--output", type=Path, help="output CSV path (default: stdout)")
    args = ap.parse_args()

    root = Path(args.root).expanduser().resolve()
    pair_dir = (root / args.pair_id)
    if not pair_dir.is_dir():
        raise SystemExit(f"ERROR: Pair directory not found: {pair_dir}")

    rows = gather_rows(pair_dir)
    # Sort by testingDate, sessionType, RPi_Type, RPi_Name for stable output
    rows.sort(key=lambda r: (r[1], r[2], r[3], r[4]))
    write_csv(rows, args.output)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
