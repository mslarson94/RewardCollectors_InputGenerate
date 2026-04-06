#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
extract_rpi_marks.py — Parse an RPi log and emit a CSV of 'mark' times for a
specific device (by IP), suitable for downstream alignment.
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import List, Sequence, Tuple

import pandas as pd


def _read_logfile(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.strip() for ln in f.readlines()]
    lines = [ln for ln in lines if ln and "-e" not in ln]
    return lines


def _fix_fraction_colon(ts: str) -> str:
    m = re.fullmatch(r"(\d{2}:\d{2}:\d{2}):(\d+)", str(ts).strip())
    return f"{m.group(1)}.{m.group(2)}" if m else str(ts).strip()


def _to_datetime_on_date(times: Sequence[str], session_date: str) -> pd.DatetimeIndex:
    out = []
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
    return pd.to_datetime(out)


def _normalize_ml_stem(stem: str, suffixes: Sequence[str]) -> str:
    """Strip any of the given suffixes (if the stem ends with them), repeatedly, then trim trailing separators."""
    base = stem
    changed = True
    while changed:
        changed = False
        for s in suffixes:
            if s and base.endswith(s):
                base = base[: -len(s)]
                changed = True
    return re.sub(r"[_-]+$", "", base)


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract per-IP mark timestamps from an RPi log")
    ap.add_argument("--log_file", required=True)
    ap.add_argument("--session_date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--device", required=True, help="Device name (e.g., ML2A)")
    ap.add_argument("--device_ip", required=True, help="Device IPv4 (e.g., 192.168.50.109)")
    ap.add_argument("--label", default="RPi", help="RPi source label (BioPac|RNS|RPi)")
    ap.add_argument("--ml_csv_file", default="", help="Optional ML CSV to borrow basename for output naming")
    ap.add_argument("--out_dir", default="", help="Directory to write the marks CSV (default: alongside ML/LOG)")
    ap.add_argument("--strip-ml-suffixes", default="_events_final,_processed", help="Comma-separated suffixes to strip from ML stem for output naming")

    args = ap.parse_args()
    log_path = Path(args.log_file)
    if not log_path.exists():
        raise FileNotFoundError(log_path)

    lines = _read_logfile(log_path)
    if len(lines) % 2:
        lines = lines[:-1]
    pairs: List[Tuple[str, str]] = list(zip(lines[0::2], lines[1::2]))

    search_ip = f"[{args.device_ip}]"
    kept = [(i, a, b) for i, (a, b) in enumerate(pairs) if search_ip in a]
    if not kept:
        raise SystemExit(f"no lines with {search_ip} in {log_path}")

    times = [b for _, _, b in kept]
    dt = _to_datetime_on_date(times, args.session_date)

    df = pd.DataFrame(
        {
            "RPi_Timestamp": dt,
            "Device": args.device,
            "DeviceIP": args.device_ip,
            "RPi_Source": args.label,
            "LogFile": log_path.name,
            "LogLineText": [a for _, a, _ in kept],
            "LogPairIndex": [i for i, _, _ in kept],
        }
    )

    suffixes = [s for s in (args.strip_ml_suffixes or "").split(",") if s]

    if args.ml_csv_file:
        ml_stem = Path(args.ml_csv_file).stem
        base = _normalize_ml_stem(ml_stem, suffixes)
        out_dir = Path(args.out_dir) if args.out_dir else Path(args.ml_csv_file).parent
    else:
        base = log_path.stem
        out_dir = Path(args.out_dir) if args.out_dir else log_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    out_csv = out_dir / f"{base}_{args.device}_{args.label}_RPiMarks.csv"
    df.to_csv(out_csv, index=False)
    print(out_csv)


if __name__ == "__main__":
    main()
