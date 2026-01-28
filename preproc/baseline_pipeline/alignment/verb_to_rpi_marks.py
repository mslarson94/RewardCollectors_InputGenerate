# file: verb_to_rpi_marks.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Convert a *_verb.csv (short) for a single IP into per-source marks ready for unification.

Writes:
  <out>/<rpi-source>/RPiMarks/<ml_root>_<device>_<rpi-source>_RPi_verb.csv

Columns:
  Device, DeviceIP, RPi_Source, markNumber, LogPairIndex,
  RPi_Time_verb (datetime),
  ML_Time_verb, RPi_Time_verb_str, Monotonic_Time_verb, Monotonic_Time_Adj_verb,
  LogFile, LogLineText, RPi_Timestamp_Source
"""

from __future__ import annotations
import argparse, re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List
import pandas as pd

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Convert _verb.csv (short) to per-source RPi_verb marks for a single IP.")
    p.add_argument("--in-csv", required=True)
    p.add_argument("--ip", required=True, help="IP without brackets, e.g. 192.168.50.156")
    p.add_argument("--session-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--device", required=True)
    p.add_argument("--rpi-source", required=True, help="BioPac or RNS")
    p.add_argument("--ml-csv-file", required=True)
    p.add_argument("--out-dir", default="", help="Root out dir; default next to ML CSV")
    p.add_argument("--timestamp-col", default="Mono_Time_verb",
                   help="Verbose column to use as canonical timestamp (e.g., ML_Time_verb, RPi_Time_verb)")
    p.add_argument("--strip-ml-suffixes", default="_events_final,_processed")
    p.add_argument("--allow-day-rollover", action="store_true")
    p.add_argument("--dedupe-sec", type=float, default=0.0)
    p.add_argument("--debug", action="store_true")
    return p.parse_args()

_TIME_RE = re.compile(r"^(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d+))?$")

def _norm_ml_stem(stem: str, suffixes: List[str]) -> str:
    base = stem
    changed = True
    while changed:
        changed = False
        for s in suffixes:
            if s and base.endswith(s):
                base = base[: -len(s)]
                changed = True
    return re.sub(r"[_-]+$", "", base)

def _parse_time_on_date(t_str: str, date_str: str) -> datetime | None:
    m = _TIME_RE.match((t_str or "").strip())
    if not m:
        return None
    hh, mm, ss, frac = m.groups()
    us = int((frac or "0").ljust(6, "0")[:6])
    return datetime.strptime(date_str, "%Y-%m-%d").replace(
        hour=int(hh), minute=int(mm), second=int(ss), microsecond=us
    )

@dataclass
class MarkRow:
    dt: datetime
    ip: str
    log_line_text: str
    mark_number: int
    log_pair_index: int
    ml_Time: str
    rpi_Time_str: str
    mono_Time: str
    mono_adj_Time: str

def _dedupe(rows: List[MarkRow], thresh_s: float) -> List[MarkRow]:
    if thresh_s <= 0 or not rows:
        return rows
    out: List[MarkRow] = []
    prev: datetime | None = None
    thr = timedelta(seconds=thresh_s)
    for r in rows:
        if prev is None or (r.dt - prev) > thr:
            out.append(r); prev = r.dt
    return out

def main() -> None:
    print('starting the verb2rpi marks script')
    args = parse_args()
    in_path = Path(args.in_csv)
    ml_csv = Path(args.ml_csv_file)
    if not in_path.exists(): raise FileNotFoundError(in_path)
    if not ml_csv.exists(): raise FileNotFoundError(ml_csv)

    raw_ip = args.ip.strip()
    ip_br = f"[{raw_ip}]"

    df = pd.read_csv(in_path, dtype="string")
    for c in ["ipAddress", "markNumber"]:
        if c not in df.columns: raise KeyError(f"missing required column: {c}")

    # Ensure *_verb columns exist (mirror from base if needed)
    def ensure(base: str):
        vb = f"{base}_verb"
        if vb not in df.columns and base in df.columns:
            df[vb] = df[base]
    for base in ["ML_Time", "RPi_Time", "Mono_Time_Raw", "Mono_Time"]:
        ensure(base)

    # Choose timestamp col (accept base or _verb)
    ts_col = args.timestamp_col
    if ts_col not in df.columns and f"{ts_col}_verb" in df.columns:
        ts_col = f"{ts_col}_verb"
    if ts_col not in df.columns:
        raise KeyError(f"timestamp column '{args.timestamp_col}' not found (also checked '{args.timestamp_col}_verb').")

    # Filter to this IP (accept [ip] or ip)
    df = df[(df["ipAddress"] == ip_br) | (df["ipAddress"] == raw_ip)]
    if df.empty:
        print(f"[info] no rows for ip={raw_ip} in {in_path.name}")
        return

    # LAST row per markNumber
    df = df.reset_index(drop=False).rename(columns={"index": "__ord"})
    rows: List[MarkRow] = []
    for mark, g in df.groupby(["markNumber"], sort=False):
        g = g.sort_values("__ord")
        last = g.iloc[-1]
        t_str = str(last[ts_col] or "")
        dt = _parse_time_on_date(t_str, args.session_date)
        if dt is None:
            if args.debug: print(f"[warn] skip malformed time '{t_str}' for mark={mark}")
            continue
        print(mark[0])
        mn = int(mark[0])                 # markNumber is 1-based
        lpi = mn - 1                   # LogPairIndex is 0-based
        rows.append(MarkRow(
            dt=dt,
            ip=raw_ip,
            log_line_text=f"{ip_br} mark {mn}",
            mark_number=mn,
            log_pair_index=lpi,
            ml_Time=str(last.get("ML_Time_verb") or ""),
            rpi_Time_str=str(last.get("RPi_Time_verb") or ""),
            mono_Time=str(last.get("Mono_Time_Raw_verb") or ""),
            mono_adj_Time=str(last.get("Mono_Time_verb") or ""),
        ))

    rows.sort(key=lambda r: r.dt)

    if args.allow_day_rollover:
        shift = timedelta(0)
        last_dt: datetime | None = None
        for i, r in enumerate(rows):
            dt = r.dt + shift
            if last_dt is not None and dt < last_dt:
                while dt <= last_dt: dt += timedelta(days=1)
                shift = dt - r.dt
            rows[i] = MarkRow(dt=dt, ip=r.ip, log_line_text=r.log_line_text,
                              mark_number=r.mark_number, log_pair_index=r.log_pair_index,
                              ml_Time=r.ml_Time, rpi_Time_str=r.rpi_Time_str,
                              mono_Time=r.mono_Time, mono_adj_Time=r.mono_adj_Time)
            last_dt = rows[i].dt

    rows = _dedupe(rows, float(args.dedupe_sec))

    suffixes = [s for s in (args.strip_ml_suffixes or "").split(",") if s]
    # derive ml_root
    base_stem = ml_csv.stem
    changed = True
    while changed:
        changed = False
        for s in suffixes:
            if s and base_stem.endswith(s):
                base_stem = base_stem[: -len(s)]
                changed = True
    ml_root = re.sub(r"[_-]+$", "", base_stem)

    out_dir = Path(args.out_dir) if args.out_dir else ml_csv.parent
    #out_dir = base_out / args.rpi_source 
    #out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"{ml_root}_{args.rpi_source}_RPi_verb.csv"

    out_df = pd.DataFrame({
        "Device": args.device,
        "DeviceIP": [r.ip for r in rows],
        "RPi_Source": args.rpi_source,
        "markNumber": [r.mark_number for r in rows],
        "LogPairIndex": [r.log_pair_index for r in rows],
        "RPi_Time_verb": [r.dt for r in rows],  # unified prefers this if present
        # sidecar 'Time' fields for traceability
        "ML_Time_verb": [r.ml_Time for r in rows],
        "RPi_Time_verb_str": [r.rpi_Time_str for r in rows],
        "Mono_Time_Raw_verb": [r.mono_Time for r in rows],
        "Mono_Time_verb": [r.mono_adj_Time for r in rows],
        # provenance
        "LogFile": in_path.name,
        "LogLineText": [r.log_line_text for r in rows],
        "RPi_Timestamp_Source": [ts_col] * len(rows),
    })

    out_df.to_csv(out_csv, index=False)
    print(f"[ok] wrote {len(out_df)} marks → {out_csv} (source={ts_col})")
    print(f"[emit] {args.rpi_source}={out_csv}")

if __name__ == "__main__":
    main()
