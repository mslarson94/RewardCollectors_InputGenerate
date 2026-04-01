#!/usr/bin/env python3
"""
Compile per-IP timezone offsets by walking RPi log files defined in a metadata Excel.
- Metadata: an .xlsx with sheet (default "MagicLeapFiles") containing columns:
    pairID_py, testingDate, sessionType, cleanedFile, and EITHER:
        a) two columns containing RPi filenames: 'BioPac_RPi' and 'RNS_RPi' (values 'none' or filename)
       OR
        b) columns 'RPi_Type' and 'RPi_filename'
- For each referenced log file, compute per-IP approximate timezone offset:
    1) Extract "true" start time from the log's filename: YYYY-MM-DD_HH_mm_ss_*.log
    2) Scan log content for the first timestamp per unique device IP:
          pattern: [<ip>] <HH:MM:SS[.micro]>  OR  two-line pairs: "[<ip>]" then next non-empty line "HH:MM:SS[.micro]"
    3) Let RAW_DELTA(ip) = first_log_time(ip) - filename_time (seconds).
    4) Per-file start lag LAG = min RAW_DELTA(ip) over ip where RAW_DELTA >= 0, else 0.
    5) Approx offset hours = round((RAW_DELTA(ip) - LAG)/3600).
- Output: a CSV with one row per (file, ip).

Usage:
    python compile_log_time_offsets.py --root /path/to/rootdir --xlsx /path/to/collatedData.xlsx --sheet MagicLeapFiles --out offsets.csv
"""
from __future__ import annotations

import argparse
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

import pandas as pd


FILENAME_RE = re.compile(
    r'^(?P<date>\d{4}-\d{2}-\d{2})_(?P<h>\d{2})_(?P<m>\d{2})_(?P<s>\d{2})_.*\.log$'
)

LINE_RE_INLINE = re.compile(
    r'\[(?P<ip>(?:\d{1,3}\.){3}\d{1,3})\]\s*(?P<time>\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)'
)

IP_ONLY_RE = re.compile(r'^\[(?P<ip>(?:\d{1,3}\.){3}\d{1,3})\]\s*$')
TIME_ONLY_RE = re.compile(r'^(?P<time>\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)\s*$')


def parse_filename_time(fname: str) -> datetime:
    name = Path(fname).name
    m = FILENAME_RE.match(name)
    if not m:
        raise ValueError(f"Log filename does not match expected pattern YYYY-MM-DD_HH_mm_ss_*.log: {name}")
    return datetime.strptime(
        f"{m['date']} {m['h']}:{m['m']}:{m['s']}", "%Y-%m-%d %H:%M:%S"
    )


def _parse_time_to_datetime(file_date: str, t: str) -> datetime:
    if "." in t:
        main, frac = t.split(".", 1)
        frac = (frac + "000000")[:6]
    else:
        main, frac = t, "000000"
    return datetime.strptime(f"{file_date} {main}", "%Y-%m-%d %H:%M:%S").replace(microsecond=int(frac))


def parse_first_times(log_path: Path, file_date: str) -> Dict[str, datetime]:
    first_seen: Dict[str, datetime] = {}
    with log_path.open("r", encoding="utf-8", errors="replace") as f:
        lines = [ln.rstrip("\n") for ln in f]

    i, n = 0, len(lines)
    while i < n:
        line = lines[i]
        # Inline pattern: [IP] HH:MM:SS(.us)
        m = LINE_RE_INLINE.search(line)
        if m:
            ip = m.group("ip")
            if ip not in first_seen:
                first_seen[ip] = _parse_time_to_datetime(file_date, m.group("time"))
            i += 1
            continue
        # Two-line pattern: "[IP]" then next non-empty line is time
        m_ip = IP_ONLY_RE.match(line.strip())
        if m_ip:
            ip = m_ip.group("ip")
            j = i + 1
            while j < n and lines[j].strip() == "":
                j += 1
            if j < n:
                m_t = TIME_ONLY_RE.match(lines[j].strip())
                if m_t and ip not in first_seen:
                    first_seen[ip] = _parse_time_to_datetime(file_date, m_t.group("time"))
            i = j + 1
            continue
        i += 1
    return first_seen


def hms_from_seconds(sec: float) -> str:
    sign = "-" if sec < 0 else ""
    sec = abs(sec)
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec - h * 3600 - m * 60
    return f"{sign}{h:02d}:{m:02d}:{s:06.3f}"


def compute_offsets_for_log(log_path: Path) -> Tuple[List[dict], dict]:
    file_dt = parse_filename_time(log_path.name)
    ip_first = parse_first_times(log_path, file_dt.strftime("%Y-%m-%d"))
    deltas = {ip: (ts - file_dt).total_seconds() for ip, ts in ip_first.items()}
    nonneg = [d for d in deltas.values() if d >= 0]
    start_lag = min(nonneg) if nonneg else 0.0

    rows = []
    for ip, d in deltas.items():
        rows.append(
            {
                "ip": ip,
                "first_log_time": ip_first[ip].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "raw_delta_sec": round(d, 6),
                "raw_delta_hms": hms_from_seconds(d),
                "start_lag_sec": round(start_lag, 6),
                "start_lag_hms": hms_from_seconds(start_lag),
                "approx_offset_hours": int(round((d - start_lag) / 3600.0)),
            }
        )
    meta = {
        "filename_time": file_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "num_ips": len(ip_first),
        "start_lag_sec": round(start_lag, 6),
        "start_lag_hms": hms_from_seconds(start_lag),
    }
    return rows, meta


def _normalize_testing_date(val) -> str:
    if pd.isna(val):
        return ""
    s = str(val).strip()
    if re.fullmatch(r"\d{2}_\d{2}_\d{4}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        y, m, d = s.split("-")
        return f"{m}_{d}_{y}"
    try:
        dt = pd.to_datetime(val)
        return dt.strftime("%m_%d_%Y")
    except Exception:
        return s


def build_candidate_paths(row: Mapping[str, object], root: Path) -> Iterable[Tuple[str, str, Path]]:
    pairID_py = str(row.get("pairID_py", "")).strip()
    testingDate = _normalize_testing_date(row.get("testingDate", ""))
    sessionType = str(row.get("sessionType", "")).strip()
    base = root / "RawData" / pairID_py / testingDate / sessionType / "RPi"

    if "RPi_Type" in row and "RPi_filename" in row and str(row["RPi_filename"]).strip().lower() not in ("", "none", "nan"):
        rpi_type = str(row["RPi_Type"]).strip()
        rpi_filename = str(row["RPi_filename"]).strip()
        yield rpi_type, rpi_filename, base / rpi_type / rpi_filename
        return

    for rpi_type in ("BioPac_RPi", "RNS_RPi"):
        if rpi_type in row:
            rpi_filename = str(row.get(rpi_type, "")).strip()
            if rpi_filename and rpi_filename.lower() not in ("none", "nan"):
                yield rpi_type, rpi_filename, base / rpi_type / rpi_filename


def compile_from_excel(root_dir: str, xlsx_path: str, sheet: str, out_csv: Optional[str]) -> pd.DataFrame:
    root = Path(root_dir)
    df = pd.read_excel(xlsx_path, sheet_name=sheet)
    if "cleanedFile" in df.columns:
        df = df.copy().drop_duplicates(subset=["cleanedFile"], keep="first")
    records: List[dict] = []

    for _, row in df.iterrows():
        cleaned = str(row.get("cleanedFile", "")).strip()
        pairID_py = str(row.get("pairID_py", "")).strip()
        testingDate = _normalize_testing_date(row.get("testingDate", ""))
        sessionType = str(row.get("sessionType", "")).strip()

        for rpi_type, rpi_filename, path in build_candidate_paths(row, root):
            rec_common = {
                "pairID_py": pairID_py,
                "testingDate": testingDate,
                "sessionType": sessionType,
                "cleanedFile": cleaned,
                "RPi_Type": rpi_type,
                "RPi_filename": rpi_filename,
                "file_path": str(path),
            }
            if not path.exists():
                records.append({**rec_common, "status": "missing"})
                continue

            try:
                per_ip_rows, meta = compute_offsets_for_log(path)
                for r in per_ip_rows:
                    records.append({**rec_common, **meta, **r, "status": "ok"})
            except Exception as e:
                records.append({**rec_common, "status": f"error: {type(e).__name__}: {e}"})

    out_df = pd.DataFrame.from_records(records)
    if out_csv:
        out_df.to_csv(out_csv, index=False)
    return out_df


def main():
    ap = argparse.ArgumentParser(description="Compile per-IP timezone offsets for RPi logs referenced by collatedData.xlsx")
    ap.add_argument("--root", required=True, help="Root directory containing RawData/…")
    ap.add_argument("--xlsx", required=True, help="Path to collatedData.xlsx")
    ap.add_argument("--sheet", default="MagicLeapFiles", help="Sheet name in the Excel workbook")
    ap.add_argument("--out", default=None, help="Output CSV path")
    args = ap.parse_args()

    df = compile_from_excel(args.root, args.xlsx, args.sheet, args.out)
    cols = [
        "pairID_py","testingDate","sessionType","cleanedFile",
        "RPi_Type","RPi_filename","file_path","status",
        "filename_time","start_lag_hms","ip","first_log_time","raw_delta_hms","approx_offset_hours"
    ]
    cols = [c for c in cols if c in df.columns]
    if cols:
        print(df[cols].to_string(index=False))
    else:
        print("No rows produced.")


if __name__ == "__main__":
    main()
