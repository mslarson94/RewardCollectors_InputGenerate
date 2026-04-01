#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch wrapper for the split pipeline (scripts 1 → 2a → 2b) over collatedData.xlsx.
"""

from __future__ import annotations

import argparse
import re
import shlex
import subprocess
from pathlib import Path
from typing import Dict, Sequence

import pandas as pd


def _parse_device_ip_map(path: Path) -> Dict[str, str]:
    text = path.read_text(encoding="utf-8")
    out: Dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"'([^']+)'\s*:\s*'([^']+)'", line)
        if m:
            out[m.group(1).strip()] = m.group(2).strip()
    return out

def _missing_like(x: object) -> bool:
    s = str(x).strip().lower()
    return s in {"", "none", "na", "n/a", "nan", "<na>", "null", "-"}

def _to_session_date(s: str) -> str:
    s = s.replace("/", "_").replace("-", "_")
    mm, dd, yy = s.split("_")
    return f"{yy}-{int(mm):02d}-{int(dd):02d}"


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


def _missing_like(x: object) -> bool:
    s = str(x).strip().lower()
    return s in {"", "none", "na", "n/a", "nan", "<na>", "null", "-"}


def _resolve_ml_csv(ml_root: Path, cleaned_value: str, suffixes: Sequence[str]) -> Path:
    """Resolve ML CSV from a 'cleanedFile' value that may include variant suffixes."""
    p = Path(cleaned_value)
    stem = _normalize_ml_stem(p.stem, suffixes)
    ext = p.suffix or ".csv"

    candidates = [
        ml_root / p.name,  # as provided
        ml_root / f"{stem}_events_final{ext}",
        ml_root / f"{stem}_processed{ext}",
        ml_root / f"{stem}{ext}",
    ]
    tried = []
    for c in candidates:
        tried.append(str(c))
        if c.exists():
            return c
    raise FileNotFoundError("could not resolve ML CSV for '" + cleaned_value + "'. Tried: " + ", ".join(tried))


def main() -> None:
    ap = argparse.ArgumentParser(description="Batch run split alignment pipeline over collatedData.xlsx")
    ap.add_argument("--collated", required=True)
    ap.add_argument("--device-ip-map", required=True)
    ap.add_argument("--code-dir", required=True)
    ap.add_argument("--base-dir", required=True)
    ap.add_argument("--proc-dir", default="FreshStart")
    ap.add_argument("--events-dir-name", default="Events_Final_NoWalks")
    ap.add_argument("--csv-timestamp-column", default="mLTimestamp")
    ap.add_argument("--event-type-column", default="lo_eventType")
    ap.add_argument("--timezone-offset", default="auto")
    ap.add_argument("--sheet", default="MagicLeapFiles")
    ap.add_argument("--out-dir", default="", help="If set, write outputs to <base>/<proc>/full/<out-dir>")
    ap.add_argument("--strip-ml-suffixes", default="_events_final,_processed", help="Comma-separated suffixes to strip from ML stem for output naming")
    ap.add_argument("--only-rows-with-rpi", action="store_true", help="Process only rows where BioPac_RPi_File or RNS_RPi_File is present (non-missing-like)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    suffixes = [s for s in (args.strip_ml_suffixes or "").split(",") if s]

    df = pd.read_excel(args.collated, sheet_name=args.sheet, dtype="string")
    for col in ["cleanedFile", "BioPac_RPi_File", "RNS_RPi_File", "pairID_py", "testingDate", "sessionType", "device"]:
        if col not in df.columns:
            raise KeyError(f"missing required column: {col}")

    if args.only_rows_with_rpi:
        before = len(df)
        df = df.loc[~(df["BioPac_RPi_File"].map(_missing_like) & df["RNS_RPi_File"].map(_missing_like))].reset_index(drop=True)
        after = len(df)
        print(f"[filter] --only-rows-with-rpi: {before} → {after} rows")

    ip_map = _parse_device_ip_map(Path(args.device_ip_map))
    code_dir = Path(args.code_dir)

    ml_root = Path(args.base_dir) / args.proc_dir / "full" / args.events_dir_name / "augmented"
    rpi_root = Path(args.base_dir) / args.proc_dir / "RawData"

    out_root = None
    if args.out_dir:
        out_root = Path(args.base_dir) / args.proc_dir / "full" / args.out_dir
        out_root.mkdir(parents=True, exist_ok=True)

    extract = code_dir / "extract_rpi_marks.py"
    merge = code_dir / "merge_ml_with_rpi_marks.py"
    summarize = code_dir / "summarize_drift.py"
    for s in (extract, merge, summarize):
        if not s.exists():
            raise FileNotFoundError(s)

    jobs = 0
    ok = 0

    for _, row in df.iterrows():
        cleaned_raw = (row["cleanedFile"] or "").strip()
        if not cleaned_raw:
            continue
        try:
            ml_csv = _resolve_ml_csv(ml_root, cleaned_raw, suffixes)
        except FileNotFoundError as e:
            print(f"[skip] {e}")
            continue

        pair = (row["pairID_py"] or "").strip()
        tdate = (row["testingDate"] or "").strip()
        sess = (row["sessionType"] or "").strip()
        device = (row["device"] or "").strip()
        ip = ip_map.get(device)
        if not ip:
            print(f"[skip] no IP for device {device}")
            continue
        session_date = _to_session_date(tdate)
        # per-row timezone override (optional column 'timezoneOffsetHours')
        tz_cell = str(row.get("timezoneOffsetHours", "") or "").strip()
        tz_arg = tz_cell if tz_cell and not _missing_like(tz_cell) else args.timezone_offset
        if args.debug:
            print(f"[tz] {ml_csv.name} -> timezone_offset_hours={tz_arg}")


        # Ensure ML CSV actually has at least one 'Mark'
        try:
            tmp_ml = pd.read_csv(ml_csv, usecols=[args.event_type_column])
            has_mark = tmp_ml[args.event_type_column].astype(str).str.strip().str.lower().eq("mark").any()
        except Exception as e:
            print(f"[skip] failed reading ML file '{ml_csv.name}': {e}")
            continue
        if not has_mark:
            print(f"\n🚫 [NO MARKS] ML CSV '{ml_csv.name}' has no rows where {args.event_type_column} == 'Mark'. Skipping this row (likely mismatched to its RPi file).\n")
            continue

        target_dir = out_root or ml_csv.parent
        ml_rootname = _normalize_ml_stem(ml_csv.stem, suffixes)

        rpi_specs = (
            ("BioPac", (row["BioPac_RPi_File"] or "").strip(), "BioPac_RPi"),
            ("RNS",    (row["RNS_RPi_File"] or "").strip(),    "RNS_RPi"),
        )

        for label, fname, subdir in rpi_specs:
            if _missing_like(fname):
                print(f"[skip] no {label} RPi file for ML CSV: {ml_csv.name}")
                continue
            log_file = rpi_root / pair / tdate / sess / "RPi" / subdir / fname
            if not log_file.exists():
                print(f"[skip] {label} log missing: {log_file}")
                continue

            # 1) extract
            jobs += 1
            cmd1 = [
                "python", str(extract),
                "--log_file", str(log_file),
                "--session_date", session_date,
                "--device", device,
                "--device_ip", ip,
                "--label", label,
                "--ml_csv_file", str(ml_csv),
                "--strip-ml-suffixes", args.strip_ml_suffixes,
            ]
            if out_root is not None:
                cmd1 += ["--out_dir", str(out_root)]

            # 2a) merge
            marks_csv = (out_root or ml_csv.parent) / f"{ml_rootname}_{device}_{label}_RPiMarks.csv"
            cmd2 = [
                "python", str(merge),
                "--ml_csv_file", str(ml_csv),
                "--rpi_marks_csv", str(marks_csv),
                "--csv_timestamp_column", args.csv_timestamp_column,
                "--event_type_column", args.event_type_column,
                "--event_type_values", "Mark",
                "--label", label,
                "--device", device,
                "--timezone_offset_hours", tz_arg,
                #"--timezone_offset_hours", args.timezone_offset,
                "--strip-ml-suffixes", args.strip_ml_suffixes,
            ]
            if out_root is not None:
                cmd2 += ["--out_dir", str(out_root)]

            # run extract + merge
            for cmd in (cmd1, cmd2):
                if args.debug:
                    print("[cmd]", " ".join(shlex.quote(x) for x in cmd))
                if args.dry_run:
                    ok += 1
                    continue
                try:
                    res = subprocess.run(cmd, check=True, capture_output=not args.debug, text=True)
                    if not args.debug and res.stdout:
                        print(res.stdout.strip())
                    ok += 1
                except subprocess.CalledProcessError as e:
                    print(f"[fail] {' '.join(cmd)}\n{e.stdout or ''}\n{e.stderr or ''}")
                    break

            # 2b) summarize only if merged exists
            merged_csv = (out_root or ml_csv.parent) / f"{ml_rootname}_{device}_{label}_events.csv"
            if merged_csv.exists():
                cmd3 = ["python", str(summarize), "--merged_ml_csv", str(merged_csv), "--label", label]
                if args.debug:
                    print("[cmd]", " ".join(shlex.quote(x) for x in cmd3))
                if not args.dry_run:
                    try:
                        res = subprocess.run(cmd3, check=True, capture_output=not args.debug, text=True)
                        if not args.debug and res.stdout:
                            print(res.stdout.strip())
                        ok += 1
                    except subprocess.CalledProcessError as e:
                        print(f"[fail] {' '.join(cmd3)}\n{e.stdout or ''}\n{e.stderr or ''}")
            else:
                print(f"[skip] summarize: merged CSV not found (likely no marks or merge skipped): {merged_csv}")

    print(f"\n=== Batch split pipeline summary ===\nsteps attempted: {jobs*3}\nsteps ok:        {ok}\nsteps failed:    {jobs*3 - ok}")


if __name__ == "__main__":
    main()
