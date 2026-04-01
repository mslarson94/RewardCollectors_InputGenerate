#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch wrapper for the split pipeline (scripts 1 → 2a → 2b → 2c) over collatedData.xlsx.

Steps per row (when inputs exist):
  1) extract_rpi_marks2.py           → writes <base>_<device>_<Label>_RPiMarks.csv
  2a) merge_ml_with_rpi_marks2.py   → writes <base>_<device>_<Label>_events.csv
  2b) summarize_drift2.py           → writes per-label drift plot + summary
  2c) merge_rpi_event_files2.py     → writes <base>_<device>_BioPacRNS_events.csv
      summarize_drift2.py (auto)    → writes combined overlay plot + summary

It expects the sheet/column conventions:
  cleanedFile, BioPac_RPi_File, RNS_RPi_File, pairID_py, testingDate, sessionType, device
Optional per-row override column:
  timezoneOffsetHours  (number or "auto")

Paths constructed as:
  ML CSV:   <base>/<proc>/full/<events_dir_name>/augmented/<cleanedFile>
  BioPac:   <base>/<proc>/RawData/<pairID_py>/<testingDate>/<sessionType>/RPi/BioPac_RPi/<BioPac_RPi_File>
  RNS:      <base>/<proc>/RawData/<pairID_py>/<testingDate>/<sessionType>/RPi/RNS_RPi/<RNS_RPi_File>
  Outputs:  <base>/<proc>/full/<out_dir>  (if --out-dir provided; else alongside each ML CSV)
"""

from __future__ import annotations

import argparse
import re
import shlex
import subprocess
from pathlib import Path
from typing import Dict, Sequence

import pandas as pd

from batchAlignHelpers import _parse_device_ip_map, _to_session_date, _normalize_ml_stem, _missing_like, _resolve_ml_csv



def fmt_cmd(cmd): 
    return " ".join(shlex.quote(str(x)) for x in cmd)

# -------------------------------
# Main
# -------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Batch run split alignment pipeline over collatedData.xlsx (with combined merge)")
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
    ap.add_argument("--strip-ml-suffixes", default="_events_final,_processed")
    ap.add_argument("--only-rows-with-rpi", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--blankRowTemplate", required=True)
    ap.add_argument("--dedupesec", default="0.05")
    ap.add_argument("--maxmatchgaps", default="1.0")
    args = ap.parse_args()

    suffixes = [s for s in (args.strip_ml_suffixes or "").split(",") if s]

    df = pd.read_excel(args.collated, sheet_name=args.sheet, dtype="string")
    for col in ["cleanedFile", "BioPac_RPi", "RNS_RPi", "pairID_py", "testingDate", "sessionType", "device"]:
        if col not in df.columns:
            raise KeyError(f"missing required column: {col}")

    if args.only_rows_with_rpi:
        before = len(df)
        df = df.loc[~(df["BioPac_RPi"].map(_missing_like) & df["RNS_RPi"].map(_missing_like))].reset_index(drop=True)
        after = len(df)
        print(f"[filter] --only-rows-with-rpi: {before} → {after} rows")

    ip_map = _parse_device_ip_map(Path(args.device_ip_map))
    code_dir = Path(args.code_dir)

    ml_root = Path(args.base_dir) / args.proc_dir / "full" / args.events_dir_name / "augmented"
    rpi_root = Path(args.base_dir) / args.proc_dir / "RawData"

    out_root = None
    debug_dir = None
    if args.out_dir:
        out_root = Path(args.base_dir) / args.proc_dir / "full" / args.out_dir
        out_root.mkdir(parents=True, exist_ok=True)

    debug_dir = (out_root or Path.cwd()) / "debugging"
    debug_dir.mkdir(parents=True, exist_ok=True)


    merge = code_dir / "merge_ml_with_rpi_marks3.py"
    summarize = code_dir / "summarize_drift2.py"
    merge_both = code_dir / "merge_rpi_event_files2.py"
    translateVerb = code_dir / "translate_verb_log.py"
    sum_verb = code_dir / "summarize_verb_marks.py"
    for s in ( merge, summarize, merge_both):
        if not s.exists():
            raise FileNotFoundError(s)

    attempted = 0
    ok = 0
    
    RESOLVE_ML_CSV_BADLIST = []
    ML_FILE_FAIL_LIST = []
    NO_MARKS_LIST = []
    MISSING_LIKE_LIST = []
    LOG_MISSING_LIST = []

    for _, row in df.iterrows():
        cleaned_raw = (row["cleanedFile"] or "").strip()
        if not cleaned_raw:
            continue
        try:
            ml_csv = _resolve_ml_csv(ml_root, cleaned_raw, suffixes)
        except FileNotFoundError as e:

            resolve_ml_csv_str = (f"[skip] {e}")
            RESOLVE_ML_CSV_BADLIST.append(resolve_ml_csv_str)
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

        # Per-row timezone override (optional column 'timezoneOffsetHours')
        tz_cell = str(row.get("timezoneOffsetHours", "") or "").strip()
        tz_arg = tz_cell if tz_cell and not _missing_like(tz_cell) else args.timezone_offset
        if args.debug:
            print(f"[tz] {ml_csv.name} -> timezone_offset_hours={tz_arg}")

        # Heavy check: ensure ML CSV actually has at least one 'Mark'
        try:
            tmp_ml = pd.read_csv(ml_csv, usecols=[args.event_type_column])
            has_mark = tmp_ml[args.event_type_column].astype(str).str.strip().str.lower().eq("mark").any()
        except Exception as e:
            ml_file_fail_str = (f"[skip] failed reading ML file '{ml_csv.name}': {e}")
            ML_FILE_FAIL_LIST.append(ml_file_fail_str)
            continue
        if not has_mark:
            no_marks_str = (f"\n🚫 [NO MARKS] ML CSV '{ml_csv.name}' has no rows where {args.event_type_column} == 'Mark'. Skipping this row.\n")
            print(no_marks_str)
            NO_MARKS_LIST.append(no_marks_str)
            continue
        target_dir = out_root or ml_csv.parent
        ml_rootname = _normalize_ml_stem(ml_csv.stem, suffixes)

        bio_merged = target_dir / "BioPac" / "Events" / f"{ml_rootname}_{device}_BioPac_events.csv"
        rns_merged = target_dir / "RNS" / "Events" / f"{ml_rootname}_{device}_RNS_events.csv"

        # Build per-source runs
        rpi_specs = (
            ("BioPac", (row["BioPac_RPi"] or "").strip(), "BioPac_RPi", bio_merged),
            ("RNS",    (row["RNS_RPi"] or "").strip(),    "RNS_RPi",    rns_merged),
        )

        for label, fname, subdir, merged_path in rpi_specs:
            if _missing_like(fname):
                missing_like_str = (f"[skip] no {label} RPi file for ML CSV: {ml_csv.name}")
                print(missing_like_str)
                MISSING_LIKE_LIST.append(missing_like_str)
                continue

            print('fname', fname)
            #names = re.split(r"[;,]\\s*", fname) if fname else []
            names = re.split(r"[;,\s]+", fname.strip()) if fname else []
            print('names', names)
            names = [nm for nm in names if nm]

            # 2a) merge
            print('starting cmd2, merge_ml_with_rpi_marks3.py')
            marks_csv = Path(args.base_dir) / args.proc_dir / "RPi_preproc" / label / "RPi_unified" / f"{ml_rootname}_{label}_RPi_unified.csv"
            cmd2 = [
                "python", str(merge),
                "--ml_csv_file", str(ml_csv),
                "--rpi_marks_csv", str(marks_csv),
                "--csv_timestamp_column", args.csv_timestamp_column,
                "--event_type_column", args.event_type_column,
                "--event_type_values", "Mark",
                "--label", label,
                "--device", device,
                "--strip-ml-suffixes", ",".join(suffixes),
                "--timezone_offset_hours", tz_arg,
                "--blankRowTemplate", str(args.blankRowTemplate),
                "--max_match_gap_s", args.maxmatchgaps,
            ]
            if out_root is not None:
                cmd2 += ["--out_dir", str(out_root)]

            # 2b) summarize — only if merged exists
            # (we will check merged_path after running extract+merge)

            if args.debug:
                print("[cmd]", fmt_cmd(cmd2))
            if args.dry_run:
                attempted += 1
                ok += 1
                continue
            try:
                attempted += 1
                res = subprocess.run(cmd2, check=True, capture_output=not args.debug, text=True)
                if not args.debug and res.stdout:
                    print(res.stdout.strip())
                ok += 1
            except subprocess.CalledProcessError as e:
                print(f"[fail] {' '.join(cmd2)}\n{e.stdout or ''}\n{e.stderr or ''}")
                break
        else:
            # Only executes if the for-loop wasn't broken -> safe to try summarize
            if merged_path.exists():
                print('starting cmd3, summarize_drift2.py')
                cmd3 = ["python", str(summarize), "--merged_ml_csv", str(merged_path), "--label", label]
                if args.debug:
                    print("[cmd]", " ".join(shlex.quote(x) for x in cmd3))
                if args.dry_run:
                    attempted += 1
                    ok += 1
                else:
                    try:
                        attempted += 1
                        res = subprocess.run(cmd3, check=True, capture_output=not args.debug, text=True)
                        if not args.debug and res.stdout:
                            print(res.stdout.strip())
                        ok += 1
                    except subprocess.CalledProcessError as e:
                        print(f"[fail] {' '.join(cmd3)}\n{e.stdout or ''}\n{e.stderr or ''}")
            else:
                print(f"[skip] summarize: merged CSV not found: {merged_path}")

        # 2c) combined merge if either exists
        if bio_merged.exists() or rns_merged.exists():
            print('starting cmd4, Merge Both RNS and BioPac merge_rpi_event_files2.py')
            cmd4 = ["python", str(merge_both)]
            if bio_merged.exists():
                cmd4 += ["--biopac_events_csv", str(bio_merged)]
            if rns_merged.exists():
                cmd4 += ["--rns_events_csv", str(rns_merged)]
            if out_root is not None:
                cmd4 += ["--out_dir", str(out_root)]
            if args.debug:
                print("[cmd]", " ".join(shlex.quote(x) for x in cmd4))
            if args.dry_run:
                attempted += 1
                ok += 1
            else:
                try:
                    attempted += 1
                    res = subprocess.run(cmd4, check=True, capture_output=not args.debug, text=True)
                    if not args.debug and res.stdout:
                        print(res.stdout.strip())
                    ok += 1
                except subprocess.CalledProcessError as e:
                    print(f"[fail] {' '.join(cmd4)}\n{e.stdout or ''}\n{e.stderr or ''}")

            # after running cmd4 (which writes <base>_<device>_BioPacRNS_events.csv)
            combined_csv = (out_root or ml_csv.parent) / f"{ml_rootname}_{device}_BioPacRNS_events.csv"
            if combined_csv.exists():
                labels = []
                if bio_merged.exists():
                    labels.append("BioPac")
                if rns_merged.exists():
                    labels.append("RNS")
                if len(labels) >= 2:
                    labels.append("Combined")
                print('starting cmd5, summarize_drift2 with merged BioPac & RNS files')

                cmd5 = ["python", str(summarize), "--merged_ml_csv", str(combined_csv)]
                if labels:
                    cmd5 += ["--labels", ",".join(labels)]
                else:
                    cmd5 = None

                if cmd5:
                    if args.debug:
                        print("[cmd]", " ".join(shlex.quote(x) for x in cmd5))
                    if args.dry_run:
                        attempted += 1
                        ok += 1
                    else:
                        try:
                            attempted += 1
                            res = subprocess.run(cmd5, check=True, capture_output=not args.debug, text=True)
                            if not args.debug and res.stdout:
                                print(res.stdout.strip())
                            ok += 1
                        except subprocess.CalledProcessError as e:
                            print(f"[fail] {' '.join(cmd5)}\n{e.stdout or ''}\n{e.stderr or ''}")

    print("\n=== Batch split pipeline summary ===")
    print(f"steps attempted: {attempted}")
    print(f"steps ok:        {ok}")
    print(f"steps failed:    {attempted - ok}")

    if RESOLVE_ML_CSV_BADLIST: 
        resolve_df = pd.DataFrame(RESOLVE_ML_CSV_BADLIST, columns=["resolve_ml_csv_fails"])
        resolve_csv = debug_dir / "resolve_ml_fails.csv"
        resolve_df.to_csv(resolve_csv, index=False)
        print(f"There were {len(RESOLVE_ML_CSV_BADLIST)} resolve_ml_csv failures. Saved list to {resolve_csv}")

    if ML_FILE_FAIL_LIST: 
        fail_df = pd.DataFrame(ML_FILE_FAIL_LIST, columns=["ml_file_fails"])
        fail_csv = debug_dir / "ml_file_fails.csv"
        fail_df.to_csv(fail_csv, index=False)
        print(f"There were {len(ML_FILE_FAIL_LIST)} ml file failures. Saved list to {fail_csv}")

    if NO_MARKS_LIST: 
        nomarks_df = pd.DataFrame(NO_MARKS_LIST, columns=["no_marks_files"])
        nomarks_csv = debug_dir / "no_marks_files.csv"
        nomarks_df.to_csv(nomarks_csv, index=False)
        print(f"There were {len(NO_MARKS_LIST)} files that were completely missing marks. Saved list to {nomarks_csv}")

    if MISSING_LIKE_LIST: 
        missingLike_df = pd.DataFrame(MISSING_LIKE_LIST, columns=["missingLikes"])
        missingLike_csv = debug_dir / "missingLikes.csv"
        missingLike_df.to_csv(missingLike_csv, index=False)
        print(f"There were {len(MISSING_LIKE_LIST)} missing like list failures. Saved list to {missingLike_csv}")

    if LOG_MISSING_LIST: 
        missingLog_df = pd.DataFrame(LOG_MISSING_LIST, columns=["missingLogs"])
        missingLog_csv = debug_dir / "missingLogs.csv"
        missingLog_df.to_csv(missingLog_csv, index=False)
        print(f"There were {len(LOG_MISSING_LIST)} missing logs. Saved list to {missingLog_csv}")


if __name__ == "__main__":
    main()
