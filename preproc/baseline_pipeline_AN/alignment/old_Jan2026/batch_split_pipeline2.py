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

    extract = code_dir / "extract_rpi_marks2.py"
    merge = code_dir / "merge_ml_with_rpi_marks2.py"
    summarize = code_dir / "summarize_drift2.py"
    merge_both = code_dir / "merge_rpi_event_files2.py"
    translateVerb = code_dir / "translate_verb_log.py"
    sum_verb = code_dir / "summarize_verb_marks.py"
    for s in (extract, merge, summarize, merge_both):
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
            # log_file = rpi_root / pair / tdate / sess / "RPi" / subdir / fname
            # if not log_file.exists():
            #     print(f"[skip] {label} log missing: {log_file}")
            #     continue

            # Build list of log names from the cell
            print('fname', fname)
            #names = re.split(r"[;,]\\s*", fname) if fname else []
            names = re.split(r"[;,\s]+", fname.strip()) if fname else []
            print('names', names)
            names = [nm for nm in names if nm]

            print('starting cmd1, extract_rpi_marks2.py')
            cmd1 = [
                "python", str(extract),  # set extract = code_dir / "extract_rpi_marks2.py"
                "--session_date", session_date,
                "--device", device,
                "--device_ip", ip,
                "--label", label,
                "--ml_csv_file", str(ml_csv),
                "--strip-ml-suffixes", ",".join(suffixes),
                "--allow_day_rollover",
                "--timezone_offset_hours", tz_arg,
                "--dedupe-sec", args.dedupesec,
            ]
            if out_root is not None:
                cmd1 += ["--out_dir", str(out_root)]

            # append all --log_file args
            for nm in names:
                log_file = rpi_root / pair / tdate / sess / "RPi" / subdir / nm
                verb_nm = nm.split(".log")[0] + "_verb.log"
                verb_dir = subdir.split("_RPi")[0] + "_verb"
                verb_log_file = rpi_root / pair / tdate / sess / "RPi_verbose" / verb_dir / verb_nm
                print('🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛')
                print('nm: ', nm)
                print('log_file: ', log_file)
                print('verb_nm: ', verb_nm)

                print('verb_log_file: ', verb_log_file)
                if not log_file.exists():
                    missing_log_str = (f"[skip] {label} log missing: {log_file}")
                    print(missing_log_str)
                    LOG_MISSING_LIST.append(missing_log_str)
                    continue
                cmd1 += ["--log_file", str(log_file)]

            # --- drop-in replacement for your current verb-out build + translator loop ---

            # 4) PREP OUTPUT: just ML stem (no full absolute path)
            verb_out_base = (out_root or Path.cwd()) / label / "RPi_Verb"
            ml_stem = Path(ml_csv).stem
            #verb_out = verb_out_base / ml_stem
            verb_actual = ml_stem + "_" + label + "_RPi_verb_full.csv"
            verb_short = ml_stem + "_" + label + "_RPi_verb.csv"
            #verb_out.mkdir(parents=True, exist_ok=True)

            print("verb_out_base:", verb_out_base)
            #print("verb_out directory:", verb_out)

            # 5) RUN TRANSLATOR ON EACH *_verb.log (one call per file)
            for nm in names:
                verb_nm = nm.rsplit(".log", 1)[0] + "_verb.log"
                verb_dir = subdir.split("_RPi")[0] + "_verb"
                verb_log_file = rpi_root / pair / tdate / sess / "RPi_verbose" / verb_dir / verb_nm

                print("verb_nm:", verb_nm)
                print("verb_log_file:", verb_log_file)

                if not verb_log_file.exists():
                    msg = f"[skip] {label} verb log missing: {verb_log_file}"
                    print(msg)
                    LOG_MISSING_LIST.append(msg)
                    continue

                #verb_csv_name = verb_nm.rsplit(".log", 1)[0] + ".csv"
                verb_out_full_csv = verb_out_base / verb_actual
                verb_out_short_csv = verb_out_base / verb_short

                cmd1b = [
                    "python", str(translateVerb),
                    "--verb-log", str(verb_log_file),   # <-- translator flag name
                    "--out-csv", str(verb_out_full_csv),
                    "--force-if-ml-rpi-mismatch",
                ]

                if args.debug:
                    print("[cmd]", " ".join(shlex.quote(x) for x in cmd1b))
                if args.dry_run:
                    attempted += 1
                    ok += 1
                else:
                    try:
                        attempted += 1
                        res = subprocess.run(cmd1b, check=True, capture_output=not args.debug, text=True)
                        if not args.debug and res.stdout:
                            print(res.stdout.strip())
                        ok += 1
                    except subprocess.CalledProcessError as e:
                        print(f"[fail] {' '.join(cmd1b)}\n{e.stdout or ''}\n{e.stderr or ''}")

                print("verb_out_short_csv: ",verb_out_short_csv)
                if verb_out_full_csv.exists():
                    print('start cmd1c, summarize_verb_marks.py')
                    cmd1c = [
                        "python", str(sum_verb),
                        "--in-csv", str(verb_out_full_csv),
                        "--out-csv", str(verb_out_short_csv),
                    ]
                    if args.debug:
                        print("[cmd]", " ".join(shlex.quote(x) for x in cmd1c))
                    if args.dry_run:
                        attempted += 1; ok += 1
                    else:
                        try:
                            attempted += 1
                            res = subprocess.run(cmd1c, check=True, capture_output=not args.debug, text=True)
                            if not args.debug and res.stdout:
                                print(res.stdout.strip())
                            ok += 1
                        except subprocess.CalledProcessError as e:
                            print(f"[fail] {' '.join(cmd1c)}\n{e.stdout or ''}\n{e.stderr or ''}")
                else:
                    print(f"[skip] summarize: missing {verb_out_full_csv}")



            # 2a) merge
            print('starting cmd2, merge_ml_with_rpi_marks2.py')
            marks_csv = target_dir / label / "RPiMarks" / f"{ml_rootname}_{device}_{label}_RPiMarks.csv"
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

            for cmd in (cmd1, cmd2):
                if args.debug:
                    print("[cmd]", " ".join(shlex.quote(x) for x in cmd))
                if args.dry_run:
                    attempted += 1
                    ok += 1
                    continue
                try:
                    attempted += 1
                    res = subprocess.run(cmd, check=True, capture_output=not args.debug, text=True)
                    if not args.debug and res.stdout:
                        print(res.stdout.strip())
                    ok += 1
                except subprocess.CalledProcessError as e:
                    print(f"[fail] {' '.join(cmd)}\n{e.stdout or ''}\n{e.stderr or ''}")
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
