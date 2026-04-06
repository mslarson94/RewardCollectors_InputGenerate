# file: rpi_preproc_pipeline.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
RPi preprocessing pipeline.

Per ML row it emits under <out_root or ml_csv.parent>/RPi_preproc/:
  RPi_simple/   <Label>/RPiMarks/<ml_root>_<device>_<Label>_RPiMarks.csv        (from extractor)
  RPi_verb/     <Label>/RPiMarks/<ml_root>_<device>_<Label>_RPi_verb.csv        (from *_verb.csv)
  RPi_unified/  <Label>/<ml_root>_<device>_<Label>_RPi_unified.csv              (outer-join simple+verb)

- Orphans from either side are kept; missing fields filled with "unknown" and warned.
- Uses: extract_rpi_marks3.py (or 2), translate_verb_log.py, summarize_verb_marks.py, verb_to_rpi_marks.py, unify_rpi_marks.py
- Does NOT merge with ML; that’s a separate pipeline.
"""

from __future__ import annotations

import argparse
import re
import shlex
import subprocess
from pathlib import Path

import pandas as pd

from batchAlignHelpers import (
    _parse_device_ip_map,
    _to_session_date,
    _normalize_ml_stem,
    _missing_like,
    _resolve_ml_csv,
)

# -------------------------------
# CLI
# -------------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="RPi preprocessing pipeline (simple + verbose → unified marks)")
    ap.add_argument("--collated", required=True)
    ap.add_argument("--device-ip-map", required=True)
    ap.add_argument("--code-dir", required=True)
    ap.add_argument("--base-dir", required=True)
    ap.add_argument("--proc-dir", default="FreshStart")
    ap.add_argument("--events-dir-name", default="Events_Final_NoWalks")
    ap.add_argument("--sheet", default="MagicLeapFiles")
    ap.add_argument("--out-dir", default="", help="Write outputs under <base>/<proc>/full/<out-dir>/RPi_preproc/... (default: next to each ML CSV)")
    ap.add_argument("--strip-ml-suffixes", default="_events_final,_processed")

    # Simple extractor tuning
    ap.add_argument("--timezone-offset", default="auto")
    ap.add_argument("--allow-day-rollover", action="store_true")
    ap.add_argument("--dedupe-sec", default="0.05")

    # Verb tuning
    ap.add_argument("--marks-timestamp-col", default="RPi_Time_verb",
                    help="Verbose time column to adopt as canonical marks time")

    # General
    ap.add_argument("--only-rows-with-rpi", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--debug", action="store_true")

    return ap.parse_args()


# -------------------------------
# Helpers
# -------------------------------

def _split_names(cell: str) -> list[str]:
    return [nm for nm in re.split(r"[;,\s]+", (cell or "").strip()) if nm]


def _run(cmd: list[str], debug: bool, dry: bool) -> bool:
    if debug:
        print("[cmd]", " ".join(shlex.quote(x) for x in cmd))
    if dry:
        return True
    try:
        res = subprocess.run(cmd, check=True, capture_output=not debug, text=True)
        if not debug and res.stdout:
            print(res.stdout.strip())
        return True
    except subprocess.CalledProcessError as e:
        print(f"[fail] {' '.join(cmd)}\n{e.stdout or ''}\n{e.stderr or ''}")
        return False


def _paths_for_ml(base_dir: Path, proc_dir: str, events_dir_name: str) -> tuple[Path, Path]:
    ml_root = base_dir / proc_dir / "full" / events_dir_name / "augmented"
    rpi_root = base_dir / proc_dir / "RawData"
    return ml_root, rpi_root


def _fmt_cmd(cmd): 
    import shlex
    return " ".join(shlex.quote(str(x)) for x in cmd)

# -------------------------------
# Main
# -------------------------------

def main() -> None:
    args = parse_args()

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

    ml_root_dir, rpi_root = _paths_for_ml(Path(args.base_dir), args.proc_dir, args.events_dir_name)

    # Scripts
    # Use your latest extractor name; fallback to v2 if you prefer.
    extract = code_dir / "extract_rpi_marks5.py"
    if not extract.exists():
        extract = code_dir / "extract_rpi_marks3.py"
    translate = code_dir / "translate_verb_log.py"
    summarize_verb = code_dir / "summarize_verb_marks.py"
    verb_to_marks = code_dir / "verb_to_rpi_marks.py"
    unify = code_dir / "unify_rpi_marks.py"

    for s in (extract, translate, summarize_verb, verb_to_marks, unify):
        if not s.exists():
            raise FileNotFoundError(s)

    attempted = ok = 0

    for _, row in df.iterrows():
        cleaned_raw = (row["cleanedFile"] or "").strip()
        if not cleaned_raw:
            continue

        # Resolve ML CSV
        try:
            ml_csv = _resolve_ml_csv(ml_root_dir, cleaned_raw, suffixes)
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
                # Per-source (BioPac, RNS)
        rpi_specs = (
            ("BioPac", (row["BioPac_RPi"] or "").strip(), "BioPac_RPi"),
            ("RNS",    (row["RNS_RPi"] or "").strip(),    "RNS_RPi"),
        )
        for label, fname, subdir in rpi_specs:
            names = _split_names(fname)
            if not names:
                print(f"[skip] {label}: no RPi files listed for ML {ml_csv.name}")
                continue
            # Target dirs
            base_out_root = Path(args.out_dir) if args.out_dir else ml_csv.parent
            preproc_root = base_out_root / "RPi_preproc"
            simple_root = preproc_root / label / "RPi_simple"
            verb_root = preproc_root / label / "RPi_verb"
            verbFull_root = preproc_root / label / "RPi_verb_full"
            unified_root = preproc_root / label / "RPi_unified"
            print(preproc_root, '\n', simple_root, '\n', verb_root)
            # for d in (simple_root, verb_root, unified_root):
            #     d.mkdir(parents=True, exist_ok=True)
            # now also create verbFull_root (for combined log + full CSV)
            for d in (simple_root, verb_root, verbFull_root, unified_root):
                d.mkdir(parents=True, exist_ok=True)

            ml_rootname = _normalize_ml_stem(ml_csv.stem, suffixes)
            print('starting the extract script (CSV-based)')

            # Collect per-log CSVs corresponding to the RPi .log names
            rpi_csv_files: list[str] = []
            for nm in names:
                # original .log path
                log_file = (
                    Path(args.base_dir)
                    / args.proc_dir
                    / "RPi_preproc"
                    / label
                    / "RPi_simple_raw"
                    / nm
                )
                # assume CSV sits next to the .log with the same stem
                csv_file = log_file.with_suffix(".csv")

                if not csv_file.exists():
                    #print(f"[skip] {label}: missing CSV for log {log_file} → expected {csv_file}")
                    continue

                rpi_csv_files.append(str(csv_file))

            if not rpi_csv_files:
                print(f"[skip] {label}: no RPi CSV files found for ML {ml_csv.name}")
            else:
                # --- SIMPLE: extractor -> simple marks into RPi_preproc/RPi_simple/<Label>/...
                cmd_simple = [
                    "python", str(extract),
                    "--session_date", session_date,
                    "--device", device,
                    "--device_ip", ip,
                    "--label", label,
                    "--ml_csv_file", str(ml_csv),
                    "--strip-ml-suffixes", ",".join(suffixes),
                    "--dedupe-sec", str(args.dedupe_sec),
                    "--timezone_offset_hours", args.timezone_offset,
                    "--out_dir", str(simple_root),
                ]
                if args.allow_day_rollover:
                    cmd_simple += ["--allow_day_rollover"]

                # add all RPi CSVs
                for csv_path in rpi_csv_files:
                    cmd_simple += ["--rpi_csv_file", csv_path]

                attempted += 1
                if _run(cmd_simple, args.debug, args.dry_run):
                    ok += 1

            # Expected simple marks path (from CSV extractor’s convention)
            simple_marks_csv = simple_root / f"{ml_rootname}_{label}_RPi_simple.csv"



            # # --- VERBOSE: translate -> summarize -> verb_to_rpi_marks (by IP)
            # # Keep a tidy substructure for diagnostics; you can simplify if you prefer.
            # verb_full  = verbFull_root / f"{ml_rootname}_{label}_RPi_verb_full.csv"
            # verb_short = verb_root / f"{ml_rootname}_{label}_RPi_verb.csv"

            # # Translate each *_verb.log into the same _verb_full.csv (last one wins if multiple)
            # for nm in names:
            #     verb_nm = nm.rsplit(".log", 1)[0] + "_verb.log"
            #     verb_dir = subdir.split("_RPi")[0] + "_verb"
            #     verb_log = Path(args.base_dir) / args.proc_dir / "RawData" / pair / tdate / sess / "RPi_verbose" / verb_dir / verb_nm
            #     if not verb_log.exists():
            #         print(f"[skip] {label} verbose log missing: {verb_log}")
            #         continue
            #     print('starting the translate script')
            #     cmd_t = [
            #         "python", str(translate),
            #         "--verb-log", str(verb_log),
            #         "--out-csv", str(verb_full),
            #         "--force-if-ml-rpi-mismatch",
            #     ]
            #     attempted += 1
            #     if _run(cmd_t, args.debug, args.dry_run): ok += 1

            # --- VERBOSE: CONCAT logs → translate once → summarize → verb_to_rpi_marks ----
            # Drop-in replacement block (concatenates multiple *_verb.log files like _simple does)

            # Paths for verbose outputs
            verb_full  = verbFull_root / f"{ml_rootname}_{label}_RPi_verb_full.csv"
            verb_short = verb_root     / f"{ml_rootname}_{label}_RPi_verb_short.csv"

            # Gather all candidate verbose logs that correspond to the simple “names”
            verb_logs = []
            for nm in names:
                # turn "<name>.log" into "<name>_verb.log" under the verbose tree
                verb_nm  = nm.rsplit(".log", 1)[0] + "_verb.log"
                verb_dir = subdir.split("_RPi")[0] + "_verb"
                p = Path(args.base_dir) / args.proc_dir / "RawData" / pair / tdate / sess / "RPi_verbose" / verb_dir / verb_nm
                if p.exists():
                    verb_logs.append(p)
                else:
                    print(f"[skip:{label}] missing verbose log: {p}")

            if verb_logs:
                # 1) Concatenate all *_verb.log into a single combined stream so mark numbering stays continuous
                combined_log = verbFull_root / f"{ml_rootname}_{label}_combined_verb.log"
                if not args.dry_run:
                    combined_log.parent.mkdir(parents=True, exist_ok=True)
                    with combined_log.open("w", encoding="utf-8") as outfh:
                        for vp in verb_logs:
                            with vp.open("r", encoding="utf-8", errors="replace") as infh:
                                txt = infh.read()
                                outfh.write(txt)
                                if txt and not txt.endswith("\n"):
                                    outfh.write("\n")

                # 2) translate_verb_log.py → *_verb_full.csv
                cmd_t = [
                    "python", str(translate),
                    "--verb-log", str(combined_log),
                    "--out-csv", str(verb_full),
                    "--force-if-ml-rpi-mismatch",
                ]
                attempted += 1
                if args.debug: print("[cmd]", _fmt_cmd(cmd_t))
                if args.dry_run:
                    ok += 1
                else:
                    try:
                        res = subprocess.run(cmd_t, check=True, capture_output=not args.debug, text=True)
                        if not args.debug and res.stdout: print(res.stdout.strip())
                        ok += 1
                    except subprocess.CalledProcessError as e:
                        print(f"[fail] {_fmt_cmd(cmd_t)}\n{e.stdout or ''}\n{e.stderr or ''}")

                # 3) summarize_verb_marks.py → *_verb.csv (short summary)
                if verb_full.exists() or args.dry_run:
                    cmd_s = ["python", str(summarize_verb), "--in-csv", str(verb_full), "--out-csv", str(verb_short)]
                    attempted += 1
                    if args.debug: print("[cmd]", _fmt_cmd(cmd_s))
                    if args.dry_run:
                        ok += 1
                    else:
                        try:
                            res = subprocess.run(cmd_s, check=True, capture_output=not args.debug, text=True)
                            if not args.debug and res.stdout: print(res.stdout.strip())
                            ok += 1
                        except subprocess.CalledProcessError as e:
                            print(f"[fail] {_fmt_cmd(cmd_s)}\n{e.stdout or ''}\n{e.stderr or ''}")
            else:
                print(f"[skip:{label}] no verbose logs located for ML={ml_csv.name}")

            verb_marks_csv = verb_root / f"{ml_rootname}_{label}_RPi_verb.csv"


            # # Summarize → _verb.csv (short, last row per mark, with _first backups retained there if you want)
            # if verb_full.exists() or args.dry_run:
            #     print('starting the summarize script')
            #     cmd_s = ["python", str(summarize_verb), "--in-csv", str(verb_full), "--out-csv", str(verb_short)]
            #     attempted += 1
            #     if _run(cmd_s, args.debug, args.dry_run): ok += 1
            # else:
            #     print(f"[skip] summarize: missing {verb_full}")

            print('checking that verb_marks_csv exists')
            # Convert summarized *_verb.csv → per-source marks (using the single IP we already have)
            #verb_marks_csv = verb_root / f"{ml_rootname}_{label}_RPi_verb.csv"
            if verb_short.exists() or args.dry_run:
                print('running verbs 2 marks')
                cmd_v = [
                    "python", str(verb_to_marks),
                    "--in-csv", str(verb_short),
                    "--ip", ip,                        # single IP from device_ip_map (no brackets)
                    "--session-date", session_date,
                    "--device", device,
                    "--rpi-source", label,
                    "--ml-csv-file", str(ml_csv),
                    "--out-dir", str(verb_root),       # under RPi_preproc/RPi_verb/<Label>/RPiMarks
                    "--timestamp-col", args.marks_timestamp_col,
                    "--strip-ml-suffixes", ",".join(suffixes),
                    "--dedupe-sec", str(args.dedupe_sec),
                ]
                if args.allow_day_rollover:
                    cmd_v += ["--allow-day-rollover"]
                attempted += 1
                if _run(cmd_v, args.debug, args.dry_run): ok += 1
            else:
                print(f"[skip] verb_to_rpi_marks: missing {verb_short}")

            print('attempting to start the unify script in the rpi_prepoc_pipeline')
            # --- UNIFY: simple + verb → RPi_unified
            unified_csv = unified_root / f"{ml_rootname}_{label}_RPi_unified.csv"
            #unified_csv.parent.mkdir(parents=True, exist_ok=True)
            cmd_u = [
                "python", str(unify),
                "--simple-marks", str(simple_marks_csv),
                "--verb-marks",   str(verb_marks_csv),
                "--out-csv", str(unified_csv),
                "--label", label,
                "--marks-timestamp-col", args.marks_timestamp_col,
                "--ml-csv-file", str(ml_csv),
                "--strip-ml-suffixes", ",".join(suffixes),
            ]
            attempted += 1
            if _run(cmd_u, args.debug, args.dry_run): ok += 1

    print("\n=== RPi preprocessing summary ===")
    print(f"steps attempted: {attempted}")
    print(f"steps ok:        {ok}")
    print(f"steps failed:    {attempted - ok}")


if __name__ == "__main__":
    main()
