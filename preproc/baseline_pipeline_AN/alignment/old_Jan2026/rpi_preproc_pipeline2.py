#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rpi_preproc_pipeline.py — Clean and consistent RPi preprocessing orchestrator.

This script now correctly separates BioPac and RNS outputs under:
    RPi_preproc/BioPac/...
    RPi_preproc/RNS/...
"""

import argparse
import subprocess
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------
# Utility function
# ---------------------------------------------------------
def _run(cmd: list[str], debug: bool = False, dry: bool = False) -> bool:
    """Run a subprocess and report failures."""
    cmd_str = " ".join(str(x) for x in cmd)
    print(f"[cmd] {cmd_str}")
    if dry:
        print("[dry-run] Skipping execution")
        return True
    try:
        subprocess.run(cmd, check=True)
        print("[ok]")
        return True
    except subprocess.CalledProcessError as e:
        print(f"[fail] {cmd_str}\n{e}")
        return False


# ---------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Run full RPi preprocessing pipeline.")
    ap.add_argument("--ml_csv_file", required=True, help="Path to ML CSV file")
    ap.add_argument("--device", required=True, help="Device name (e.g. ML2A)")
    ap.add_argument("--device_ip", required=True, help="Device IP (e.g. 192.168.50.156)")
    ap.add_argument("--session_date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--rpi_logs", nargs="+", required=True, help="List of RPi log file(s)")
    ap.add_argument("--timezone_offset_hours", default="auto")
    ap.add_argument("--dedupe-sec", type=float, default=0.0)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--dry", action="store_true")
    args = ap.parse_args()

    ml_csv = Path(args.ml_csv_file)
    code_dir = Path(__file__).parent.resolve()
    out_root = ml_csv.parent / "RPi_preproc"

    extract_script = code_dir / "extract_rpi_marks4.py"
    translate_script = code_dir / "translate_verb_log.py"
    summarize_script = code_dir / "summarize_verb_marks.py"
    verb_to_rpi_script = code_dir / "verb_to_rpi_marks.py"
    unify_script = code_dir / "unify_rpi_marks.py"

    session_date = args.session_date
    device = args.device
    device_ip = args.device_ip
    ml_root = ml_csv.stem

    print(f"\n=== Starting RPi preprocessing for {device} ({session_date}) ===\n")

    # ---------------------------------------------------------
    # PROCESS EACH LABEL SEPARATELY
    # ---------------------------------------------------------
    for label in ["BioPac", "RNS"]:
        print(f"\n========== Processing {label} ==========\n")

        # 1️⃣  Extract simple marks
        simple_dir = out_root / label / "RPi_simple"
        simple_dir.mkdir(parents=True, exist_ok=True)
        extract_cmd = [
            "python", str(extract_script),
            "--log_file", *args.rpi_logs,
            "--session_date", session_date,
            "--device", device,
            "--device_ip", device_ip,
            "--label", label,
            "--ml_csv_file", str(ml_csv),
            "--timezone_offset_hours", args.timezone_offset_hours,
            "--dedupe-sec", str(args.dedupe_sec),
            "--out_dir", str(simple_dir)
        ]
        _run(extract_cmd, args.debug, args.dry)

        # 2️⃣  Translate verbose logs
        verb_full_dir = out_root / label / "RPi_verb_full"
        verb_full_dir.mkdir(parents=True, exist_ok=True)
        translate_cmd = [
            "python", str(translate_script),
            "--label", label,
            "--ml_csv_file", str(ml_csv),
            "--out_dir", str(verb_full_dir)
        ]
        _run(translate_cmd, args.debug, args.dry)

        # 3️⃣  Summarize verbose marks
        verb_dir = out_root / label / "RPi_verb"
        verb_dir.mkdir(parents=True, exist_ok=True)
        summarize_cmd = [
            "python", str(summarize_script),
            "--label", label,
            "--ml_csv_file", str(ml_csv),
            "--out_dir", str(verb_dir)
        ]
        _run(summarize_cmd, args.debug, args.dry)

        # 4️⃣  Convert verb summaries to per-IP RPi marks
        verb_to_rpi_cmd = [
            "python", str(verb_to_rpi_script),
            "--session-date", session_date,
            "--device", device,
            "--rpi-source", label,
            "--ml-csv-file", str(ml_csv),
            "--out-dir", str(verb_dir)
        ]
        _run(verb_to_rpi_cmd, args.debug, args.dry)

        # 5️⃣  Unify simple + verbose marks
        unified_dir = out_root / label / "RPi_unified"
        unified_dir.mkdir(parents=True, exist_ok=True)

        simple_csv = simple_dir / f"{ml_root}_{device}_{label}_RPiMarks.csv"
        verb_csv = verb_dir / f"{ml_root}_{device}_{label}_RPi_verb.csv"
        unified_csv = unified_dir / f"{ml_root}_{device}_{label}_RPi_unified.csv"

        unify_cmd = [
            "python", str(unify_script),
            "--device", device,
            "--label", label,
            "--simple-marks", str(simple_csv),
            "--verb-marks", str(verb_csv),
            "--ml-csv-file", str(ml_csv),
            "--out-csv", str(unified_csv)
        ]
        _run(unify_cmd, args.debug, args.dry)

        print(f"\n[done] Finished processing {label}\n")

    print("\n=== All processing complete! ===\n")


if __name__ == "__main__":
    main()
