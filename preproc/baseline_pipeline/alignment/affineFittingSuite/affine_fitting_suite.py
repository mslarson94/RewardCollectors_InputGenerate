# file: alignment/batch_split_pipeline4.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import shlex
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional
import sys

import pandas as pd

from RC_utilities.alignHelpers.batchAlignHelpers import (
    _missing_like,
    _normalize_ml_stem,
    _parse_device_ip_map,
    _resolve_ml_csv,
)


@dataclass
class StageResult:
    pair: str
    testing_date: str
    session_type: str
    device: str
    device_ip: str
    label: str
    ml_csv: str
    ml_rootname: str
    stage: str
    status: str
    message: str
    auto_matched_marks_csv: str
    hybrid_matched_marks_csv: str
    manual_matched_marks_csv: str
    auto_aligned_csv: str
    hybrid_aligned_csv: str
    manual_aligned_csv: str
    auto_combined_csv: str
    hybrid_combined_csv: str
    manual_combined_csv: str
    output_path: str


def fmt_cmd(cmd: list[str]) -> str:
    return " ".join(shlex.quote(str(x)) for x in cmd)


def run_cmd(cmd: list[str], debug: bool, dry_run: bool) -> tuple[bool, str]:
    if debug:
        print("[cmd]", fmt_cmd(cmd))

    if dry_run:
        return True, "[dry-run]"

    try:
        res = subprocess.run(
            cmd,
            check=True,
            capture_output=not debug,
            text=True,
        )
        stdout = (res.stdout or "").strip()
        return True, stdout
    except subprocess.CalledProcessError as exc:
        stdout = (exc.stdout or "").strip()
        stderr = (exc.stderr or "").strip()
        return False, "\n".join(
            part for part in (stdout, stderr) if part
        )


def record(
    all_results: list[StageResult],
    *,
    pair: str,
    testing_date: str,
    session_type: str,
    device: str,
    device_ip: str,
    label: str,
    ml_csv: Path,
    ml_rootname: str,
    stage: str,
    status: str,
    message: str,
    auto_matched_marks_csv: Path,
    hybrid_matched_marks_csv: Path,
    manual_matched_marks_csv: Path,
    auto_aligned_csv: Path,
    hybrid_aligned_csv: Path,
    manual_aligned_csv: Path,
    auto_combined_csv: Path,
    hybrid_combined_csv: Path,
    manual_combined_csv: Path,
    output_path: str = "",
) -> None:
    result = StageResult(
        pair=pair,
        testing_date=testing_date,
        session_type=session_type,
        device=device,
        device_ip=device_ip,
        label=label,
        ml_csv=str(ml_csv),
        ml_rootname=ml_rootname,
        stage=stage,
        status=status,
        message=message,
        auto_matched_marks_csv=str(auto_matched_marks_csv),
        hybrid_matched_marks_csv=str(hybrid_matched_marks_csv),
        manual_matched_marks_csv=str(manual_matched_marks_csv),
        auto_aligned_csv=str(auto_aligned_csv),
        hybrid_aligned_csv=str(hybrid_aligned_csv),
        manual_aligned_csv=str(manual_aligned_csv),
        auto_combined_csv=str(auto_combined_csv),
        hybrid_combined_csv=str(hybrid_combined_csv),
        manual_combined_csv=str(manual_combined_csv),
        output_path=output_path,
    )
    all_results.append(result)

    prefix = f"[{pair} | {testing_date} | {session_type} | {device} | {label} | {ml_rootname}] [{stage}] [{status}]"

    if output_path and message:
        print(f"{prefix} {message} -> {output_path}")
    elif message:
        print(f"{prefix} {message}")
    elif output_path:
        print(f"{prefix} {output_path}")
    else:
        print(prefix)


def summarize_results(all_results: list[StageResult]) -> None:
    print("\n=== Batch split pipeline summary ===")

    if not all_results:
        print("No stage results recorded.")
        return

    summary: dict[tuple[str, str], int] = {}

    for result in all_results:
        key = (result.stage, result.status)
        summary[key] = summary.get(key, 0) + 1

    for (stage, status), count in sorted(summary.items()):
        print(f"{stage:22s} {status:6s} {count}")
 

def write_stage_report(all_results: list[StageResult], report_csv: Optional[str]) -> None:
    if not report_csv:
        return

    out_path = Path(report_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame([asdict(x) for x in all_results]).to_csv(out_path, index=False)

    print(f"[report] wrote stage report -> {out_path}")


def _safe_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def main() -> None:
    ap = argparse.ArgumentParser(description="Batch global affine fitting from frozen automatic, hybrid, and manual ML/RPi mark correspondences.")

    ap.add_argument("--collated", required=True)
    ap.add_argument("--device-ip-map", required=True)
    ap.add_argument("--code-dir", required=True)
    ap.add_argument("--base-dir", required=True)

    ap.add_argument("--proc-dir", default="FreshStart")
    ap.add_argument("--events-dir-name", default="Events_Final_NoWalks")
    ap.add_argument("--csv-timestamp-column", default="mLT_orig")
    ap.add_argument("--event-type-column", default="lo_eventType")
    ap.add_argument("--sheet", default="MagicLeapFiles")
    ap.add_argument("--out-dir", default="")

    ap.add_argument("--strip_ml_suffixes", default="_events_final,_processed")
    ap.add_argument("--only-rows-with-rpi", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--debug", action="store_true")

    ap.add_argument("--blankRowTemplate", required=True)

    ap.add_argument("--sigma_clip", default="4.0")

    ap.add_argument("--stage-report-csv", default="")
    ap.add_argument("--rpi-preproc-dir", required=True, help="RPi preprocessing directory relative to <base-dir>/<proc-dir>.")
    args = ap.parse_args()

    suffixes = [
        s
        for s in (args.strip_ml_suffixes or "").split(",")
        if s
    ]

    rpi_preproc_arg = Path(args.rpi_preproc_dir)

    df = pd.read_excel(args.collated, sheet_name=args.sheet, dtype="string")

    required_cols = [
        "cleanedFile",
        "BioPac_RPi",
        "RNS_RPi",
        "pairID_py",
        "testingDate",
        "sessionType",
        "device",
    ]

    for col in required_cols:
        if col not in df.columns:
            raise KeyError(f"missing required column: {col}")

    if args.only_rows_with_rpi:
        before = len(df)
        df = df.loc[~(df["BioPac_RPi"].map(_missing_like) & df["RNS_RPi"].map(_missing_like))].reset_index(drop=True)

        print(f"[filter] --only-rows-with-rpi: {before} -> {len(df)} rows")

    ip_map = _parse_device_ip_map(Path(args.device_ip_map))

    code_dir = Path(args.code_dir)

    global_affine_script = code_dir / "fit_global_affine_from_matches.py"
    summarize_script = code_dir / "summarize_alignment3.py"
    merge_both_script = code_dir / "merge_rpi_event_files2.py"
    

    required_scripts = [
        global_affine_script,
        summarize_script,
        merge_both_script,
    ]


    for script_path in required_scripts:
        if not script_path.exists():
            raise FileNotFoundError(script_path)

    ml_root = Path(args.base_dir) / args.proc_dir / args.events_dir_name
    

    out_root = None
    if args.out_dir:
        out_root = Path(args.base_dir) / args.proc_dir / rpi_preproc_arg / args.out_dir
        
        out_root.mkdir(parents=True, exist_ok=True)

    debug_dir = (out_root or Path.cwd()) / "debugging"
    
    debug_dir.mkdir(parents=True, exist_ok=True)

    all_results: list[StageResult] = []

    resolve_ml_fail_list: list[str] = []
    ml_file_fail_list: list[str] = []
    no_marks_list: list[str] = []
    missing_like_list: list[str] = []
    missing_matches_list: list[str] = []

    for _, row in df.iterrows():
        cleaned_raw = _safe_text(row["cleanedFile"])

        if not cleaned_raw:
            continue

        try:
            ml_csv = _resolve_ml_csv(
                ml_root,
                cleaned_raw,
                suffixes,
            )
        except FileNotFoundError as exc:
            msg = f"[skip] {exc}"
            resolve_ml_fail_list.append(msg)
            print(msg)
            continue

        pair = _safe_text(row["pairID_py"])
        testing_date = _safe_text(row["testingDate"])
        session_type = _safe_text(row["sessionType"])
        device = _safe_text(row["device"])
        device_ip = ip_map.get(device, "")

        if not device_ip:
            print(f"[skip] no IP for device {device}")
            continue

        try:
            tmp_ml = pd.read_csv(ml_csv, usecols=[args.event_type_column])

            has_mark = tmp_ml[args.event_type_column].astype(str).str.strip().str.lower().eq("mark").any()
            

        except Exception as exc:
            msg = f"[skip] failed reading ML file {ml_csv.name!r}: {exc}"
            ml_file_fail_list.append(msg)
            print(msg)
            continue

        if not has_mark:
            msg = f"[NO MARKS] ML CSV {ml_csv.name!r} has no rows where {args.event_type_column} == 'Mark'."
            no_marks_list.append(msg)
            print(msg)
            continue

        target_dir = (out_root or ml_csv.parent)
        aligned_root = target_dir / "affineFitData"
        aligned_root.mkdir(parents=True, exist_ok=True)


        ml_rootname = _normalize_ml_stem(ml_csv.stem, suffixes,)

        bio_current_ok = {
            "auto": False,
            "hybrid": False,
            "manual": False,
        }

        rns_current_ok = {
            "auto": False,
            "hybrid": False,
            "manual": False,
        }

        methods = ("auto", "hybrid", "manual")

        combined_csv_dict = {
            method: aligned_root / method / "BioPacRNS" / f"{ml_rootname}_BioPacRNS_{device}_events.csv"
            for method in methods
        }

        for combined_csv in combined_csv_dict.values():
            combined_csv.parent.mkdir(parents=True, exist_ok=True)

        bio_aligned_by_method: dict[str, Path] = {}
        rns_aligned_by_method: dict[str, Path] = {}

        source_specs = [("BioPac", "BioPac_RPi"), ("RNS", "RNS_RPi")]

        for label, source_col in source_specs:
            fname = _safe_text(row[source_col])
            

            auto_matched_marks_csv =  target_dir / "MarkMatching" / "auto" / label  / f"{ml_rootname}_{label}_{device}_matched_marks.csv"
            hybrid_matched_marks_csv =  target_dir / "MarkMatching" / "hybrid" / label  / f"{ml_rootname}_{label}_{device}_matched_marks.csv"
            manual_matched_marks_csv =  target_dir / "MarkMatching" / "manual" / label  / f"{ml_rootname}_{label}_{device}_matched_marks.csv"
            
            matched_marks_dict = {
                "auto": auto_matched_marks_csv,
                "hybrid": hybrid_matched_marks_csv,
                "manual": manual_matched_marks_csv,
            }

            aligned_csv_dict = {
                method: (aligned_root / method / label / f"{ml_rootname}_{label}_{device}_aligned_with_RPi.csv")
                for method in matched_marks_dict
            }

            for aligned_csv in aligned_csv_dict.values():
                aligned_csv.parent.mkdir(parents=True, exist_ok=True)


            if _missing_like(fname):
                msg = f"no {label} RPi file listed for ML CSV: {ml_csv.name}"
                
                missing_like_list.append(msg)

                record(
                    all_results,
                    pair=pair,
                    testing_date=testing_date,
                    session_type=session_type,
                    device=device,
                    device_ip=device_ip,
                    label=label,
                    ml_csv=ml_csv,
                    ml_rootname=ml_rootname,
                    stage="global_affine",
                    status="skip",
                    message=msg,

                    auto_matched_marks_csv=auto_matched_marks_csv,
                    hybrid_matched_marks_csv=hybrid_matched_marks_csv,
                    manual_matched_marks_csv=manual_matched_marks_csv,
                    auto_aligned_csv=aligned_csv_dict["auto"],
                    hybrid_aligned_csv=aligned_csv_dict["hybrid"],
                    manual_aligned_csv=aligned_csv_dict["manual"],
                    auto_combined_csv=combined_csv_dict["auto"],
                    hybrid_combined_csv=combined_csv_dict["hybrid"],
                    manual_combined_csv=combined_csv_dict["manual"],
                )
                continue





            # -------------------------------------------------------------
            # Stage 1: final global affine fit from frozen correspondences.
            # -------------------------------------------------------------
            for method, matched_marks_csv in matched_marks_dict.items():
                if (not args.dry_run and not matched_marks_csv.exists()):
                    msg = f"missing {method} matched mark pairs CSV: {matched_marks_csv}"
                    
                    missing_matches_list.append(msg)

                    record(
                        all_results,
                        pair=pair,
                        testing_date=testing_date,
                        session_type=session_type,
                        device=device,
                        device_ip=device_ip,
                        label=label,
                        ml_csv=ml_csv,
                        ml_rootname=ml_rootname,
                        stage="global_affine",
                        status="skip",
                        message=msg,
                        
                        auto_matched_marks_csv=auto_matched_marks_csv,
                        hybrid_matched_marks_csv=hybrid_matched_marks_csv,
                        manual_matched_marks_csv=manual_matched_marks_csv,
                        auto_aligned_csv=aligned_csv_dict["auto"],
                        hybrid_aligned_csv=aligned_csv_dict["hybrid"],
                        manual_aligned_csv=aligned_csv_dict["manual"],
                        auto_combined_csv=combined_csv_dict["auto"],
                        hybrid_combined_csv=combined_csv_dict["hybrid"],
                        manual_combined_csv=combined_csv_dict["manual"],
                    )
                    continue

                cmd_global = [
                    sys.executable,
                    str(global_affine_script),
                    "--ml_csv_file",
                    str(ml_csv),
                    "--matched_marks_csv",
                    str(matched_marks_csv),
                    "--csv_timestamp_column",
                    args.csv_timestamp_column,
                    "--label",
                    label,
                    "--device",
                    device,
                    "--blankRowTemplate",
                    str(args.blankRowTemplate),
                    "--sigma_clip",
                    args.sigma_clip,
                    "--strip_ml_suffixes",
                    ",".join(suffixes),
                    "--out_dir",
                    str(aligned_csv_dict[method].parent),

                ]


                ok_global, msg_global = run_cmd(
                    cmd_global,
                    args.debug,
                    args.dry_run,
                )

                record(
                    all_results,
                    pair=pair,
                    testing_date=testing_date,
                    session_type=session_type,
                    device=device,
                    device_ip=device_ip,
                    label=label,
                    ml_csv=ml_csv,
                    ml_rootname=ml_rootname,
                    stage="global_affine",
                    status=(
                        "ok"
                        if ok_global
                        else "fail"
                    ),
                    message=(msg_global or f"global affine complete | {method} matched marks"),
                    
                    auto_matched_marks_csv=auto_matched_marks_csv,
                    hybrid_matched_marks_csv=hybrid_matched_marks_csv,
                    manual_matched_marks_csv=manual_matched_marks_csv,
                    auto_aligned_csv=aligned_csv_dict["auto"],
                    hybrid_aligned_csv=aligned_csv_dict["hybrid"],
                    manual_aligned_csv=aligned_csv_dict["manual"],
                    auto_combined_csv=combined_csv_dict["auto"],
                    hybrid_combined_csv=combined_csv_dict["hybrid"],
                    manual_combined_csv=combined_csv_dict["manual"],
                    output_path=(
                        str(aligned_csv_dict[method])
                        if (
                            args.dry_run
                            or aligned_csv_dict[method].exists()
                        )
                        else ""
                    ),
                )

                if not ok_global:
                    continue

                if not args.dry_run and not aligned_csv_dict[method].exists():
                    record(
                        all_results,
                        pair=pair,
                        testing_date=testing_date,
                        session_type=session_type,
                        device=device,
                        device_ip=device_ip,
                        label=label,
                        ml_csv=ml_csv,
                        ml_rootname=ml_rootname,
                        stage="global_affine_output_check",
                        status="fail",
                        message=f"global affine command succeeded but aligned CSV was not created | {method} matched marks",
                        
                        auto_matched_marks_csv=auto_matched_marks_csv,
                        hybrid_matched_marks_csv=hybrid_matched_marks_csv,
                        manual_matched_marks_csv=manual_matched_marks_csv,
                        auto_aligned_csv=aligned_csv_dict["auto"],
                        hybrid_aligned_csv=aligned_csv_dict["hybrid"],
                        manual_aligned_csv=aligned_csv_dict["manual"],
                        auto_combined_csv=combined_csv_dict["auto"],
                        hybrid_combined_csv=combined_csv_dict["hybrid"],
                        manual_combined_csv=combined_csv_dict["manual"],
                    )
                    continue

                if label == "BioPac":
                    bio_current_ok[method] = True
                    bio_aligned_by_method[method] = aligned_csv_dict[method]

                elif label == "RNS":
                    rns_current_ok[method] = True
                    rns_aligned_by_method[method] = aligned_csv_dict[method]



                # -------------------------------------------------------------
                # Existing per-source summary stage.
                # -------------------------------------------------------------
                if (args.dry_run or aligned_csv_dict[method].exists()):
                    cmd_summarize = [
                        sys.executable,
                        str(summarize_script),
                        "--merged_ml_csv",
                        str(aligned_csv_dict[method]),
                        "--label",
                        label,
                        "--timeCol",
                        args.csv_timestamp_column,
                    ]

                    ok_sum, msg_sum = run_cmd(
                        cmd_summarize,
                        args.debug,
                        args.dry_run,
                    )

                    record(
                        all_results,
                        pair=pair,
                        testing_date=testing_date,
                        session_type=session_type,
                        device=device,
                        device_ip=device_ip,
                        label=label,
                        ml_csv=ml_csv,
                        ml_rootname=ml_rootname,
                        stage="summarize_source",
                        status=(
                            "ok"
                            if ok_sum
                            else "fail"
                        ),
                        message= msg_sum or "source summarize complete",
                        
                        auto_matched_marks_csv=auto_matched_marks_csv,
                        hybrid_matched_marks_csv=hybrid_matched_marks_csv,
                        manual_matched_marks_csv=manual_matched_marks_csv,
                        auto_aligned_csv=aligned_csv_dict["auto"],
                        hybrid_aligned_csv=aligned_csv_dict["hybrid"],
                        manual_aligned_csv=aligned_csv_dict["manual"],
                        auto_combined_csv=combined_csv_dict["auto"],
                        hybrid_combined_csv=combined_csv_dict["hybrid"],
                        manual_combined_csv=combined_csv_dict["manual"],
                        output_path=str(aligned_csv_dict[method]),
                    )

        # -------------------------------------------------------------
        # Existing combined BioPac/RNS event merge.
        # -------------------------------------------------------------
        for method in methods: 
            bio_exists = bio_current_ok[method]
            rns_exists = rns_current_ok[method]

            if bio_exists or rns_exists:
                cmd_merge_both = [
                    sys.executable,
                    str(merge_both_script),
                    "--csv_timestamp_column",
                    args.csv_timestamp_column,
                ]

                if bio_exists:
                    cmd_merge_both += ["--biopac_events_csv", str(bio_aligned_by_method[method])]

                if rns_exists:
                    cmd_merge_both += ["--rns_events_csv", str(rns_aligned_by_method[method])]

                
                cmd_merge_both += ["--out_dir", str(combined_csv_dict[method].parent)]

                ok_both, msg_both = run_cmd(
                    cmd_merge_both,
                    args.debug,
                    args.dry_run,
                )

                record(
                    all_results,
                    pair=pair,
                    testing_date=testing_date,
                    session_type=session_type,
                    device=device,
                    device_ip=device_ip,
                    label="BioPacRNS",
                    ml_csv=ml_csv,
                    ml_rootname=ml_rootname,
                    stage="merge_combined",
                    status=(
                        "ok"
                        if ok_both
                        else "fail"
                    ),
                    message=(
                        msg_both
                        or "combined merge complete"
                    ),
                    auto_matched_marks_csv=Path(""),
                    hybrid_matched_marks_csv=Path(""),
                    manual_matched_marks_csv=Path(""),
                    auto_aligned_csv=Path(""),
                    hybrid_aligned_csv=Path(""),
                    manual_aligned_csv=Path(""),
                    auto_combined_csv=combined_csv_dict["auto"],
                    hybrid_combined_csv=combined_csv_dict["hybrid"],
                    manual_combined_csv=combined_csv_dict["manual"],
                    output_path=(
                        str(combined_csv_dict[method])
                        if (
                            args.dry_run
                            or combined_csv_dict[method].exists()
                        )
                        else ""
                    ),
                )

                if (ok_both and ( args.dry_run or combined_csv_dict[method].exists())):
                    combined_labels = []

                    if bio_exists:
                        combined_labels.append("BioPac")

                    if rns_exists:
                        combined_labels.append("RNS")

                    for summary_label in combined_labels:
                        cmd_sum_combined = [
                            sys.executable,
                            str(summarize_script),
                            "--merged_ml_csv",
                            str(combined_csv_dict[method]),
                            "--label",
                            summary_label,
                            "--timeCol",
                            args.csv_timestamp_column,
                        ]

                        ok_sum_combined, msg_sum_combined = run_cmd(
                            cmd_sum_combined,
                            args.debug,
                            args.dry_run,
                        )

                        record(
                            all_results,
                            pair=pair,
                            testing_date=testing_date,
                            session_type=session_type,
                            device=device,
                            device_ip=device_ip,
                            label=summary_label,
                            ml_csv=ml_csv,
                            ml_rootname=ml_rootname,
                            stage="summarize_combined",
                            status=(
                                "ok"
                                if ok_sum_combined
                                else "fail"
                            ),
                            message=msg_sum_combined or "combined summarize complete",

                            auto_matched_marks_csv=Path(""),
                            hybrid_matched_marks_csv=Path(""),
                            manual_matched_marks_csv=Path(""),
                            auto_aligned_csv=Path(""),
                            hybrid_aligned_csv=Path(""),
                            manual_aligned_csv=Path(""),
                            auto_combined_csv=combined_csv_dict["auto"],
                            hybrid_combined_csv=combined_csv_dict["hybrid"],
                            manual_combined_csv=combined_csv_dict["manual"],
                            output_path=str(combined_csv_dict[method]),
                        )
                    

            else:
                record(
                    all_results,
                    pair=pair,
                    testing_date=testing_date,
                    session_type=session_type,
                    device=device,
                    device_ip=device_ip,
                    label="BioPacRNS",
                    ml_csv=ml_csv,
                    ml_rootname=ml_rootname,
                    stage="merge_combined",
                    status="skip",
                    message="no BioPac or RNS aligned outputs available",
                    
                    auto_matched_marks_csv=Path(""),
                    hybrid_matched_marks_csv=Path(""),
                    manual_matched_marks_csv=Path(""),
                    auto_aligned_csv=Path(""),
                    hybrid_aligned_csv=Path(""),
                    manual_aligned_csv=Path(""),
                    auto_combined_csv=combined_csv_dict["auto"],
                    hybrid_combined_csv=combined_csv_dict["hybrid"],
                    manual_combined_csv=combined_csv_dict["manual"],
                )



    summarize_results(all_results)

    write_stage_report(all_results, args.stage_report_csv or str(debug_dir / "batch_split_stage_report.csv"))

    if resolve_ml_fail_list:
        pd.DataFrame(resolve_ml_fail_list, columns=["resolve_ml_csv_fails"]).to_csv(debug_dir / "resolve_ml_fails.csv", index=False)

    if ml_file_fail_list:
        pd.DataFrame(ml_file_fail_list, columns=["ml_file_fails"]).to_csv(debug_dir / "ml_file_fails.csv", index=False)

    if no_marks_list:
        pd.DataFrame(no_marks_list, columns=["no_marks_files"]).to_csv(debug_dir / "no_marks_files.csv", index=False)

    if missing_like_list:
        pd.DataFrame(missing_like_list, columns=["missingLikes"]).to_csv(debug_dir / "missingLikes.csv", index=False)

    if missing_matches_list:
        pd.DataFrame(missing_matches_list, columns=["missingMarkMatches"]).to_csv(debug_dir / "missing_matches_list.csv", index=False)

if __name__ == "__main__":
    main()
