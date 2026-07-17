# file: alignment/batch_split_pipeline3.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import shlex
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from batchAlignHelpers import (
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
    rpi_unified_csv: str
    merged_csv: str
    combined_csv: str
    output_path: str


def fmt_cmd(cmd: list[str]) -> str:
    return " ".join(shlex.quote(str(x)) for x in cmd)


def run_cmd(cmd: list[str], debug: bool, dry_run: bool) -> tuple[bool, str]:
    if debug:
        print("[cmd]", fmt_cmd(cmd))
    if dry_run:
        return True, "[dry-run]"
    try:
        res = subprocess.run(cmd, check=True, capture_output=not debug, text=True)
        stdout = (res.stdout or "").strip()
        return True, stdout
    except subprocess.CalledProcessError as e:
        stdout = (e.stdout or "").strip()
        stderr = (e.stderr or "").strip()
        return False, "\n".join(part for part in [stdout, stderr] if part)


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
    rpi_unified_csv: Path,
    merged_csv: Path,
    combined_csv: Path,
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
        rpi_unified_csv=str(rpi_unified_csv),
        merged_csv=str(merged_csv),
        combined_csv=str(combined_csv),
        output_path=output_path,
    )
    all_results.append(result)

    prefix = (
        f"[{pair} | {testing_date} | {session_type} | "
        f"{device} | {label} | {ml_rootname}] [{stage}] [{status}]"
    )
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
        print(f"{stage:18s} {status:6s} {count}")


def write_stage_report(all_results: list[StageResult], report_csv: Optional[str]) -> None:
    if not report_csv:
        return
    out_path = Path(report_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([asdict(x) for x in all_results]).to_csv(out_path, index=False)
    print(f"[report] wrote stage report -> {out_path}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Batch run split alignment pipeline over collatedData.xlsx"
    )
    ap.add_argument("--collated", required=True)
    ap.add_argument("--device-ip-map", required=True)
    ap.add_argument("--code-dir", required=True)
    ap.add_argument("--base-dir", required=True)
    ap.add_argument("--proc-dir", default="FreshStart")
    ap.add_argument("--events-dir-name", default="Events_Final_NoWalks")
    ap.add_argument("--csv-timestamp-column", default="eMLT_orig")
    ap.add_argument("--event-type-column", default="lo_eventType")
    ap.add_argument("--timezone-offset", default="auto")
    ap.add_argument("--sheet", default="MagicLeapFiles")
    ap.add_argument("--out-dir", default="")
    ap.add_argument("--strip-ml-suffixes", default="_events_final,_processed")
    ap.add_argument("--only-rows-with-rpi", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--blankRowTemplate", required=True)
    ap.add_argument("--dedupesec", default="0.05")
    ap.add_argument("--maxmatchgaps", default="1.0")
    ap.add_argument("--stage-report-csv", default="")
    args = ap.parse_args()

    suffixes = [s for s in (args.strip_ml_suffixes or "").split(",") if s]

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
        df = df.loc[
            ~(df["BioPac_RPi"].map(_missing_like) & df["RNS_RPi"].map(_missing_like))
        ].reset_index(drop=True)
        print(f"[filter] --only-rows-with-rpi: {before} → {len(df)} rows")

    ip_map = _parse_device_ip_map(Path(args.device_ip_map))
    code_dir = Path(args.code_dir)

    ml_root = Path(args.base_dir) / args.proc_dir / args.events_dir_name

    out_root = None
    if args.out_dir:
        out_root = Path(args.base_dir) / args.proc_dir / args.out_dir
        out_root.mkdir(parents=True, exist_ok=True)

    debug_dir = (out_root or Path.cwd()) / "debugging"
    debug_dir.mkdir(parents=True, exist_ok=True)

    merge = code_dir / "merge_ml_with_rpi_marks3.py"
    summarize = code_dir / "summarize_drift2.py"
    merge_both = code_dir / "merge_rpi_event_files2.py"
    for script_path in (merge, summarize, merge_both):
        if not script_path.exists():
            raise FileNotFoundError(script_path)

    all_results: list[StageResult] = []

    resolve_ml_fail_list: list[str] = []
    ml_file_fail_list: list[str] = []
    no_marks_list: list[str] = []
    missing_like_list: list[str] = []
    missing_unified_list: list[str] = []

    for _, row in df.iterrows():
        cleaned_raw = (row["cleanedFile"] or "").strip()
        if not cleaned_raw:
            continue

        try:
            ml_csv = _resolve_ml_csv(ml_root, cleaned_raw, suffixes)
        except FileNotFoundError as e:
            msg = f"[skip] {e}"
            resolve_ml_fail_list.append(msg)
            print(msg)
            continue

        pair = (row["pairID_py"] or "").strip()
        testing_date = (row["testingDate"] or "").strip()
        session_type = (row["sessionType"] or "").strip()
        device = (row["device"] or "").strip()
        device_ip = ip_map.get(device, "")

        if not device_ip:
            msg = f"[skip] no IP for device {device}"
            print(msg)
            continue

        tz_cell = str(row.get("timezoneOffsetHours", "") or "").strip()
        tz_arg = tz_cell if tz_cell and not _missing_like(tz_cell) else args.timezone_offset

        try:
            tmp_ml = pd.read_csv(ml_csv, usecols=[args.event_type_column])
            has_mark = (
                tmp_ml[args.event_type_column]
                .astype(str)
                .str.strip()
                .str.lower()
                .eq("mark")
                .any()
            )
        except Exception as e:
            msg = f"[skip] failed reading ML file '{ml_csv.name}': {e}"
            ml_file_fail_list.append(msg)
            print(msg)
            continue

        if not has_mark:
            msg = (
                f"🚫 [NO MARKS] ML CSV '{ml_csv.name}' has no rows where "
                f"{args.event_type_column} == 'Mark'. Skipping this row."
            )
            no_marks_list.append(msg)
            print(msg)
            continue

        target_dir = out_root or ml_csv.parent
        ml_rootname = _normalize_ml_stem(ml_csv.stem, suffixes)

        bio_merged = target_dir / f"{ml_rootname}_BioPac_{device}_aligned_with_RPi.csv"
        rns_merged = target_dir / f"{ml_rootname}_RNS_{device}_aligned_with_RPi.csv"
        combined_csv = target_dir / "BioPacRNS" / f"{ml_rootname}_{device}_BioPacRNS_events.csv"

        source_specs = [
            ("BioPac", "BioPac_RPi", bio_merged),
            ("RNS", "RNS_RPi", rns_merged),
        ]

        for label, source_col, merged_csv in source_specs:
            fname = (row[source_col] or "").strip()
            rpi_unified_csv = (
                Path(args.base_dir)
                / args.proc_dir
                / "RPi_preproc"
                / label
                / "RPi_unified"
                / f"{ml_rootname}_{label}_RPi_unified.csv"
            )

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
                    stage="merge",
                    status="skip",
                    message=msg,
                    rpi_unified_csv=rpi_unified_csv,
                    merged_csv=merged_csv,
                    combined_csv=combined_csv,
                )
                continue

            if not args.dry_run and not rpi_unified_csv.exists():
                msg = f"missing unified RPi marks CSV: {rpi_unified_csv}"
                missing_unified_list.append(msg)
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
                    stage="merge",
                    status="skip",
                    message=msg,
                    rpi_unified_csv=rpi_unified_csv,
                    merged_csv=merged_csv,
                    combined_csv=combined_csv,
                )
                continue

            cmd_merge = [
                "python",
                str(merge),
                "--ml_csv_file",
                str(ml_csv),
                "--rpi_marks_csv",
                str(rpi_unified_csv),
                "--csv_timestamp_column",
                args.csv_timestamp_column,
                "--event_type_column",
                args.event_type_column,
                "--event_type_values",
                "Mark",
                "--label",
                label,
                "--device",
                device,
                "--strip-ml-suffixes",
                ",".join(suffixes),
                "--timezone_offset_hours",
                tz_arg,
                "--blankRowTemplate",
                str(args.blankRowTemplate),
                "--max_match_gap_s",
                args.maxmatchgaps,
            ]
            if out_root is not None:
                cmd_merge += ["--out_dir", str(out_root)]

            ok_merge, msg_merge = run_cmd(cmd_merge, args.debug, args.dry_run)
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
                stage="merge",
                status="ok" if ok_merge else "fail",
                message=msg_merge or "merge complete",
                rpi_unified_csv=rpi_unified_csv,
                merged_csv=merged_csv,
                combined_csv=combined_csv,
                output_path=str(merged_csv) if (args.dry_run or merged_csv.exists()) else "",
            )

            if not ok_merge:
                continue

            if args.dry_run or merged_csv.exists():
                cmd_summarize = [
                    "python",
                    str(summarize),
                    "--merged_ml_csv",
                    str(merged_csv),
                    "--label",
                    label,
                ]
                ok_sum, msg_sum = run_cmd(cmd_summarize, args.debug, args.dry_run)
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
                    status="ok" if ok_sum else "fail",
                    message=msg_sum or "source summarize complete",
                    rpi_unified_csv=rpi_unified_csv,
                    merged_csv=merged_csv,
                    combined_csv=combined_csv,
                    output_path=str(merged_csv),
                )
            else:
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
                    status="skip",
                    message=f"merged CSV not found after merge: {merged_csv}",
                    rpi_unified_csv=rpi_unified_csv,
                    merged_csv=merged_csv,
                    combined_csv=combined_csv,
                )

        bio_exists = args.dry_run or bio_merged.exists()
        rns_exists = args.dry_run or rns_merged.exists()

        if bio_exists or rns_exists:
            cmd_merge_both = ["python", str(merge_both)]
            if bio_exists:
                cmd_merge_both += ["--biopac_events_csv", str(bio_merged)]
            if rns_exists:
                cmd_merge_both += ["--rns_events_csv", str(rns_merged)]
            if out_root is not None:
                cmd_merge_both += ["--out_dir", str(out_root)]

            ok_both, msg_both = run_cmd(cmd_merge_both, args.debug, args.dry_run)
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
                status="ok" if ok_both else "fail",
                message=msg_both or "combined merge complete",
                rpi_unified_csv=Path(""),
                merged_csv=Path(""),
                combined_csv=combined_csv,
                output_path=str(combined_csv) if (args.dry_run or combined_csv.exists()) else "",
            )

            if ok_both and (args.dry_run or combined_csv.exists()):
                labels = []
                if bio_exists:
                    labels.append("BioPac")
                if rns_exists:
                    labels.append("RNS")
                if len(labels) >= 2:
                    labels.append("Combined")

                cmd_sum_combined = [
                    "python",
                    str(summarize),
                    "--merged_ml_csv",
                    str(combined_csv),
                ]
                if labels:
                    cmd_sum_combined += ["--labels", ",".join(labels)]

                ok_sum_combined, msg_sum_combined = run_cmd(
                    cmd_sum_combined, args.debug, args.dry_run
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
                    stage="summarize_combined",
                    status="ok" if ok_sum_combined else "fail",
                    message=msg_sum_combined or "combined summarize complete",
                    rpi_unified_csv=Path(""),
                    merged_csv=Path(""),
                    combined_csv=combined_csv,
                    output_path=str(combined_csv),
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
                rpi_unified_csv=Path(""),
                merged_csv=Path(""),
                combined_csv=combined_csv,
            )

    summarize_results(all_results)
    write_stage_report(
        all_results,
        args.stage_report_csv or str(debug_dir / "batch_split_stage_report.csv"),
    )

    if resolve_ml_fail_list:
        pd.DataFrame(resolve_ml_fail_list, columns=["resolve_ml_csv_fails"]).to_csv(
            debug_dir / "resolve_ml_fails.csv",
            index=False,
        )
    if ml_file_fail_list:
        pd.DataFrame(ml_file_fail_list, columns=["ml_file_fails"]).to_csv(
            debug_dir / "ml_file_fails.csv",
            index=False,
        )
    if no_marks_list:
        pd.DataFrame(no_marks_list, columns=["no_marks_files"]).to_csv(
            debug_dir / "no_marks_files.csv",
            index=False,
        )
    if missing_like_list:
        pd.DataFrame(missing_like_list, columns=["missingLikes"]).to_csv(
            debug_dir / "missingLikes.csv",
            index=False,
        )
    if missing_unified_list:
        pd.DataFrame(missing_unified_list, columns=["missingUnified"]).to_csv(
            debug_dir / "missingUnified.csv",
            index=False,
        )


if __name__ == "__main__":
    main()