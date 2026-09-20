#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

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
    rpi_marks_csv: str
    auto_matched_csv: str
    hybrid_matched_csv: str
    manual_matched_csv: str
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
            part
            for part in (stdout, stderr)
            if part
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
    rpi_marks_csv: Path,
    auto_matched_csv: Path,
    hybrid_matched_csv: Path,
    manual_matched_csv: Path,
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
        rpi_marks_csv=str(rpi_marks_csv),
        auto_matched_csv=str(auto_matched_csv),
        hybrid_matched_csv=str(hybrid_matched_csv),
        manual_matched_csv=str(manual_matched_csv),
        output_path=output_path,
    )

    all_results.append(result)

    prefix = (
        f"[{pair} | {testing_date} | {session_type} | "
        f"{device} | {label} | {ml_rootname}] "
        f"[{stage}] [{status}]"
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
    print("\n=== Mark matching suite summary ===")

    if not all_results:
        print("No stage results recorded.")
        return

    summary: dict[tuple[str, str], int] = {}

    for result in all_results:
        key = (result.stage, result.status)
        summary[key] = summary.get(key, 0) + 1

    for (stage, status), count in sorted(summary.items()):
        print(f"{stage:22s} {status:6s} {count}")


def write_stage_report(
    all_results: list[StageResult],
    report_csv: Optional[str],
) -> None:
    if not report_csv:
        return

    out_path = Path(report_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [asdict(x) for x in all_results]
    ).to_csv(
        out_path,
        index=False,
    )

    print(f"[report] wrote stage report -> {out_path}")


def _safe_text(value) -> str:
    if pd.isna(value):
        return ""

    return str(value).strip()


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Batch generation of frozen ML/RPi mark correspondences for "
            "automatic, manual-exclusion-assisted, and manual matching."
        )
    )

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

    ap.add_argument(
        "--strip_ml_suffixes",
        "--strip-ml-suffixes",
        dest="strip_ml_suffixes",
        default="_events_final,_processed",
    )

    ap.add_argument("--only-rows-with-rpi", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--debug", action="store_true")

    ap.add_argument("--initial_match_gap_s", default="1.0")
    ap.add_argument("--final_match_gap_s", default="0.35")
    ap.add_argument("--coarse_search_window_s", default="30.0")
    ap.add_argument("--sigma_clip", default="4.0")
    ap.add_argument("--burst_gap_s", default="30.0")
    ap.add_argument(
        "--manual_filter_time_tolerance_s",
        default="0.005",
    )

    ap.add_argument("--stage-report-csv", default="")

    ap.add_argument(
        "--rpi-preproc-dir",
        required=True,
        help=(
            "RPi preprocessing directory relative to "
            "<base-dir>/<proc-dir>."
        ),
    )

    ap.add_argument(
        "--rpi_time_type",
        required=True,
        help="Exact RPi timestamp column used by the matching scripts.",
    )

    args = ap.parse_args()

    suffixes = [
        s
        for s in (args.strip_ml_suffixes or "").split(",")
        if s
    ]

    rpi_preproc_arg = Path(args.rpi_preproc_dir)

    df = pd.read_excel(
        args.collated,
        sheet_name=args.sheet,
        dtype="string",
    )

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
            ~(
                df["BioPac_RPi"].map(_missing_like)
                & df["RNS_RPi"].map(_missing_like)
            )
        ].reset_index(drop=True)

        print(
            f"[filter] --only-rows-with-rpi: "
            f"{before} -> {len(df)} rows"
        )

    ip_map = _parse_device_ip_map(
        Path(args.device_ip_map)
    )

    code_dir = Path(args.code_dir)

    auto_matcher_script = (
        code_dir / "auto_match_ml_rpi_marks.py"
    )
    hybrid_matcher_script = (
        code_dir / "hybrid_match_ml_rpi_marks.py"
    )
    manual_matcher_script = (
        code_dir / "manual_match_ml_rpi_marks.py"
    )

    required_scripts = [
        auto_matcher_script,
        hybrid_matcher_script,
        manual_matcher_script,
    ]

    for script_path in required_scripts:
        if not script_path.exists():
            raise FileNotFoundError(script_path)

    ml_root = (
        Path(args.base_dir)
        / args.proc_dir
        / args.events_dir_name
    )

    out_root = None

    if args.out_dir:
        out_root = (
            Path(args.base_dir)
            / args.proc_dir
            / rpi_preproc_arg
            / args.out_dir
        )

        out_root.mkdir(
            parents=True,
            exist_ok=True,
        )

    debug_dir = (
        out_root
        or Path.cwd()
    ) / "debugging"

    debug_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    all_results: list[StageResult] = []

    resolve_ml_fail_list: list[str] = []
    ml_file_fail_list: list[str] = []
    no_marks_list: list[str] = []
    missing_like_list: list[str] = []
    missing_rpi_list: list[str] = []

    for _, row in df.iterrows():
        cleaned_raw = _safe_text(
            row["cleanedFile"]
        )

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

        pair = _safe_text(
            row["pairID_py"]
        )

        testing_date = _safe_text(
            row["testingDate"]
        )

        session_type = _safe_text(
            row["sessionType"]
        )

        device = _safe_text(
            row["device"]
        )

        device_ip = ip_map.get(
            device,
            "",
        )

        if not device_ip:
            print(
                f"[skip] no IP for device {device}"
            )
            continue

        try:
            tmp_ml = pd.read_csv(
                ml_csv,
                usecols=[args.event_type_column],
            )

            has_mark = (
                tmp_ml[args.event_type_column]
                .astype(str)
                .str.strip()
                .str.lower()
                .eq("mark")
                .any()
            )

        except Exception as exc:
            msg = (
                f"[skip] failed reading ML file "
                f"{ml_csv.name!r}: {exc}"
            )

            ml_file_fail_list.append(msg)
            print(msg)
            continue

        if not has_mark:
            msg = (
                f"[NO MARKS] ML CSV {ml_csv.name!r} "
                f"has no rows where "
                f"{args.event_type_column} == 'Mark'."
            )

            no_marks_list.append(msg)
            print(msg)
            continue

        target_dir = (
            out_root
            or ml_csv.parent
        )

        ml_rootname = _normalize_ml_stem(
            ml_csv.stem,
            suffixes,
        )

        source_specs = [
            ("BioPac", "BioPac_RPi"),
            ("RNS", "RNS_RPi"),
        ]

        for label, source_col in source_specs:
            fname = _safe_text(
                row[source_col]
            )

            rpi_marks_csv = (
                Path(args.base_dir)
                / args.proc_dir
                / rpi_preproc_arg
                / label
                / "RPi_unified"
                / f"{ml_rootname}_{label}_RPi_unified.csv"
            )

            mark_singles_csv = (
                Path(args.base_dir)
                / args.proc_dir
                / rpi_preproc_arg
                / f"markMatches_{label}"
                / f"{ml_rootname}_{label}_mark_singles.csv"
            )

            mark_pairs_csv = (
                Path(args.base_dir)
                / args.proc_dir
                / rpi_preproc_arg
                / f"markMatches_{label}"
                / f"{ml_rootname}_{label}_mark_matches.csv"
            )

            auto_dir = (
                target_dir
                / "MarkMatching"
                / "auto"
                / label
            )

            hybrid_dir = (
                target_dir
                / "MarkMatching"
                / "hybrid"
                / label
            )

            manual_dir = (
                target_dir
                / "MarkMatching"
                / "manual"
                / label
            )

            auto_dir.mkdir(
                parents=True,
                exist_ok=True,
            )

            hybrid_dir.mkdir(
                parents=True,
                exist_ok=True,
            )

            manual_dir.mkdir(
                parents=True,
                exist_ok=True,
            )

            auto_matched_csv = (
                auto_dir
                / f"{ml_rootname}_{label}_{device}_matched_marks.csv"
            )

            hybrid_matched_csv = (
                hybrid_dir
                / f"{ml_rootname}_{label}_{device}_matched_marks.csv"
            )

            manual_matched_csv = (
                manual_dir
                / f"{ml_rootname}_{label}_{device}_matched_marks.csv"
            )

            if _missing_like(fname):
                msg = (
                    f"no {label} RPi file listed for "
                    f"ML CSV: {ml_csv.name}"
                )

                missing_like_list.append(msg)

                for stage in (
                    "automatic_match",
                    "hybrid_match",
                    "manual_match",
                ):
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
                        stage=stage,
                        status="skip",
                        message=msg,
                        rpi_marks_csv=rpi_marks_csv,
                        auto_matched_csv=auto_matched_csv,
                        hybrid_matched_csv=hybrid_matched_csv,
                        manual_matched_csv=manual_matched_csv,
                    )

                continue

            if (
                not args.dry_run
                and not rpi_marks_csv.exists()
            ):
                msg = (
                    f"missing RPi marks CSV: "
                    f"{rpi_marks_csv}"
                )

                missing_rpi_list.append(msg)

                for stage in (
                    "automatic_match",
                    "hybrid_match",
                    "manual_match",
                ):
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
                        stage=stage,
                        status="skip",
                        message=msg,
                        rpi_marks_csv=rpi_marks_csv,
                        auto_matched_csv=auto_matched_csv,
                        hybrid_matched_csv=hybrid_matched_csv,
                        manual_matched_csv=manual_matched_csv,
                    )

                continue

            if not args.dry_run:
                auto_matched_csv.unlink(
                    missing_ok=True
                )

            # =========================================================
            # Automatic matching
            # =========================================================

            cmd_auto_match = [
                sys.executable,
                str(auto_matcher_script),
                "--ml_csv_file",
                str(ml_csv),
                "--rpi_marks_csv",
                str(rpi_marks_csv),
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
                "--strip_ml_suffixes",
                ",".join(suffixes),
                "--rpi_time_type",
                args.rpi_time_type,
                "--initial_match_gap_s",
                args.initial_match_gap_s,
                "--final_match_gap_s",
                args.final_match_gap_s,
                "--coarse_search_window_s",
                args.coarse_search_window_s,
                "--sigma_clip",
                args.sigma_clip,
                "--burst_gap_s",
                args.burst_gap_s,
                "--out_dir",
                str(auto_dir),
            ]

            ok_match_auto, msg_match_auto = run_cmd(
                cmd_auto_match,
                args.debug,
                args.dry_run,
            )

            if not ok_match_auto:
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
                    stage="automatic_match",
                    status="fail",
                    message=msg_match_auto,
                    rpi_marks_csv=rpi_marks_csv,
                    auto_matched_csv=auto_matched_csv,
                    hybrid_matched_csv=hybrid_matched_csv,
                    manual_matched_csv=manual_matched_csv,
                )

            elif (
                not args.dry_run
                and not auto_matched_csv.exists()
            ):
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
                    stage="automatic_match",
                    status="fail",
                    message=(
                        "matched-mark CSV not found "
                        "after automatic matcher"
                    ),
                    rpi_marks_csv=rpi_marks_csv,
                    auto_matched_csv=auto_matched_csv,
                    hybrid_matched_csv=hybrid_matched_csv,
                    manual_matched_csv=manual_matched_csv,
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
                    stage="automatic_match",
                    status="ok",
                    message=(
                        msg_match_auto
                        or "automatic matching complete"
                    ),
                    rpi_marks_csv=rpi_marks_csv,
                    auto_matched_csv=auto_matched_csv,
                    hybrid_matched_csv=hybrid_matched_csv,
                    manual_matched_csv=manual_matched_csv,
                    output_path=str(auto_matched_csv),
                )

            # =========================================================
            # Hybrid matching
            # =========================================================

            if (
                not args.dry_run
                and not mark_singles_csv.exists()
            ):
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
                    stage="hybrid_match",
                    status="skip",
                    message=(
                        f"missing mark_singles CSV: "
                        f"{mark_singles_csv}"
                    ),
                    rpi_marks_csv=rpi_marks_csv,
                    auto_matched_csv=auto_matched_csv,
                    hybrid_matched_csv=hybrid_matched_csv,
                    manual_matched_csv=manual_matched_csv,
                )

            else:
                cmd_hybrid_match = [
                    sys.executable,
                    str(hybrid_matcher_script),
                    "--ml_csv_file",
                    str(ml_csv),
                    "--rpi_marks_csv",
                    str(rpi_marks_csv),
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
                    "--strip_ml_suffixes",
                    ",".join(suffixes),
                    "--rpi_time_type",
                    args.rpi_time_type,
                    "--initial_match_gap_s",
                    args.initial_match_gap_s,
                    "--final_match_gap_s",
                    args.final_match_gap_s,
                    "--coarse_search_window_s",
                    args.coarse_search_window_s,
                    "--sigma_clip",
                    args.sigma_clip,
                    "--burst_gap_s",
                    args.burst_gap_s,
                    "--out_dir",
                    str(hybrid_dir),
                    "--manual_filter_time_tolerance_s",
                    args.manual_filter_time_tolerance_s,
                    "--mark_singles_csv",
                    str(mark_singles_csv),
                ]

                ok_match_hybrid, msg_match_hybrid = run_cmd(
                    cmd_hybrid_match,
                    args.debug,
                    args.dry_run,
                )

                if not ok_match_hybrid:
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
                        stage="hybrid_match",
                        status="fail",
                        message=msg_match_hybrid,
                        rpi_marks_csv=rpi_marks_csv,
                        auto_matched_csv=auto_matched_csv,
                        hybrid_matched_csv=hybrid_matched_csv,
                        manual_matched_csv=manual_matched_csv,
                    )

                elif (
                    not args.dry_run
                    and not hybrid_matched_csv.exists()
                ):
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
                        stage="hybrid_match",
                        status="fail",
                        message=(
                            "matched-mark CSV not found "
                            "after hybrid matcher"
                        ),
                        rpi_marks_csv=rpi_marks_csv,
                        auto_matched_csv=auto_matched_csv,
                        hybrid_matched_csv=hybrid_matched_csv,
                        manual_matched_csv=manual_matched_csv,
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
                        stage="hybrid_match",
                        status="ok",
                        message=(
                            msg_match_hybrid
                            or "hybrid matching complete"
                        ),
                        rpi_marks_csv=rpi_marks_csv,
                        auto_matched_csv=auto_matched_csv,
                        hybrid_matched_csv=hybrid_matched_csv,
                        manual_matched_csv=manual_matched_csv,
                        output_path=str(hybrid_matched_csv),
                    )

            # =========================================================
            # Manual matching
            # =========================================================

            manual_missing: list[str] = []

            if (
                not args.dry_run
                and not mark_singles_csv.exists()
            ):
                manual_missing.append(
                    f"mark_singles CSV: {mark_singles_csv}"
                )

            if (
                not args.dry_run
                and not mark_pairs_csv.exists()
            ):
                manual_missing.append(
                    f"mark_matches CSV: {mark_pairs_csv}"
                )

            if manual_missing:
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
                    stage="manual_match",
                    status="skip",
                    message=(
                        "missing manual annotation file(s): "
                        + "; ".join(manual_missing)
                    ),
                    rpi_marks_csv=rpi_marks_csv,
                    auto_matched_csv=auto_matched_csv,
                    hybrid_matched_csv=hybrid_matched_csv,
                    manual_matched_csv=manual_matched_csv,
                )

            else:
                cmd_manual_match = [
                    sys.executable,
                    str(manual_matcher_script),
                    "--ml_csv_file",
                    str(ml_csv),
                    "--rpi_marks_csv",
                    str(rpi_marks_csv),
                    "--manual_matches_csv",
                    str(mark_pairs_csv),
                    "--manual_singles_csv",
                    str(mark_singles_csv),
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
                    "--strip_ml_suffixes",
                    ",".join(suffixes),
                    "--rpi_time_type",
                    args.rpi_time_type,
                    "--burst_gap_s",
                    args.burst_gap_s,
                    "--manual_time_tolerance_s",
                    args.manual_filter_time_tolerance_s,
                    "--out_dir",
                    str(manual_dir),
                ]

                ok_match_manual, msg_match_manual = run_cmd(
                    cmd_manual_match,
                    args.debug,
                    args.dry_run,
                )

                if not ok_match_manual:
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
                        stage="manual_match",
                        status="fail",
                        message=msg_match_manual,
                        rpi_marks_csv=rpi_marks_csv,
                        auto_matched_csv=auto_matched_csv,
                        hybrid_matched_csv=hybrid_matched_csv,
                        manual_matched_csv=manual_matched_csv,
                    )

                elif (
                    not args.dry_run
                    and not manual_matched_csv.exists()
                ):
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
                        stage="manual_match",
                        status="fail",
                        message=(
                            "matched-mark CSV not found "
                            "after manual matcher"
                        ),
                        rpi_marks_csv=rpi_marks_csv,
                        auto_matched_csv=auto_matched_csv,
                        hybrid_matched_csv=hybrid_matched_csv,
                        manual_matched_csv=manual_matched_csv,
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
                        stage="manual_match",
                        status="ok",
                        message=(
                            msg_match_manual
                            or "manual matching complete"
                        ),
                        rpi_marks_csv=rpi_marks_csv,
                        auto_matched_csv=auto_matched_csv,
                        hybrid_matched_csv=hybrid_matched_csv,
                        manual_matched_csv=manual_matched_csv,
                        output_path=str(manual_matched_csv),
                    )

    summarize_results(
        all_results
    )

    write_stage_report(
        all_results,
        args.stage_report_csv
        or str(
            debug_dir
            / "mark_matching_stage_report.csv"
        ),
    )

    if resolve_ml_fail_list:
        pd.DataFrame(
            resolve_ml_fail_list,
            columns=["resolve_ml_csv_fails"],
        ).to_csv(
            debug_dir / "resolve_ml_fails.csv",
            index=False,
        )

    if ml_file_fail_list:
        pd.DataFrame(
            ml_file_fail_list,
            columns=["ml_file_fails"],
        ).to_csv(
            debug_dir / "ml_file_fails.csv",
            index=False,
        )

    if no_marks_list:
        pd.DataFrame(
            no_marks_list,
            columns=["no_marks_files"],
        ).to_csv(
            debug_dir / "no_marks_files.csv",
            index=False,
        )

    if missing_like_list:
        pd.DataFrame(
            missing_like_list,
            columns=["missingLikes"],
        ).to_csv(
            debug_dir / "missingLikes.csv",
            index=False,
        )

    if missing_rpi_list:
        pd.DataFrame(
            missing_rpi_list,
            columns=["missingRpiMarks"],
        ).to_csv(
            debug_dir / "missingRpiMarks.csv",
            index=False,
        )


if __name__ == "__main__":
    main()
