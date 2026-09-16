# file: rpi_preproc_pipeline.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import re
import shlex
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

from RC_utilities.alignHelpers.batchAlignHelpers import (
    _missing_like,
    _normalize_ml_stem,
    _parse_device_ip_map,
    _resolve_ml_csv,
    _to_session_date,
)


@dataclass
class StageResult:
    pair: str
    testing_date: str
    session_type: str
    session_date: str
    device: str
    device_ip: str
    label: str
    source_col: str
    ml_csv: str
    ml_rootname: str
    listed_names: str
    stage: str
    status: str
    message: str
    n_listed_names: int
    n_simple_raw_csvs: int
    n_verbose_logs: int
    simple_marks_csv: str
    verb_full_csv: str
    verb_short_csv: str
    verb_marks_csv: str
    unified_csv: str
    output_path: str


@dataclass
class SourceContext:
    pair: str
    testing_date: str
    session_type: str
    session_date: str
    device: str
    device_ip: str
    label: str
    source_col: str
    ml_csv: Path
    ml_rootname: str
    listed_names: list[str]
    preproc_root: Path
    simple_root: Path
    verb_root: Path
    verb_full_root: Path
    unified_root: Path
    simple_marks_csv: Path
    verb_full_csv: Path
    verb_short_csv: Path
    verb_marks_csv: Path
    unified_csv: Path
    simple_raw_csvs: list[Path] = field(default_factory=list)
    verbose_logs: list[Path] = field(default_factory=list)
    stage_results: list[StageResult] = field(default_factory=list)

    @property
    def tag(self) -> str:
        return (
            f"{self.pair} | {self.testing_date} | {self.session_type} | "
            f"{self.device} | {self.label} | {self.ml_rootname}"
        )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="RPi preprocessing pipeline (simple + verbose -> unified marks)")
    ap.add_argument("--collated", required=True)
    ap.add_argument("--device-ip-map", required=True)
    ap.add_argument("--code-dir", required=True)
    ap.add_argument("--base-dir", required=True)
    ap.add_argument("--proc-dir", default="FreshStart")
    ap.add_argument("--events-dir-name", default="Events_Final_NoWalks")
    ap.add_argument("--sheet", default="MagicLeapFiles")
    ap.add_argument("--out-dir", default="", help="Write outputs under <out-dir>/RPi_preproc/... (default: next to each ML CSV)")
    ap.add_argument("--strip-ml-suffixes", default="_eventsFlat,_processed")
    ap.add_argument("--timezone-offset", default="auto")
    ap.add_argument("--allow-day-rollover", action="store_true")
    ap.add_argument("--dedupe-sec", default="0.05")
    ap.add_argument("--marks-timestamp-col", default="RPi_Time_verb", help="Verbose time column to adopt as canonical marks time")
    ap.add_argument("--only-rows-with-rpi", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--stage-report-csv", default="", help="Optional CSV path for per-stage run report")
    ap.add_argument("--timeCol", default="mLT_orig", help="Magic Leap timestamp column")
    return ap.parse_args()


def _split_names(cell: str) -> list[str]:
    return [nm for nm in re.split(r"[;,\s]+", (cell or "").strip()) if nm]


def _fmt_cmd(cmd: list[str]) -> str:
    return " ".join(shlex.quote(str(x)) for x in cmd)


def _run(cmd: list[str], debug: bool, dry: bool) -> tuple[bool, str]:
    if debug:
        print("[cmd]", _fmt_cmd(cmd))
    if dry:
        return True, "[dry-run]"
    try:
        res = subprocess.run(cmd, check=True, capture_output=not debug, text=True)
        stdout = (res.stdout or "").strip()
        return True, stdout
    except subprocess.CalledProcessError as e:
        stdout = (e.stdout or "").strip()
        stderr = (e.stderr or "").strip()
        msg = "\n".join(part for part in [stdout, stderr] if part)
        return False, msg


def _paths_for_ml(base_dir: Path, proc_dir: str, events_dir_name: str) -> tuple[Path, Path]:
    ml_root = base_dir / proc_dir / events_dir_name
    raw_root = base_dir / proc_dir / "RawData"
    return ml_root, raw_root


def _ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def resolve_script_paths(code_dir: Path) -> dict[str, Path]:

    scripts = {
        "extract": code_dir / "extract_rpi_marks5.py",
        "translate": code_dir / "translate_verb_log.py",
        "summarize_verb": code_dir / "summarize_verb_marks.py",
        "verb_to_marks": code_dir / "verb_to_rpi_marks.py",
        "unify": code_dir / "unify_rpi_marks.py",
    }
    missing = [str(path) for path in scripts.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required scripts: {missing}")
    return scripts


def _record(
    ctx: SourceContext,
    stage: str,
    status: str,
    message: str = "",
    output_path: str = "",
) -> None:
    result = StageResult(
        pair=ctx.pair,
        testing_date=ctx.testing_date,
        session_type=ctx.session_type,
        session_date=ctx.session_date,
        device=ctx.device,
        device_ip=ctx.device_ip,
        label=ctx.label,
        source_col=ctx.source_col,
        ml_csv=str(ctx.ml_csv),
        ml_rootname=ctx.ml_rootname,
        listed_names=";".join(ctx.listed_names),
        stage=stage,
        status=status,
        message=message,
        n_listed_names=len(ctx.listed_names),
        n_simple_raw_csvs=len(ctx.simple_raw_csvs),
        n_verbose_logs=len(ctx.verbose_logs),
        simple_marks_csv=str(ctx.simple_marks_csv),
        verb_full_csv=str(ctx.verb_full_csv),
        verb_short_csv=str(ctx.verb_short_csv),
        verb_marks_csv=str(ctx.verb_marks_csv),
        unified_csv=str(ctx.unified_csv),
        output_path=output_path,
    )
    ctx.stage_results.append(result)

    prefix = f"[{ctx.tag}] [{stage}] [{status}]"
    if message and output_path:
        print(f"{prefix} {message} -> {output_path}")
    elif message:
        print(f"{prefix} {message}")
    elif output_path:
        print(f"{prefix} {output_path}")
    else:
        print(prefix)


def build_source_context(
    *,
    args: argparse.Namespace,
    row: pd.Series,
    label: str,
    source_col: str,
    ml_csv: Path,
    ml_rootname: str,
    device_ip: str,
) -> SourceContext:
    pair = (row["pairID_py"] or "").strip()
    testing_date = (row["testingDate"] or "").strip()
    session_type = (row["sessionType"] or "").strip()
    session_date = _to_session_date(testing_date)
    device = (row["device"] or "").strip()
    listed_names = _split_names((row[source_col] or "").strip())

    base_out_root = Path(args.out_dir) if args.out_dir else ml_csv.parent
    #preproc_root = base_out_root / "RPi_preproc"
    preproc_root = base_out_root
    simple_root = preproc_root / label / "RPi_simple"
    verb_root = preproc_root / label / "RPi_verb"
    verb_full_root = preproc_root / label / "RPi_verb_full"
    unified_root = preproc_root / label / "RPi_unified"

    _ensure_dirs(simple_root, verb_root, verb_full_root, unified_root)

    return SourceContext(
        pair=pair,
        testing_date=testing_date,
        session_type=session_type,
        session_date=session_date,
        device=device,
        device_ip=device_ip,
        label=label,
        source_col=source_col,
        ml_csv=ml_csv,
        ml_rootname=ml_rootname,
        listed_names=listed_names,
        preproc_root=preproc_root,
        simple_root=simple_root,
        verb_root=verb_root,
        verb_full_root=verb_full_root,
        unified_root=unified_root,
        simple_marks_csv=simple_root / f"{ml_rootname}_{label}_RPi_simple.csv",
        verb_full_csv=verb_full_root / f"{ml_rootname}_{label}_RPi_verb_full.csv",
        verb_short_csv=verb_root / f"{ml_rootname}_{label}_RPi_verb_short.csv",
        verb_marks_csv=verb_root / f"{ml_rootname}_{label}_RPi_verb.csv",
        unified_csv=unified_root / f"{ml_rootname}_{label}_RPi_unified.csv",
    )


def collect_simple_raw_csvs(ctx: SourceContext, base_dir: Path, proc_dir: str) -> None:
    simple_raw_csvs: list[Path] = []
    for name in ctx.listed_names:
        csv_path = ctx.preproc_root / ctx.label / "RPi_simple_raw" / Path(name).with_suffix(".csv")
        if csv_path.exists():
            simple_raw_csvs.append(csv_path)

    ctx.simple_raw_csvs = simple_raw_csvs

    if simple_raw_csvs:
        _record(ctx, "discover_simple", "ok", f"found {len(simple_raw_csvs)} simple raw CSV(s)")
    else:
        _record(ctx, "discover_simple", "skip", "no simple raw CSVs found")


def collect_verbose_logs(ctx: SourceContext, base_dir: Path, proc_dir: str) -> None:
    verbose_logs: list[Path] = []
    verb_dir = ctx.source_col.split("_RPi")[0] + "_verb"

    for name in ctx.listed_names:
        verb_name = Path(name).stem + "_verb.log"
        verb_path = base_dir / proc_dir / "RawData" / ctx.pair / ctx.testing_date / ctx.session_type / "RPi_verbose" / verb_dir / verb_name
        if verb_path.exists():
            verbose_logs.append(verb_path)

    ctx.verbose_logs = verbose_logs

    if verbose_logs:
        _record(ctx, "discover_verbose", "ok", f"found {len(verbose_logs)} verbose log(s)")
    else:
        _record(ctx, "discover_verbose", "skip", "no verbose logs found")


def run_simple_stage(ctx: SourceContext, args: argparse.Namespace, scripts: dict[str, Path]) -> None:
    if not ctx.simple_raw_csvs:
        _record(ctx, "simple", "skip", "no simple raw CSV inputs")
        return

    cmd = [
        "python",
        str(scripts["extract"]),
        "--session_date",
        ctx.session_date,
        "--device",
        ctx.device,
        "--device_ip",
        ctx.device_ip,
        "--label",
        ctx.label,
        "--ml_csv_file",
        str(ctx.ml_csv),
        "--strip-ml-suffixes",
        args.strip_ml_suffixes,
        "--dedupe-sec",
        str(args.dedupe_sec),
        "--timezone_offset_hours",
        args.timezone_offset,
        "--out_dir",
        ctx.simple_root,
        "--timeCol",
        args.timeCol,
    ]
    if args.allow_day_rollover:
        cmd.append("--allow_day_rollover")
    for csv_path in ctx.simple_raw_csvs:
        cmd.extend(["--rpi_csv_file", str(csv_path)])

    ok, msg = _run(cmd, args.debug, args.dry_run)
    _record(
        ctx,
        "simple",
        "ok" if ok else "fail",
        msg or "simple extraction complete",
        str(ctx.simple_marks_csv) if (args.dry_run or ctx.simple_marks_csv.exists()) else "",
    )


def run_verbose_stage(ctx: SourceContext, args: argparse.Namespace, scripts: dict[str, Path]) -> None:
    if not ctx.verbose_logs:
        _record(ctx, "verbose", "skip", "no verbose log inputs")
        return

    combined_log = ctx.verb_full_root / f"{ctx.ml_rootname}_{ctx.label}_combined_verb.log"

    if not args.dry_run:
        combined_log.parent.mkdir(parents=True, exist_ok=True)
        with combined_log.open("w", encoding="utf-8") as outfh:
            for vp in ctx.verbose_logs:
                with vp.open("r", encoding="utf-8", errors="replace") as infh:
                    text = infh.read()
                    outfh.write(text)
                    if text and not text.endswith("\n"):
                        outfh.write("\n")

    _record(
        ctx,
        "verbose_concat",
        "ok",
        f"combined {len(ctx.verbose_logs)} verbose log(s)",
        str(combined_log),
    )

    cmd_translate = [
        "python",
        str(scripts["translate"]),
        "--verb-log",
        str(combined_log),
        "--out-csv",
        str(ctx.verb_full_csv),
        "--ip",
        str(ctx.device_ip)
    ]
    ok_t, msg_t = _run(cmd_translate, args.debug, args.dry_run)
    _record(
        ctx,
        "verbose_translate",
        "ok" if ok_t else "fail",
        msg_t or "translate complete",
        str(ctx.verb_full_csv) if (args.dry_run or ctx.verb_full_csv.exists()) else "",
    )
    if not ok_t:
        return

    cmd_summarize = [
        "python",
        str(scripts["summarize_verb"]),
        "--in-csv",
        str(ctx.verb_full_csv),
        "--out-csv",
        str(ctx.verb_short_csv),
    ]
    ok_s, msg_s = _run(cmd_summarize, args.debug, args.dry_run)
    _record(
        ctx,
        "verbose_summarize",
        "ok" if ok_s else "fail",
        msg_s or "summarize complete",
        str(ctx.verb_short_csv) if (args.dry_run or ctx.verb_short_csv.exists()) else "",
    )
    if not ok_s:
        return

    cmd_marks = [
        "python",
        str(scripts["verb_to_marks"]),
        "--in-csv",
        str(ctx.verb_short_csv),
        "--ip",
        ctx.device_ip,
        "--session-date",
        ctx.session_date,
        "--device",
        ctx.device,
        "--rpi-source",
        ctx.label,
        "--ml-csv-file",
        str(ctx.ml_csv),
        "--out-dir",
        str(ctx.verb_root),
        "--timestamp-col",
        args.marks_timestamp_col,
        "--strip-ml-suffixes",
        args.strip_ml_suffixes,
        "--dedupe-sec",
        str(args.dedupe_sec),
    ]
    if args.allow_day_rollover:
        cmd_marks.append("--allow-day-rollover")

    ok_m, msg_m = _run(cmd_marks, args.debug, args.dry_run)
    _record(
        ctx,
        "verb_to_marks",
        "ok" if ok_m else "fail",
        msg_m or "verb_to_marks complete",
        str(ctx.verb_marks_csv) if (args.dry_run or ctx.verb_marks_csv.exists()) else "",
    )


def run_unify_stage(ctx: SourceContext, args: argparse.Namespace, scripts: dict[str, Path]) -> None:
    simple_exists = ctx.simple_marks_csv.exists()
    verb_exists = ctx.verb_marks_csv.exists()

    if not args.dry_run and not (simple_exists or verb_exists):
        _record(ctx, "unify", "skip", "no simple or verb marks available")
        return

    cmd = [
        "python",
        str(scripts["unify"]),
        "--simple-marks",
        str(ctx.simple_marks_csv),
        "--verb-marks",
        str(ctx.verb_marks_csv),
        "--out-csv",
        str(ctx.unified_csv),
        "--label",
        ctx.label,
        "--marks-timestamp-col",
        args.marks_timestamp_col,
        "--ml-csv-file",
        str(ctx.ml_csv),
        "--strip-ml-suffixes",
        args.strip_ml_suffixes,
    ]
    ok, msg = _run(cmd, args.debug, args.dry_run)
    _record(
        ctx,
        "unify",
        "ok" if ok else "fail",
        msg or f"simple_exists={simple_exists} verb_exists={verb_exists}",
        str(ctx.unified_csv) if (args.dry_run or ctx.unified_csv.exists()) else "",
    )


def process_source(
    *,
    ctx: SourceContext,
    args: argparse.Namespace,
    scripts: dict[str, Path],
    base_dir: Path,
    proc_dir: str,
) -> list[StageResult]:
    _record(ctx, "source", "start", f"listed_names={ctx.listed_names}")
    collect_simple_raw_csvs(ctx, base_dir, proc_dir)
    collect_verbose_logs(ctx, base_dir, proc_dir)
    run_simple_stage(ctx, args, scripts)
    run_verbose_stage(ctx, args, scripts)
    run_unify_stage(ctx, args, scripts)
    _record(ctx, "source", "done", "finished source processing")
    return ctx.stage_results


def summarize_results(all_results: list[StageResult]) -> None:
    print("\n=== RPi preprocessing summary ===")
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
    pd.DataFrame([asdict(result) for result in all_results]).to_csv(out_path, index=False)
    print(f"[report] wrote stage report -> {out_path}")


def main() -> None:
    args = parse_args()
    suffixes = [s for s in (args.strip_ml_suffixes or "").split(",") if s]

    df = pd.read_excel(args.collated, sheet_name=args.sheet, dtype="string")
    required = [
        "cleanedFile",
        "BioPac_RPi",
        "RNS_RPi",
        "pairID_py",
        "testingDate",
        "sessionType",
        "device",
    ]
    for col in required:
        if col not in df.columns:
            raise KeyError(f"missing required column: {col}")

    if args.only_rows_with_rpi:
        before = len(df)
        df = df.loc[~(df["BioPac_RPi"].map(_missing_like) & df["RNS_RPi"].map(_missing_like))].reset_index(drop=True)
        print(f"[filter] --only-rows-with-rpi: {before} -> {len(df)} rows")

    ip_map = _parse_device_ip_map(Path(args.device_ip_map))
    scripts = resolve_script_paths(Path(args.code_dir))
    ml_root_dir, _ = _paths_for_ml(Path(args.base_dir), args.proc_dir, args.events_dir_name)

    all_results: list[StageResult] = []

    for _, row in df.iterrows():
        cleaned_raw = (row["cleanedFile"] or "").strip()
        if not cleaned_raw:
            continue

        try:
            ml_csv = _resolve_ml_csv(ml_root_dir, cleaned_raw, suffixes)
        except FileNotFoundError as e:
            print(f"[skip] {e}")
            continue

        device = (row["device"] or "").strip()
        device_ip = ip_map.get(device)
        if not device_ip:
            print(f"[skip] no IP for device {device}")
            continue

        ml_rootname = _normalize_ml_stem(ml_csv.stem, suffixes)

        for label, source_col in [("BioPac", "BioPac_RPi"), ("RNS", "RNS_RPi")]:
            listed_names = _split_names((row[source_col] or "").strip())
            if not listed_names:
                print(f"[skip] {label}: no RPi files listed for ML {ml_csv.name}")
                continue

            ctx = build_source_context(
                args=args,
                row=row,
                label=label,
                source_col=source_col,
                ml_csv=ml_csv,
                ml_rootname=ml_rootname,
                device_ip=device_ip,
            )
            all_results.extend(
                process_source(
                    ctx=ctx,
                    args=args,
                    scripts=scripts,
                    base_dir=Path(args.base_dir),
                    proc_dir=args.proc_dir,
                )
            )

    summarize_results(all_results)
    write_stage_report(all_results, args.stage_report_csv)


if __name__ == "__main__":
    main()