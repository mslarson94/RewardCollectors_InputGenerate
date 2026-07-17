# batch_read_rpi_logs_to_csv.py

from __future__ import annotations

import argparse
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


DEFAULT_REQUIRED_COLUMNS = ("pairID_py", "testingDate", "sessionType")


@dataclass(frozen=True)
class SessionKey:
    pair_id_py: str
    testing_date: str
    session_type: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Batch wrapper for read_rpi_logs_to_csv.py. "
            "Scans session-level RPi directories from a collated workbook and "
            "runs the parser once per session/source directory."
        )
    )
    parser.add_argument("--collated_xlsx", required=True, help="Path to collated Excel workbook.")
    parser.add_argument("--sheet_name", default="MagicLeapFiles", help="Worksheet name.")
    parser.add_argument("--raw_data_dir", required=True, help="Base RawData directory.")
    parser.add_argument("--rpi_preproc_dir", required=True, help="Base RPi_preproc directory.")
    parser.add_argument("--read_script", required=True, help="Path to read_rpi_logs_to_csv.py")
    parser.add_argument("--python_bin", default=sys.executable, help="Python executable for subprocess calls.")
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=["BioPac", "RNS"],
        default=["BioPac", "RNS"],
        help="RPi source folders to process.",
    )
    parser.add_argument(
        "--output_mode",
        choices=["flat", "nested"],
        default="flat",
        help=(
            "flat:  RPi_preproc/{source}/RPi_simple_raw\n"
            "nested: RPi_preproc/{source}/RPi_simple_raw/{pairID_py}/{testingDate}/{sessionType}"
        ),
    )
    parser.add_argument(
        "--summary_csv",
        default="",
        help="Optional explicit path for the batch summary CSV.",
    )
    parser.add_argument(
        "--pair_col",
        default="pairID_py",
        help="Workbook column holding pair IDs.",
    )
    parser.add_argument(
        "--date_col",
        default="testingDate",
        help="Workbook column holding testing dates.",
    )
    parser.add_argument(
        "--session_col",
        default="sessionType",
        help="Workbook column holding session types.",
    )
    parser.add_argument(
        "--only_pair_ids",
        nargs="*",
        default=None,
        help="Optional subset of pair IDs.",
    )
    parser.add_argument(
        "--only_testing_dates",
        nargs="*",
        default=None,
        help="Optional subset of testing dates.",
    )
    parser.add_argument(
        "--only_session_types",
        nargs="*",
        default=None,
        help="Optional subset of session types.",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Print planned work without executing subprocesses.",
    )
    parser.add_argument(
        "--stop_on_error",
        action="store_true",
        help="Exit on first subprocess failure.",
    )
    parser.add_argument(
        "--check_flat_collisions",
        action="store_true",
        help="In flat mode, scan all discovered .log basenames and fail if any duplicate basenames are found.",
    )
    return parser.parse_args()


def normalize_cell(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def load_sessions(
    collated_xlsx: Path,
    sheet_name: str,
    pair_col: str,
    date_col: str,
    session_col: str,
    only_pair_ids: set[str] | None,
    only_testing_dates: set[str] | None,
    only_session_types: set[str] | None,
) -> list[SessionKey]:
    df = pd.read_excel(collated_xlsx, sheet_name=sheet_name)

    required_columns = [pair_col, date_col, session_col]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns in sheet '{sheet_name}': {missing}. "
            f"Available columns: {list(df.columns)}"
        )

    session_df = df[[pair_col, date_col, session_col]].copy()
    session_df.columns = list(DEFAULT_REQUIRED_COLUMNS)

    for col in DEFAULT_REQUIRED_COLUMNS:
        session_df[col] = session_df[col].map(normalize_cell)

    session_df = session_df[
        (session_df["pairID_py"] != "")
        & (session_df["testingDate"] != "")
        & (session_df["sessionType"] != "")
    ].drop_duplicates()

    if only_pair_ids:
        session_df = session_df[session_df["pairID_py"].isin(only_pair_ids)]
    if only_testing_dates:
        session_df = session_df[session_df["testingDate"].isin(only_testing_dates)]
    if only_session_types:
        session_df = session_df[session_df["sessionType"].isin(only_session_types)]

    session_df = session_df.sort_values(list(DEFAULT_REQUIRED_COLUMNS))

    return [
        SessionKey(
            pair_id_py=row["pairID_py"],
            testing_date=row["testingDate"],
            session_type=row["sessionType"],
        )
        for _, row in session_df.iterrows()
    ]


def build_log_dir(raw_data_dir: Path, session: SessionKey, source: str) -> Path:
    return (
        raw_data_dir
        / session.pair_id_py
        / session.testing_date
        / session.session_type
        / "RPi"
        / f"{source}_RPi"
    )


def build_out_dir(
    rpi_preproc_dir: Path,
    session: SessionKey,
    source: str,
    output_mode: str,
) -> Path:
    base = rpi_preproc_dir / source / "RPi_simple_raw"
    if output_mode == "flat":
        return base
    return base / session.pair_id_py / session.testing_date / session.session_type


def iter_log_files(log_dir: Path) -> list[Path]:
    return sorted(p for p in log_dir.glob("*.log") if p.is_file())


def scan_flat_collisions(session_sources: Iterable[tuple[SessionKey, str, Path]]) -> dict[str, list[str]]:
    basename_to_paths: dict[str, list[str]] = defaultdict(list)

    for _, _, log_dir in session_sources:
        for log_path in iter_log_files(log_dir):
            basename_to_paths[log_path.name].append(str(log_path))

    return {
        basename: paths
        for basename, paths in basename_to_paths.items()
        if len(paths) > 1
    }


def run_subprocess(
    python_bin: str,
    read_script: Path,
    log_dir: Path,
    out_dir: Path,
    dry_run: bool,
) -> tuple[str, str]:
    command = [
        python_bin,
        str(read_script),
        "--log_dir",
        str(log_dir),
        "--out_dir",
        str(out_dir),
    ]

    if dry_run:
        return "dry_run", " ".join(command)

    out_dir.mkdir(parents=True, exist_ok=True)

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )

    stdout = completed.stdout.strip()
    stderr = completed.stderr.strip()
    message = "\n".join(part for part in (stdout, stderr) if part)

    if completed.returncode == 0:
        return "ok", message
    return "error", message


def write_summary(summary_rows: list[dict[str, object]], summary_csv: Path) -> None:
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(summary_rows).to_csv(summary_csv, index=False)


def main() -> int:
    args = parse_args()

    collated_xlsx = Path(args.collated_xlsx).resolve()
    raw_data_dir = Path(args.raw_data_dir).resolve()
    rpi_preproc_dir = Path(args.rpi_preproc_dir).resolve()
    read_script = Path(args.read_script).resolve()

    if not collated_xlsx.exists():
        raise FileNotFoundError(f"collated_xlsx not found: {collated_xlsx}")
    if not raw_data_dir.is_dir():
        raise NotADirectoryError(f"raw_data_dir not found: {raw_data_dir}")
    if not read_script.exists():
        raise FileNotFoundError(f"read_script not found: {read_script}")

    sessions = load_sessions(
        collated_xlsx=collated_xlsx,
        sheet_name=args.sheet_name,
        pair_col=args.pair_col,
        date_col=args.date_col,
        session_col=args.session_col,
        only_pair_ids=set(args.only_pair_ids) if args.only_pair_ids else None,
        only_testing_dates=set(args.only_testing_dates) if args.only_testing_dates else None,
        only_session_types=set(args.only_session_types) if args.only_session_types else None,
    )

    if not sessions:
        print("[info] No sessions matched the provided filters.")
        return 0

    session_sources: list[tuple[SessionKey, str, Path]] = []
    for session in sessions:
        for source in args.sources:
            log_dir = build_log_dir(raw_data_dir, session, source)
            if log_dir.exists() and log_dir.is_dir():
                session_sources.append((session, source, log_dir))

    if args.output_mode == "flat" and args.check_flat_collisions:
        collisions = scan_flat_collisions(session_sources)
        if collisions:
            print("[error] Duplicate .log basenames detected across input directories.")
            for basename, paths in sorted(collisions.items()):
                print(f"\n{basename}")
                for path in paths:
                    print(f"  {path}")
            print("\n[info] Use --output_mode nested or resolve basename collisions.")
            return 1

    summary_rows: list[dict[str, object]] = []

    for session in sessions:
        for source in args.sources:
            log_dir = build_log_dir(raw_data_dir, session, source)
            out_dir = build_out_dir(
                rpi_preproc_dir=rpi_preproc_dir,
                session=session,
                source=source,
                output_mode=args.output_mode,
            )

            if not log_dir.exists():
                summary_rows.append(
                    {
                        "pairID_py": session.pair_id_py,
                        "testingDate": session.testing_date,
                        "sessionType": session.session_type,
                        "source": source,
                        "status": "missing_dir",
                        "log_dir": str(log_dir),
                        "out_dir": str(out_dir),
                        "n_log_files": 0,
                        "message": "Input directory does not exist.",
                    }
                )
                print(f"[skip] Missing directory: {log_dir}")
                continue

            log_files = iter_log_files(log_dir)
            if not log_files:
                summary_rows.append(
                    {
                        "pairID_py": session.pair_id_py,
                        "testingDate": session.testing_date,
                        "sessionType": session.session_type,
                        "source": source,
                        "status": "no_logs",
                        "log_dir": str(log_dir),
                        "out_dir": str(out_dir),
                        "n_log_files": 0,
                        "message": "Directory exists but contains no .log files.",
                    }
                )
                print(f"[skip] No .log files in: {log_dir}")
                continue

            print(
                f"[run] {session.pair_id_py} | {session.testing_date} | "
                f"{session.session_type} | {source} | {len(log_files)} log(s)"
            )

            status, message = run_subprocess(
                python_bin=args.python_bin,
                read_script=read_script,
                log_dir=log_dir,
                out_dir=out_dir,
                dry_run=args.dry_run,
            )

            summary_rows.append(
                {
                    "pairID_py": session.pair_id_py,
                    "testingDate": session.testing_date,
                    "sessionType": session.session_type,
                    "source": source,
                    "status": status,
                    "log_dir": str(log_dir),
                    "out_dir": str(out_dir),
                    "n_log_files": len(log_files),
                    "message": message,
                }
            )

            if status == "error":
                print(f"[error] Failed: {log_dir}")
                if args.stop_on_error:
                    summary_csv = (
                        Path(args.summary_csv).resolve()
                        if args.summary_csv
                        else rpi_preproc_dir / "read_rpi_logs_to_csv_batch_summary.csv"
                    )
                    write_summary(summary_rows, summary_csv)
                    print(f"[info] Wrote partial summary: {summary_csv}")
                    return 1

    summary_csv = (
        Path(args.summary_csv).resolve()
        if args.summary_csv
        else rpi_preproc_dir / "read_rpi_logs_to_csv_batch_summary.csv"
    )
    write_summary(summary_rows, summary_csv)

    summary_df = pd.DataFrame(summary_rows)
    status_counts = summary_df["status"].value_counts(dropna=False).to_dict() if not summary_df.empty else {}
    print(f"[info] Wrote summary: {summary_csv}")
    print(f"[info] Status counts: {status_counts}")

    return 0 if "error" not in status_counts else 1


if __name__ == "__main__":
    raise SystemExit(main())