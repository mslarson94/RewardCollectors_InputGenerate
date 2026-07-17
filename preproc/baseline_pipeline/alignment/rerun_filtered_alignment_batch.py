# file: rerun_filtered_alignment_batch.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import shlex
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd


EVENT_SUFFIXES = [
    "_eventsFlat_filtered",
    "_events_final_filtered",
    "_events_filtered",
    "_processed_filtered",
    "_filtered",
]

RPI_SUFFIXES = {
    "BioPac": "_BioPac_RPi_unified_filtered",
    "RNS": "_RNS_RPi_unified_filtered",
}


@dataclass
class StageResult:
    events_file: str
    rpi_file: str
    label: str
    device: str
    base_stem: str
    stage: str
    status: str
    message: str
    output_path: str


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Batch rerun alignment + drift summaries from filtered Events/RPi copies."
    )
    ap.add_argument("--events-dir", type=Path, required=True, help="Directory containing filtered Events CSVs")
    ap.add_argument("--rpi-dir", type=Path, required=True, help="Directory containing filtered *_RPi_unified_filtered.csv files")
    ap.add_argument("--code-dir", type=Path, required=True, help="Directory containing merge/summarize/combined scripts")
    ap.add_argument("--out-dir", type=Path, required=True, help="Directory for filtered aligned outputs")
    ap.add_argument("--blank-row-template", type=Path, required=True, help="Path to NewRowInfo.csv or equivalent template")
    ap.add_argument("--csv-timestamp-column", default="eMLT_orig")
    ap.add_argument("--event-type-column", default="lo_eventType")
    ap.add_argument("--event-type-values", default="Mark")
    ap.add_argument("--timezone-offset", default="auto")
    ap.add_argument("--strip-ml-suffixes", default="_eventsFlat,_processed")
    ap.add_argument("--max-match-gap-s", default="1.0")
    ap.add_argument("--only-label", choices=["BioPac", "RNS"], default="")
    ap.add_argument("--skip-existing", action="store_true", help="Skip source pairs whose aligned output already exists")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()


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


def strip_suffixes(stem: str, suffixes: list[str]) -> str:
    base = stem
    changed = True
    while changed:
        changed = False
        for suffix in suffixes:
            if suffix and base.endswith(suffix):
                base = base[: -len(suffix)]
                changed = True
    return base.rstrip("_-")


def detect_device(events_csv: Path) -> str:
    df = pd.read_csv(events_csv, usecols=lambda c: c == "device")
    if "device" not in df.columns or df.empty:
        raise KeyError(f"Could not detect device column in {events_csv}")
    vals = [str(v).strip() for v in df["device"].dropna().unique().tolist() if str(v).strip()]
    if not vals:
        raise ValueError(f"No non-empty device values found in {events_csv}")
    if len(vals) > 1:
        raise ValueError(f"Multiple device values found in {events_csv}: {vals}")
    return vals[0]


def build_pairs(events_dir: Path, rpi_dir: Path, only_label: str) -> list[tuple[Path, Path, str, str]]:
    event_files = sorted(p for p in events_dir.glob("*.csv") if p.is_file())
    rpi_files = sorted(p for p in rpi_dir.glob("*.csv") if p.is_file())

    event_map: dict[str, Path] = {}
    for path in event_files:
        base = strip_suffixes(path.stem, EVENT_SUFFIXES)
        event_map[base] = path

    rpi_map: dict[tuple[str, str], Path] = {}
    for path in rpi_files:
        for label, suffix in RPI_SUFFIXES.items():
            if only_label and label != only_label:
                continue
            if path.stem.endswith(suffix):
                base = path.stem[: -len(suffix)].rstrip("_-")
                rpi_map[(base, label)] = path
                break

    pairs: list[tuple[Path, Path, str, str]] = []
    for base, events_file in sorted(event_map.items()):
        for label in ("BioPac", "RNS"):
            if only_label and label != only_label:
                continue
            rpi_file = rpi_map.get((base, label))
            if rpi_file is not None:
                pairs.append((events_file, rpi_file, label, base))
    return pairs


def main() -> None:
    args = parse_args()

    events_dir = args.events_dir.expanduser()
    rpi_dir = args.rpi_dir.expanduser()
    code_dir = args.code_dir.expanduser()
    out_dir = args.out_dir.expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    merge_script = code_dir / "merge_ml_with_rpi_marks3.py"
    summarize_script = code_dir / "summarize_drift2.py"
    merge_both_script = code_dir / "merge_rpi_event_files2.py"

    for p in (merge_script, summarize_script, merge_both_script, args.blank_row_template.expanduser()):
        if not Path(p).exists():
            raise FileNotFoundError(p)

    pairs = build_pairs(events_dir, rpi_dir, args.only_label)
    if not pairs:
        raise SystemExit("No filtered Events/RPi pairs found.")

    results: list[StageResult] = []

    combined_candidates: dict[tuple[str, str], dict[str, Path]] = {}

    for events_file, rpi_file, label, base in pairs:
        try:
            device = detect_device(events_file)
        except Exception as e:
            results.append(
                StageResult(
                    events_file=str(events_file),
                    rpi_file=str(rpi_file),
                    label=label,
                    device="",
                    base_stem=base,
                    stage="detect_device",
                    status="fail",
                    message=str(e),
                    output_path="",
                )
            )
            continue

        aligned_out = out_dir / f"{base}_{label}_{device}_aligned_with_RPi.csv"

        if args.skip_existing and aligned_out.exists() and not args.dry_run:
            results.append(
                StageResult(
                    events_file=str(events_file),
                    rpi_file=str(rpi_file),
                    label=label,
                    device=device,
                    base_stem=base,
                    stage="merge",
                    status="skip",
                    message=f"Aligned output exists: {aligned_out}",
                    output_path=str(aligned_out),
                )
            )
            combined_candidates.setdefault((base, device), {})[label] = aligned_out
            continue

        merge_cmd = [
            "python",
            str(merge_script),
            "--ml_csv_file",
            str(events_file),
            "--rpi_marks_csv",
            str(rpi_file),
            "--csv_timestamp_column",
            args.csv_timestamp_column,
            "--event_type_column",
            args.event_type_column,
            "--event_type_values",
            args.event_type_values,
            "--label",
            label,
            "--device",
            device,
            "--timezone_offset_hours",
            args.timezone_offset,
            "--blankRowTemplate",
            str(args.blank_row_template.expanduser()),
            "--max_match_gap_s",
            str(args.max_match_gap_s),
            "--strip-ml-suffixes",
            args.strip_ml_suffixes,
            "--out_dir",
            str(out_dir),
        ]
        ok_merge, msg_merge = run_cmd(merge_cmd, args.debug, args.dry_run)
        results.append(
            StageResult(
                events_file=str(events_file),
                rpi_file=str(rpi_file),
                label=label,
                device=device,
                base_stem=base,
                stage="merge",
                status="ok" if ok_merge else "fail",
                message=msg_merge or "merge complete",
                output_path=str(aligned_out) if (args.dry_run or aligned_out.exists()) else "",
            )
        )
        if not ok_merge:
            continue

        if args.dry_run or aligned_out.exists():
            combined_candidates.setdefault((base, device), {})[label] = aligned_out

            summarize_cmd = [
                "python",
                str(summarize_script),
                "--merged_ml_csv",
                str(aligned_out),
                "--label",
                label,
            ]
            ok_sum, msg_sum = run_cmd(summarize_cmd, args.debug, args.dry_run)
            results.append(
                StageResult(
                    events_file=str(events_file),
                    rpi_file=str(rpi_file),
                    label=label,
                    device=device,
                    base_stem=base,
                    stage="summarize_source",
                    status="ok" if ok_sum else "fail",
                    message=msg_sum or "source summarize complete",
                    output_path=str(aligned_out),
                )
            )

    for (base, device), by_label in sorted(combined_candidates.items()):
        bio_path = by_label.get("BioPac")
        rns_path = by_label.get("RNS")
        if not bio_path and not rns_path:
            continue

        combined_out = out_dir / "BioPacRNS" / f"{base}_{device}_BioPacRNS_events.csv"

        merge_both_cmd = ["python", str(merge_both_script)]
        if bio_path:
            merge_both_cmd += ["--biopac_events_csv", str(bio_path)]
        if rns_path:
            merge_both_cmd += ["--rns_events_csv", str(rns_path)]
        merge_both_cmd += ["--out_dir", str(out_dir)]

        ok_both, msg_both = run_cmd(merge_both_cmd, args.debug, args.dry_run)
        results.append(
            StageResult(
                events_file="",
                rpi_file="",
                label="BioPacRNS",
                device=device,
                base_stem=base,
                stage="merge_combined",
                status="ok" if ok_both else "fail",
                message=msg_both or "combined merge complete",
                output_path=str(combined_out) if (args.dry_run or combined_out.exists()) else "",
            )
        )

        if ok_both and (args.dry_run or combined_out.exists()):
            labels = []
            if bio_path:
                labels.append("BioPac")
            if rns_path:
                labels.append("RNS")
            if len(labels) >= 2:
                labels.append("Combined")

            sum_combined_cmd = [
                "python",
                str(summarize_script),
                "--merged_ml_csv",
                str(combined_out),
            ]
            if labels:
                sum_combined_cmd += ["--labels", ",".join(labels)]

            ok_sum_combined, msg_sum_combined = run_cmd(sum_combined_cmd, args.debug, args.dry_run)
            results.append(
                StageResult(
                    events_file="",
                    rpi_file="",
                    label="BioPacRNS",
                    device=device,
                    base_stem=base,
                    stage="summarize_combined",
                    status="ok" if ok_sum_combined else "fail",
                    message=msg_sum_combined or "combined summarize complete",
                    output_path=str(combined_out),
                )
            )

    results_df = pd.DataFrame([asdict(r) for r in results])
    stage_report = out_dir / "filtered_alignment_stage_report.csv"
    if not args.dry_run:
        results_df.to_csv(stage_report, index=False)

    print(f"[ok] processed pairs: {len(pairs)}")
    if not args.dry_run:
        print(f"[ok] wrote stage report -> {stage_report}")


if __name__ == "__main__":
    main()