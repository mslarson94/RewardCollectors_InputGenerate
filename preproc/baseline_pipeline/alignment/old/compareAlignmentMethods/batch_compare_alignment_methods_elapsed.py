#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


AUTO_SUMMARY_SUFFIX = "_alignment_summary.csv"
AUTO_PAIRS_SUFFIX = "_clock_pairs.csv"
FILTERED_SUMMARY_SUFFIX = "_manualFiltered_alignment_summary.csv"
FILTERED_PAIRS_SUFFIX = "_manualFiltered_clock_pairs.csv"


@dataclass
class MethodFiles:
    automatic_summary: Optional[Path] = None
    automatic_pairs: Optional[Path] = None
    filtered_summary: Optional[Path] = None
    filtered_pairs: Optional[Path] = None

    def complete(self) -> bool:
        return all(
            p is not None
            for p in (
                self.automatic_summary,
                self.automatic_pairs,
                self.filtered_summary,
                self.filtered_pairs,
            )
        )

    def missing(self) -> list[str]:
        out = []
        if self.automatic_summary is None:
            out.append("automatic summary")
        if self.automatic_pairs is None:
            out.append("automatic pairs")
        if self.filtered_summary is None:
            out.append("manual-filtered summary")
        if self.filtered_pairs is None:
            out.append("manual-filtered pairs")
        return out


def _base_key(path: Path) -> tuple[str, str] | None:
    """
    Return (base_key, kind).

    Expected outputs:
      <base>_alignment_summary.csv
      <base>_clock_pairs.csv
      <base>_manualFiltered_alignment_summary.csv
      <base>_manualFiltered_clock_pairs.csv

    The base may itself contain underscores.
    """
    name = path.name

    if name.endswith(FILTERED_SUMMARY_SUFFIX):
        return name[: -len(FILTERED_SUMMARY_SUFFIX)], "filtered_summary"

    if name.endswith(FILTERED_PAIRS_SUFFIX):
        return name[: -len(FILTERED_PAIRS_SUFFIX)], "filtered_pairs"

    if name.endswith(AUTO_SUMMARY_SUFFIX):
        base = name[: -len(AUTO_SUMMARY_SUFFIX)]
        # Prevent accidental classification of manualFiltered files if naming changes.
        if base.endswith("_manualFiltered"):
            return None
        return base, "automatic_summary"

    if name.endswith(AUTO_PAIRS_SUFFIX):
        base = name[: -len(AUTO_PAIRS_SUFFIX)]
        if base.endswith("_manualFiltered"):
            return None
        return base, "automatic_pairs"

    return None


def _safe_name(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_") or "comparison"


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Scan a directory for automatic/manualFiltered alignment outputs, "
            "pair them by common base stem, and run compare_alignment_methods.py "
            "for every complete pair."
        )
    )
    ap.add_argument(
        "--input_dir",
        required=True,
        help="Directory containing all alignment output CSV files.",
    )
    ap.add_argument(
        "--compare_script",
        default="",
        help=(
            "Path to compare_alignment_methods_elapsed.py. "
            "Defaults to the copy next to this batch script."
        ),
    )
    ap.add_argument(
        "--out_dir",
        default="",
        help=(
            "Root directory for comparison outputs. "
            "Defaults to <input_dir>/AlignmentComparisons."
        ),
    )
    ap.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively scan subdirectories instead of only input_dir.",
    )
    ap.add_argument(
        "--dry_run",
        action="store_true",
        help="Show discovered pairs and commands without running comparisons.",
    )
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Exit nonzero if any incomplete group is discovered.",
    )
    args = ap.parse_args()

    input_dir = Path(args.input_dir).expanduser().resolve()
    if not input_dir.is_dir():
        raise NotADirectoryError(input_dir)

    compare_script = (
        Path(args.compare_script).expanduser().resolve()
        if args.compare_script
        else Path(__file__).resolve().with_name("compare_alignment_methods_elapsed.py")
    )
    if not compare_script.exists():
        raise FileNotFoundError(
            f"Could not find compare_alignment_methods_elapsed.py: {compare_script}"
        )

    output_root = (
        Path(args.out_dir).expanduser().resolve()
        if args.out_dir
        else input_dir / "AlignmentComparisons"
    )
    output_root.mkdir(parents=True, exist_ok=True)

    candidates = (
        input_dir.rglob("*.csv")
        if args.recursive
        else input_dir.glob("*.csv")
    )

    groups: Dict[str, MethodFiles] = {}
    ignored = 0

    for path in sorted(candidates):
        parsed = _base_key(path)
        if parsed is None:
            ignored += 1
            continue

        base, kind = parsed
        group = groups.setdefault(base, MethodFiles())

        existing = getattr(group, kind)
        if existing is not None:
            raise RuntimeError(
                f"Duplicate file for group '{base}', kind '{kind}':\n"
                f"  {existing}\n"
                f"  {path}"
            )
        setattr(group, kind, path.resolve())

    if not groups:
        print(f"[skip] no comparison-ready alignment files found in {input_dir}")
        raise SystemExit(0)

    complete = {k: v for k, v in groups.items() if v.complete()}
    incomplete = {k: v for k, v in groups.items() if not v.complete()}

    print(
        f"[scan] groups={len(groups)} "
        f"complete={len(complete)} "
        f"incomplete={len(incomplete)} "
        f"ignored_csv={ignored}"
    )

    if incomplete:
        print("\n[incomplete groups]")
        for base, files in sorted(incomplete.items()):
            print(f"  - {base}")
            print(f"      missing: {', '.join(files.missing())}")

    failures: list[tuple[str, int]] = []

    for i, (base, files) in enumerate(sorted(complete.items()), start=1):
        assert files.automatic_summary is not None
        assert files.automatic_pairs is not None
        assert files.filtered_summary is not None
        assert files.filtered_pairs is not None

        safe_base = _safe_name(base)
        session_out = output_root / safe_base
        session_out.mkdir(parents=True, exist_ok=True)

        cmd = [
            sys.executable,
            str(compare_script),
            "--automatic_summary",
            str(files.automatic_summary),
            "--filtered_summary",
            str(files.filtered_summary),
            "--automatic_pairs",
            str(files.automatic_pairs),
            "--filtered_pairs",
            str(files.filtered_pairs),
            "--out_dir",
            str(session_out),
            "--name",
            safe_base,
        ]

        print(f"\n[{i}/{len(complete)}] {base}")
        print(f"  automatic summary : {files.automatic_summary.name}")
        print(f"  automatic pairs   : {files.automatic_pairs.name}")
        print(f"  filtered summary  : {files.filtered_summary.name}")
        print(f"  filtered pairs    : {files.filtered_pairs.name}")
        print(f"  output             : {session_out}")

        if args.dry_run:
            print("  [dry-run] " + " ".join(f'"{x}"' if " " in x else x for x in cmd))
            continue

        proc = subprocess.run(cmd, check=False)
        if proc.returncode != 0:
            failures.append((base, proc.returncode))
            print(f"  [fail] compare script exited {proc.returncode}")
        else:
            print("  [ok] comparison complete")

    print("\n[summary]")
    print(f"  complete groups run : {len(complete)}")
    print(f"  incomplete groups   : {len(incomplete)}")
    print(f"  failed comparisons  : {len(failures)}")
    print(f"  output root         : {output_root}")

    if failures:
        print("\n[failed groups]")
        for base, code in failures:
            print(f"  - {base} (exit {code})")
        raise SystemExit(1)

    if args.strict and incomplete:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
