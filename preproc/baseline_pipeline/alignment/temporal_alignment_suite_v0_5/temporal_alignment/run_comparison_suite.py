from __future__ import annotations
import argparse, subprocess, sys
from pathlib import Path


def run(cmd):
    print("[cmd]", " ".join(map(str, cmd)))
    subprocess.run(cmd, check=True)


def main():
    ap = argparse.ArgumentParser(description="Run full comparison and validation suite.")
    ap.add_argument("--insample", action="append", default=[])
    ap.add_argument("--heldout", action="append", default=[])
    ap.add_argument("--matching-comparison", default="")
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    od = Path(args.out_dir)
    tables = od/"tables"
    plots = od/"plots"
    summary = od/"summary"
    tables.mkdir(parents=True, exist_ok=True)
    plots.mkdir(parents=True, exist_ok=True)
    summary.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "-m",
        "temporal_alignment.comparison.build_validation_report",
        "--out-dir", str(tables),
    ]
    for x in args.insample:
        cmd += ["--insample", x]
    for x in args.heldout:
        cmd += ["--heldout", x]
    if args.matching_comparison:
        cmd += ["--matching-comparison", args.matching_comparison]
    run(cmd)

    run([
        sys.executable, "-m",
        "temporal_alignment.comparison.plot_validation",
        "--summary", str(tables/"model_validation_summary.csv"),
        "--points", str(tables/"all_validation_points.csv"),
        "--out-dir", str(plots),
    ])

    run([
        sys.executable, "-m",
        "temporal_alignment.comparison.summarize_validation",
        "--summary", str(tables/"model_validation_summary.csv"),
        "--out-dir", str(summary),
    ])
    print(f"[ok] complete comparison suite -> {od}")


if __name__ == "__main__":
    main()
