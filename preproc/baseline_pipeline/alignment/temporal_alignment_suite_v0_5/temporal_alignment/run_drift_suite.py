from __future__ import annotations
import argparse, subprocess, sys
from pathlib import Path


def run(cmd):
    print("[cmd]", " ".join(map(str, cmd)))
    subprocess.run(cmd, check=True)


def main():
    ap = argparse.ArgumentParser(description="Run complete offset/drift characterization suite.")
    ap.add_argument("--matched-marks", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--by", choices=["session","chunk","both"], default="both")
    ap.add_argument("--chunk-col", default="chunk_id")
    ap.add_argument("--robust", action=argparse.BooleanOptionalAction, default=True)
    args = ap.parse_args()

    od = Path(args.out_dir)
    calc = od/"calculation"
    plots = od/"plots"
    calc.mkdir(parents=True, exist_ok=True)
    plots.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "-m", "temporal_alignment.drift.calculate_drift",
        "--matched-marks", args.matched_marks,
        "--out-dir", str(calc),
        "--by", args.by,
        "--chunk-col", args.chunk_col,
    ]
    if not args.robust:
        cmd.append("--no-robust")
    run(cmd)

    points = calc/"drift_points.csv"
    if points.exists():
        run([
            sys.executable, "-m", "temporal_alignment.drift.plot_drift",
            "--drift-points", str(points),
            "--out-dir", str(plots),
        ])

    run([
        sys.executable, "-m", "temporal_alignment.drift.summarize_drift",
        "--drift-summary", str(calc/"drift_summary.csv"),
        "--out", str(od/"drift_report.csv"),
    ])
    print(f"[ok] complete drift suite -> {od}")


if __name__ == "__main__":
    main()
