#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# alignment_qc_suite.py
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd


GLOBAL_QC_FILENAME = "global_affine_qc_all_sessions.csv"
BURST_RESIDUAL_FILENAME = "burst_residual_session_summary.csv"
BURST_SHIFT_FILENAME = "burst_shift_session_summary.csv"
MASTER_FILENAME = "alignment_qc_master_summary.csv"
RUN_REPORT_FILENAME = "alignment_qc_stage_report.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the complete global-across-session alignment QC suite and generate a merged session-level QC summary.")

    parser.add_argument("--input-dir", required=True, help="Root containing *_global_affine_summary.csv and *_global_affine_mark_diagnostics.csv files.")
    parser.add_argument("--code-dir", required=True, help="Directory containing classify_global_affine_qc.py, characterize_burst_residuals.py, and characterize_burst_shifts.py.",)
    parser.add_argument("--out-dir", default="", help="Output root for the QC suite. Defaults to <input-dir>/AlignmentQC.")
    parser.add_argument("--recursive", action=argparse.BooleanOptionalAction, default=True, help="Recursively search the alignment output directory.")

    # ---------------------------------------------------------
    # Global affine QC thresholds
    # ---------------------------------------------------------

    parser.add_argument("--max-rmse-ms", type=float, default=50.0)
    parser.add_argument("--max-mad-ms", type=float, default=30.0)
    parser.add_argument("--min-inlier-fraction", type=float, default=0.80)
    parser.add_argument("--use-accumulated-drift", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--max-abs-accumulated-drift-ms", type=float, default=100.0)
    parser.add_argument("--min-drift-span-s", type=float, default=60.0)
    parser.add_argument("--insufficient-span-counts-as-fail", action=argparse.BooleanOptionalAction, default=False)

    # ---------------------------------------------------------
    # Local-structure descriptive thresholds
    #
    # These do NOT change the original global-affine QC result.
    # They are used to describe local residual structure.
    # ---------------------------------------------------------

    parser.add_argument("--high-burst-range-ms", type=float, default=100.0, help="Descriptive threshold for a large range of stabilized burst median residuals.")
    parser.add_argument("--high-burst-shift-ms", type=float, default=50.0, help="Descriptive threshold for a large median absolute consecutive burst shift.")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--dry-run", action="store_true")

    return parser.parse_args()


def run_command(command: list[str], *, debug: bool, dry_run: bool,) -> tuple[bool, str]:

    printable = " ".join(str(item) for item in command)

    if debug or dry_run:
        print(f"[cmd] {printable}")

    if dry_run:
        return True, "dry_run"

    result = subprocess.run(
        command,
        text=True,
        capture_output=True,
    )

    if debug and result.stdout:
        print(result.stdout)

    if result.returncode != 0:
        message = (
            result.stderr.strip()
            or result.stdout.strip()
            or f"exit code {result.returncode}"
        )
        return False, message

    return True, result.stdout.strip()


def session_from_global_summary_path(value: object,) -> str:

    if pd.isna(value):
        return ""

    path = Path(str(value))

    suffix = "_global_affine_summary.csv"

    if path.name.endswith(suffix):
        return path.name[:-len(suffix)]

    return path.stem


def normalize_session_column(df: pd.DataFrame, source: str) -> pd.DataFrame:

    out = df.copy()

    if source == "global_qc":
        if "summary_file" not in out.columns:
            raise KeyError("Global QC output is missing 'summary_file'.")

        out["session"] = out["summary_file"].map(session_from_global_summary_path)

    else:
        if "session" not in out.columns:
            raise KeyError(f"{source} output is missing 'session'.")

        out["session"] = out["session"].astype(str).str.strip()
        

    return out


def prefix_columns(df: pd.DataFrame, prefix: str, keep: set[str]) -> pd.DataFrame:

    rename_map = {
        column: f"{prefix}{column}"
        for column in df.columns
        if column not in keep
    }

    return df.rename(columns=rename_map)


def safe_numeric(df: pd.DataFrame, column: str) -> pd.Series:

    if column not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype=float)

    return pd.to_numeric(df[column], errors="coerce")


def safe_bool(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)
    series = df[column]
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)

    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})
    


def build_master_summary(
    global_qc_csv: Path,
    burst_residual_csv: Path,
    burst_shift_csv: Path,
    *,
    high_burst_range_ms: float,
    high_burst_shift_ms: float,
) -> pd.DataFrame:

    global_df = pd.read_csv(global_qc_csv)
    residual_df = pd.read_csv(burst_residual_csv)
    shift_df = pd.read_csv(burst_shift_csv)
    global_df = normalize_session_column(global_df, "global_qc")
    residual_df = normalize_session_column(residual_df, "burst_residual")
    shift_df = normalize_session_column(shift_df, "burst_shift")

    # ---------------------------------------------------------
    # Verify one row per session in each summary.
    # ---------------------------------------------------------

    for name, frame in (
        ("global QC", global_df),
        ("burst residual", residual_df),
        ("burst shift", shift_df)):

        duplicated = frame["session"].duplicated(keep=False)

        if duplicated.any():
            duplicates = frame.loc[duplicated, "session"].astype(str).tolist()
            

            raise ValueError(f"{name} contains duplicate sessions: {duplicates[:10]}")

    # ---------------------------------------------------------
    # Prefix diagnostic tables so similarly named metrics
    # remain unambiguous in the master file.
    # ---------------------------------------------------------

    residual_df = prefix_columns(residual_df, prefix="burst_residual_", keep={"session"})
    shift_df = prefix_columns(shift_df, prefix="burst_shift_", keep={"session"})

    # ---------------------------------------------------------
    # Merge.
    #
    # Global QC is the anchor because it defines the set of
    # sessions successfully reaching the global-affine summary.
    # ---------------------------------------------------------

    master = global_df.merge(residual_df, on="session", how="left", validate="1:1")

    master = master.merge(shift_df, on="session", how="left", validate="1:1")

    # ---------------------------------------------------------
    # Convenience metrics in milliseconds.
    # ---------------------------------------------------------

    second_columns = {
        "burst_residual_all_rmse_s": "burst_residual_all_rmse_ms",
        "burst_residual_all_mad_s": "burst_residual_all_mad_ms",
        "burst_residual_stable_rmse_s": "burst_residual_stable_rmse_ms",
        "burst_residual_stable_mad_s": "burst_residual_stable_mad_ms",
        "burst_residual_rmse_improvement_excluding_first_s": "burst_residual_rmse_improvement_excluding_first_ms",
        "burst_residual_mad_improvement_excluding_first_s": "burst_residual_mad_improvement_excluding_first_ms",
        "burst_shift_stable_burst_median_range_s": "burst_shift_stable_burst_median_range_ms",
        "burst_shift_median_abs_burst_shift_s": "burst_shift_median_abs_burst_shift_ms",
        "burst_shift_max_abs_burst_shift_s": "burst_shift_max_abs_burst_shift_ms",
    }

    for source_col, target_col in second_columns.items():
        if source_col in master.columns:
            master[target_col] = pd.to_numeric(master[source_col], errors="coerce")* 1000.0
            

    # ---------------------------------------------------------
    # Local burst-structure flags.
    #
    # These are intentionally separate from global_affine_pass.
    # ---------------------------------------------------------

    burst_range_ms = safe_numeric(master, "burst_shift_stable_burst_median_range_ms")
    burst_shift_ms = safe_numeric(master, "burst_shift_median_abs_burst_shift_ms")

    master["high_burst_median_range"] = burst_range_ms > high_burst_range_ms
    master["high_median_burst_shift"] = burst_shift_ms > high_burst_shift_ms
    master["high_local_burst_movement"] = master["high_burst_median_range"] | master["high_median_burst_shift"]
    master["local_structure_available"] = burst_range_ms.notna() | burst_shift_ms.notna()
    

    # ---------------------------------------------------------
    # Descriptive two-dimensional category.
    #
    # This is NOT a replacement for qc_classification.
    # It describes global quality x local structure.
    # ---------------------------------------------------------

    global_pass = safe_bool(master, "global_affine_pass")

    local_high = master["high_local_burst_movement"]

    local_available = master["local_structure_available"]

    conditions = [
        ~local_available,

        global_pass
        & ~local_high,

        global_pass
        & local_high,

        ~global_pass
        & ~local_high,

        ~global_pass
        & local_high,
    ]

    choices = [
        "local_structure_unavailable",
        "global_pass_low_local_movement",
        "global_pass_high_local_movement",
        "global_fail_low_local_movement",
        "global_fail_high_local_movement",
    ]

    master["global_x_local_qc_category"] = np.select(conditions, choices, default="unclassified")

    # ---------------------------------------------------------
    # Canonical five-way QC routing category.
    # ---------------------------------------------------------

    five_way_map = {
        "global_pass_low_local_movement": "global_pass_low_local_movement",
        "global_pass_high_local_movement": "global_pass_high_local_movement",
        "global_fail_low_local_movement": "global_fail_low_local_movement",
        "global_fail_high_local_movement": "global_fail_high_local_movement",
        "local_structure_unavailable": "insufficient_local_structure"
    }

    master["alignment_qc_category_5way"] = master["global_x_local_qc_category"].map(five_way_map)
    
    if master["alignment_qc_category_5way"].isna().any():
        bad = (master.loc[master["alignment_qc_category_5way"].isna(), "global_x_local_qc_category"].value_counts(dropna=False).to_dict())
        raise ValueError(f"Unmapped QC categories encountered: {bad}")


    # ---------------------------------------------------------
    # First-mark effect descriptive fields.
    # ---------------------------------------------------------

    all_rmse = safe_numeric(master, "burst_residual_all_rmse_ms")
    stable_rmse = safe_numeric(master, "burst_residual_stable_rmse_ms")
    all_mad = safe_numeric(master, "burst_residual_all_mad_ms")
    stable_mad = safe_numeric(master, "burst_residual_stable_mad_ms")

    master["stable_marks_improve_rmse"] = (all_rmse.notna() & stable_rmse.notna() & (stable_rmse < all_rmse))
    master["stable_marks_improve_mad"] = ( all_mad.notna() & stable_mad.notna() & (stable_mad < all_mad))

    # ---------------------------------------------------------
    # Useful ordering.
    # ---------------------------------------------------------

    preferred = [
        "session",
        "label",
        "device",

        "qc_classification",
        "global_affine_pass",
        "failure_reasons",
        "n_failed_criteria",

        "global_x_local_qc_category",
        "alignment_qc_category_5way",

        "residual_rmse_ms",
        "residual_mad_ms",
        "inlier_fraction_of_matched",

        "matched_mark_span_s",
        "matched_mark_span_min",
        "sufficient_drift_span",

        "clock_drift_ppm",
        "drift_accumulated_over_elapsed_ms",

        "n_matched_marks",
        "n_affine_inliers",

        "burst_residual_all_rmse_ms",
        "burst_residual_stable_rmse_ms",
        "burst_residual_rmse_improvement_excluding_first_ms",

        "burst_residual_all_mad_ms",
        "burst_residual_stable_mad_ms",
        "burst_residual_mad_improvement_excluding_first_ms",

        "stable_marks_improve_rmse",
        "stable_marks_improve_mad",

        "burst_residual_first_to_stable_median_abs_ratio",

        "burst_shift_n_bursts_total",
        "burst_shift_n_bursts_with_stable_marks",

        "burst_shift_stable_burst_median_range_ms",
        "burst_shift_median_abs_burst_shift_ms",
        "burst_shift_max_abs_burst_shift_ms",

        "burst_shift_burst_trend_slope_s_per_min",
        "burst_shift_burst_trend_r2",

        "high_burst_median_range",
        "high_median_burst_shift",
        "high_local_burst_movement",
        "local_structure_available",

        "global_x_local_qc_category",

        "summary_file",
        "ml_file",
        "matched_marks_file",
    ]

    preferred_existing = [
        column
        for column in preferred
        if column in master.columns
    ]

    remaining = [
        column
        for column in master.columns
        if column not in preferred_existing
    ]

    master = master[
        preferred_existing
        + remaining
    ]

    return master


def write_stage_report(rows: list[dict], path: Path) -> None:

    pd.DataFrame(rows).to_csv(path, index=False)


def main() -> None:
    args = parse_args()

    input_dir = Path(args.input_dir).expanduser().resolve()

    code_dir = Path(args.code_dir).expanduser().resolve()

    out_dir = (
        Path(args.out_dir).expanduser().resolve()
        if args.out_dir
        else input_dir / "AlignmentQC"
    )

    if not input_dir.exists():
        raise FileNotFoundError(input_dir)

    if not code_dir.exists():
        raise FileNotFoundError(code_dir)

    classify_script = code_dir / "classify_global_affine_qc.py"
    burst_residual_script = code_dir / "characterize_burst_residuals.py"
    burst_shift_script = code_dir / "characterize_burst_shifts.py"
    

    for script in (classify_script, burst_residual_script, burst_shift_script):
        if not script.exists():
            raise FileNotFoundError(script)

    global_qc_dir = out_dir / "GlobalAffineQC"
    burst_residual_dir = out_dir / "BurstResidualQC"
    burst_shift_dir = out_dir / "BurstShiftQC"
    

    for directory in (out_dir, global_qc_dir, burst_residual_dir, burst_shift_dir):
        directory.mkdir(parents=True, exist_ok=True)

    stage_rows: list[dict] = []

    # =========================================================
    # Stage 1: global affine QC
    # =========================================================

    command = [
        sys.executable,
        str(classify_script),

        "--input_dir",
        str(input_dir),

        "--out_dir",
        str(global_qc_dir),

        "--max-rmse-ms",
        str(args.max_rmse_ms),

        "--max-mad-ms",
        str(args.max_mad_ms),

        "--min-inlier-fraction",
        str(args.min_inlier_fraction),

        "--max-abs-accumulated-drift-ms",
        str(args.max_abs_accumulated_drift_ms),

        "--min-drift-span-s",
        str(args.min_drift_span_s),
    ]

    if args.recursive:
        command.append("--recursive")

    command.append(
        "--use-accumulated-drift"
        if args.use_accumulated_drift
        else "--no-use-accumulated-drift"
    )

    command.append(
        "--insufficient-span-counts-as-fail"
        if args.insufficient_span_counts_as_fail
        else "--no-insufficient-span-counts-as-fail"
    )

    ok, message = run_command(
        command,
        debug=args.debug,
        dry_run=args.dry_run,
    )

    stage_rows.append(
        {
            "stage": "global_affine_qc",
            "status": (
                "ok"
                if ok
                else "fail"
            ),
            "message": message,
        }
    )

    if not ok:
        write_stage_report(stage_rows, out_dir / RUN_REPORT_FILENAME)
        raise RuntimeError(f"Global affine QC stage failed:\n{message}")

    # =========================================================
    # Stage 2: burst-position residual characterization
    # =========================================================

    command = [
        sys.executable,
        str(burst_residual_script),

        "--root",
        str(input_dir),

        "--out-dir",
        str(burst_residual_dir),
    ]

    if args.recursive:
        command.append("--recursive")

    ok, message = run_command(
        command,
        debug=args.debug,
        dry_run=args.dry_run,
    )

    stage_rows.append(
        {
            "stage": "burst_residual_qc",
            "status": (
                "ok"
                if ok
                else "fail"
            ),
            "message": message,
        }
    )

    if not ok:
        write_stage_report(stage_rows, out_dir / RUN_REPORT_FILENAME)
        raise RuntimeError(f"Burst residual stage failed:\n{message}")

    # =========================================================
    # Stage 3: burst-to-burst shift characterization
    # =========================================================

    command = [
        sys.executable,
        str(burst_shift_script),

        "--root",
        str(input_dir),

        "--out-dir",
        str(burst_shift_dir),
    ]

    if args.recursive:
        command.append("--recursive")

    ok, message = run_command(
        command,
        debug=args.debug,
        dry_run=args.dry_run,
    )

    stage_rows.append(
        {
            "stage": "burst_shift_qc",
            "status": (
                "ok"
                if ok
                else "fail"
            ),
            "message": message,
        }
    )

    if not ok:
        write_stage_report(stage_rows, out_dir / RUN_REPORT_FILENAME)

        raise RuntimeError(
            f"Burst shift stage failed:\n{message}")

    # Dry-run stops before files are read.
    if args.dry_run:
        write_stage_report(stage_rows, out_dir / RUN_REPORT_FILENAME)
        print("[done] dry run complete")
        return

    # =========================================================
    # Stage 4: combined master table
    # =========================================================

    global_qc_csv = global_qc_dir / GLOBAL_QC_FILENAME
    burst_residual_csv = burst_residual_dir / BURST_RESIDUAL_FILENAME
    burst_shift_csv = burst_shift_dir / BURST_SHIFT_FILENAME
    

    for path in (global_qc_csv, burst_residual_csv, burst_shift_csv):
        if not path.exists():
            raise FileNotFoundError(f"Expected QC output was not created: {path}")

    master = build_master_summary(
        global_qc_csv,
        burst_residual_csv,
        burst_shift_csv,
        high_burst_range_ms=(args.high_burst_range_ms),
        high_burst_shift_ms=(args.high_burst_shift_ms),
    )


    master_path = out_dir / MASTER_FILENAME
    

    master.to_csv(master_path, index=False)

    stage_rows.append(
        {
            "stage": "master_summary",
            "status": "ok",
            "message": f"{len(master)} sessions -> {master_path}",
        }
    )


    # =========================================================
    # Five-way QC category manifests
    # =========================================================

    category_dir = out_dir / "Categories"
    category_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    five_way_categories = [
        "global_pass_low_local_movement",
        "global_pass_high_local_movement",
        "global_fail_low_local_movement",
        "global_fail_high_local_movement",
        "insufficient_local_structure",
    ]

    for category in five_way_categories:
        category_df = master.loc[
            master["alignment_qc_category_5way"] == category
        ].copy()

        category_df.to_csv(
            category_dir / f"{category}.csv",
            index=False,
        )

        print(
            f"[category] {category}: "
            f"{len(category_df)} sessions"
        )

    category_counts = master["alignment_qc_category_5way"].value_counts(dropna=False).rename_axis("alignment_qc_category_5way").reset_index(name="n_sessions")
    category_counts.to_csv(out_dir / "alignment_qc_category_counts.csv", index=False)

    write_stage_report(stage_rows, out_dir / RUN_REPORT_FILENAME)

    print()
    print(f"[done] QC suite complete")
    print(f"[done] sessions in master table: {len(master)}")
    print(f"[done] master summary -> {master_path}")
    print(f"[done] QC root -> {out_dir}")


if __name__ == "__main__":
    main()