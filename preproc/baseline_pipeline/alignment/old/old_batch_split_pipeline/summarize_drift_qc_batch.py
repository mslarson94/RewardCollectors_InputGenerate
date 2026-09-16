# summarize_drift_qc_batch.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass
class DriftQcRow:
    file_path: str
    file_name: str
    label: str
    drift_col: str
    n_matched: int
    mean_drift_s: float
    median_drift_s: float
    std_drift_s: float
    min_drift_s: float
    max_drift_s: float
    max_abs_drift_s: float
    pct_abs_over_0p25: float
    pct_abs_over_0p5: float
    pct_abs_over_0p75: float
    pct_positive: float
    pct_negative: float
    qc_status: str
    qc_reason: str


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Batch drift QC summarizer for *_aligned_with_RPi.csv files."
    )
    ap.add_argument("--in-dir", type=Path, required=True, help="Root directory to scan recursively.")
    ap.add_argument("--out-dir", type=Path, required=True, help="Directory for QC summary outputs.")
    ap.add_argument(
        "--glob",
        default="*_aligned_with_RPi.csv",
        help="Filename glob to scan recursively (default: *_aligned_with_RPi.csv).",
    )
    ap.add_argument(
        "--include-combined",
        action="store_true",
        help="Also scan *BioPacRNS_events.csv files for any drift columns present.",
    )
    ap.add_argument("--recheck-max-abs", type=float, default=0.75)
    ap.add_argument("--recheck-pct-over-0p5", type=float, default=0.25)
    ap.add_argument("--recheck-std", type=float, default=0.35)
    ap.add_argument("--spot-max-abs", type=float, default=0.4)
    ap.add_argument("--spot-std", type=float, default=0.2)
    ap.add_argument("--spot-median-abs", type=float, default=0.15)
    ap.add_argument("--min-n-for-hard-recheck", type=int, default=8)
    return ap.parse_args()


def find_candidate_files(root: Path, pattern: str, include_combined: bool) -> list[Path]:
    files = sorted(p for p in root.rglob(pattern) if p.is_file())
    if include_combined:
        files.extend(sorted(p for p in root.rglob("*BioPacRNS_events.csv") if p.is_file()))
    deduped = sorted(set(files))
    return deduped


def find_drift_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if str(c).endswith("_RPi_Timestamp_drift")]


def infer_label_from_drift_col(col: str) -> str:
    return col.replace("_RPi_Timestamp_drift", "")


def pct(mask: np.ndarray) -> float:
    if mask.size == 0:
        return float("nan")
    return float(mask.mean())


def classify_row(
    n_matched: int,
    median_abs: float,
    std_drift: float,
    max_abs: float,
    pct_over_0p5: float,
    recheck_max_abs: float,
    recheck_pct_over_0p5: float,
    recheck_std: float,
    spot_max_abs: float,
    spot_std: float,
    spot_median_abs: float,
    min_n_for_hard_recheck: int,
) -> tuple[str, str]:
    reasons: list[str] = []

    if n_matched == 0:
        return "no_data", "no matched drift rows"

    if n_matched >= min_n_for_hard_recheck and max_abs >= recheck_max_abs:
        reasons.append(f"max_abs>={recheck_max_abs}")
    if n_matched >= min_n_for_hard_recheck and pct_over_0p5 >= recheck_pct_over_0p5:
        reasons.append(f"pct_abs_over_0p5>={recheck_pct_over_0p5}")
    if n_matched >= min_n_for_hard_recheck and std_drift >= recheck_std:
        reasons.append(f"std>={recheck_std}")
    if reasons:
        return "recheck", "; ".join(reasons)

    reasons = []
    if max_abs >= spot_max_abs:
        reasons.append(f"max_abs>={spot_max_abs}")
    if std_drift >= spot_std:
        reasons.append(f"std>={spot_std}")
    if median_abs >= spot_median_abs:
        reasons.append(f"abs(median)>={spot_median_abs}")
    if reasons:
        return "spot_check", "; ".join(reasons)

    return "ok", "within thresholds"


def summarize_one_file(path: Path, args: argparse.Namespace) -> list[DriftQcRow]:
    df = pd.read_csv(path)
    drift_cols = find_drift_columns(df)
    rows: list[DriftQcRow] = []

    for col in drift_cols:
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        vals = series.to_numpy(dtype=float)

        if vals.size == 0:
            status, reason = "no_data", "no matched drift rows"
            rows.append(
                DriftQcRow(
                    file_path=str(path),
                    file_name=path.name,
                    label=infer_label_from_drift_col(col),
                    drift_col=col,
                    n_matched=0,
                    mean_drift_s=float("nan"),
                    median_drift_s=float("nan"),
                    std_drift_s=float("nan"),
                    min_drift_s=float("nan"),
                    max_drift_s=float("nan"),
                    max_abs_drift_s=float("nan"),
                    pct_abs_over_0p25=float("nan"),
                    pct_abs_over_0p5=float("nan"),
                    pct_abs_over_0p75=float("nan"),
                    pct_positive=float("nan"),
                    pct_negative=float("nan"),
                    qc_status=status,
                    qc_reason=reason,
                )
            )
            continue

        abs_vals = np.abs(vals)
        mean_drift = float(np.mean(vals))
        median_drift = float(np.median(vals))
        std_drift = float(np.std(vals, ddof=1)) if vals.size > 1 else 0.0
        min_drift = float(np.min(vals))
        max_drift = float(np.max(vals))
        max_abs = float(np.max(abs_vals))
        pct_025 = pct(abs_vals > 0.25)
        pct_05 = pct(abs_vals > 0.5)
        pct_075 = pct(abs_vals > 0.75)
        pct_pos = pct(vals > 0)
        pct_neg = pct(vals < 0)

        status, reason = classify_row(
            n_matched=int(vals.size),
            median_abs=abs(median_drift),
            std_drift=std_drift,
            max_abs=max_abs,
            pct_over_0p5=pct_05,
            recheck_max_abs=args.recheck_max_abs,
            recheck_pct_over_0p5=args.recheck_pct_over_0p5,
            recheck_std=args.recheck_std,
            spot_max_abs=args.spot_max_abs,
            spot_std=args.spot_std,
            spot_median_abs=args.spot_median_abs,
            min_n_for_hard_recheck=args.min_n_for_hard_recheck,
        )

        rows.append(
            DriftQcRow(
                file_path=str(path),
                file_name=path.name,
                label=infer_label_from_drift_col(col),
                drift_col=col,
                n_matched=int(vals.size),
                mean_drift_s=mean_drift,
                median_drift_s=median_drift,
                std_drift_s=std_drift,
                min_drift_s=min_drift,
                max_drift_s=max_drift,
                max_abs_drift_s=max_abs,
                pct_abs_over_0p25=pct_025,
                pct_abs_over_0p5=pct_05,
                pct_abs_over_0p75=pct_075,
                pct_positive=pct_pos,
                pct_negative=pct_neg,
                qc_status=status,
                qc_reason=reason,
            )
        )

    return rows


def status_rank(status: str) -> int:
    order = {
        "recheck": 3,
        "spot_check": 2,
        "ok": 1,
        "no_data": 0,
    }
    return order.get(status, -1)


def build_compact_summary(df_long: pd.DataFrame) -> pd.DataFrame:
    if df_long.empty:
        return df_long.copy()

    tmp = df_long.copy()
    tmp["status_rank"] = tmp["qc_status"].map(status_rank)
    tmp = tmp.sort_values(
        by=["file_path", "status_rank", "max_abs_drift_s"],
        ascending=[True, False, False],
        kind="stable",
    )
    compact = tmp.groupby("file_path", as_index=False).first()
    compact = compact.drop(columns=["status_rank"])
    return compact


def print_overview(df_long: pd.DataFrame) -> None:
    print("\n=== Drift QC overview ===")
    if df_long.empty:
        print("No drift rows found.")
        return

    status_counts = df_long["qc_status"].value_counts(dropna=False).to_dict()
    for status in ["recheck", "spot_check", "ok", "no_data"]:
        if status in status_counts:
            print(f"{status:10s} {status_counts[status]}")

    print("\nTop recheck candidates:")
    recheck = df_long[df_long["qc_status"] == "recheck"].copy()
    if recheck.empty:
        print("None")
        return

    show_cols = [
        "file_name",
        "label",
        "n_matched",
        "median_drift_s",
        "std_drift_s",
        "max_abs_drift_s",
        "pct_abs_over_0p5",
        "qc_reason",
    ]
    print(recheck.sort_values(["max_abs_drift_s", "std_drift_s"], ascending=[False, False])[show_cols].head(20).to_string(index=False))


def main() -> None:
    args = parse_args()
    in_dir = args.in_dir.expanduser()
    out_dir = args.out_dir.expanduser()

    if not in_dir.is_dir():
        raise NotADirectoryError(f"in-dir is not a directory: {in_dir}")

    files = find_candidate_files(in_dir, args.glob, args.include_combined)
    if not files:
        raise SystemExit(f"No candidate files found in {in_dir} with glob {args.glob!r}")

    rows: list[DriftQcRow] = []
    for path in files:
        try:
            rows.extend(summarize_one_file(path, args))
        except Exception as e:
            rows.append(
                DriftQcRow(
                    file_path=str(path),
                    file_name=path.name,
                    label="",
                    drift_col="",
                    n_matched=0,
                    mean_drift_s=float("nan"),
                    median_drift_s=float("nan"),
                    std_drift_s=float("nan"),
                    min_drift_s=float("nan"),
                    max_drift_s=float("nan"),
                    max_abs_drift_s=float("nan"),
                    pct_abs_over_0p25=float("nan"),
                    pct_abs_over_0p5=float("nan"),
                    pct_abs_over_0p75=float("nan"),
                    pct_positive=float("nan"),
                    pct_negative=float("nan"),
                    qc_status="error",
                    qc_reason=str(e),
                )
            )

    df_long = pd.DataFrame([asdict(r) for r in rows])
    df_compact = build_compact_summary(df_long)

    out_dir.mkdir(parents=True, exist_ok=True)
    long_csv = out_dir / "drift_qc_long.csv"
    compact_csv = out_dir / "drift_qc_compact.csv"
    df_long.to_csv(long_csv, index=False)
    df_compact.to_csv(compact_csv, index=False)

    print(f"[ok] wrote long QC table -> {long_csv}")
    print(f"[ok] wrote compact QC table -> {compact_csv}")
    print_overview(df_long)


if __name__ == "__main__":
    main()