from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv, write_json
from temporal_alignment.common.model_io import require_columns, MATCH_REQUIRED, fit_mask
from temporal_alignment.common.affine import fit_affine
from temporal_alignment.common.apply import to_unix_s
from temporal_alignment.common.stats import summarize_residuals


def fit_one(group: pd.DataFrame, robust: bool, label: str) -> tuple[dict, pd.DataFrame]:
    work = group.loc[fit_mask(group)].sort_values("corrected_ml_time").reset_index(drop=True)
    if len(work) < 2:
        return {
            "scope": label,
            "status": "insufficient_marks",
            "n_used": int(len(work)),
        }, pd.DataFrame()

    x = to_unix_s(work["corrected_ml_time"])
    y = to_unix_s(work["rpi_time"])

    model = fit_affine(x, y, robust=robust)
    pred = model.predict_unix_s(x)
    fitted_offset = pred - x
    residual = y - pred
    raw_offset = y - x
    elapsed = x - x.min()

    summary = {
        "scope": label,
        "status": "ok",
        "n_used": model.n_used,
        "reference_ml_time": str(pd.to_datetime(model.origin_unix_s, unit="s")),
        "reference_offset_s": model.intercept_s,
        "affine_slope": model.slope,
        "clock_skew_ppm": model.skew_ppm,
        "drift_s_per_hour": (model.slope - 1.0) * 3600.0,
        "drift_ms_per_min": (model.slope - 1.0) * 60000.0,
        "observed_offset_change_s": float(raw_offset[-1] - raw_offset[0]) if len(raw_offset) > 1 else np.nan,
        "fitted_offset_change_s": float(fitted_offset[-1] - fitted_offset[0]) if len(fitted_offset) > 1 else np.nan,
        "duration_s": float(elapsed[-1] - elapsed[0]) if len(elapsed) > 1 else 0.0,
        "raw_offset_start_s": float(raw_offset[0]),
        "raw_offset_end_s": float(raw_offset[-1]),
        "fitted_offset_start_s": float(fitted_offset[0]),
        "fitted_offset_end_s": float(fitted_offset[-1]),
        **summarize_residuals(residual),
    }

    points = work.copy()
    points["scope"] = label
    points["elapsed_s"] = elapsed
    points["raw_offset_recomputed_s"] = raw_offset
    points["fitted_offset_s"] = fitted_offset
    points["drift_residual_s"] = residual
    points["drift_rate_s_per_s"] = model.slope - 1.0

    return summary, points


def main():
    ap = argparse.ArgumentParser(
        description="Characterize clock offset and linear drift/skew without transforming timestamps."
    )
    ap.add_argument("--matched-marks", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--robust", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument(
        "--by",
        choices=["session", "chunk", "both"],
        default="both",
        help="Fit one session-wide model, per-chunk models, or both.",
    )
    ap.add_argument(
        "--chunk-col",
        default="chunk_id",
        help="Chunk ID column used when --by includes chunk.",
    )
    args = ap.parse_args()

    df = read_csv(args.matched_marks)
    require_columns(df, MATCH_REQUIRED, "matched marks")

    summaries = []
    point_frames = []

    if args.by in {"session", "both"}:
        s, p = fit_one(df, robust=args.robust, label="session")
        summaries.append(s)
        if not p.empty:
            point_frames.append(p)

    if args.by in {"chunk", "both"}:
        if args.chunk_col not in df.columns:
            raise KeyError(
                f"--by {args.by} requested, but chunk column {args.chunk_col!r} is absent"
            )
        chunked = df.loc[df[args.chunk_col].notna()].copy()
        for chunk_id, g in chunked.groupby(args.chunk_col, sort=False):
            s, p = fit_one(g, robust=args.robust, label=f"chunk:{chunk_id}")
            s["chunk_id"] = chunk_id
            summaries.append(s)
            if not p.empty:
                p["chunk_id"] = chunk_id
                point_frames.append(p)

    od = Path(args.out_dir)
    summary_df = pd.DataFrame(summaries)
    write_csv(summary_df, od / "drift_summary.csv")

    if point_frames:
        points = pd.concat(point_frames, ignore_index=True)
        write_csv(points, od / "drift_points.csv")
    else:
        points = pd.DataFrame()

    overall = {
        "robust": bool(args.robust),
        "by": args.by,
        "n_input_rows": int(len(df)),
        "n_usable_rows": int(fit_mask(df).sum()),
        "n_models_ok": int((summary_df.get("status") == "ok").sum()) if "status" in summary_df else 0,
        "n_models_total": int(len(summary_df)),
    }
    write_json(overall, od / "drift_run_summary.json")
    print(f"[ok] drift calculation -> {od}")


if __name__ == "__main__":
    main()
