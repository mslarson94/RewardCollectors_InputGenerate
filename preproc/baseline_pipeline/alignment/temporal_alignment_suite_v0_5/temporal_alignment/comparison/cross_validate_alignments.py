from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv, write_json
from temporal_alignment.common.affine import fit_affine
from temporal_alignment.common.model_io import require_columns, MATCH_REQUIRED, fit_mask
from temporal_alignment.common.stats import summarize_residuals


def _to_unix(series: pd.Series) -> np.ndarray:
    return pd.to_datetime(series, errors="raise").astype("int64").to_numpy(float) / 1e9


def _global_loocv(df: pd.DataFrame, robust: bool) -> pd.DataFrame:
    work = df.loc[fit_mask(df)].sort_values("corrected_ml_time").reset_index(drop=True)
    if len(work) < 3:
        raise ValueError("Global LOOCV requires at least 3 usable matched marks")
    x = _to_unix(work["corrected_ml_time"])
    y = _to_unix(work["rpi_time"])

    rows = []
    for i in range(len(work)):
        keep = np.ones(len(work), dtype=bool)
        keep[i] = False
        model = fit_affine(x[keep], y[keep], robust=robust)
        pred = float(model.predict_unix_s([x[i]])[0])
        rows.append({
            **work.iloc[i].to_dict(),
            "model_name": "global_affine",
            "cv_scheme": "leave_one_mark_out",
            "held_out": True,
            "used_for_fit": False,
            "predicted_rpi_time": pd.to_datetime(pred, unit="s"),
            "aligned_time": pd.to_datetime(pred, unit="s"),
            "predicted_offset_s": pred - x[i],
            "residual_s": y[i] - pred,
            "segment_id": "global",
        })
    return pd.DataFrame(rows)


def _cluster_boundary_loocv(df: pd.DataFrame) -> pd.DataFrame:
    require_columns(df, ["chunk_id", "chunk_role", "cluster_id"], "cluster-aware CV input")
    usable = df.loc[fit_mask(df)].copy()
    rows = []

    for chunk_id, ch in usable.loc[usable["chunk_id"].notna()].groupby("chunk_id", sort=False):
        start = ch.loc[ch["chunk_role"].eq("start")].copy()
        end = ch.loc[ch["chunk_role"].eq("end")].copy()
        if len(start) < 2 or len(end) < 2:
            continue

        # Hold out each boundary mark individually.
        for boundary_name, boundary_df in [("start", start), ("end", end)]:
            for idx in boundary_df.index:
                start_train = start.drop(index=idx) if boundary_name == "start" else start
                end_train = end.drop(index=idx) if boundary_name == "end" else end
                if start_train.empty or end_train.empty:
                    continue

                ds = float(np.median(start_train["raw_offset_s"].astype(float)))
                de = float(np.median(end_train["raw_offset_s"].astype(float)))
                T0 = float(np.median(_to_unix(start_train["corrected_ml_time"])))
                T1 = float(np.median(_to_unix(end_train["corrected_ml_time"])))
                if T1 <= T0:
                    continue

                held = ch.loc[idx]
                t = float(_to_unix(pd.Series([held["corrected_ml_time"]]))[0])
                obs = float(_to_unix(pd.Series([held["rpi_time"]]))[0])
                frac = (t - T0) / (T1 - T0)
                fitted_offset = ds + (de - ds) * frac
                pred = t + fitted_offset

                row = held.to_dict()
                row.update({
                    "model_name": "cluster_aware_affine",
                    "cv_scheme": "leave_one_boundary_mark_out",
                    "held_out": True,
                    "used_for_fit": False,
                    "predicted_rpi_time": pd.to_datetime(pred, unit="s"),
                    "aligned_time": pd.to_datetime(pred, unit="s"),
                    "predicted_offset_s": fitted_offset,
                    "residual_s": obs - pred,
                    "segment_id": str(chunk_id),
                    "held_out_boundary_role": boundary_name,
                })
                rows.append(row)

    if not rows:
        raise ValueError(
            "No cluster-aware held-out predictions were possible. "
            "Each complete chunk needs at least 2 usable start marks and 2 usable end marks."
        )
    return pd.DataFrame(rows)


def _blocked_kfold(df: pd.DataFrame, k: int, robust: bool) -> pd.DataFrame:
    work = df.loc[fit_mask(df)].sort_values("corrected_ml_time").reset_index(drop=True)
    n = len(work)
    if n < max(4, k):
        raise ValueError("Not enough usable matched marks for blocked k-fold CV")
    k = min(k, n)
    fold_edges = np.linspace(0, n, k + 1, dtype=int)
    x = _to_unix(work["corrected_ml_time"])
    y = _to_unix(work["rpi_time"])

    rows = []
    for f in range(k):
        lo, hi = fold_edges[f], fold_edges[f+1]
        test = np.zeros(n, dtype=bool)
        test[lo:hi] = True
        train = ~test
        if train.sum() < 2 or test.sum() == 0:
            continue
        model = fit_affine(x[train], y[train], robust=robust)
        preds = model.predict_unix_s(x[test])
        for j, pred in zip(np.flatnonzero(test), preds):
            row = work.iloc[j].to_dict()
            row.update({
                "model_name": "global_affine",
                "cv_scheme": f"blocked_{k}_fold",
                "fold": f + 1,
                "held_out": True,
                "used_for_fit": False,
                "predicted_rpi_time": pd.to_datetime(pred, unit="s"),
                "aligned_time": pd.to_datetime(pred, unit="s"),
                "predicted_offset_s": pred - x[j],
                "residual_s": y[j] - pred,
                "segment_id": "global",
            })
            rows.append(row)
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description="Held-out validation for temporal alignment models.")
    ap.add_argument("--matched-marks", required=True)
    ap.add_argument("--model", required=True, choices=["global", "cluster-aware"])
    ap.add_argument("--scheme", default="loocv", choices=["loocv", "blocked-kfold"])
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--robust", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    df = read_csv(args.matched_marks)
    require_columns(df, MATCH_REQUIRED, "matched marks")

    if args.model == "global":
        if args.scheme == "loocv":
            out = _global_loocv(df, robust=args.robust)
        else:
            out = _blocked_kfold(df, k=args.k, robust=args.robust)
    else:
        if args.scheme != "loocv":
            raise SystemExit("Cluster-aware v0.2 currently supports LOOCV only")
        out = _cluster_boundary_loocv(df)

    od = Path(args.out_dir)
    write_csv(out, od / f"cv_{args.model}_{args.scheme}.csv")
    summary = {
        "model": args.model,
        "scheme": args.scheme,
        "n_predictions": len(out),
        **summarize_residuals(out["residual_s"].to_numpy(float)),
    }
    write_json(summary, od / f"cv_{args.model}_{args.scheme}_summary.json")
    print(f"[ok] cross-validation -> {od}")


if __name__ == "__main__":
    main()
