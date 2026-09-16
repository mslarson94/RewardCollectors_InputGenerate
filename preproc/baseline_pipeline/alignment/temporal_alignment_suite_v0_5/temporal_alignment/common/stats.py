from __future__ import annotations
import numpy as np


def mad(x) -> float:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return float("nan")
    med = np.median(x)
    return float(np.median(np.abs(x - med)))


def robust_sigma(x) -> float:
    m = mad(x)
    return float(1.4826 * m) if np.isfinite(m) else float("nan")


def summarize_residuals(x) -> dict:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return {
            "n": 0, "median_residual_s": np.nan, "median_abs_residual_s": np.nan,
            "mad_residual_s": np.nan, "rmse_s": np.nan, "p95_abs_residual_s": np.nan,
            "max_abs_residual_s": np.nan,
        }
    ax = np.abs(x)
    return {
        "n": int(x.size),
        "median_residual_s": float(np.median(x)),
        "median_abs_residual_s": float(np.median(ax)),
        "mad_residual_s": mad(x),
        "rmse_s": float(np.sqrt(np.mean(x*x))),
        "p95_abs_residual_s": float(np.quantile(ax, 0.95)),
        "max_abs_residual_s": float(np.max(ax)),
    }
