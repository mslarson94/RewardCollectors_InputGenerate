from __future__ import annotations
from dataclasses import dataclass
import numpy as np


@dataclass(frozen=True)
class AffineFit:
    slope: float
    intercept_s: float
    origin_unix_s: float
    n_used: int
    iterations: int

    def predict_unix_s(self, ml_unix_s):
        x = np.asarray(ml_unix_s, dtype=float) - self.origin_unix_s
        return self.origin_unix_s + self.intercept_s + self.slope * x

    @property
    def skew_ppm(self) -> float:
        return (self.slope - 1.0) * 1e6


def _weighted_lstsq(x, y, w):
    X = np.column_stack([x, np.ones_like(x)])
    sw = np.sqrt(w)
    beta, *_ = np.linalg.lstsq(X * sw[:, None], y * sw, rcond=None)
    return float(beta[0]), float(beta[1])


def fit_affine(ml_unix_s, rpi_unix_s, robust: bool = True, max_iter: int = 30) -> AffineFit:
    ml = np.asarray(ml_unix_s, dtype=float)
    rpi = np.asarray(rpi_unix_s, dtype=float)
    mask = np.isfinite(ml) & np.isfinite(rpi)
    ml, rpi = ml[mask], rpi[mask]
    if ml.size < 2:
        raise ValueError("Need at least two valid matched marks for affine fitting")

    origin = float(np.min(ml))
    x = ml - origin
    y = rpi - origin
    w = np.ones_like(x)
    a, b = _weighted_lstsq(x, y, w)

    if not robust:
        return AffineFit(a, b, origin, len(x), 1)

    for it in range(1, max_iter + 1):
        resid = y - (a*x + b)
        med = np.median(resid)
        scale = 1.4826 * np.median(np.abs(resid - med))
        if not np.isfinite(scale) or scale < 1e-12:
            return AffineFit(a, b, origin, len(x), it)
        c = 1.345 * scale
        ar = np.abs(resid - med)
        new_w = np.ones_like(ar)
        large = ar > c
        new_w[large] = c / ar[large]
        new_a, new_b = _weighted_lstsq(x, y, new_w)
        if max(abs(new_a-a), abs(new_b-b)) < 1e-12:
            a, b = new_a, new_b
            return AffineFit(a, b, origin, len(x), it)
        a, b, w = new_a, new_b, new_w

    return AffineFit(a, b, origin, len(x), max_iter)
