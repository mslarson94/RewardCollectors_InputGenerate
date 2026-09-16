from __future__ import annotations
import numpy as np
import pandas as pd


def to_unix_s(series: pd.Series) -> np.ndarray:
    return pd.to_datetime(series, errors="raise").astype("int64").to_numpy(float) / 1e9


def from_unix_s(values) -> pd.Series:
    return pd.to_datetime(np.asarray(values, dtype=float), unit="s")


def apply_affine_unix(unix_s, slope: float, intercept_s: float, origin_unix_s: float):
    unix_s = np.asarray(unix_s, dtype=float)
    x = unix_s - origin_unix_s
    return origin_unix_s + intercept_s + slope * x
