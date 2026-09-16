from __future__ import annotations
import pandas as pd


def parse_times(series: pd.Series, name: str) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    bad = parsed.isna() & series.notna()
    if bad.any():
        examples = series.loc[bad].astype(str).head(5).tolist()
        raise ValueError(f"Unparseable timestamps in {name}: {examples}")
    return parsed


def seconds_from_origin(series: pd.Series, origin: pd.Timestamp | None = None):
    parsed = pd.to_datetime(series, errors="raise")
    if origin is None:
        origin = parsed.min()
    return (parsed - origin).dt.total_seconds().to_numpy(float), origin


def assert_monotonic(series: pd.Series, name: str, strict: bool = True) -> None:
    s = pd.to_datetime(series, errors="raise").reset_index(drop=True)
    diffs = s.diff().dropna().dt.total_seconds()
    ok = bool((diffs > 0).all()) if strict else bool((diffs >= 0).all())
    if not ok:
        raise ValueError(f"{name} is not {'strictly ' if strict else ''}monotonic increasing")
