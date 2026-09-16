# batchAlignHelpers.py
'''
Author: Myra Saraí Larson 10/09/2025
helper script for my batch align script pipeline for magic leap to raspberry pi files 
'''
from __future__ import annotations
import argparse
import os
import re
import numpy as np
import pandas as pd
import shlex
import subprocess
import matplotlib.pyplot as plt

from dataclasses import dataclass
from datetime import timedelta, datetime
from pathlib import Path
from typing import List, Optional, Sequence, Tuple
# -------------------------------
# Extract RPi Marks Utilities
# -------------------------------

def _read_logfile(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.rstrip("\n") for ln in f]
    return lines


def _fix_fraction_colon(ts: str) -> str:
    """
    Some logs use HH:MM:SS:ffffff; convert to HH:MM:SS.ffffff
    """
    m = re.fullmatch(r"(\d{2}:\d{2}:\d{2}):(\d+)", str(ts).strip())
    return f"{m.group(1)}.{m.group(2)}" if m else str(ts).strip()


def _to_datetime_on_date(times: Sequence[str], session_date: str) -> List[datetime]:
    out: List[datetime] = []
    for t in times:
        t = _fix_fraction_colon(t)
        parsed = None
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(f"{session_date} {t}", fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            raise ValueError(f"Unparseable time: '{t}' (session_date={session_date})")
        out.append(parsed)
    return out


def _scan_ip_pairs(lines: List[str], search_ip: str) -> List[Tuple[int, str, str]]:
    """
    Return list of (pair_index, header_line, ts_line) for the given IP.

    We consider any line containing the exact substring `search_ip` (e.g., "[192.168.50.109]")
    as a header; the very next non-empty line must be a time string (HH:MM:SS[.ffffff]).
    """
    time_re = re.compile(r"^\d{2}:\d{2}:\d{2}(?::\d+|\.\d+)?$")
    matches: List[Tuple[int, str, str]] = []
    pair_idx = 0
    i = 0
    N = len(lines)
    while i < N:
        header = lines[i]
        if search_ip in header:
            # find next non-empty line
            j = i + 1
            while j < N and (lines[j] is None or lines[j].strip() == ""):
                j += 1
            if j < N:
                ts_line = _fix_fraction_colon(lines[j].strip())
                if time_re.match(ts_line):
                    matches.append((pair_idx, header.strip(), ts_line))
                    pair_idx += 1
                    i = j + 1
                    continue
        i += 1
    return matches



def _scan_all_ip_pairs(lines: List[str]) -> List[Tuple[str, int, str, str]]:
    """
    Scan the log lines and return a list of (ip, pair_index, header_line, ts_line)
    for *all* IP addresses.

    pair_index is local to each IP address (i.e., starts at 0 for each IP and
    increments independently).

    We treat any line containing an IPv4 address in square brackets, e.g. "[192.168.50.109]",
    as a header; the very next non-empty line must be a time string
    (HH:MM:SS[.ffffff] or HH:MM:SS:ffffff after fixing).
    """
    time_re = re.compile(r"^\d{2}:\d{2}:\d{2}(?::\d+|\.\d+)?$")
    ip_re = re.compile(r"\[(\d{1,3}(?:\.\d{1,3}){3})\]")

    matches: List[Tuple[str, int, str, str]] = []
    pair_idx_by_ip: dict[str, int] = {}

    i = 0
    N = len(lines)
    while i < N:
        header = lines[i]
        if header is None:
            i += 1
            continue

        m_ip = ip_re.search(header)
        if m_ip:
            ip = m_ip.group(1)
            # find next non-empty line
            j = i + 1
            while j < N and (lines[j] is None or lines[j].strip() == ""):
                j += 1
            if j < N:
                ts_line_raw = lines[j].strip()
                ts_line = _fix_fraction_colon(ts_line_raw)
                if time_re.match(ts_line):
                    pair_idx = pair_idx_by_ip.get(ip, 0)
                    matches.append((ip, pair_idx, header.strip(), ts_line))
                    pair_idx_by_ip[ip] = pair_idx + 1
                    i = j + 1
                    continue
        i += 1

    return matches

# -------------------------------
# Merge ML with RPi Marks Utilities
# -------------------------------

def _select_mark_rows(df: pd.DataFrame, col: str, values: Sequence[str]) -> pd.DataFrame:
    vals = {str(v).strip().lower() for v in values}
    mask = df[col].astype(str).str.strip().str.lower().isin(vals)
    out = df.loc[mask].copy()
    if out.empty:
        raise ValueError(f"no rows where {col} in {sorted(vals)}")
    return out


def _auto_offset_hours(ml_times: pd.Series, rpi_times: pd.Series) -> float:
    """
    Estimate only the coarse timezone / wall-clock-hour difference.

    IMPORTANT: this intentionally rounds to a whole hour.  The previous
    implementation returned the full median difference in hours, which could
    silently remove the sub-hour clock offset that the alignment model is
    supposed to estimate.
    """
    ml = pd.to_datetime(ml_times, errors="coerce").dropna().reset_index(drop=True)
    rp = pd.to_datetime(rpi_times, errors="coerce").dropna().reset_index(drop=True)
    n = int(min(10, len(ml), len(rp)))
    if n == 0:
        return 0.0
    diffs_s = (ml.iloc[:n] - rp.iloc[:n]).dt.total_seconds().to_numpy()
    return float(np.rint(np.nanmedian(diffs_s) / 3600.0))

def _round_offset_hours(x: float, step: float = 1.0) -> float:
    """Round offset to nearest multiple of `step` hours (default: 1 hour)."""
    return round(x / step) * step


def _nearest_unique_alignment_v1(ml_times: pd.Series, rpi_times: pd.Series, max_gap: Optional[float]) -> Tuple[np.ndarray, np.ndarray, List[str]]:
    ml = ml_times.reset_index(drop=True).to_numpy(dtype="datetime64[ns]")
    rpi = rpi_times.reset_index(drop=True).to_numpy(dtype="datetime64[ns]")
    n, m = len(ml), len(rpi)
    match_idx = np.full(n, -1, dtype=int)
    deltas = np.full(n, np.nan, dtype=float)
    reasons: List[str] = ["" for _ in range(n)]
    last_j = -1
    for i in range(n):
        if last_j + 1 >= m:
            for k in range(i, n):
                reasons[k] = "exhausted log"
            break
        # search candidates >= last_j+1
        diffs = np.abs((rpi[last_j+1:] - ml[i]).astype("timedelta64[ns]").astype("int64")) / 1e9
        j_rel = int(np.argmin(diffs))
        j = last_j + 1 + j_rel
        if (max_gap is not None) and (diffs[j_rel] > max_gap):
            reasons[i] = f"no log within ≤{max_gap:.3f}s"
            continue
        match_idx[i] = j
        deltas[i] = ((rpi[j] - ml[i]).astype("timedelta64[ns]").astype("int64")) / 1e9
        last_j = j
    return match_idx, deltas, reasons

def _nearest_unique_alignment(
    ml_times: pd.Series,
    rpi_times: pd.Series,
    max_gap: Optional[float],
) -> Tuple[np.ndarray, np.ndarray, List[str]]:

    ml = pd.to_datetime(
        ml_times,
        errors="coerce",
    ).reset_index(drop=True)

    rpi = pd.to_datetime(
        rpi_times,
        errors="coerce",
    ).reset_index(drop=True)

    n = len(ml)
    m = len(rpi)

    match_idx = np.full(n, -1, dtype=int)
    deltas = np.full(n, np.nan, dtype=float)
    reasons: List[str] = ["" for _ in range(n)]

    last_j = -1

    for i in range(n):
        if pd.isna(ml.iloc[i]):
            reasons[i] = "invalid ML timestamp"
            continue

        candidate_indices = [
            j
            for j in range(last_j + 1, m)
            if pd.notna(rpi.iloc[j])
        ]

        if not candidate_indices:
            for k in range(i, n):
                if not reasons[k]:
                    reasons[k] = "exhausted log"
            break

        diffs = np.array(
            [
                abs(
                    (
                        rpi.iloc[j] - ml.iloc[i]
                    ).total_seconds()
                )
                for j in candidate_indices
            ],
            dtype=float,
        )

        j_rel = int(np.argmin(diffs))
        j = candidate_indices[j_rel]
        gap_s = float(diffs[j_rel])

        if max_gap is not None and gap_s > max_gap:
            reasons[i] = f"no log within ≤{max_gap:.3f}s"
            continue

        match_idx[i] = j
        deltas[i] = (
            rpi.iloc[j] - ml.iloc[i]
        ).total_seconds()

        last_j = j

    return match_idx, deltas, reasons
# -------------------------------
# Summarize Drift Utilities
# -------------------------------

def _series_for_label(df: pd.DataFrame, label: str, timeCol: str):
    drift_col = f"{label}_RPi_Timestamp_drift"
    ts_col = f"{label}_RPi_Timestamp"
    if drift_col not in df.columns or ts_col not in df.columns:
        return None
    ml_ts = pd.to_datetime(df.get(timeCol, pd.Series([pd.NaT]*len(df))), errors="coerce")
    mask = (~df[drift_col].isna()) & (~ml_ts.isna()) & (~df[ts_col].isna())
    if not mask.any():
        return None
    x = (np.arange(len(df)) + 1)[mask.to_numpy()]  # event index positions of this label's matched rows
    y = df.loc[mask, drift_col].astype(float).to_numpy()
    return x, y, ml_ts.loc[mask].reset_index(drop=True)


def _plot_single(label: str, x: np.ndarray, y: np.ndarray, title: str, out_png: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.scatter(x, y)
    ax.set_title(title)
    ax.set_xlabel("Event Index")
    ax.set_ylabel("Drift (s)")
    ax.grid(True, alpha=0.4)
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def _summarize(y: np.ndarray) -> dict:
    return {
        "n_matched": int(len(y)),
        "mean_drift_s": float(np.nanmean(y)) if len(y) else float("nan"),
        "median_drift_s": float(np.nanmedian(y)) if len(y) else float("nan"),
        "max_abs_drift_s": float(np.nanmax(np.abs(y))) if len(y) else float("nan"),
    }


def _available_labels(df: pd.DataFrame) -> List[str]:
    labels = []
    for prefix in ("BioPac", "RNS"):
        if f"{prefix}_RPi_Timestamp_drift" in df.columns:
            labels.append(prefix)
    # allow any other prefixes that follow the pattern *_RPi_Timestamp_drift
    for col in df.columns:
        if col.endswith("_RPi_Timestamp_drift"):
            prefix = col[:-len("_RPi_Timestamp_drift")]
            if prefix not in labels:
                labels.append(prefix)
    return labels


# -------------------------------
# Merge RPi Event Utilities
# -------------------------------

def _parse_base_and_device(stem: str) -> Tuple[str, str, Optional[str]]:
    # Expecting: <base>_<device>_<Label>_events
    parts = stem.split("_")
    if len(parts) < 4 or parts[-1] != "events":
        # Fallback: try to find last 3 tokens device/label/events
        m = re.search(r"(.+)_([^_]+)_([^_]+)_events$", stem)
        if not m:
            raise ValueError(f"cannot parse base/device/label from stem '{stem}'")
        return m.group(1), m.group(2), m.group(3)
    base = "_".join(parts[:-3])
    device = parts[-3]
    label = parts[-2]
    return base, device, label


def _is_label_col(col: str) -> bool:
    return col.endswith("_RPi_Timestamp") or col.endswith("_RPi_Timestamp_drift") or col.endswith("_RPi_Matched") or col.endswith("_RPi_MatchReason")


# -------------------------------
# Batch Split Pipeline Utilities
# -------------------------------

def _parse_device_ip_map(path: Path) -> Dict[str, str]:
    text = path.read_text(encoding="utf-8")
    out: Dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"'([^']+)'\s*:\s*'([^']+)'", line)
        if m:
            out[m.group(1).strip()] = m.group(2).strip()
    return out


def _to_session_date(s: str) -> str:
    """Convert testingDate like '02_17_2025' or '02-17-2025' to '2025-02-17'."""
    s = s.replace("/", "_").replace("-", "_")
    mm, dd, yy = s.split("_")
    return f"{yy}-{int(mm):02d}-{int(dd):02d}"


def _normalize_ml_stem(stem: str, suffixes: Sequence[str]) -> str:
    """Strip any of the given suffixes (if stem ends with them), repeatedly.
    Then trim trailing underscores/hyphens."""
    base = stem
    changed = True
    while changed:
        changed = False
        for s in suffixes:
            if s and base.endswith(s):
                base = base[: -len(s)]
                changed = True
    return re.sub(r"[_-]+$", "", base)


def _missing_like(x: object) -> bool:
    s = str(x).strip().lower()
    return s in {"", "none", "na", "n/a", "nan", "<na>", "null", "-"}


def _resolve_ml_csv(ml_root: Path, cleaned_value: str, suffixes: Sequence[str]) -> Path:
    """Resolve ML CSV from a 'cleanedFile' value that may include variant suffixes."""
    p = Path(cleaned_value)
    stem = _normalize_ml_stem(p.stem, suffixes)
    ext = p.suffix or ".csv"

    candidates = [
        ml_root / p.name,  # as provided
        ml_root / f"{stem}_eventsFlat{ext}",
        ml_root / f"{stem}_eventsFlat{ext}",
        ml_root / f"{stem}_processed{ext}",
        ml_root / f"{stem}_earliestRoundStart{ext}",
    ]
    tried = []
    for c in candidates:
        tried.append(str(c))
        if c.exists():
            return c
    raise FileNotFoundError("could not resolve ML CSV for '" + cleaned_value + "'. Tried: " + ", ".join(tried))




# -------------------------------
# Robust affine clock alignment
# -------------------------------

@dataclass(frozen=True)
class AffineClockModel:
    """
    Map timestamps from ML time into RPi time:

        rpi_time ~= reference_time + offset_at_reference_s
                   + rate * (ml_time - reference_time)

    drift_ppm = (rate - 1) * 1e6
    """
    reference_time: pd.Timestamp
    offset_at_reference_s: float
    rate: float
    drift_ppm: float
    n_input_pairs: int
    n_inliers: int
    residual_median_s: float
    residual_mad_s: float
    residual_rmse_s: float


def _datetime_series(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, errors="coerce").reset_index(drop=True)


def _mad_scale(values: np.ndarray) -> float:
    v = np.asarray(values, dtype=float)
    v = v[np.isfinite(v)]
    if len(v) == 0:
        return float("nan")
    med = float(np.median(v))
    mad = float(np.median(np.abs(v - med)))
    return 1.4826 * mad


def _predict_rpi_from_ml(
    ml_times: pd.Series,
    model: AffineClockModel,
) -> pd.Series:
    ml = pd.to_datetime(ml_times, errors="coerce")
    elapsed_s = (ml - model.reference_time).dt.total_seconds()
    predicted = (
        model.reference_time
        + pd.to_timedelta(
            model.offset_at_reference_s + model.rate * elapsed_s,
            unit="s",
        )
    )
    return pd.Series(predicted, index=ml.index)


def _predict_ml_from_rpi(
    rpi_times: pd.Series,
    model: AffineClockModel,
) -> pd.Series:
    if not np.isfinite(model.rate) or model.rate == 0:
        raise ValueError("Affine model has invalid/zero rate")
    rp = pd.to_datetime(rpi_times, errors="coerce")
    y_s = (rp - model.reference_time).dt.total_seconds()
    x_s = (y_s - model.offset_at_reference_s) / model.rate
    predicted = model.reference_time + pd.to_timedelta(x_s, unit="s")
    return pd.Series(predicted, index=rp.index)


def _fit_affine_clock(
    ml_times: pd.Series,
    rpi_times: pd.Series,
    *,
    sigma_clip: float = 4.0,
    max_iterations: int = 8,
    min_pairs: int = 3,
    max_abs_residual_s: Optional[float] = None,
) -> Tuple[AffineClockModel, np.ndarray, np.ndarray]:
    """
    Robustly fit RPi time as an affine function of ML time.

    Returns:
      model
      inlier_mask over the input pairs
      residual_s = observed_rpi - predicted_rpi for every valid input pair
                   (NaN for rows with invalid timestamps)
    """
    ml = pd.to_datetime(ml_times, errors="coerce").reset_index(drop=True)
    rp = pd.to_datetime(rpi_times, errors="coerce").reset_index(drop=True)

    valid = (ml.notna() & rp.notna()).to_numpy()
    if int(valid.sum()) < min_pairs:
        raise ValueError(
            f"Need at least {min_pairs} valid timestamp pairs for affine fit; "
            f"got {int(valid.sum())}"
        )

    valid_idx = np.flatnonzero(valid)
    ml_v = ml.iloc[valid_idx].reset_index(drop=True)
    rp_v = rp.iloc[valid_idx].reset_index(drop=True)

    # Center near the middle of the session for numerical stability and for an
    # interpretable "offset at reference time".
    reference_time = pd.Timestamp(ml_v.iloc[len(ml_v) // 2])
    x = (ml_v - reference_time).dt.total_seconds().to_numpy(dtype=float)
    y = (rp_v - reference_time).dt.total_seconds().to_numpy(dtype=float)

    keep = np.ones(len(x), dtype=bool)
    for _ in range(max_iterations):
        if int(keep.sum()) < min_pairs:
            break

        slope, intercept = np.polyfit(x[keep], y[keep], 1)
        residual = y - (intercept + slope * x)

        center = float(np.median(residual[keep]))
        scale = _mad_scale(residual[keep])

        if not np.isfinite(scale) or scale <= 1e-12:
            new_keep = keep.copy()
        else:
            new_keep = np.abs(residual - center) <= sigma_clip * scale

        if max_abs_residual_s is not None:
            new_keep &= np.abs(residual) <= float(max_abs_residual_s)

        if int(new_keep.sum()) < min_pairs:
            break
        if np.array_equal(new_keep, keep):
            keep = new_keep
            break
        keep = new_keep

    slope, intercept = np.polyfit(x[keep], y[keep], 1)
    residual_v = y - (intercept + slope * x)

    full_residual = np.full(len(ml), np.nan, dtype=float)
    full_inlier = np.zeros(len(ml), dtype=bool)
    full_residual[valid_idx] = residual_v
    full_inlier[valid_idx] = keep

    inlier_resid = residual_v[keep]
    rmse = float(np.sqrt(np.mean(np.square(inlier_resid)))) if len(inlier_resid) else float("nan")
    model = AffineClockModel(
        reference_time=reference_time,
        offset_at_reference_s=float(intercept),
        rate=float(slope),
        drift_ppm=float((slope - 1.0) * 1e6),
        n_input_pairs=int(valid.sum()),
        n_inliers=int(keep.sum()),
        residual_median_s=float(np.median(inlier_resid)) if len(inlier_resid) else float("nan"),
        residual_mad_s=float(_mad_scale(inlier_resid)) if len(inlier_resid) else float("nan"),
        residual_rmse_s=rmse,
    )
    return model, full_inlier, full_residual


def _shift_datetime_series(values: pd.Series, seconds: float) -> pd.Series:
    v = pd.to_datetime(values, errors="coerce")
    return v + pd.to_timedelta(float(seconds), unit="s")


def _candidate_clock_offsets(
    ml_times: pd.Series,
    rpi_times: pd.Series,
    *,
    search_window_s: float = 30.0,
    quantization_s: float = 0.05,
    max_points: int = 80,
) -> np.ndarray:
    """
    Generate plausible RPi-minus-ML offsets directly from the two mark trains.
    Candidate scoring is performed separately; this function makes no manual
    pairing assumptions.
    """
    ml = pd.to_datetime(ml_times, errors="coerce").dropna().sort_values().reset_index(drop=True)
    rp = pd.to_datetime(rpi_times, errors="coerce").dropna().sort_values().reset_index(drop=True)
    if len(ml) == 0 or len(rp) == 0:
        return np.array([0.0])

    # Evenly subsample very large inputs so the cross-product remains bounded.
    if len(ml) > max_points:
        idx = np.linspace(0, len(ml) - 1, max_points).round().astype(int)
        ml = ml.iloc[np.unique(idx)].reset_index(drop=True)
    if len(rp) > max_points:
        idx = np.linspace(0, len(rp) - 1, max_points).round().astype(int)
        rp = rp.iloc[np.unique(idx)].reset_index(drop=True)

    ml_ns = ml.to_numpy(dtype="datetime64[ns]").astype("int64")
    rp_ns = rp.to_numpy(dtype="datetime64[ns]").astype("int64")
    diffs_s = (rp_ns[:, None] - ml_ns[None, :]).astype(float) / 1e9
    vals = diffs_s[np.abs(diffs_s) <= float(search_window_s)]
    if len(vals) == 0:
        return np.array([0.0])

    q = max(float(quantization_s), 1e-6)
    rounded = np.round(vals / q) * q
    unique, counts = np.unique(rounded, return_counts=True)
    order = np.argsort(counts)[::-1]

    # The strongest modes plus zero as a safety candidate.
    candidates = unique[order[: min(100, len(order))]]
    candidates = np.unique(np.concatenate([candidates, np.array([0.0])]))
    return candidates.astype(float)


def _automatic_affine_alignment_v1(
    ml_times: pd.Series,
    rpi_times: pd.Series,
    *,
    initial_match_gap_s: float = 0.75,
    final_match_gap_s: float = 0.35,
    coarse_search_window_s: float = 30.0,
    sigma_clip: float = 4.0,
    max_iterations: int = 5,
    min_pairs: int = 3,
) -> Tuple[np.ndarray, np.ndarray, List[str], AffineClockModel, np.ndarray, np.ndarray]:
    """
    Fully automatic alignment:
      1. infer a coarse sub-hour offset from the two mark trains,
      2. monotonic/unique matching under that offset,
      3. robust affine clock fit,
      4. rematch using affine-predicted RPi times,
      5. refit until pairings stabilize.

    No manual chunking, ratings, or hand-selected mark matches are used.
    """
    ml = pd.to_datetime(ml_times, errors="coerce").reset_index(drop=True)
    rp = pd.to_datetime(rpi_times, errors="coerce").reset_index(drop=True)

    candidates = _candidate_clock_offsets(
        ml, rp, search_window_s=coarse_search_window_s
    )

    best = None
    for offset_s in candidates:
        shifted_ml = _shift_datetime_series(ml, offset_s)
        idx, delta, reasons = _nearest_unique_alignment(
            shifted_ml, rp, max_gap=float(initial_match_gap_s)
        )
        matched = idx >= 0
        n = int(matched.sum())
        if n < min_pairs:
            continue

        abs_delta = np.abs(delta[matched])
        score = (
            n,
            -float(np.nanmedian(abs_delta)),
            -float(np.nanmean(abs_delta)),
        )
        if best is None or score > best[0]:
            best = (score, offset_s, idx, delta, reasons)

    if best is None:
        raise ValueError(
            "Automatic mark pairing could not find enough initial pairs. "
            "Increase --coarse_search_window_s or --initial_match_gap_s."
        )

    _, _, match_idx, _, reasons = best
    previous_pairs = None
    model = None
    pair_inliers = None
    pair_residual = None

    for _ in range(max_iterations):
        matched_i = np.flatnonzero(match_idx >= 0)
        matched_j = match_idx[matched_i].astype(int)
        if len(matched_i) < min_pairs:
            raise ValueError("Too few matched pairs to fit affine clock model")

        model, local_inlier, local_residual = _fit_affine_clock(
            ml.iloc[matched_i].reset_index(drop=True),
            rp.iloc[matched_j].reset_index(drop=True),
            sigma_clip=sigma_clip,
            min_pairs=min_pairs,
            max_abs_residual_s=max(float(initial_match_gap_s), float(final_match_gap_s)) * 2.0,
        )

        # Re-match every ML mark against where the affine model predicts its
        # RPi counterpart should occur.
        predicted_rpi = _predict_rpi_from_ml(ml, model)
        new_idx, new_delta, new_reasons = _nearest_unique_alignment(
            predicted_rpi, rp, max_gap=float(final_match_gap_s)
        )

        pairs = tuple(zip(np.flatnonzero(new_idx >= 0).tolist(), new_idx[new_idx >= 0].tolist()))
        if previous_pairs == pairs:
            match_idx, reasons = new_idx, new_reasons
            break
        previous_pairs = pairs
        match_idx, reasons = new_idx, new_reasons

    # Final fit and diagnostics on the final pairing.
    matched_i = np.flatnonzero(match_idx >= 0)
    matched_j = match_idx[matched_i].astype(int)
    model, inliers_local, residual_local = _fit_affine_clock(
        ml.iloc[matched_i].reset_index(drop=True),
        rp.iloc[matched_j].reset_index(drop=True),
        sigma_clip=sigma_clip,
        min_pairs=min_pairs,
        max_abs_residual_s=max(float(initial_match_gap_s), float(final_match_gap_s)) * 2.0,
    )

    predicted_rpi_all = _predict_rpi_from_ml(ml, model)
    deltas_s = np.full(len(ml), np.nan, dtype=float)
    fit_inlier_by_ml = np.zeros(len(ml), dtype=bool)
    residual_by_ml = np.full(len(ml), np.nan, dtype=float)

    for local_k, (i, j) in enumerate(zip(matched_i, matched_j)):
        deltas_s[i] = (rp.iloc[j] - ml.iloc[i]).total_seconds()
        fit_inlier_by_ml[i] = bool(inliers_local[local_k])
        residual_by_ml[i] = float(residual_local[local_k])

    for i in range(len(reasons)):
        if match_idx[i] >= 0:
            reasons[i] = "matched_inlier" if fit_inlier_by_ml[i] else "matched_affine_outlier"
        elif not reasons[i]:
            reasons[i] = "unmatched"

    return (
        match_idx,
        deltas_s,
        reasons,
        model,
        fit_inlier_by_ml,
        residual_by_ml,
    )




def _automatic_affine_alignment(
    ml_times: pd.Series,
    rpi_times: pd.Series,
    *,
    initial_match_gap_s: float = 0.75,
    final_match_gap_s: float = 0.35,
    coarse_search_window_s: float = 30.0,
    sigma_clip: float = 4.0,
    max_iterations: int = 5,
    min_pairs: int = 3,
) -> Tuple[np.ndarray, np.ndarray, List[str], AffineClockModel, np.ndarray, np.ndarray]:
    """
    Fully automatic alignment:
      1. infer a coarse sub-hour offset from the two mark trains,
      2. monotonic/unique matching under that offset,
      3. robust affine clock fit,
      4. rematch using affine-predicted RPi times,
      5. refit until pairings stabilize.

    No manual chunking, ratings, or hand-selected mark matches are used.
    """
    ml = pd.to_datetime(ml_times, errors="coerce").reset_index(drop=True)
    rp = pd.to_datetime(rpi_times, errors="coerce").reset_index(drop=True)

    candidates = _candidate_clock_offsets(
        ml, rp, search_window_s=coarse_search_window_s
    )

    print("\n[AUTO ALIGN DEBUG]")
    print(f"  n_ml_marks={ml.notna().sum()}")
    print(f"  n_rpi_marks={rp.notna().sum()}")
    print(f"  initial_match_gap_s={initial_match_gap_s}")
    print(f"  final_match_gap_s={final_match_gap_s}")
    print(f"  coarse_search_window_s={coarse_search_window_s}")
    print(f"  n_offset_candidates={len(candidates)}")

    ml_valid = ml.dropna()
    rp_valid = rp.dropna()

    if len(ml_valid):
        print(f"  ML first={ml_valid.iloc[0]}")
        print(f"  ML last ={ml_valid.iloc[-1]}")

    if len(rp_valid):
        print(f"  RPi first={rp_valid.iloc[0]}")
        print(f"  RPi last ={rp_valid.iloc[-1]}")

    print(f"  offset_candidates={candidates}")

    best = None

    for offset_s in candidates:
        shifted_ml = _shift_datetime_series(ml, offset_s)

        idx, delta, reasons = _nearest_unique_alignment(
            shifted_ml,
            rp,
            max_gap=float(initial_match_gap_s),
        )

        matched = idx >= 0
        n = int(matched.sum())

        matched_deltas = delta[matched]

        print(f"  candidate offset={offset_s:+.6f}s -> matched={n}/{len(ml)}")

        if n:
            print(f"    matched deltas_s={np.round(matched_deltas, 6).tolist()}")

        if n < min_pairs:
            continue

        abs_delta = np.abs(delta[matched])

        score = (
            n,
            -float(np.nanmedian(abs_delta)),
            -float(np.nanmean(abs_delta)),
        )

        if best is None or score > best[0]:
            best = (
                score,
                offset_s,
                idx,
                delta,
                reasons,
            )
    if best is None:
        print("  RESULT: no candidate produced enough initial pairs")
        print(f"  required min_pairs={min_pairs}")

        raise ValueError(
            "Automatic mark pairing could not find enough initial pairs. "
            "Increase --coarse_search_window_s or --initial_match_gap_s."
        )
    score, best_offset_s, match_idx, best_delta, reasons = best

    print(
        f"  BEST initial offset={best_offset_s:+.6f}s "
        f"with {int((match_idx >= 0).sum())} matched pairs"
    )

    print(
        "  BEST initial deltas_s="
        f"{np.round(best_delta[match_idx >= 0], 6).tolist()}"
    )
    # for offset_s in candidates:
    #     shifted_ml = _shift_datetime_series(ml, offset_s)
    #     idx, delta, reasons = _nearest_unique_alignment(
    #         shifted_ml, rp, max_gap=float(initial_match_gap_s)
    #     )
    #     matched = idx >= 0
    #     n = int(matched.sum())
    #     if n < min_pairs:
    #         continue

    #     abs_delta = np.abs(delta[matched])
    #     score = (
    #         n,
    #         -float(np.nanmedian(abs_delta)),
    #         -float(np.nanmean(abs_delta)),
    #     )
    #     if best is None or score > best[0]:
    #         best = (score, offset_s, idx, delta, reasons)

    # if best is None:
    #     raise ValueError(
    #         "Automatic mark pairing could not find enough initial pairs. "
    #         "Increase --coarse_search_window_s or --initial_match_gap_s."
    #     )

    #_, _, match_idx, _, reasons = best
    previous_pairs = None
    model = None
    pair_inliers = None
    pair_residual = None

    for _ in range(max_iterations):
        matched_i = np.flatnonzero(match_idx >= 0)
        matched_j = match_idx[matched_i].astype(int)
        if len(matched_i) < min_pairs:
            raise ValueError("Too few matched pairs to fit affine clock model")

        model, local_inlier, local_residual = _fit_affine_clock(
            ml.iloc[matched_i].reset_index(drop=True),
            rp.iloc[matched_j].reset_index(drop=True),
            sigma_clip=sigma_clip,
            min_pairs=min_pairs,
            max_abs_residual_s=max(float(initial_match_gap_s), float(final_match_gap_s)) * 2.0,
        )

        # Re-match every ML mark against where the affine model predicts its
        # RPi counterpart should occur.
        predicted_rpi = _predict_rpi_from_ml(ml, model)
        new_idx, new_delta, new_reasons = _nearest_unique_alignment(
            predicted_rpi, rp, max_gap=float(final_match_gap_s)
        )

        pairs = tuple(zip(np.flatnonzero(new_idx >= 0).tolist(), new_idx[new_idx >= 0].tolist()))
        if previous_pairs == pairs:
            match_idx, reasons = new_idx, new_reasons
            break
        previous_pairs = pairs
        match_idx, reasons = new_idx, new_reasons

    # Final fit and diagnostics on the final pairing.
    matched_i = np.flatnonzero(match_idx >= 0)
    matched_j = match_idx[matched_i].astype(int)
    model, inliers_local, residual_local = _fit_affine_clock(
        ml.iloc[matched_i].reset_index(drop=True),
        rp.iloc[matched_j].reset_index(drop=True),
        sigma_clip=sigma_clip,
        min_pairs=min_pairs,
        max_abs_residual_s=max(float(initial_match_gap_s), float(final_match_gap_s)) * 2.0,
    )

    predicted_rpi_all = _predict_rpi_from_ml(ml, model)
    deltas_s = np.full(len(ml), np.nan, dtype=float)
    fit_inlier_by_ml = np.zeros(len(ml), dtype=bool)
    residual_by_ml = np.full(len(ml), np.nan, dtype=float)

    for local_k, (i, j) in enumerate(zip(matched_i, matched_j)):
        deltas_s[i] = (rp.iloc[j] - ml.iloc[i]).total_seconds()
        fit_inlier_by_ml[i] = bool(inliers_local[local_k])
        residual_by_ml[i] = float(residual_local[local_k])

    for i in range(len(reasons)):
        if match_idx[i] >= 0:
            reasons[i] = "matched_inlier" if fit_inlier_by_ml[i] else "matched_affine_outlier"
        elif not reasons[i]:
            reasons[i] = "unmatched"

    return (
        match_idx,
        deltas_s,
        reasons,
        model,
        fit_inlier_by_ml,
        residual_by_ml,
    )
