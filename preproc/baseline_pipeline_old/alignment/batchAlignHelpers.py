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
    n = int(min(10, len(ml_times), len(rpi_times)))
    diffs = (ml_times.iloc[:n].reset_index(drop=True) - rpi_times.iloc[:n].reset_index(drop=True)).dt.total_seconds()
    return float(np.median(diffs) / 3600.0)

def _round_offset_hours(x: float, step: float = 1.0) -> float:
    """Round offset to nearest multiple of `step` hours (default: 1 hour)."""
    return round(x / step) * step


def _nearest_unique_alignment(ml_times: pd.Series, rpi_times: pd.Series, max_gap: Optional[float]) -> Tuple[np.ndarray, np.ndarray, List[str]]:
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


# -------------------------------
# Summarize Drift Utilities
# -------------------------------

def _series_for_label(df: pd.DataFrame, label: str):
    drift_col = f"{label}_RPi_Timestamp_drift"
    ts_col = f"{label}_RPi_Timestamp"
    if drift_col not in df.columns or ts_col not in df.columns:
        return None
    ml_ts = pd.to_datetime(df.get("mLTimestamp", pd.Series([pd.NaT]*len(df))), errors="coerce")
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
        ml_root / f"{stem}_events_final{ext}",
        ml_root / f"{stem}_processed{ext}",
        ml_root / f"{stem}{ext}",
    ]
    tried = []
    for c in candidates:
        tried.append(str(c))
        if c.exists():
            return c
    raise FileNotFoundError("could not resolve ML CSV for '" + cleaned_value + "'. Tried: " + ", ".join(tried))


