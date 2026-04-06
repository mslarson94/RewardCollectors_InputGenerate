#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
plot_drift_with_metadata.py — Reworked for aligning Magic Leap 'Mark' events to RPi log lines.

Key changes vs. your previous version:
  • Filters ML rows to lo_eventType ∈ {"Mark", "Marks"} (case-insensitive).
  • Parses ML timestamps robustly.
  • Extracts RPi timestamps for the specified device IP and builds datetimes on session_date.
  • Supports --timezone_offset_hours auto (computes best-fit offset from data) or a numeric value.
  • Performs nearest, order-preserving one-to-one alignment (each ML Mark matched to the closest *later-or-same* RPi time).
  • Exports an extended CSV with alignments and drift stats; draws the same drift plot with jump annotations.

Usage (CLI):
  python plot_drift_with_metadata.py \
    --ml_csv_file path/to/ObsReward_A_02_17_2025_15_11_events_final.csv \
    --log_file path/to/2025-02-17_15_08_32_071285381.log \
    --csv_timestamp_column mLTimestamp \
    --event_type_column lo_eventType \
    --event_type_values Mark \
    --log_device_ip 192.168.50.109 \
    --timezone_offset_hours auto \
    --session_date 2025-02-17

Notes:
  • If you prefer to override auto offset, pass a number (e.g., 8). For your sample files, 8 hours aligns perfectly.
  • max_match_gap_s can be increased if needed; defaults to 1.0s.
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ===============================
# 📦 Version Info
# ===============================
VERSION_NUMBER = 3

# -------------------------------
# Utilities
# -------------------------------
def _read_logfile(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.strip() for ln in f.readlines()]
    # Drop empty lines and lines containing '-e'
    lines = [ln for ln in lines if ln and "-e" not in ln]
    return lines


def _fix_fraction_colon(ts: str) -> str:
    """Convert 'HH:MM:SS:ffffff' -> 'HH:MM:SS.ffffff' when needed."""
    if ts is None:
        return ts
    m = re.fullmatch(r"(\d{2}:\d{2}:\d{2}):(\d+)", str(ts).strip())
    if m:
        return f"{m.group(1)}.{m.group(2)}"
    return str(ts).strip()


def _to_datetime_on_date(times: Sequence[str], session_date: str) -> pd.DatetimeIndex:
    """Build timezone-naive datetimes on a given session_date ('YYYY-MM-DD')
    from time strings like 'HH:MM:SS.ffffff' (fraction optional).
    """
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
    return pd.to_datetime(out)


def parse_raspi_filename_time(fname: str) -> Optional[datetime]:
    """Parses filenames like: 2025-03-17_13_19_53_658434492.log
    Returns a naive datetime (no tz info) if possible, else None.
    """
    try:
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})_(\d{2})_(\d{2})_(\d{2})", fname)
        if not m:
            return None
        yyyy, mm, dd, HH, MM, SS = map(int, m.groups())
        return datetime(yyyy, mm, dd, HH, MM, SS)
    except Exception:
        return None


@dataclass
class DriftResults:
    index: pd.Series
    ml_times: pd.Series
    log_times: pd.Series
    drift_sec: pd.Series
    delta_drift_sec: pd.Series
    log_tags: pd.Series


# -------------------------------
# Alignment helpers
# -------------------------------
def _auto_offset_hours(ml_times: pd.Series, log_times_naive: pd.Series) -> float:
    """Estimate offset in hours so that log_times_naive + offset ≈ ml_times.
    Uses median of first up-to-10 pairwise differences (index-wise).
    """
    n = int(min(10, len(ml_times), len(log_times_naive)))
    if n == 0:
        return 0.0
    diffs = (
        ml_times.iloc[:n].reset_index(drop=True) - log_times_naive.iloc[:n].reset_index(drop=True)
    ).dt.total_seconds()
    return float(np.median(diffs) / 3600.0)


def _nearest_unique_alignment(
    ml_times: pd.Series, log_times: pd.Series, max_gap: Optional[float] = 1.0
) -> Tuple[np.ndarray, np.ndarray]:
    """Monotonic nearest-neighbor alignment.

    For each ML time (ascending), choose the closest RPi time among indices ≥ last match.
    If max_gap is not None, disallow matches with |Δ| > max_gap seconds (mark as -1 / NaN).

    Returns
    -------
    (match_idx, deltas_sec)
      match_idx: int array (len = len(ml_times)), the chosen log index or -1.
      deltas_sec: float array, (log - ml) in seconds (NaN if unmatched).
    """
    ml = ml_times.reset_index(drop=True).to_numpy(dtype="datetime64[ns]")
    log = log_times.reset_index(drop=True).to_numpy(dtype="datetime64[ns]")
    n, m = len(ml), len(log)
    match_idx = np.full(n, -1, dtype=int)
    deltas = np.full(n, np.nan, dtype=float)
    last_j = -1

    for i in range(n):
        if last_j + 1 >= m:
            break
        candidates = np.arange(last_j + 1, m)
        # absolute differences in seconds
        diffs = np.abs(
            (log[candidates] - ml[i]).astype("timedelta64[ns]").astype("int64")
        ) / 1e9
        j_rel = int(np.argmin(diffs))
        j = int(candidates[j_rel])
        if (max_gap is not None) and (diffs[j_rel] > max_gap):
            # leave unmatched
            continue
        match_idx[i] = j
        deltas[i] = (
            (log[j] - ml[i]).astype("timedelta64[ns]").astype("int64") / 1e9
        )
        last_j = j

    return match_idx, deltas


# -------------------------------
# Core compute/plot
# -------------------------------
def _compute_drift(ml_times: pd.Series, log_times: pd.Series, log_tag: str) -> DriftResults:
    n_events = min(len(ml_times), len(log_times))
    ml_times = ml_times.iloc[:n_events].reset_index(drop=True)
    log_times = log_times.iloc[:n_events].reset_index(drop=True)
    drift = (log_times - ml_times).dt.total_seconds()
    delta = drift.diff()
    tags = pd.Series([log_tag] * n_events, dtype="string")
    return DriftResults(
        index=pd.Series(np.arange(1, n_events + 1), name="Index"),
        ml_times=ml_times.rename("ML_Timestamp"),
        log_times=log_times.rename("RPi_Timestamp"),
        drift_sec=drift.rename("Drift_sec"),
        delta_drift_sec=delta.rename("DeltaDrift"),
        log_tags=tags.rename("RPi_SourceFile"),
    )


def annotate_drift_jumps(
    ax: plt.Axes,
    ml_times: Sequence[pd.Timestamp] | Sequence[datetime],
    drift_sec: Sequence[float],
    threshold: float = 0.5,
    show_text: bool = True,
) -> None:
    drift = np.asarray(drift_sec, dtype=float)
    diffs = np.concatenate([[0.0], np.diff(drift)])
    suspicious = np.where(np.abs(diffs) > threshold)[0]  # 0-based indices
    if suspicious.size == 0:
        print(f"✅ No suspicious drift jumps exceeding {threshold:.2f} sec found.")
        return

    print(f"\n🚨 Suspected Drift Jumps (Threshold: {threshold:.2f}s)")
    for idx0 in suspicious:
        idx1 = idx0 + 1  # event index (1-based for display)
        ts = ml_times[idx0]
        print(f"🔹 Index {idx1} | Time = {ts} | Δ Drift = {diffs[idx0]:.3f} sec")

    # annotate on plot; x-axis uses event indices starting at 1
    x_pts = suspicious + 1
    ax.plot(x_pts, drift[suspicious], "o", markersize=6, linewidth=2, color="red")
    if show_text:
        for k, idx0 in enumerate(suspicious):
            x = int(idx0 + 1)
            ax.text(
                x + 1,
                drift[idx0],
                f"← {diffs[idx0]:.1f}s",
                color="red",
                fontsize=8,
                va="center",
            )


def _plot_and_save(
    results: DriftResults,
    ml_file_base: str,
    threshold_for_annotation: float = 0.5,
) -> str:
    out_plot = f"{ml_file_base}_DriftPlot.png"
    fig, axes = plt.subplots(2, 1, figsize=(12, 6), constrained_layout=True)
    ax_top, ax_bottom = axes

    # Top: scatter drift by event index, colored by tag (single tag here, but future-proof)
    unique_tags = pd.unique(results.log_tags.astype(str))
    for tag in unique_tags:
        mask = results.log_tags.astype(str) == str(tag)
        ax_top.scatter(
            results.index[mask],
            results.drift_sec[mask],
            s=32,
            marker="o",
            label=str(tag),
        )
    ax_top.set_title(f"Drift vs Event Index — {ml_file_base.replace('_', ' ')}")
    ax_top.set_xlabel("Event Index")
    ax_top.set_ylabel("Drift (s)")
    ax_top.grid(True, alpha=0.4)
    ax_top.legend(loc="center left", bbox_to_anchor=(1.0, 0.5))

    # Bottom: derivative of drift
    ax_bottom.plot(results.index.iloc[1:], results.delta_drift_sec.iloc[1:], "-x")
    ax_bottom.set_xlabel("Event Index")
    ax_bottom.set_ylabel("Δ Drift (s)")
    ax_bottom.set_title("Derivative of Drift")
    ax_bottom.grid(True, alpha=0.4)
    ax_bottom.axhline(1.0, linestyle="--", linewidth=1.0, color="r")
    ax_bottom.text(
        results.index.iloc[-1],
        1.0,
        "  1s Threshold",
        va="center",
        ha="left",
        color="r",
    )

    # Annotate suspicious jumps on top axis before saving
    annotate_drift_jumps(
        ax_top,
        list(results.ml_times),
        list(results.drift_sec),
        threshold=threshold_for_annotation,
        show_text=True,
    )

    fig.suptitle("Drift Analysis", fontsize=14, y=1.02)
    fig.savefig(out_plot, dpi=150)
    plt.close(fig)
    print(f"🖼️ Plot saved to: {out_plot}")
    return out_plot


# -------------------------------
# High-level pipeline
# -------------------------------
def _select_mark_rows(
    df: pd.DataFrame,
    event_type_column: str,
    values: Sequence[str],
) -> pd.DataFrame:
    vals = {str(v).strip().lower() for v in values}
    mask = df[event_type_column].astype(str).str.strip().str.lower().isin(vals)
    out = df.loc[mask].copy()
    if out.empty:
        raise ValueError(
            f"No rows found where {event_type_column} in {sorted(vals)}. "
            f"Unique values: {sorted(df[event_type_column].astype(str).unique())[:12]}..."
        )
    return out


def plot_drift_with_metadata(
    *,
    ml_csv_file: str,
    log_file: str,
    csv_timestamp_column: str = "mLTimestamp",
    event_type_column: str = "lo_eventType",
    event_type_values: Sequence[str] = ("Mark", "Marks"),
    log_device_ip: str = "",
    timezone_offset_hours: float | int | timedelta | str = "auto",
    session_date: str = "",
    annotate_threshold: float = 0.5,
    max_match_gap_s: Optional[float] = 1.0,
) -> Tuple[str, str]:
    """
    Align ML 'Mark' events to RPi log entries for a given device IP.

    Returns
    -------
    (out_plot_path, out_csv_path)
    """
    print(f"\n📦 Running plot_drift_with_metadata (Version {VERSION_NUMBER})")

    if not ml_csv_file or not os.path.exists(ml_csv_file):
        raise FileNotFoundError(f"Magic Leap CSV not found: {ml_csv_file}")
    if not log_file or not os.path.exists(log_file):
        raise FileNotFoundError(f"RPi log file not found: {log_file}")
    if not session_date:
        raise ValueError("session_date is required (format: YYYY-MM-DD).")
    if not log_device_ip:
        raise ValueError("log_device_ip is required (e.g., 192.168.1.50).")

    # # -------------------------------
    # # Load Magic Leap marks
    # # -------------------------------
    # ml_df = pd.read_csv(ml_csv_file)
    # if csv_timestamp_column not in ml_df.columns:
    #     raise KeyError(f"Column '{csv_timestamp_column}' not found in ML CSV.")
    # if event_type_column not in ml_df.columns:
    #     raise KeyError(f"Column '{event_type_column}' not found in ML CSV.")

    # marks_df = _select_mark_rows(ml_df, event_type_column, event_type_values)
    # # Parse to datetime (robust)
    # ml_times = pd.to_datetime(marks_df[csv_timestamp_column], errors="coerce")
    # if ml_times.isna().any():
    #     bad = int(ml_times.isna().sum())
    #     raise ValueError(f"{bad} ML timestamps failed to parse in column '{csv_timestamp_column}'.")

    # -------------------------------
    # Load Magic Leap Timestamps (Mark rows only)
    # -------------------------------
    ml_df = pd.read_csv(ml_csv_file)
    if csv_timestamp_column not in ml_df.columns:
        raise KeyError(f"Column '{csv_timestamp_column}' not found in ML CSV.")

    # If the low-level event type column exists, keep only Mark/Marks
    if "lo_eventType" in ml_df.columns:
        ml_df = ml_df[ml_df["lo_eventType"].astype(str).str.lower().isin(["mark", "marks"])].copy()

    raw_ts = ml_df[csv_timestamp_column].astype(str).str.strip()
    ml_times = pd.to_datetime(raw_ts, errors="coerce")
    if ml_times.isna().any():
        bad = int(ml_times.isna().sum())
        raise ValueError(f"{bad} ML timestamps failed to parse in '{csv_timestamp_column}'.")
    print(ml_times.head())


    # -------------------------------
    # Load and Parse RPi Log File
    # -------------------------------
    log_lines = _read_logfile(log_file)

    # Parse filename time (local)
    log_base = os.path.splitext(os.path.basename(log_file))[0]
    _ = parse_raspi_filename_time(log_base)  # kept for parity; not used further

    # Reshape log into [IP, timestamp] rows
    if len(log_lines) % 2 != 0:
        print("⚠️ Odd number of lines in log file, trimming last line.")
        log_lines = log_lines[:-1]
    pairs: List[Tuple[str, str]] = list(zip(log_lines[0::2], log_lines[1::2]))

    # Identify relevant device marks in log
    search_ip = f"[{log_device_ip}]"
    timestamps = [b for (a, b) in pairs if search_ip in a]
    if len(timestamps) == 0:
        raise ValueError(
            f"No log entries matched the target IP marker '{search_ip}'."
        )

    # Build naive (no offset) RPi times on session_date
    log_times_naive = pd.Series(_to_datetime_on_date(timestamps, session_date), name="RPi_Timestamp")

    # Normalize tz offset
    if isinstance(timezone_offset_hours, str):
        if timezone_offset_hours.strip().lower() == "auto":
            est = _auto_offset_hours(ml_times, log_times_naive)
            print(f"🧭 Auto-estimated timezone offset: {est:.6f} hours")
            tz_offset = timedelta(hours=est)
        else:
            tz_offset = timedelta(hours=float(timezone_offset_hours))
    elif isinstance(timezone_offset_hours, timedelta):
        tz_offset = timezone_offset_hours
    else:
        tz_offset = timedelta(hours=float(timezone_offset_hours))

    # Apply offset to log times to bring into ML time frame
    log_times = (log_times_naive + pd.to_timedelta(tz_offset)).reset_index(drop=True)

    # -------------------------------
    # Alignment (nearest, order-preserving)
    # -------------------------------
    match_idx, deltas_sec = _nearest_unique_alignment(ml_times, log_times, max_gap=max_match_gap_s)

    # Build export frame with alignment
    matched_mask = match_idx >= 0
    chosen_log_times = pd.Series([pd.NaT] * len(ml_times), dtype="datetime64[ns]")
    chosen_log_times.loc[matched_mask] = log_times.iloc[match_idx[matched_mask]].values
    drift_series = pd.Series(deltas_sec, name="Drift_sec")
    delta_series = drift_series.diff().rename("DeltaDrift")

    export_df = pd.DataFrame(
        {
            "Index": np.arange(1, len(ml_times) + 1),
            "ML_Timestamp": ml_times.values,
            "RPi_Timestamp": chosen_log_times.values,
            "Drift_sec": drift_series.values,
            "DeltaDrift": delta_series.values,
            "Matched_Log_Index": match_idx,
            "Matched": matched_mask.astype(int),
            "RPi_SourceFile": log_base,
        }
    )

    # For plotting, use only matched rows (drop NaT)
    plot_ready = export_df.loc[matched_mask].copy()
    results = DriftResults(
        index=plot_ready["Index"].reset_index(drop=True),
        ml_times=plot_ready["ML_Timestamp"].reset_index(drop=True),
        log_times=plot_ready["RPi_Timestamp"].reset_index(drop=True),
        drift_sec=plot_ready["Drift_sec"].reset_index(drop=True),
        delta_drift_sec=plot_ready["DeltaDrift"].reset_index(drop=True),
        log_tags=pd.Series([log_base] * len(plot_ready), dtype="string"),
    )

    # -------------------------------
    # Plotting
    # -------------------------------
    ml_file_base = os.path.splitext(os.path.basename(ml_csv_file))[0]
    out_plot = _plot_and_save(results, ml_file_base, annotate_threshold)

    # -------------------------------
    # Export Results
    # -------------------------------
    out_csv = f"{ml_file_base}_DriftAligned.csv"
    export_df.to_csv(out_csv, index=False)
    print(f"📄 CSV saved to: {out_csv}")

    # -------------------------------
    # Summary
    # -------------------------------
    n_events = int(export_df.shape[0])
    n_matched = int(matched_mask.sum())
    mean_drift = float(np.nanmean(export_df["Drift_sec"])) if n_events else float("nan")
    max_abs_delta = float(np.nanmax(np.abs(export_df["DeltaDrift"]))) if n_events else float("nan")

    print("\n--- Summary ---")
    print(f"Total ML mark events: {n_events}")
    print(f"Matched to RPi lines: {n_matched}")
    print(f"Mean drift (matched): {mean_drift:.3f} s")
    print(f"Max |Δ drift| (matched): {max_abs_delta:.3f} s")
    print("----------------")

    return out_plot, out_csv


# -------------------------------
# CLI
# -------------------------------
def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Align ML 'Mark' events to RPi log entries and visualize drift (nearest alignment)."
    )
    p.add_argument("--ml_csv_file", required=True, type=str, help="Path to Magic Leap CSV file.")
    p.add_argument("--log_file", required=True, type=str, help="Path to Raspberry Pi log file.")
    p.add_argument(
        "--csv_timestamp_column",
        default="mLTimestamp",
        type=str,
        help="Name of the timestamp column in the ML CSV.",
    )
    p.add_argument(
        "--event_type_column",
        default="lo_eventType",
        type=str,
        help="Name of the event-type column in the ML CSV.",
    )
    p.add_argument(
        "--event_type_values",
        default="Mark",
        type=str,
        help="Comma-separated low-level event types to keep (e.g., 'Mark,Marks').",
    )
    p.add_argument(
        "--log_device_ip",
        default="",
        type=str,
        help="Target device IP to match in the RPi log (e.g., 192.168.1.50).",
    )
    p.add_argument(
        "--timezone_offset_hours",
        default="auto",
        type=str,
        help=(
            "Hours to add to RPi times (local → ML frame). "
            "Use a number (e.g., 8) or 'auto' to estimate from data."
        ),
    )
    p.add_argument(
        "--session_date",
        required=True,
        type=str,
        help="Session date in YYYY-MM-DD.",
    )
    p.add_argument(
        "--annotate_threshold",
        default=0.5,
        type=float,
        help="Threshold (seconds) to flag drift jumps in the plot.",
    )
    p.add_argument(
        "--max_match_gap_s",
        default=1.0,
        type=float,
        help="Reject matches farther than this (seconds); set <=0 to disable.",
    )
    return p


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _build_argparser().parse_args(argv)
    event_values = [v.strip() for v in args.event_type_values.split(",") if v.strip()]
    max_gap = None if (args.max_match_gap_s is not None and args.max_match_gap_s <= 0) else float(args.max_match_gap_s)
    plot_drift_with_metadata(
        ml_csv_file=args.ml_csv_file,
        log_file=args.log_file,
        csv_timestamp_column=args.csv_timestamp_column,
        event_type_column=args.event_type_column,
        event_type_values=event_values,
        log_device_ip=args.log_device_ip,
        timezone_offset_hours=args.timezone_offset_hours,
        session_date=args.session_date,
        annotate_threshold=args.annotate_threshold,
        max_match_gap_s=max_gap,
    )


if __name__ == "__main__":
    main()
