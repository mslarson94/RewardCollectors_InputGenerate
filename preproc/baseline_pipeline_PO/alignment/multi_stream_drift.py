#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
multi_stream_drift.py

Flexible drift calculator for multiple timestamp streams (e.g. ML, RPi, LFP),
with optional "midi chunk" (two adjacent chunks) drift summaries + plots.

- Reads up to three independent CSVs (ML, RPi, LFP).
- Each CSV must have:
    * a mark column (shared identifier across streams, e.g. markNumber_aligned)
    * a timestamp column (string or datetime).
- Joins them on the mark column (outer join).
- Computes drift (in seconds) for all available stream pairs:
    drift_<A>_minus_<B> = A_timestamp - B_timestamp.

Optional midi-chunk analysis:
- You can supply a separate CSV with chunk annotations (e.g. trimmed RPi or LFP file)
  that has the same mark column plus a chunk column (e.g. con_chunk_RPi).
- Then specify one or more midi-chunk starts (k) via --midi-chunks.
  For each k, the script will:
    * restrict to rows with chunk in {k, k+1},
    * summarize each drift_* column (n, mean, median, max abs),
    * produce a scatter plot vs event index for that midi-chunk.

Examples
--------
ML + RPi only:

    python multi_stream_drift.py \
        --mark-col markNumber_aligned \
        --ml-csv  R037_mergedML_trim.csv  --ml-time-col  mLTimestamp \
        --rpi-csv R037_mergedRPi_trim.csv --rpi-time-col RPi_Time_verb \
        --out-csv R037_ML_RPi_drift.csv \
        --print-summary

ML + RPi + LFP with midi-chunk summaries:

    python multi_stream_drift.py \
        --mark-col markNumber_aligned \
        --ml-csv  R019_mergedML_trim.csv   --ml-time-col  mLTimestamp \
        --rpi-csv R019_mergedRPi_trim.csv  --rpi-time-col RPi_Time_verb \
        --lfp-csv R019_mergedLFP_trim.csv  --lfp-time-col time_abs \
        --chunk-csv R019_mergedRPi_trim.csv \
        --chunk-col con_chunk_RPi \
        --midi-chunks 16,23,24 \
        --out-csv R019_ML_RPi_LFP_drift.csv \
        --print-summary
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from batchAlignHelpers import _summarize, _plot_single  # reuse existing helpers

# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------


def _add_stream_args(ap: argparse.ArgumentParser, name: str) -> None:
    """
    Add --<name>-csv and --<name>-time-col args.
    Example for name="ml": --ml-csv, --ml-time-col
    """
    ap.add_argument(
        f"--{name}-csv",
        default="",
        help=f"CSV for {name.upper()} stream (optional).",
    )
    ap.add_argument(
        f"--{name}-time-col",
        default="",
        help=f"Timestamp column name in the {name.upper()} CSV.",
    )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Compute drift between multiple timestamp streams (ML / RPi / LFP), with optional midi-chunk summaries."
    )
    ap.add_argument(
        "--mark-col",
        default="markNumber_aligned",
        help="Shared mark/ID column present in all provided CSVs (default: markNumber_aligned).",
    )
    _add_stream_args(ap, "ml")
    _add_stream_args(ap, "rpi")
    _add_stream_args(ap, "lfp")

    ap.add_argument(
        "--out-csv",
        default="",
        help="Output CSV path (default: <first_stream_stem>_drift.csv).",
    )
    ap.add_argument(
        "--print-summary",
        action="store_true",
        help="Print simple global summary stats (mean/std/count) for each drift column.",
    )

    # Chunk / midi-chunk options
    ap.add_argument(
        "--chunk-csv",
        default="",
        help="Optional CSV containing chunk annotations (must have mark_col and chunk_col). "
             "Typically a trimmed RPi or LFP file.",
    )
    ap.add_argument(
        "--chunk-col",
        default="",
        help="Chunk column name in --chunk-csv (e.g. con_chunk_RPi or con_chunk_LFP).",
    )
    ap.add_argument(
        "--midi-chunks",
        default="",
        help="Comma-separated starting chunk IDs for midi-chunks. "
             "Each k defines a midi-chunk {k, k+1}. Example: '16,23,24'.",
    )
    ap.add_argument(
        "--midi-out-dir",
        default="",
        help="Optional output directory for midi-chunk plots/summaries "
             "(default: <out_csv_dir>/DriftMidi).",
    )

    return ap.parse_args()


# ---------------------------------------------------------------------
# Core load/merge/drift
# ---------------------------------------------------------------------


def _load_stream(
    name: str,
    csv_path: str,
    time_col: str,
    mark_col: str,
) -> pd.DataFrame:
    """
    Load one stream CSV, keep only mark_col + time_col,
    and rename time_col -> f"{name}_Timestamp".
    """
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(f"{name.upper()} CSV not found: {p}")

    df = pd.read_csv(p)

    if mark_col not in df.columns:
        raise KeyError(f"{name.upper()} CSV is missing mark_col '{mark_col}'")

    if time_col not in df.columns:
        raise KeyError(f"{name.upper()} CSV is missing time_col '{time_col}'")

    out = df[[mark_col, time_col]].copy()
    out = out.rename(columns={time_col: f"{name}_Timestamp"})
    return out


def _merge_streams(
    mark_col: str,
    streams: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Outer-merge all provided stream dataframes on mark_col.
    """
    items: List[Tuple[str, pd.DataFrame]] = list(streams.items())
    if not items:
        raise ValueError("No streams provided; nothing to merge.")

    merged = items[0][1]
    for _, df in items[1:]:
        merged = merged.merge(df, on=mark_col, how="outer")
    return merged


def _compute_drift_cols(df: pd.DataFrame, stream_names: List[str]) -> pd.DataFrame:
    """
    For each pair (A,B) of streams, compute:
        drift_<A>_minus_<B> = A_timestamp - B_timestamp (seconds).
    Also adds the negative companion drift_<B>_minus_<A>.
    """
    # Parse all timestamp columns to datetime.
    for name in stream_names:
        col = f"{name}_Timestamp"
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Compute pairwise drift.
    for i, a in enumerate(stream_names):
        col_a = f"{a}_Timestamp"
        if col_a not in df.columns:
            continue
        for b in stream_names[i + 1 :]:
            col_b = f"{b}_Timestamp"
            if col_b not in df.columns:
                continue

            drift_ab = f"drift_{a}_minus_{b}"
            drift_ba = f"drift_{b}_minus_{a}"

            a_ts = df[col_a]
            b_ts = df[col_b]

            good = (~a_ts.isna()) & (~b_ts.isna())
            diff = np.full(len(df), np.nan, dtype=float)

            if good.any():
                diff[good.to_numpy()] = (
                    a_ts[good] - b_ts[good]
                ).dt.total_seconds().to_numpy()

            df[drift_ab] = diff
            df[drift_ba] = -diff  # exact negative

    return df


def _print_global_summary(df: pd.DataFrame) -> None:
    print("[summary] Global drift statistics (seconds):")
    for col in df.columns:
        if not col.startswith("drift_"):
            continue
        series = df[col].dropna()
        if series.empty:
            continue
        mean = series.mean()
        std = series.std()
        n = series.count()
        print(f"  {col:25s}  n={n:4d}  mean={mean: .6f}  std={std: .6f}")


# ---------------------------------------------------------------------
# Midi-chunk helpers
# ---------------------------------------------------------------------


def _parse_midi_chunks(s: str) -> List[int]:
    """
    Parse comma-separated ints from --midi-chunks.
    Each int k defines a midi-chunk {k, k+1}.
    """
    if not s:
        return []
    out: List[int] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out


def _summarize_midi_chunks(
    df: pd.DataFrame,
    chunk_col: str,
    midi_starts: List[int],
    out_dir: Path,
    stem: str,
) -> None:
    """
    For each midi-chunk {k, k+1} and each drift_* column,
    compute summary stats and scatter plot.
    """
    if chunk_col not in df.columns:
        print(f"[midi] chunk_col '{chunk_col}' not in merged drift table; skipping midi-chunk summaries.")
        return

    drift_cols = [c for c in df.columns if c.startswith("drift_")]
    if not drift_cols:
        print("[midi] No drift_* columns present; skipping midi-chunk summaries.")
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    for k in midi_starts:
        chunks = {k, k + 1}
        mask_chunk = df[chunk_col].isin(chunks)

        if not mask_chunk.any():
            print(f"[midi] No rows for midi-chunk {k}-{k+1} (chunk_col={chunk_col}); skipping.")
            continue

        # Use a simple 1..N index within this midi-chunk for plotting
        idx_within = np.arange(1, int(mask_chunk.sum()) + 1)

        for col in drift_cols:
            series = df.loc[mask_chunk, col].dropna()
            if series.empty:
                continue

            # x: event index within midi-chunk
            x = idx_within[: len(series)]
            y = series.to_numpy(dtype=float)

            # Plot
            title = f"Drift vs Event Index — {stem} [{col}] chunks {k},{k+1}"
            out_png = out_dir / f"{stem}_{col}_chunks{k}-{k+1}_DriftPlot.png"
            _plot_single(col, x, y, title, out_png)

            # Summary CSV
            stats = _summarize(y)
            summary_path = out_dir / f"{stem}_{col}_chunks{k}-{k+1}_DriftSummary.csv"
            pd.DataFrame([stats]).to_csv(summary_path, index=False)

            print(
                f"[midi] {col:25s} chunks {k}-{k+1}: "
                f"n={stats['n_matched']}, mean={stats['mean_drift_s']:.6f}, "
                f"median={stats['median_drift_s']:.6f}, max_abs={stats['max_abs_drift_s']:.6f}"
            )


# ---------------------------------------------------------------------
# main
# ---------------------------------------------------------------------


def main() -> None:
    print("multi_stream_drift: main started")
    args = parse_args()

    mark_col = args.mark_col

    streams: Dict[str, pd.DataFrame] = {}

    # Collect whichever streams the user provided.
    if args.ml_csv:
        if not args.ml_time_col:
            raise ValueError("You must specify --ml-time-col when using --ml-csv.")
        streams["ML"] = _load_stream("ML", args.ml_csv, args.ml_time_col, mark_col)

    if args.rpi_csv:
        if not args.rpi_time_col:
            raise ValueError("You must specify --rpi-time-col when using --rpi-csv.")
        streams["RPI"] = _load_stream("RPI", args.rpi_csv, args.rpi_time_col, mark_col)

    if args.lfp_csv:
        if not args.lfp_time_col:
            raise ValueError("You must specify --lfp-time-col when using --lfp-csv.")
        streams["LFP"] = _load_stream("LFP", args.lfp_csv, args.lfp_time_col, mark_col)

    if len(streams) < 2:
        raise ValueError("Provide at least two streams (e.g. ML+RPI, RPI+LFP, ML+LFP).")

    merged = _merge_streams(mark_col, streams)
    stream_names = list(streams.keys())
    merged = _compute_drift_cols(merged, stream_names)

    # If a chunk CSV is provided, merge chunk_col in by mark_col
    if args.chunk_csv:
        if not args.chunk_col:
            raise ValueError("You must specify --chunk-col when using --chunk-csv.")
        chunk_path = Path(args.chunk_csv)
        if not chunk_path.exists():
            raise FileNotFoundError(f"chunk_csv not found: {chunk_path}")
        df_chunk = pd.read_csv(chunk_path)
        if mark_col not in df_chunk.columns:
            raise KeyError(f"chunk_csv is missing mark_col '{mark_col}'")
        if args.chunk_col not in df_chunk.columns:
            raise KeyError(f"chunk_csv is missing chunk_col '{args.chunk_col}'")
        chunk_small = df_chunk[[mark_col, args.chunk_col]].copy()
        merged = merged.merge(chunk_small, on=mark_col, how="left")

    # Decide output path.
    if args.out_csv:
        out_path = Path(args.out_csv)
    else:
        # Use the first provided stream's CSV stem as base.
        first_name = stream_names[0].lower()
        first_csv = getattr(args, f"{first_name}_csv")
        stem = Path(first_csv).stem
        out_path = Path(stem + "_drift.csv")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)
    print(f"[ok] wrote merged drift table with {len(merged)} rows → {out_path}")

    if args.print_summary:
        _print_global_summary(merged)

    # Midi-chunk summaries + plots
    midi_starts = _parse_midi_chunks(args.midi_chunks)
    if midi_starts and args.chunk_col:
        if args.midi_out_dir:
            midi_dir = Path(args.midi_out_dir)
        else:
            midi_dir = out_path.parent / "DriftMidi"
        stem = out_path.stem
        _summarize_midi_chunks(merged, args.chunk_col, midi_starts, midi_dir, stem)
    elif midi_starts and not args.chunk_col:
        print("[midi] --midi-chunks given but no --chunk-col; skipping midi-chunk summaries.")


if __name__ == "__main__":
    main()
