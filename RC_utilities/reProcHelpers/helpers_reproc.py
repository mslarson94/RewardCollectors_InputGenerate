#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
helpers_reproc.py

Shared helpers for your reprocessing pipeline built around:
- AppTime as the canonical session clock
- Block/Round interval tables derived from *_events*.csv
- Augmenting *_processed.csv with elapsed time + distance + speed
- Propagating metrics back into events via origRow_start/origRow_end
- PinDrop enrichment + cumulative path ordering

Dependencies: pandas, numpy
Optional: pass a logger object with .log(str) (your WarningLogger) or .warning(str).

Key rule baked in everywhere:
- TRUE rounds are only RoundNum in [1..100]. Everything else (special rounds, >100) is excluded.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Optional, Dict, Any, Tuple

import numpy as np
import pandas as pd


# -----------------------------------------------------------------------------
# Round number policy
# -----------------------------------------------------------------------------

DEFAULT_MAX_TRUE_ROUNDNUM = 100


def is_true_roundnum(x: Any, *, max_round: int = DEFAULT_MAX_TRUE_ROUNDNUM) -> bool:
    """True rounds are integer RoundNum in [1..max_round] (excludes special + >max_round)."""
    try:
        xi = int(x)
    except Exception:
        return False
    return 1 <= xi <= int(max_round)


# -----------------------------------------------------------------------------
# Logging / warnings (optional)
# -----------------------------------------------------------------------------

def _log(logger: Optional[Any], msg: str) -> None:
    if logger is None:
        return
    if hasattr(logger, "log"):
        logger.log(msg)
    elif hasattr(logger, "warning"):
        logger.warning(msg)


# -----------------------------------------------------------------------------
# Key normalization and merge helpers
# -----------------------------------------------------------------------------

def normalize_keys(
    df: pd.DataFrame,
    keys: Sequence[str],
    *,
    int_keys: bool = True,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Normalize join keys across dataframes to reduce merge bugs.

    - For numeric keys (default): coerce to pandas nullable Int64
    - For string keys: strip whitespace (set int_keys=False)
    """
    out = df if inplace else df.copy()
    for k in keys:
        if k not in out.columns:
            continue
        if int_keys:
            out[k] = pd.to_numeric(out[k], errors="coerce").astype("Int64")
        else:
            out[k] = out[k].astype("string").str.strip()
    return out


def assert_unique(df: pd.DataFrame, keys: Sequence[str], *, name: str = "table") -> None:
    """Raise ValueError if df has duplicate rows with the same key tuple."""
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing key columns: {missing}")
    dups = df.duplicated(list(keys), keep=False)
    if dups.any():
        example = df.loc[dups, list(keys)].head(10)
        raise ValueError(
            f"{name} has duplicate keys for {list(keys)}. Examples:\n{example.to_string(index=False)}"
        )


def report_merge_coverage(
    merged: pd.DataFrame,
    *,
    indicator_col: str = "__merge_indicator__",
    logger: Optional[Any] = None,
    label: str = "",
) -> Dict[str, int]:
    """
    Report how many rows matched in a merge using pandas merge indicator.
    Returns counts dict. Logs summary if logger provided.
    """
    if indicator_col not in merged.columns:
        return {}
    counts = merged[indicator_col].value_counts(dropna=False).to_dict()
    both = int(counts.get("both", 0))
    left_only = int(counts.get("left_only", 0))
    right_only = int(counts.get("right_only", 0))
    total = int(len(merged))

    prefix = f"[{label}] " if label else ""
    _log(logger, f"{prefix}merge coverage: total={total}, both={both}, left_only={left_only}, right_only={right_only}")
    return {"total": total, "both": both, "left_only": left_only, "right_only": right_only}


def safe_merge(
    left: pd.DataFrame,
    right: pd.DataFrame,
    keys: Sequence[str],
    *,
    how: str = "left",
    validate: Optional[str] = None,
    suffixes: Tuple[str, str] = ("", "_r"),
    indicator: bool = True,
    logger: Optional[Any] = None,
    label: str = "",
    int_keys: bool = True,
) -> pd.DataFrame:
    """
    Safer pd.merge wrapper:
    - normalizes key types on both sides
    - optionally validates cardinality (validate='1:1', 'm:1', ...)
    - adds merge indicator under a non-conflicting name and logs coverage
    """
    l = normalize_keys(left, keys, int_keys=int_keys, inplace=False)
    r = normalize_keys(right, keys, int_keys=int_keys, inplace=False)

    # (Optional resume-safe) drop existing indicator cols if present
    for col in ["_merge", "__merge_indicator__"]:
        if col in l.columns:
            l = l.drop(columns=[col])
        if col in r.columns:
            r = r.drop(columns=[col])

    indicator_name = "__merge_indicator__" if indicator else False

    merged = pd.merge(
        l, r,
        on=list(keys),
        how=how,
        validate=validate,
        suffixes=suffixes,
        indicator=indicator_name,
    )

    if indicator:
        report_merge_coverage(merged, indicator_col="__merge_indicator__", logger=logger, label=label)

    return merged



# -----------------------------------------------------------------------------
# Interval table builders (from events)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class IntervalBuildConfig:
    """Column names and event markers for the events file schema."""
    hi_event_col: str = "hi_eventType"
    lo_event_col: str = "lo_eventType"
    block_start_hi: str = "BlockStructure"
    block_start_lo: str = "BlockStart"
    block_end_hi: str = "BlockStructure"
    block_end_lo: str = "BlockEnd"
    truecontent_start_lo: str = "TrueContentStart"
    round_start_lo: str = "RoundStart"
    round_end_lo: str = "RoundEnd"

    block_instance_col: str = "BlockInstance"
    block_num_col: str = "BlockNum"
    round_num_col: str = "RoundNum"
    start_time_col: str = "start_AppTime"


def _coerce_numeric(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def build_block_intervals(
    events_df: pd.DataFrame,
    processed_df: Optional[pd.DataFrame] = None,
    *,
    cfg: IntervalBuildConfig = IntervalBuildConfig(),
    logger: Optional[Any] = None,
) -> pd.DataFrame:
    """
    Build one row per block: (BlockInstance, BlockNum, block_start_AppTime, block_end_AppTime, block_dur_s)

    If block_end is missing in events, optionally fill from processed_df max AppTime per block.
    """
    ev = _coerce_numeric(events_df, [cfg.block_instance_col, cfg.block_num_col, cfg.start_time_col])
    ev = normalize_keys(ev, [cfg.block_instance_col, cfg.block_num_col], inplace=True)

    hi = cfg.hi_event_col
    lo = cfg.lo_event_col
    t = cfg.start_time_col

    bs = ev[(ev[hi] == cfg.block_start_hi) & (ev[lo] == cfg.block_start_lo)][
        [cfg.block_instance_col, cfg.block_num_col, t]
    ].rename(columns={t: "block_start_AppTime"})

    be = ev[(ev[hi] == cfg.block_end_hi) & (ev[lo] == cfg.block_end_lo)][
        [cfg.block_instance_col, cfg.block_num_col, t]
    ].rename(columns={t: "block_end_AppTime"})

    blocks = safe_merge(
        bs, be, [cfg.block_instance_col, cfg.block_num_col],
        how="left", validate="1:1", logger=logger, label="blocks start/end"
    )

    # fallback: if BlockEnd missing, use processed max AppTime
    if processed_df is not None and "block_end_AppTime" in blocks.columns:
        if "AppTime" in processed_df.columns:
            proc = processed_df.copy()
            proc = _coerce_numeric(proc, ["AppTime", cfg.block_instance_col, cfg.block_num_col])
            proc = normalize_keys(proc, [cfg.block_instance_col, cfg.block_num_col], inplace=True)
            pmax = (
                proc.groupby([cfg.block_instance_col, cfg.block_num_col], dropna=False)["AppTime"]
                .max()
                .reset_index()
                .rename(columns={"AppTime": "proc_max_AppTime"})
            )
            blocks = safe_merge(
                blocks, pmax, [cfg.block_instance_col, cfg.block_num_col],
                how="left", validate="1:1", logger=logger, label="blocks add proc_max_AppTime"
            )
            missing_end = blocks["block_end_AppTime"].isna() & blocks["proc_max_AppTime"].notna()
            if missing_end.any():
                _log(logger, f"Filled {int(missing_end.sum())} missing block_end_AppTime values from processed max AppTime.")
                blocks.loc[missing_end, "block_end_AppTime"] = blocks.loc[missing_end, "proc_max_AppTime"]
            blocks = blocks.drop(columns=["proc_max_AppTime"], errors="ignore")
        else:
            _log(logger, "processed_df missing AppTime; cannot fallback-fill block_end_AppTime")

    blocks["block_dur_s"] = blocks["block_end_AppTime"] - blocks["block_start_AppTime"]
    blocks = blocks.sort_values([cfg.block_instance_col, "block_start_AppTime"]).reset_index(drop=True)
    return blocks


def build_round_intervals(
    events_df: pd.DataFrame,
    blocks_df: pd.DataFrame,
    *,
    mode: str = "truecontent",
    max_round: int = DEFAULT_MAX_TRUE_ROUNDNUM,
    cfg: IntervalBuildConfig = IntervalBuildConfig(),
    logger: Optional[Any] = None,
) -> pd.DataFrame:
    """
    Build one row per TRUE round:
      (BlockInstance, BlockNum, RoundNum, round_start_AppTime, round_end_AppTime, round_dur_s, round_index_in_block)

    mode:
      - "truecontent" (recommended): TrueContentStart boundaries; end is next TrueContentStart (same block instance) or block end.
      - "roundstartend": RoundStart/RoundEnd boundaries (only safe if perfectly paired).

    Always excludes RoundNum > max_round.
    """
    if mode not in {"truecontent", "roundstartend"}:
        raise ValueError(f"mode must be 'truecontent' or 'roundstartend', got {mode}")

    ev = _coerce_numeric(events_df, [cfg.block_instance_col, cfg.block_num_col, cfg.round_num_col, cfg.start_time_col])
    ev = normalize_keys(ev, [cfg.block_instance_col, cfg.block_num_col, cfg.round_num_col], inplace=True)

    blocks = blocks_df.copy()
    blocks = normalize_keys(blocks, [cfg.block_instance_col, cfg.block_num_col], inplace=True)

    lo = cfg.lo_event_col
    t = cfg.start_time_col

    if mode == "truecontent":
        rs = ev[ev[lo] == cfg.truecontent_start_lo][
            [cfg.block_instance_col, cfg.block_num_col, cfg.round_num_col, t]
        ].rename(columns={t: "round_start_AppTime"})
        rs = rs[rs[cfg.round_num_col].apply(lambda x: is_true_roundnum(x, max_round=max_round))].copy()
        rs = rs.sort_values([cfg.block_instance_col, "round_start_AppTime"]).reset_index(drop=True)
        rs["next_round_start"] = rs.groupby(cfg.block_instance_col)["round_start_AppTime"].shift(-1)

        rounds = safe_merge(
            rs, blocks[[cfg.block_instance_col, cfg.block_num_col, "block_end_AppTime"]],
            [cfg.block_instance_col, cfg.block_num_col],
            how="left", validate="m:1", logger=logger, label="rounds add block_end"
        )
        rounds["round_end_AppTime"] = rounds["next_round_start"].fillna(rounds["block_end_AppTime"])
        rounds = rounds.drop(columns=["next_round_start"], errors="ignore")
        rounds = rounds.drop(columns=["block_end_AppTime"], errors="ignore")

    else:
        rs = ev[ev[lo] == cfg.round_start_lo][
            [cfg.block_instance_col, cfg.block_num_col, cfg.round_num_col, t]
        ].rename(columns={t: "round_start_AppTime"})
        re = ev[ev[lo] == cfg.round_end_lo][
            [cfg.block_instance_col, cfg.block_num_col, cfg.round_num_col, t]
        ].rename(columns={t: "round_end_AppTime"})

        rs = rs[rs[cfg.round_num_col].apply(lambda x: is_true_roundnum(x, max_round=max_round))].copy()
        re = re[re[cfg.round_num_col].apply(lambda x: is_true_roundnum(x, max_round=max_round))].copy()

        rounds = safe_merge(
            rs, re, [cfg.block_instance_col, cfg.block_num_col, cfg.round_num_col],
            how="left", validate="1:1", logger=logger, label="rounds start/end"
        )
        rounds = safe_merge(
            rounds, blocks[[cfg.block_instance_col, cfg.block_num_col, "block_end_AppTime"]],
            [cfg.block_instance_col, cfg.block_num_col],
            how="left", validate="m:1", logger=logger, label="rounds add block_end"
        )
        missing = rounds["round_end_AppTime"].isna() & rounds["block_end_AppTime"].notna()
        if missing.any():
            _log(logger, f"Filled {int(missing.sum())} missing round_end_AppTime values with block_end_AppTime.")
            rounds.loc[missing, "round_end_AppTime"] = rounds.loc[missing, "block_end_AppTime"]

    rounds["round_dur_s"] = rounds["round_end_AppTime"] - rounds["round_start_AppTime"]
    rounds = rounds.sort_values([cfg.block_instance_col, "round_start_AppTime"]).reset_index(drop=True)
    rounds["round_index_in_block"] = rounds.groupby(cfg.block_instance_col).cumcount() + 1

    rounds = rounds.rename(columns={cfg.round_num_col: "RoundNum"})
    rounds["RoundNum"] = rounds["RoundNum"].astype("Int64")
    rounds = rounds.drop(columns=["block_end_AppTime"], errors="ignore")
    return rounds


def merge_block_and_round_intervals(
    blocks_df: pd.DataFrame,
    rounds_df: pd.DataFrame,
    *,
    cfg: IntervalBuildConfig = IntervalBuildConfig(),
    logger: Optional[Any] = None,
) -> pd.DataFrame:
    """Merge blocks + rounds into a combined interval table keyed by (BlockInstance, BlockNum, RoundNum)."""
    blocks = normalize_keys(blocks_df.copy(), [cfg.block_instance_col, cfg.block_num_col], inplace=True)
    rounds = normalize_keys(rounds_df.copy(), [cfg.block_instance_col, cfg.block_num_col], inplace=True)
    combined = safe_merge(
        rounds, blocks, [cfg.block_instance_col, cfg.block_num_col],
        how="left", validate="m:1", logger=logger, label="merge rounds->blocks"
    )
    return combined


# -----------------------------------------------------------------------------
# Assign intervals to processed rows (merge_asof by block instance)
# -----------------------------------------------------------------------------

def merge_asof_by_group(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    group_col: str,
    left_on: str,
    right_on: str,
    direction: str = "backward",
    allow_exact_matches: bool = True,
    suffixes: Tuple[str, str] = ("", "_int"),
) -> pd.DataFrame:
    """Apply pd.merge_asof within each group_col."""
    out_parts = []
    for gval, lpart in left.groupby(group_col, sort=False):
        rpart = right[right[group_col] == gval]
        if rpart.empty:
            merged = lpart.copy()
            for c in right.columns:
                if c not in merged.columns:
                    merged[c] = np.nan
            out_parts.append(merged)
            continue

        lpart = lpart.sort_values(left_on)
        rpart = rpart.sort_values(right_on)
        merged = pd.merge_asof(
            lpart, rpart,
            left_on=left_on, right_on=right_on,
            direction=direction,
            allow_exact_matches=allow_exact_matches,
            suffixes=suffixes,
        )
        out_parts.append(merged)

    return pd.concat(out_parts, ignore_index=True)


def _series_equal_with_nan(a: pd.Series, b: pd.Series, *, numeric_tol: float = 0.0) -> pd.Series:
    """
    Elementwise equality treating NaNs as equal.
    For numeric columns, uses tolerance if numeric_tol > 0.
    Returns boolean mask of "equal".
    """
    a_isna = a.isna()
    b_isna = b.isna()
    both_na = a_isna & b_isna

    # numeric compare with tolerance if possible
    if numeric_tol and pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b):
        diff = (a.astype("float64") - b.astype("float64")).abs()
        eq = diff <= float(numeric_tol)
        eq = eq.fillna(False)
    else:
        eq = (a.astype("string") == b.astype("string"))
        eq = eq.fillna(False)

    return both_na | eq

def cleanup_merge_suffixes(
    df: pd.DataFrame,
    *,
    suffixes: tuple[str, ...] = ("_int", "_r"),
    drop_indicator_cols: bool = True,
    numeric_tol: float = 0.0,
    logger=None,
    keep_conflict_cols: bool = False,
) -> pd.DataFrame:
    """
    Coalesce and drop merge-suffix duplicates.

    For each suffix column like 'foo_int' or 'foo_r':
      - If base 'foo' exists:
          - fill foo with foo_suffix where foo is NA
          - if both non-NA and different -> log a warning
          - drop foo_suffix (unless keep_conflict_cols=True)
      - If base doesn't exist:
          - rename foo_suffix -> foo

    Also optionally drops indicator columns like '_merge' or '__merge_indicator__'.
    """
    out = df.copy()

    if drop_indicator_cols:
        for c in ["_merge", "__merge_indicator__"]:
            if c in out.columns:
                out = out.drop(columns=[c])

    # helper logging
    def _log(msg: str):
        if logger is None:
            return
        if hasattr(logger, "log"):
            logger.log(msg)
        elif hasattr(logger, "warning"):
            logger.warning(msg)

    for suf in suffixes:
        suf_cols = [c for c in out.columns if c.endswith(suf)]
        for sc in suf_cols:
            base = sc[: -len(suf)]
            if base == "" or base == sc:
                continue

            if base not in out.columns:
                out = out.rename(columns={sc: base})
                continue

            a = out[base]
            b = out[sc]

            # identify real conflicts: both non-null and different
            both_present = a.notna() & b.notna()
            equal_mask = _series_equal_with_nan(a, b, numeric_tol=numeric_tol)
            conflicts = both_present & (~equal_mask)

            if conflicts.any():
                n_conf = int(conflicts.sum())
                _log(f"cleanup_merge_suffixes: {n_conf} conflicts for '{base}' vs '{sc}' (keeping '{base}', filling NA from '{sc}').")

                if keep_conflict_cols:
                    out[f"{base}{suf}_conflict"] = conflicts

            # coalesce: prefer base, fill missing from suffix
            out[base] = out[base].combine_first(out[sc])

            # drop suffix col unless explicitly keeping it
            if not keep_conflict_cols:
                out = out.drop(columns=[sc])

    return out


def augment_processed_with_intervals(
    processed_df: pd.DataFrame,
    blocks_df: pd.DataFrame,
    rounds_df: pd.DataFrame,
    *,
    max_round: int = DEFAULT_MAX_TRUE_ROUNDNUM,
    time_col: str = "AppTime",
    cfg: IntervalBuildConfig = IntervalBuildConfig(),
    logger: Optional[Any] = None,
) -> pd.DataFrame:
    """
    Adds interval metadata + elapsed time + fractions to processed rows.

    Adds:
      block_start_AppTime, block_end_AppTime, block_dur_s
      round_start_AppTime, round_end_AppTime, round_dur_s
      RoundNum_interval_assigned
      inBlockInterval, inRoundInterval
      blockElapsed_s, roundElapsed_s
      blockFrac, roundFrac
    """
    df = processed_df.copy()
    if time_col not in df.columns:
        raise ValueError(f"processed_df missing {time_col}")

    df = _coerce_numeric(df, [time_col, cfg.block_instance_col, cfg.block_num_col])
    df = normalize_keys(df, [cfg.block_instance_col, cfg.block_num_col], inplace=True)

    blocks = blocks_df.copy()
    blocks = _coerce_numeric(blocks, ["block_start_AppTime", "block_end_AppTime", "block_dur_s",
                                      cfg.block_instance_col, cfg.block_num_col])
    blocks = normalize_keys(blocks, [cfg.block_instance_col, cfg.block_num_col], inplace=True)

    # attach blocks via (BlockInstance, BlockNum)
    df = safe_merge(
        df, blocks[[cfg.block_instance_col, cfg.block_num_col, "block_start_AppTime", "block_end_AppTime", "block_dur_s"]],
        [cfg.block_instance_col, cfg.block_num_col],
        how="left", validate="m:1", logger=logger, label="processed add blocks"
    )

    # attach rounds via merge_asof in each BlockInstance
    rounds = rounds_df.copy().rename(columns={"RoundNum": "RoundNum_interval_assigned"})
    rounds = rounds[rounds["RoundNum_interval_assigned"].apply(lambda x: is_true_roundnum(x, max_round=max_round))].copy()
    rounds = _coerce_numeric(rounds, ["round_start_AppTime", "round_end_AppTime", "round_dur_s", cfg.block_instance_col])
    rounds = normalize_keys(rounds, [cfg.block_instance_col], inplace=True)

    df = merge_asof_by_group(
        df.sort_values(time_col),
        rounds[[cfg.block_instance_col, "RoundNum_interval_assigned",
               "round_start_AppTime", "round_end_AppTime", "round_dur_s", "round_index_in_block"]].copy(),
        group_col=cfg.block_instance_col,
        left_on=time_col,
        right_on="round_start_AppTime",
        direction="backward",
        allow_exact_matches=True,
    )

    # membership flags
    df["inBlockInterval"] = (
        df[time_col].notna()
        & df["block_start_AppTime"].notna()
        & df["block_end_AppTime"].notna()
        & (df[time_col] >= df["block_start_AppTime"])
        & (df[time_col] <= df["block_end_AppTime"])
    )
    df["inRoundInterval"] = (
        df[time_col].notna()
        & df["round_start_AppTime"].notna()
        & df["round_end_AppTime"].notna()
        & (df[time_col] >= df["round_start_AppTime"])
        & (df[time_col] < df["round_end_AppTime"])
    )

    # elapsed + fraction
    df["blockElapsed_s"] = df[time_col] - df["block_start_AppTime"]
    df["roundElapsed_s"] = df[time_col] - df["round_start_AppTime"]
    df["blockFrac"] = df["blockElapsed_s"] / df["block_dur_s"]
    df["roundFrac"] = df["roundElapsed_s"] / df["round_dur_s"]
    # total session elapsed (AppTime anchored to first valid AppTime)
    session_start = pd.to_numeric(df[time_col], errors="coerce").min()
    df["session_start_AppTime"] = session_start
    df["totalSessionElapsed_s"] = df[time_col] - session_start

    return df


# -----------------------------------------------------------------------------
# Kinematics: step distance, total distance, speed
# -----------------------------------------------------------------------------

def compute_step_distance(
    df: pd.DataFrame,
    pos_cols: Sequence[str],
    *,
    time_col: str = "AppTime",
    group_keys: Optional[Sequence[str]] = None,
    out_dt: str = "dt",
    out_step: str = "stepDist",
    min_dt: float = 1e-9,
) -> pd.DataFrame:
    """
    Compute per-row dt and step distance between consecutive samples.

      dt[b]      = time[b] - time[b-1]
      stepDist[b]= sqrt((x[b]-x[b-1])^2 + (y[b]-y[b-1])^2 + (z[b]-z[b-1])^2)

    This is the standard discrete approximation of path length for sampled motion.

    group_keys recommended: ['BlockInstance'] or ['BlockInstance','BlockNum']
    """
    if time_col not in df.columns:
        raise ValueError(f"Missing {time_col}")
    for c in pos_cols:
        if c not in df.columns:
            raise ValueError(f"Missing position column {c}")

    out = df.copy()
    out[time_col] = pd.to_numeric(out[time_col], errors="coerce")

    if group_keys:
        out = out.sort_values(list(group_keys) + [time_col]).reset_index(drop=True)
        out[out_dt] = out.groupby(list(group_keys))[time_col].diff()
        diffs = []
        for c in pos_cols:
            out[c] = pd.to_numeric(out[c], errors="coerce")
            diffs.append(out.groupby(list(group_keys))[c].diff())
    else:
        out = out.sort_values([time_col]).reset_index(drop=True)
        out[out_dt] = out[time_col].diff()
        diffs = []
        for c in pos_cols:
            out[c] = pd.to_numeric(out[c], errors="coerce")
            diffs.append(out[c].diff())

    sq = np.zeros(len(out), dtype="float64")
    for d in diffs:
        sq += (pd.to_numeric(d, errors="coerce").to_numpy(dtype="float64") ** 2)
    out[out_step] = np.sqrt(sq)

    bad_dt = out[out_dt].isna() | (out[out_dt] <= float(min_dt))
    out.loc[bad_dt, out_step] = np.nan
    return out


def aggregate_total_distance(
    df: pd.DataFrame,
    group_keys: Sequence[str],
    *,
    step_col: str = "stepDist",
    out_col: str = "totDist",
    how: str = "sum",
) -> pd.DataFrame:
    """Aggregate step distances by group and merge back (e.g., totDistRound, totDistBlock)."""
    out = df.copy()
    if step_col not in out.columns:
        raise ValueError(f"Missing {step_col} to aggregate")

    agg = (
        out.groupby(list(group_keys), dropna=False)[step_col]
        .agg(how)
        .reset_index()
        .rename(columns={step_col: out_col})
    )
    out = safe_merge(out, agg, list(group_keys), how="left", validate="m:1", indicator=False)
    return out


def compute_speed(
    df: pd.DataFrame,
    *,
    step_col: str = "stepDist",
    dt_col: str = "dt",
    out_col: str = "currSpeed",
    min_dt: float = 1e-9,
) -> pd.DataFrame:
    """Compute instantaneous speed = stepDist / dt."""
    out = df.copy()
    if step_col not in out.columns or dt_col not in out.columns:
        raise ValueError(f"Need {step_col} and {dt_col} to compute speed")

    step = pd.to_numeric(out[step_col], errors="coerce")
    dt = pd.to_numeric(out[dt_col], errors="coerce")
    speed = step / dt
    speed[(dt.isna()) | (dt <= float(min_dt))] = np.nan
    out[out_col] = speed
    return out


# -----------------------------------------------------------------------------
# Augment events with per-row metrics using origRow ranges
# -----------------------------------------------------------------------------

def augment_events_from_reprocessed(
    events_df: pd.DataFrame,
    reproc_df: pd.DataFrame,
    cols: Sequence[str],
    *,
    start_col: str = "origRow_start",
    end_col: str = "origRow_end",
    out_suffix_start: str = "_start",
    out_suffix_end: str = "_end",
    logger: Optional[Any] = None,
) -> pd.DataFrame:
    """
    For each event row, pull reprocessed metric values at origRow_start and origRow_end.
    Adds columns like: f"{col}_start", f"{col}_end"
    """
    out = events_df.copy()
    out[start_col] = pd.to_numeric(out.get(start_col), errors="coerce")
    out[end_col] = pd.to_numeric(out.get(end_col), errors="coerce")

    reproc = reproc_df.reset_index(drop=True)
    n = len(reproc)

    def _pull(series_idx: pd.Series, col: str) -> pd.Series:
        idx = series_idx.fillna(-1).astype(int)
        ok = (idx >= 0) & (idx < n)
        result = pd.Series(np.nan, index=series_idx.index, dtype="float64")
        if col not in reproc.columns:
            _log(logger, f"reproc missing column {col}; cannot pull into events")
            return result
        vals = pd.to_numeric(reproc[col], errors="coerce")
        result.loc[ok] = vals.iloc[idx[ok]].to_numpy()
        return result

    for c in cols:
        out[f"{c}{out_suffix_start}"] = _pull(out[start_col], c)
        out[f"{c}{out_suffix_end}"] = _pull(out[end_col], c)

    for which, colname in [("start", start_col), ("end", end_col)]:
        idx = out[colname]
        bad = idx.notna() & ((idx < 0) | (idx >= n))
        if bad.any():
            _log(logger, f"{int(bad.sum())} events have out-of-range origRow_{which} indices for reproc length {n}.")

    return out

def augment_events_with_positions_from_reprocessed(
    events_df: pd.DataFrame,
    reproc_df: pd.DataFrame,
    *,
    pos_cols: Sequence[str] = ("HeadPosAnchored_x", "HeadPosAnchored_y", "HeadPosAnchored_z"),
    start_col: str = "origRow_start",
    end_col: str = "origRow_end",
    out_suffix_start: str = "_start",
    out_suffix_end: str = "_end",
    logger: Optional[Any] = None,
) -> pd.DataFrame:
    """
    Convenience wrapper: pull position columns (x/y/z) from reprocessed at origRow_start/end.

    Result columns will be: HeadPosAnchored_x_start/end, HeadPosAnchored_y_start/end, ...
    """
    return augment_events_from_reprocessed(
        events_df,
        reproc_df,
        cols=list(pos_cols),
        start_col=start_col,
        end_col=end_col,
        out_suffix_start=out_suffix_start,
        out_suffix_end=out_suffix_end,
        logger=logger,
    )
# -----------------------------------------------------------------------------
# Pin drop helpers
# -----------------------------------------------------------------------------

def extract_pindrop_moments(
    events_df: pd.DataFrame,
    *,
    lo_event_col: str = "lo_eventType",
    pindrop_lo_value: str = "PinDrop_Moment",
    keep_cols: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """Filter PinDrop_Moment rows from events."""
    df = events_df.copy()
    if lo_event_col not in df.columns:
        raise ValueError(f"events missing {lo_event_col}")
    pins = df[df[lo_event_col] == pindrop_lo_value].copy()

    if keep_cols is None:
        keep_cols = [
            "BlockInstance", "BlockNum", "RoundNum",
            "chestPin_num",
            "dropDist", "coinLabel", "actualClosestCoinLabel", "actualClosestCoinDist",
            "origRow_start", "origRow_end",
            "start_AppTime", "end_AppTime",
        ]
    cols = [c for c in keep_cols if c in pins.columns]
    return pins[cols].copy()


def intervals_add_pindrops_wide(
    intervals_df: pd.DataFrame,
    pindrops_df: pd.DataFrame,
    *,
    keys: Sequence[str] = ("BlockInstance", "BlockNum", "RoundNum"),
    pin_col: str = "chestPin_num",
    value_cols: Sequence[str] = ("dropDist", "coinLabel", "actualClosestCoinLabel", "actualClosestCoinDist"),
    logger: Optional[Any] = None,
) -> pd.DataFrame:
    """
    Wide interval enrichment: one row per round; pivot chestPin_num into pin1_/pin2_/pin3_ columns.
    """
    pins = pindrops_df.copy()
    pins = normalize_keys(pins, list(keys) + [pin_col], inplace=True)

    dup = pins.duplicated(list(keys) + [pin_col])
    if dup.any():
        _log(logger, f"PinDrop rows not unique by {list(keys)+[pin_col]}; keeping first for pivot.")
        pins = pins.drop_duplicates(list(keys) + [pin_col], keep="first")

    wide = pins.pivot_table(index=list(keys), columns=pin_col, values=list(value_cols), aggfunc="first")
    wide.columns = [f"pin{int(pin)}_{val}" for val, pin in wide.columns]
    wide = wide.reset_index()

    intervals = normalize_keys(intervals_df.copy(), list(keys), inplace=True)
    out = safe_merge(intervals, wide, list(keys), how="left", validate="1:1",
                     logger=logger, label="intervals add pindrops (wide)")
    return out


def compute_cumulative_path(
    pindrops_long_df: pd.DataFrame,
    *,
    group_keys: Sequence[str] = ("BlockInstance", "BlockNum", "RoundNum"),
    order_key: str = "chestPin_num",
    label_col: str = "actualClosestCoinLabel",
    out_step_col: str = "path_step_in_group",
    out_path_col: str = "path_order",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Long format: per pin drop row compute path step; also compute per-round concatenated path string.
    Returns: (pindrops_long_with_steps, per_group_path_strings)
    """
    df = pindrops_long_df.copy()
    sort_cols = list(group_keys) + [order_key]
    df = df.sort_values(sort_cols).reset_index(drop=True)
    df[out_step_col] = df.groupby(list(group_keys), sort=False).cumcount() + 1

    paths = (
        df.groupby(list(group_keys), dropna=False)[label_col]
        .apply(lambda s: " -> ".join(map(str, s.tolist())))
        .reset_index(name=out_path_col)
    )
    return df, paths
