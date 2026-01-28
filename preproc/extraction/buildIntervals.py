from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


def build_intervals_between_lo_eventTypes(
    events_df: pd.DataFrame,
    *,
    start_type: str,
    end_type: str,
    start_type_abr: str,
    end_type_abr: str,
    key_cols: Sequence[str] = ("BlockNum", "RoundNum", "BlockInstance"),
    lo_event_col: str = "lo_eventType",
    time_col: str = "eMLT_orig",
    keep_cols: Sequence[str] | None = None,
    drop_exact_duplicates: bool = True,
    require_end_after_start: bool = True,
    keep_unmatched_starts: bool = False,
) -> pd.DataFrame:
    """
    Create intervals between two lo_eventTypes within each unique key.

    - Keeps extra columns from BOTH the start-row and end-row (renamed with trailing abbreviations).
      Example keep col: HeadPosAnchored_x_start -> HeadPosAnchored_x_start_rs and _re
    - Drops exact duplicate rows first (optional).
    - Matches each start to the next end AFTER it (default), within the same key.
    """

    def require_cols(df: pd.DataFrame, cols: Iterable[str]) -> None:
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise KeyError(f"Missing required column(s): {missing}. Present: {list(df.columns)}")

    def suffix_map(cols: Sequence[str], abr: str) -> dict[str, str]:
        return {c: f"{c}_{abr}" for c in cols}

    keep_cols = list(keep_cols or [])
    # Don’t allow key cols or marker cols in keep list (we handle them separately)
    keep_cols = [c for c in keep_cols if c not in set(key_cols) | {lo_event_col}]

    require_cols(events_df, list(key_cols) + [lo_event_col, time_col])
    if keep_cols:
        require_cols(events_df, keep_cols)

    df = events_df.copy()
    if drop_exact_duplicates:
        df = df.drop_duplicates()

    # Coerce time for safe sorting / matching
    df[time_col] = pd.to_numeric(df[time_col], errors="coerce")

    # Filter start/end rows
    starts = df[df[lo_event_col] == start_type].copy()
    ends = df[df[lo_event_col] == end_type].copy()

    # Only the columns we need + keep columns
    start_cols = list(key_cols) + [time_col] + keep_cols
    end_cols   = list(key_cols) + [time_col] + keep_cols

    starts = starts[start_cols].sort_values(list(key_cols) + [time_col], kind="mergesort")
    ends   = ends[end_cols].sort_values(list(key_cols) + [time_col], kind="mergesort")

    # Rename start/end payload columns with trailing abbreviations (INCLUDING time_col)
    starts = starts.rename(columns=suffix_map([time_col] + keep_cols, start_type_abr))
    ends   = ends.rename(columns=suffix_map([time_col] + keep_cols, end_type_abr))

    start_time_out = f"{time_col}_{start_type_abr}"
    end_time_out   = f"{time_col}_{end_type_abr}"

    # asof-merge: for each start, grab the next end within the same key
    merged = pd.merge_asof(
        starts,
        ends,
        by=list(key_cols),
        left_on=start_time_out,
        right_on=end_time_out,
        direction="forward",
        allow_exact_matches=not require_end_after_start,
    )

    # duration
    merged["duration"] = merged[end_time_out] - merged[start_time_out]

    if not keep_unmatched_starts:
        merged = merged[merged[end_time_out].notna()].copy()

    return merged.reset_index(drop=True)


# ---- Example ----
# events = pd.read_csv("...events.csv")
# round_intervals = build_intervals_between_lo_eventTypes(
#     events,
#     start_type="RoundStart",
#     end_type="RoundEnd",
#     start_type_abr="rs",
#     end_type_abr="re",
#     time_col="eMLT_orig",
#     key_cols=("BlockNum", "RoundNum", "BlockInstance"),
#     keep_cols=["HeadPosAnchored_x_start", "HeadPosAnchored_y_start"],  # whatever you want
#     drop_exact_duplicates=True,
#     require_end_after_start=True,
# )
# -> columns include:
#   HeadPosAnchored_x_start_rs, HeadPosAnchored_x_start_re, eMLT_orig_rs, eMLT_orig_re, duration, + key cols
