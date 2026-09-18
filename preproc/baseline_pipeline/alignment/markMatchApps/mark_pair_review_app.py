# alignment/mark_pair_review_app.py

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


logger = logging.getLogger("mark_pair_review_app")
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(handler)


EVENT_SUFFIXES = [
    "_eventsFlat",
    "_events_final",
    "_events",
    "_processed",
    "_earliestRoundStart",
]

RPI_SUFFIXES = {
    "BioPac": "_BioPac_RPi_unified",
    "RNS": "_RNS_RPi_unified",
}

MATCH_COLUMNS = [
    "pair_id",
    "events_file",
    "rpi_file",
    "label",
    "block",
    "events_mark_id",
    "rpi_mark_id",
    "events_time",
    "rpi_time",
    "delta_seconds",
    "exclude",
    "reason",
    "reviewed_at",
]

SINGLE_COLUMNS = [
    "events_file",
    "rpi_file",
    "label",
    "stream",
    "mark_id",
    "ordinal",
    "mark_time",
    "block",
    "matched_pair_id",
    "exclude",
    "reason",
    "reviewed_at",
]

PAIR_REVIEW_COLUMNS = [
    "pair_id",
    "events_file",
    "rpi_file",
    "label",
    "block",
    "events_mark_id",
    "rpi_mark_id",
    "events_time",
    "rpi_time",
    "delta_seconds",
    "match_exclude",
    "match_reason",
    "vote",
    "vote_label",
    "voted_at",
]

VOTE_LABELS = {
    1: "hard_exclusion",
    2: "soft_exclusion",
    3: "soft_accept",
    4: "hard_accept",
}

VOTE_HIGHLIGHTS = {
    1: ("red", 0.5),
    2: ("red", 0.2),
    3: ("blue", 0.2),
    4: ("blue", 0.5),
}


@dataclass(frozen=True)
class FilePair:
    events_file: Path
    rpi_file: Path
    label: str


@dataclass
class LoadedPair:
    pair: FilePair
    events_marks: pd.DataFrame
    rpi_marks: pd.DataFrame
    blocks: pd.DataFrame
    matches: pd.DataFrame
    singles: pd.DataFrame
    reviews: pd.DataFrame
    ts_col: str
    current_block: str | int = "All"


def strip_suffixes(
    stem: str,
    suffixes: list[str],
) -> str:
    base = stem
    changed = True

    while changed:
        changed = False

        for suffix in suffixes:
            if suffix and base.endswith(suffix):
                base = base[: -len(suffix)]
                changed = True

    return base.rstrip("_-")


def base_name_for_pair(
    pair: FilePair,
) -> str:
    return strip_suffixes(
        pair.events_file.stem,
        EVENT_SUFFIXES,
    )


def match_path_for_pair(
    match_dir: Path,
    pair: FilePair,
) -> Path:
    return (
        match_dir
        / f"{base_name_for_pair(pair)}_{pair.label}_mark_matches.csv"
    )


def singles_path_for_pair(
    match_dir: Path,
    pair: FilePair,
) -> Path:
    return (
        match_dir
        / f"{base_name_for_pair(pair)}_{pair.label}_mark_singles.csv"
    )


def review_path_for_pair(
    review_dir: Path,
    pair: FilePair,
) -> Path:
    return (
        review_dir
        / f"{base_name_for_pair(pair)}_{pair.label}_pair_votes.csv"
    )


def detect_events_columns(
    df: pd.DataFrame,
) -> dict[str, Optional[str]]:
    cols = {
        column.lower(): column
        for column in df.columns
    }

    return {
        "ts": cols.get("mlt_orig"),
        "etype": (
            cols.get("lo_eventtype")
            or cols.get("eventtype")
            or cols.get("event_type")
            or cols.get("type")
        ),
        "block": (
            cols.get("blocknum")
            or cols.get("block")
            or cols.get("blockid")
            or cols.get("block_id")
            or cols.get("lo_block")
        ),
    }


def detect_rpi_time_column(
    df: pd.DataFrame,
) -> Optional[str]:
    preferred = (
        "RPi_Time_unified",
        "RPi_Time_verb",
        "ML_Time_verb",
        "RPi_Time_simple",
        "Mono_Time_verb",
        "Mono_Time_Raw_verb",
    )

    for name in preferred:
        if name in df.columns:
            return name

    for column in df.columns:
        if (
            df[column].dtype == object
            and any(
                token in column.lower()
                for token in (
                    "time",
                    "timestamp",
                    "date",
                    "datetime",
                )
            )
        ):
            return column

    return None


def build_blocks(
    events: pd.DataFrame,
    ts_col: str,
    event_col: str,
    block_col: Optional[str],
) -> pd.DataFrame:
    ordered = events.sort_values(ts_col).copy()

    event_types = (
        ordered[event_col]
        .astype(str)
        .str.lower()
    )

    starts = ordered[
        event_types.eq("blockstart")
    ]

    ends = ordered[
        event_types.eq("blockend")
    ]

    if (
        not starts.empty
        and not ends.empty
        and block_col
        and block_col in ordered.columns
    ):
        blocks = (
            starts[[ts_col, block_col]]
            .merge(
                ends[[ts_col, block_col]],
                on=block_col,
                how="left",
                suffixes=("_start", "_end"),
            )
            .rename(
                columns={
                    block_col: "block",
                    f"{ts_col}_start": "start",
                    f"{ts_col}_end": "end",
                }
            )
        )

    elif (
        block_col
        and block_col in ordered.columns
    ):
        grouped = ordered.groupby(
            block_col
        )[ts_col]

        blocks = pd.DataFrame(
            {
                "block": grouped.min().index,
                "start": grouped.min().values,
                "end": grouped.max().values,
            }
        )

    else:
        blocks = pd.DataFrame(
            {
                "block": [1],
                "start": [ordered[ts_col].min()],
                "end": [ordered[ts_col].max()],
            }
        )

    blocks = (
        blocks.sort_values("start")
        .reset_index(drop=True)
    )

    next_starts = (
        blocks["start"]
        .shift(-1)
    )

    blocks["end"] = (
        blocks["end"]
        .where(
            blocks["end"].notna(),
            next_starts,
        )
    )

    blocks["end"] = (
        blocks["end"]
        .fillna(
            ordered[ts_col].max()
        )
    )

    return (
        blocks.sort_values("block")
        .reset_index(drop=True)
    )


def assign_block(
    times: pd.Series,
    blocks: pd.DataFrame,
) -> pd.Series:
    ordered = blocks.sort_values(
        "start"
    )

    starts = (
        pd.to_datetime(
            ordered["start"]
        )
        .values
        .astype("datetime64[ns]")
    )

    ends = (
        pd.to_datetime(
            ordered["end"]
        )
        .values
        .astype("datetime64[ns]")
    )

    ids = ordered[
        "block"
    ].values

    values = (
        pd.to_datetime(times)
        .values
        .astype("datetime64[ns]")
    )

    indices = (
        np.searchsorted(
            starts,
            values,
            side="right",
        )
        - 1
    )

    indices = np.clip(
        indices,
        0,
        len(ordered) - 1,
    )

    valid = (
        (values >= starts[indices])
        & (values <= ends[indices])
    )

    result = np.where(
        valid,
        ids[indices],
        np.nan,
    )

    return pd.Series(
        result,
        index=times.index,
    )


def prepare_marks_df(
    df: pd.DataFrame,
    stream: str,
    ts_col: str,
    blocks: pd.DataFrame,
) -> pd.DataFrame:
    out = (
        df.copy()
        .reset_index(drop=True)
    )

    out["block"] = assign_block(
        out[ts_col],
        blocks,
    )

    out["ordinal"] = np.arange(
        len(out),
        dtype=int,
    )

    out["mark_id"] = [
        f"{stream}_{ordinal:04d}"
        for ordinal in out["ordinal"]
    ]

    out["stream"] = stream
    out["match_exclude"] = False

    return out


def parse_bool_series(
    series: pd.Series,
) -> pd.Series:
    return (
        series.astype(str)
        .str.lower()
        .isin(
            {
                "true",
                "1",
                "yes",
                "y",
            }
        )
    )


def load_matches_csv(
    path: Path,
) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(
            columns=MATCH_COLUMNS
        )

    df = pd.read_csv(
        path,
        dtype=str,
    ).fillna("")

    for column in MATCH_COLUMNS:
        if column not in df.columns:
            df[column] = ""

    df = df[
        MATCH_COLUMNS
    ].copy()

    df["exclude"] = parse_bool_series(
        df["exclude"]
    )

    df["events_time"] = pd.to_datetime(
        df["events_time"],
        errors="coerce",
    )

    df["rpi_time"] = pd.to_datetime(
        df["rpi_time"],
        errors="coerce",
    )

    df["delta_seconds"] = pd.to_numeric(
        df["delta_seconds"],
        errors="coerce",
    )

    return df


def load_singles_csv(
    path: Path,
) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(
            columns=SINGLE_COLUMNS
        )

    df = pd.read_csv(
        path,
        dtype=str,
    ).fillna("")

    for column in SINGLE_COLUMNS:
        if column not in df.columns:
            df[column] = ""

    df = df[
        SINGLE_COLUMNS
    ].copy()

    df["exclude"] = parse_bool_series(
        df["exclude"]
    )

    df["mark_time"] = pd.to_datetime(
        df["mark_time"],
        errors="coerce",
    )

    return df


def build_review_table(
    pair: FilePair,
    matches: pd.DataFrame,
    saved_review_path: Path,
) -> pd.DataFrame:
    rows: list[
        dict[str, object]
    ] = []

    for _, match in matches.iterrows():
        rows.append(
            {
                "pair_id": str(
                    match["pair_id"]
                ),
                "events_file": str(
                    pair.events_file
                ),
                "rpi_file": str(
                    pair.rpi_file
                ),
                "label": pair.label,
                "block": match.get(
                    "block",
                    "",
                ),
                "events_mark_id": str(
                    match["events_mark_id"]
                ),
                "rpi_mark_id": str(
                    match["rpi_mark_id"]
                ),
                "events_time": pd.to_datetime(
                    match["events_time"]
                ),
                "rpi_time": pd.to_datetime(
                    match["rpi_time"]
                ),
                "delta_seconds": match[
                    "delta_seconds"
                ],
                "match_exclude": bool(
                    match["exclude"]
                ),
                "match_reason": str(
                    match.get(
                        "reason",
                        "",
                    )
                ),
                "vote": "",
                "vote_label": "",
                "voted_at": "",
            }
        )

    review = pd.DataFrame(
        rows,
        columns=PAIR_REVIEW_COLUMNS,
    )

    if not saved_review_path.exists():
        return review

    saved = pd.read_csv(
        saved_review_path,
        dtype=str,
    ).fillna("")

    if saved.empty:
        return review

    for column in PAIR_REVIEW_COLUMNS:
        if column not in saved.columns:
            saved[column] = ""

    saved = (
        saved[
            [
                "pair_id",
                "vote",
                "vote_label",
                "voted_at",
            ]
        ]
        .drop_duplicates(
            subset=["pair_id"],
            keep="last",
        )
    )

    review = review.merge(
        saved,
        on="pair_id",
        how="left",
        suffixes=(
            "",
            "_saved",
        ),
    )

    for column in (
        "vote",
        "vote_label",
        "voted_at",
    ):
        saved_column = (
            f"{column}_saved"
        )

        review[column] = (
            review[saved_column]
            .where(
                review[saved_column]
                .notna()
                & review[saved_column]
                .ne(""),
                review[column],
            )
        )

        review = review.drop(
            columns=[saved_column]
        )

    return review[
        PAIR_REVIEW_COLUMNS
    ].copy()


def apply_single_exclusions(
    marks: pd.DataFrame,
    singles: pd.DataFrame,
    stream: str,
) -> pd.DataFrame:
    out = marks.copy()

    if singles.empty:
        return out

    subset = singles[
        singles["stream"]
        .astype(str)
        .eq(stream)
    ][
        [
            "mark_id",
            "exclude",
        ]
    ].copy()

    if subset.empty:
        return out

    subset = subset.drop_duplicates(
        subset=["mark_id"],
        keep="last",
    )

    merged = out.merge(
        subset,
        on="mark_id",
        how="left",
    )

    merged["match_exclude"] = (
        merged["exclude"]
        .fillna(False)
        .astype(bool)
    )

    merged = merged.drop(
        columns=["exclude"]
    )

    return merged


def load_pair(
    pair: FilePair,
    match_dir: Path,
    review_dir: Path,
) -> LoadedPair:
    events = pd.read_csv(
        pair.events_file
    )

    detected = detect_events_columns(
        events
    )

    ts_col = detected["ts"]
    event_col = detected["etype"]
    block_col = detected["block"]

    if not ts_col or not event_col:
        raise RuntimeError(
            "Events file must contain "
            "mLT_orig and an event type column: "
            f"{pair.events_file}"
        )

    events[ts_col] = pd.to_datetime(
        events[ts_col],
        errors="coerce",
    )

    events = events.dropna(
        subset=[ts_col]
    ).copy()

    keep_types = {
        "blockstart",
        "blockend",
        "roundstart",
        "roundend",
        "mark",
    }

    events = events[
        events[event_col]
        .astype(str)
        .str.lower()
        .isin(keep_types)
    ].copy()

    events = events.sort_values(
        ts_col
    )

    blocks = build_blocks(
        events,
        ts_col,
        event_col,
        block_col,
    )

    if blocks.empty:
        raise RuntimeError(
            f"No blocks could be constructed: {pair.events_file}"
        )

    events_marks = events[
        events[event_col]
        .astype(str)
        .str.lower()
        .eq("mark")
    ].copy()

    events_marks = prepare_marks_df(
        events_marks,
        "events",
        ts_col,
        blocks,
    )

    rpi = pd.read_csv(
        pair.rpi_file
    )

    rpi_time_col = detect_rpi_time_column(
        rpi
    )

    if not rpi_time_col:
        raise RuntimeError(
            "Could not detect RPi datetime column: "
            f"{pair.rpi_file}"
        )

    rpi[ts_col] = pd.to_datetime(
        rpi[rpi_time_col],
        errors="coerce",
    )

    rpi = (
        rpi.dropna(
            subset=[ts_col]
        )
        .sort_values(ts_col)
        .copy()
    )

    rpi_marks = prepare_marks_df(
        rpi,
        "rpi",
        ts_col,
        blocks,
    )

    matches = load_matches_csv(
        match_path_for_pair(
            match_dir,
            pair,
        )
    )

    singles = load_singles_csv(
        singles_path_for_pair(
            match_dir,
            pair,
        )
    )

    events_marks = apply_single_exclusions(
        events_marks,
        singles,
        "events",
    )

    rpi_marks = apply_single_exclusions(
        rpi_marks,
        singles,
        "rpi",
    )

    reviews = build_review_table(
        pair,
        matches,
        review_path_for_pair(
            review_dir,
            pair,
        ),
    )

    logger.info(
        (
            "Loaded pair: %s | "
            "events=%d rpi=%d "
            "matches=%d singles=%d"
        ),
        base_name_for_pair(pair),
        len(events_marks),
        len(rpi_marks),
        len(matches),
        len(singles),
    )

    return LoadedPair(
        pair=pair,
        events_marks=events_marks,
        rpi_marks=rpi_marks,
        blocks=blocks,
        matches=matches,
        singles=singles,
        reviews=reviews,
        ts_col=ts_col,
        current_block="All",
    )


def load_manifest(
    path: Path,
) -> list[FilePair]:
    df = pd.read_csv(path)

    required = [
        "events_file",
        "rpi_file",
        "label",
    ]

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise KeyError(
            f"Manifest missing columns: {missing}"
        )

    return [
        FilePair(
            events_file=Path(
                str(row["events_file"])
            ).expanduser(),
            rpi_file=Path(
                str(row["rpi_file"])
            ).expanduser(),
            label=str(
                row["label"]
            ).strip(),
        )
        for _, row in df.iterrows()
    ]


def build_pairs_from_directories(
    events_dir: Path,
    rpi_dir: Path,
) -> list[FilePair]:
    events_dir = events_dir.expanduser()
    rpi_dir = rpi_dir.expanduser()

    if not events_dir.is_dir():
        raise NotADirectoryError(
            f"events_dir is not a directory: {events_dir}"
        )

    if not rpi_dir.is_dir():
        raise NotADirectoryError(
            f"rpi_dir is not a directory: {rpi_dir}"
        )

    event_map: dict[
        str,
        Path,
    ] = {}

    for path in sorted(
        events_dir.glob("*.csv")
    ):
        if not path.is_file():
            continue

        base = strip_suffixes(
            path.stem,
            EVENT_SUFFIXES,
        )

        event_map[
            base
        ] = path

    rpi_map: dict[
        tuple[str, str],
        Path,
    ] = {}

    for path in sorted(
        rpi_dir.glob("*.csv")
    ):
        if not path.is_file():
            continue

        for (
            label,
            suffix,
        ) in RPI_SUFFIXES.items():
            if not path.stem.endswith(
                suffix
            ):
                continue

            base = path.stem[
                : -len(suffix)
            ].rstrip("_-")

            rpi_map[
                (
                    base,
                    label,
                )
            ] = path

            break

    pairs: list[FilePair] = []

    for (
        base,
        events_file,
    ) in sorted(
        event_map.items()
    ):
        for label in (
            "BioPac",
            "RNS",
        ):
            rpi_file = rpi_map.get(
                (
                    base,
                    label,
                )
            )

            if rpi_file is None:
                continue

            pairs.append(
                FilePair(
                    events_file=events_file,
                    rpi_file=rpi_file,
                    label=label,
                )
            )

    return pairs


def pair_has_match_data(
    match_dir: Path,
    pair: FilePair,
) -> bool:
    path = match_path_for_pair(
        match_dir,
        pair,
    )

    if not path.exists():
        return False

    try:
        df = pd.read_csv(path)
    except Exception:
        return False

    return not df.empty


def pair_review_complete(
    match_dir: Path,
    review_dir: Path,
    pair: FilePair,
) -> bool:
    match_path = match_path_for_pair(
        match_dir,
        pair,
    )

    review_path = review_path_for_pair(
        review_dir,
        pair,
    )

    if (
        not match_path.exists()
        or not review_path.exists()
    ):
        return False

    try:
        matches = load_matches_csv(
            match_path
        )

        reviews = pd.read_csv(
            review_path,
            dtype=str,
        ).fillna("")
    except Exception:
        return False

    active = matches[
        ~matches[
            "exclude"
        ].astype(bool)
    ]

    if active.empty:
        return True

    if (
        "pair_id"
        not in reviews.columns
        or "vote"
        not in reviews.columns
    ):
        return False

    voted_ids = set(
        reviews.loc[
            reviews["vote"]
            .astype(str)
            .isin(
                {
                    "1",
                    "2",
                    "3",
                    "4",
                }
            ),
            "pair_id",
        ].astype(str)
    )

    active_ids = set(
        active["pair_id"]
        .astype(str)
    )

    return active_ids.issubset(
        voted_ids
    )


class PairVoteReviewApp:
    def __init__(
        self,
        pairs: list[FilePair],
        match_dir: Path,
        review_dir: Path,
        start_index: int = 0,
    ) -> None:
        if not pairs:
            raise ValueError(
                "No file pairs available."
            )

        self.pairs = pairs
        self.match_dir = match_dir
        self.review_dir = review_dir

        self.review_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.current_index = max(
            0,
            min(
                start_index,
                len(pairs) - 1,
            ),
        )

        self.loaded = load_pair(
            self.pairs[
                self.current_index
            ],
            self.match_dir,
            self.review_dir,
        )

        self.selected_pair_id: Optional[
            str
        ] = None

        self.default_xlim: Optional[
            tuple[float, float]
        ] = None

        self.default_ylim: Optional[
            tuple[float, float]
        ] = None

        self._help_artist = None
        self._status_artist = None

        self.fig, self.ax = plt.subplots(
            figsize=(16, 7)
        )

        self.press_callback_id = (
            self.fig.canvas.mpl_connect(
                "button_press_event",
                self.on_click,
            )
        )

        self.key_callback_id = (
            self.fig.canvas.mpl_connect(
                "key_press_event",
                self.on_key,
            )
        )

        self.scroll_callback_id = (
            self.fig.canvas.mpl_connect(
                "scroll_event",
                self.on_scroll,
            )
        )

        logger.info(
            (
                "Callbacks connected: "
                "mouse=%s key=%s scroll=%s"
            ),
            self.press_callback_id,
            self.key_callback_id,
            self.scroll_callback_id,
        )

        self.draw(
            reset_view=True
        )

    def current_pair(
        self,
    ) -> FilePair:
        return self.pairs[
            self.current_index
        ]

    def current_review_path(
        self,
    ) -> Path:
        return review_path_for_pair(
            self.review_dir,
            self.current_pair(),
        )

    def toolbar_navigation_active(
        self,
    ) -> bool:
        """Return True while Matplotlib pan/zoom mode is active."""
        toolbar = getattr(
            self.fig.canvas,
            "toolbar",
            None,
        )

        if toolbar is None:
            return False

        mode = getattr(
            toolbar,
            "mode",
            "",
        )

        mode_text = (
            str(mode)
            .strip()
            .lower()
        )

        return bool(
            mode_text
            and mode_text not in {
                "none",
                "_mode.none",
            }
        )

    def save_reviews(
        self,
    ) -> None:
        path = (
            self.current_review_path()
        )

        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        out = (
            self.loaded.reviews.copy()
        )

        for column in (
            "events_time",
            "rpi_time",
        ):
            if column not in out.columns:
                continue

            out[column] = (
                pd.to_datetime(
                    out[column],
                    errors="coerce",
                )
                .apply(
                    lambda value: (
                        ""
                        if pd.isna(value)
                        else value.isoformat(
                            sep=" "
                        )
                    )
                )
            )

        out.to_csv(
            path,
            index=False,
        )

        logger.info(
            "Saved pair reviews -> %s",
            path,
        )

    def goto_pair(
        self,
        index: int,
    ) -> None:
        self.save_reviews()

        self.current_index = max(
            0,
            min(
                index,
                len(self.pairs) - 1,
            ),
        )

        self.loaded = load_pair(
            self.current_pair(),
            self.match_dir,
            self.review_dir,
        )

        self.selected_pair_id = None

        logger.info(
            "Moved to session %d/%d",
            self.current_index + 1,
            len(self.pairs),
        )

        self.draw(
            reset_view=True
        )

    def current_block_bounds(
        self,
    ) -> tuple[
        pd.Timestamp,
        pd.Timestamp,
    ]:
        blocks = self.loaded.blocks

        if (
            str(
                self.loaded.current_block
            ).lower()
            == "all"
        ):
            return (
                pd.to_datetime(
                    blocks["start"].min()
                ),
                pd.to_datetime(
                    blocks["end"].max()
                ),
            )

        block_number = int(
            self.loaded.current_block
        )

        rows = blocks[
            pd.to_numeric(
                blocks["block"],
                errors="coerce",
            ).eq(
                block_number
            )
        ]

        if rows.empty:
            return (
                pd.to_datetime(
                    blocks["start"].min()
                ),
                pd.to_datetime(
                    blocks["end"].max()
                ),
            )

        row = rows.iloc[0]

        return (
            pd.to_datetime(
                row["start"]
            ),
            pd.to_datetime(
                row["end"]
            ),
        )

    def visible_marks(
        self,
    ) -> tuple[
        pd.DataFrame,
        pd.DataFrame,
    ]:
        current = (
            self.loaded.current_block
        )

        if (
            str(current).lower()
            == "all"
        ):
            return (
                self.loaded.events_marks.copy(),
                self.loaded.rpi_marks.copy(),
            )

        block_number = int(
            current
        )

        events = (
            self.loaded.events_marks[
                pd.to_numeric(
                    self.loaded.events_marks[
                        "block"
                    ],
                    errors="coerce",
                ).eq(
                    block_number
                )
            ].copy()
        )

        rpi = (
            self.loaded.rpi_marks[
                pd.to_numeric(
                    self.loaded.rpi_marks[
                        "block"
                    ],
                    errors="coerce",
                ).eq(
                    block_number
                )
            ].copy()
        )

        return (
            events,
            rpi,
        )

    def pair_is_visible(
        self,
        row: pd.Series,
    ) -> bool:
        if (
            str(
                self.loaded.current_block
            ).lower()
            == "all"
        ):
            return True

        block_number = int(
            self.loaded.current_block
        )

        pair_block = pd.to_numeric(
            pd.Series(
                [
                    row.get(
                        "block",
                        np.nan,
                    )
                ]
            ),
            errors="coerce",
        ).iloc[0]

        if not pd.isna(
            pair_block
        ):
            return (
                int(pair_block)
                == block_number
            )

        start, end = (
            self.current_block_bounds()
        )

        event_time = pd.to_datetime(
            row["events_time"]
        )

        rpi_time = pd.to_datetime(
            row["rpi_time"]
        )

        return (
            start
            <= event_time
            <= end
            or start
            <= rpi_time
            <= end
        )

    def visible_matches(
        self,
    ) -> pd.DataFrame:
        if (
            self.loaded.matches.empty
        ):
            return (
                self.loaded.matches.copy()
            )

        mask = (
            self.loaded.matches.apply(
                self.pair_is_visible,
                axis=1,
            )
        )

        return (
            self.loaded.matches[
                mask
            ].copy()
        )

    def vote_for_pair(
        self,
        pair_id: str,
    ) -> Optional[int]:
        rows = (
            self.loaded.reviews[
                self.loaded.reviews[
                    "pair_id"
                ]
                .astype(str)
                .eq(pair_id)
            ]
        )

        if rows.empty:
            return None

        raw = str(
            rows.iloc[0].get(
                "vote",
                "",
            )
        ).strip()

        if raw not in {
            "1",
            "2",
            "3",
            "4",
        }:
            return None

        return int(raw)

    def set_vote(
        self,
        vote: int,
    ) -> None:
        if (
            self.selected_pair_id
            is None
        ):
            self.set_status(
                "Select a pair first."
            )
            return

        if vote not in (
            1,
            2,
            3,
            4,
        ):
            return

        pair_rows = (
            self.loaded.matches[
                self.loaded.matches[
                    "pair_id"
                ]
                .astype(str)
                .eq(
                    self.selected_pair_id
                )
            ]
        )

        if pair_rows.empty:
            self.set_status(
                "Selected pair no longer exists."
            )
            return

        pair_row = (
            pair_rows.iloc[0]
        )

        if bool(
            pair_row["exclude"]
        ):
            self.set_status(
                (
                    f"{self.selected_pair_id} "
                    "was excluded during matching. "
                    "Vote was not changed."
                )
            )
            return

        current_xlim = (
            self.ax.get_xlim()
        )

        current_ylim = (
            self.ax.get_ylim()
        )

        mask = (
            self.loaded.reviews[
                "pair_id"
            ]
            .astype(str)
            .eq(
                self.selected_pair_id
            )
        )

        if not mask.any():
            return

        self.loaded.reviews.loc[
            mask,
            "vote",
        ] = str(vote)

        self.loaded.reviews.loc[
            mask,
            "vote_label",
        ] = VOTE_LABELS[
            vote
        ]

        self.loaded.reviews.loc[
            mask,
            "voted_at",
        ] = (
            datetime.now()
            .isoformat(
                timespec="seconds"
            )
        )

        logger.info(
            (
                "PAIR VOTE: "
                "%s -> %d (%s)"
            ),
            self.selected_pair_id,
            vote,
            VOTE_LABELS[vote],
        )

        self.save_reviews()

        self.set_status(
            (
                f"{self.selected_pair_id}: "
                f"{vote} "
                f"{VOTE_LABELS[vote]}"
            )
        )

        self.draw(
            reset_view=False
        )

        self.ax.set_xlim(
            *current_xlim
        )

        self.ax.set_ylim(
            *current_ylim
        )

        self.fig.canvas.draw_idle()

    def clear_vote(
        self,
    ) -> None:
        if (
            self.selected_pair_id
            is None
        ):
            self.set_status(
                "Select a pair first."
            )
            return

        current_xlim = (
            self.ax.get_xlim()
        )

        current_ylim = (
            self.ax.get_ylim()
        )

        mask = (
            self.loaded.reviews[
                "pair_id"
            ]
            .astype(str)
            .eq(
                self.selected_pair_id
            )
        )

        if not mask.any():
            return

        self.loaded.reviews.loc[
            mask,
            [
                "vote",
                "vote_label",
                "voted_at",
            ],
        ] = ""

        logger.info(
            "PAIR VOTE CLEARED: %s",
            self.selected_pair_id,
        )

        self.save_reviews()

        self.draw(
            reset_view=False
        )

        self.ax.set_xlim(
            *current_xlim
        )

        self.ax.set_ylim(
            *current_ylim
        )

        self.fig.canvas.draw_idle()

    def find_nearest_pair(
        self,
        event,
        tolerance_pixels: float = 14.0,
    ) -> Optional[str]:
        if (
            event.inaxes != self.ax
            or event.x is None
            or event.y is None
        ):
            return None

        matches = (
            self.visible_matches()
        )

        if matches.empty:
            return None

        click = np.array(
            [
                float(event.x),
                float(event.y),
            ],
            dtype=float,
        )

        best_pair: Optional[
            str
        ] = None

        best_distance = float(
            "inf"
        )

        for _, row in (
            matches.iterrows()
        ):
            event_time = (
                mdates.date2num(
                    pd.to_datetime(
                        row[
                            "events_time"
                        ]
                    )
                )
            )

            rpi_time = (
                mdates.date2num(
                    pd.to_datetime(
                        row[
                            "rpi_time"
                        ]
                    )
                )
            )

            point_a = np.array(
                self.ax.transData.transform(
                    (
                        event_time,
                        1.0,
                    )
                ),
                dtype=float,
            )

            point_b = np.array(
                self.ax.transData.transform(
                    (
                        rpi_time,
                        0.0,
                    )
                ),
                dtype=float,
            )

            segment = (
                point_b
                - point_a
            )

            denominator = float(
                np.dot(
                    segment,
                    segment,
                )
            )

            if denominator == 0:
                distance = float(
                    np.linalg.norm(
                        click
                        - point_a
                    )
                )
            else:
                fraction = float(
                    np.dot(
                        click
                        - point_a,
                        segment,
                    )
                    / denominator
                )

                fraction = float(
                    np.clip(
                        fraction,
                        0.0,
                        1.0,
                    )
                )

                closest = (
                    point_a
                    + fraction
                    * segment
                )

                distance = float(
                    np.linalg.norm(
                        click
                        - closest
                    )
                )

            if (
                distance
                < best_distance
            ):
                best_distance = (
                    distance
                )

                best_pair = str(
                    row["pair_id"]
                )

        logger.debug(
            (
                "PAIR HIT: "
                "pair=%r "
                "distance=%.2f px"
            ),
            best_pair,
            best_distance,
        )

        if (
            best_distance
            > tolerance_pixels
        ):
            return None

        return best_pair

    def on_click(
        self,
        event,
    ) -> None:
        if self.toolbar_navigation_active():
            logger.debug(
                (
                    "Ignoring pair selection because "
                    "toolbar pan/zoom is active."
                )
            )
            return

        current_xlim = (
            self.ax.get_xlim()
        )

        current_ylim = (
            self.ax.get_ylim()
        )

        pair_id = (
            self.find_nearest_pair(
                event
            )
        )

        if pair_id is None:
            self.selected_pair_id = (
                None
            )

            self.set_status(
                "No pair selected."
            )

            self.draw(
                reset_view=False
            )

            self.ax.set_xlim(
                *current_xlim
            )

            self.ax.set_ylim(
                *current_ylim
            )

            self.fig.canvas.draw_idle()
            return

        self.selected_pair_id = (
            pair_id
        )

        vote = self.vote_for_pair(
            pair_id
        )

        vote_text = (
            "unvoted"
            if vote is None
            else (
                f"{vote} "
                f"{VOTE_LABELS[vote]}"
            )
        )

        logger.info(
            "PAIR SELECTED: %s (%s)",
            pair_id,
            vote_text,
        )

        self.set_status(
            f"Selected {pair_id}: {vote_text}"
        )

        self.draw(
            reset_view=False
        )

        self.ax.set_xlim(
            *current_xlim
        )

        self.ax.set_ylim(
            *current_ylim
        )

        self.fig.canvas.draw_idle()

    def on_scroll(
        self,
        event,
    ) -> None:
        """Zoom horizontally around the mouse cursor."""
        if (
            event.inaxes != self.ax
            or event.xdata is None
        ):
            return

        current_xlim = (
            self.ax.get_xlim()
        )

        left = float(
            current_xlim[0]
        )

        right = float(
            current_xlim[1]
        )

        cursor = float(
            event.xdata
        )

        current_width = (
            right - left
        )

        if current_width <= 0:
            return

        if event.button == "up":
            scale_factor = 0.75

        elif event.button == "down":
            scale_factor = 1.35

        else:
            logger.debug(
                "Unknown scroll event: %r",
                event.button,
            )
            return

        new_width = (
            current_width
            * scale_factor
        )

        left_fraction = (
            (cursor - left)
            / current_width
        )

        new_left = (
            cursor
            - left_fraction
            * new_width
        )

        new_right = (
            new_left
            + new_width
        )

        logger.info(
            (
                "SCROLL ZOOM: "
                "button=%s | "
                "old_xlim=(%.8f, %.8f) | "
                "new_xlim=(%.8f, %.8f)"
            ),
            event.button,
            left,
            right,
            new_left,
            new_right,
        )

        self.ax.set_xlim(
            new_left,
            new_right,
        )

        self.fig.canvas.draw_idle()

    def next_block(
        self,
        step: int,
    ) -> None:
        blocks = sorted(
            self.loaded.blocks[
                "block"
            ]
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )

        if not blocks:
            return

        current = (
            self.loaded.current_block
        )

        if (
            str(current).lower()
            == "all"
        ):
            index = (
                -1
                if step > 0
                else 0
            )
        else:
            try:
                index = (
                    blocks.index(
                        int(current)
                    )
                )
            except ValueError:
                index = 0

        new_index = (
            index + step
        )

        if (
            new_index < 0
            or new_index
            >= len(blocks)
        ):
            self.loaded.current_block = (
                "All"
            )
        else:
            self.loaded.current_block = (
                blocks[
                    new_index
                ]
            )

        self.selected_pair_id = None

        logger.info(
            "BLOCK VIEW: %s",
            self.loaded.current_block,
        )

        self.draw(
            reset_view=True
        )

    def reset_zoom(
        self,
    ) -> None:
        if (
            self.default_xlim
            and self.default_ylim
        ):
            self.ax.set_xlim(
                *self.default_xlim
            )

            self.ax.set_ylim(
                *self.default_ylim
            )

            self.fig.canvas.draw_idle()

            logger.info(
                "VIEW RESET"
            )

    def set_status(
        self,
        message: str,
    ) -> None:
        logger.info(
            "STATUS: %s",
            message,
        )

        if (
            self._status_artist
            is not None
        ):
            try:
                self._status_artist.remove()
            except Exception:
                pass

        self._status_artist = (
            self.fig.text(
                0.01,
                0.045,
                message,
                fontsize=9,
            )
        )

        self.fig.canvas.draw_idle()

    def draw_stream(
        self,
        df: pd.DataFrame,
        ts_col: str,
        y: float,
        base_color: str,
    ) -> None:
        if df.empty:
            return

        kept = df[
            ~df[
                "match_exclude"
            ].astype(bool)
        ].copy()

        excluded = df[
            df[
                "match_exclude"
            ].astype(bool)
        ].copy()

        if not kept.empty:
            times = pd.to_datetime(
                kept[ts_col]
            )

            self.ax.vlines(
                times,
                y,
                0.5,
                color=base_color,
                linewidth=1.4,
                alpha=0.85,
            )

            self.ax.scatter(
                times,
                np.full(
                    len(kept),
                    y,
                ),
                color=base_color,
                s=20,
                zorder=5,
            )

        if not excluded.empty:
            times = pd.to_datetime(
                excluded[ts_col]
            )

            self.ax.vlines(
                times,
                y,
                0.5,
                color="gray",
                linewidth=1.0,
                alpha=0.5,
                linestyles="dotted",
            )

            self.ax.scatter(
                times,
                np.full(
                    len(excluded),
                    y,
                ),
                color="black",
                marker="x",
                s=45,
                zorder=7,
            )

    def draw_pair_connection(
        self,
        row: pd.Series,
        pair_number: int,
    ) -> None:
        pair_id = str(
            row["pair_id"]
        )

        event_time = (
            pd.to_datetime(
                row[
                    "events_time"
                ]
            )
        )

        rpi_time = (
            pd.to_datetime(
                row[
                    "rpi_time"
                ]
            )
        )

        matcher_excluded = bool(
            row["exclude"]
        )

        base_color = (
            plt.get_cmap(
                "tab20"
            )(
                pair_number % 20
            )
        )

        vote = self.vote_for_pair(
            pair_id
        )

        if (
            vote is not None
            and not matcher_excluded
        ):
            highlight_color, alpha = (
                VOTE_HIGHLIGHTS[
                    vote
                ]
            )

            self.ax.plot(
                [
                    event_time,
                    rpi_time,
                ],
                [
                    1.0,
                    0.0,
                ],
                color=highlight_color,
                linewidth=9.0,
                alpha=alpha,
                solid_capstyle="round",
                zorder=2,
            )

        self.ax.plot(
            [
                event_time,
                rpi_time,
            ],
            [
                1.0,
                0.0,
            ],
            color=base_color,
            linewidth=(
                1.3
                if matcher_excluded
                else 2.6
            ),
            alpha=(
                0.25
                if matcher_excluded
                else 0.95
            ),
            linestyle=(
                ":"
                if matcher_excluded
                else "-"
            ),
            zorder=4,
        )

        if (
            self.selected_pair_id
            == pair_id
        ):
            self.ax.plot(
                [
                    event_time,
                    rpi_time,
                ],
                [
                    1.0,
                    0.0,
                ],
                color="gold",
                linewidth=13.0,
                alpha=0.28,
                solid_capstyle="round",
                zorder=1,
            )

            self.ax.plot(
                [
                    event_time,
                    rpi_time,
                ],
                [
                    1.0,
                    0.0,
                ],
                color=base_color,
                linewidth=3.0,
                alpha=1.0,
                zorder=6,
            )

        midpoint = (
            event_time
            + (
                rpi_time
                - event_time
            )
            / 2
        )

        if matcher_excluded:
            self.ax.scatter(
                [midpoint],
                [0.5],
                marker="x",
                color="black",
                s=55,
                zorder=8,
            )

        if vote is not None:
            self.ax.text(
                midpoint,
                0.5,
                str(vote),
                fontsize=8,
                fontweight="bold",
                ha="center",
                va="center",
                bbox={
                    "boxstyle": "circle,pad=0.25",
                    "facecolor": "white",
                    "edgecolor": "black",
                    "alpha": 0.8,
                },
                zorder=10,
            )

    def vote_counts(
        self,
    ) -> dict[int, int]:
        counts = {
            1: 0,
            2: 0,
            3: 0,
            4: 0,
        }

        for value in (
            self.loaded.reviews[
                "vote"
            ]
            .astype(str)
        ):
            if value in (
                "1",
                "2",
                "3",
                "4",
            ):
                counts[
                    int(value)
                ] += 1

        return counts

    def draw(
        self,
        reset_view: bool = True,
    ) -> None:
        previous_xlim = None
        previous_ylim = None

        if not reset_view:
            previous_xlim = (
                self.ax.get_xlim()
            )

            previous_ylim = (
                self.ax.get_ylim()
            )

        self.ax.clear()

        pair = self.current_pair()
        ts_col = self.loaded.ts_col

        start, end = (
            self.current_block_bounds()
        )

        events, rpi = (
            self.visible_marks()
        )

        matches = (
            self.visible_matches()
        )

        self.ax.axvspan(
            start,
            end,
            alpha=0.04,
        )

        self.draw_stream(
            events,
            ts_col,
            y=1.0,
            base_color="red",
        )

        self.draw_stream(
            rpi,
            ts_col,
            y=0.0,
            base_color="blue",
        )

        for (
            pair_number,
            (_, row),
        ) in enumerate(
            matches.iterrows()
        ):
            self.draw_pair_connection(
                row,
                pair_number,
            )

        for _, block in (
            self.loaded.blocks
            .iterrows()
        ):
            block_start = (
                pd.to_datetime(
                    block["start"]
                )
            )

            block_end = (
                pd.to_datetime(
                    block["end"]
                )
            )

            if (
                block_end < start
                or block_start > end
            ):
                continue

            self.ax.axvline(
                block_start,
                linestyle="--",
                alpha=0.35,
                linewidth=0.8,
            )

            self.ax.axvline(
                block_end,
                linestyle="--",
                alpha=0.35,
                linewidth=0.8,
            )

        counts = (
            self.vote_counts()
        )

        active_matches = int(
            (
                ~self.loaded.matches[
                    "exclude"
                ].astype(bool)
            ).sum()
        )

        matcher_excluded = int(
            self.loaded.matches[
                "exclude"
            ]
            .astype(bool)
            .sum()
        )

        selected_text = (
            "none"
            if self.selected_pair_id
            is None
            else self.selected_pair_id
        )

        self.ax.set_title(
            (
                f"Pair Vote Review — "
                f"{pair.label} — "
                f"Block {self.loaded.current_block}\n"
                f"[{self.current_index + 1}/"
                f"{len(self.pairs)}] "
                f"{pair.events_file.name} | "
                f"{pair.rpi_file.name}\n"
                f"selected={selected_text} | "
                f"active_pairs={active_matches} | "
                f"matcher_excluded={matcher_excluded} | "
                f"votes "
                f"1={counts[1]} "
                f"2={counts[2]} "
                f"3={counts[3]} "
                f"4={counts[4]}"
            )
        )

        self.ax.set_yticks(
            [0, 1]
        )

        self.ax.set_yticklabels(
            [
                "RPi Mark",
                "Events Mark",
            ]
        )

        self.ax.set_xlabel(
            f"Time ({ts_col})"
        )

        self.ax.xaxis.set_major_formatter(
            mdates.DateFormatter(
                "%H:%M:%S"
            )
        )

        self.default_xlim = (
            mdates.date2num(
                start
            ),
            mdates.date2num(
                end
            ),
        )

        self.default_ylim = (
            -0.2,
            1.2,
        )

        if reset_view:
            self.ax.set_xlim(
                *self.default_xlim
            )

            self.ax.set_ylim(
                *self.default_ylim
            )

        elif (
            previous_xlim is not None
            and previous_ylim is not None
        ):
            self.ax.set_xlim(
                *previous_xlim
            )

            self.ax.set_ylim(
                *previous_ylim
            )

        if (
            self._help_artist
            is not None
        ):
            try:
                self._help_artist.remove()
            except Exception:
                pass

        help_text = (
            "click pair=line select | "
            "1=hard exclude | "
            "2=soft exclude | "
            "3=soft accept | "
            "4=hard accept | "
            "0=clear vote\n"
            "scroll up/down=zoom | "
            "up/down=block | "
            "a=all blocks | "
            "left/right=session | "
            "r=reset view | "
            "s=save | "
            "q=quit"
        )

        self._help_artist = (
            self.fig.text(
                0.01,
                0.01,
                help_text,
                fontsize=9,
            )
        )

        self.fig.tight_layout(
            rect=(
                0,
                0.07,
                1,
                1,
            )
        )

        self.fig.canvas.draw_idle()

    def on_key(
        self,
        event,
    ) -> None:
        key = (
            str(event.key).lower()
            if event.key
            else ""
        )

        logger.info(
            "KEY PRESS: %r",
            key,
        )

        if key in (
            "1",
            "2",
            "3",
            "4",
        ):
            self.set_vote(
                int(key)
            )
            return

        if key == "0":
            self.clear_vote()
            return

        if key == "right":
            if (
                self.current_index
                < len(self.pairs) - 1
            ):
                self.goto_pair(
                    self.current_index + 1
                )
            return

        if key == "left":
            if self.current_index > 0:
                self.goto_pair(
                    self.current_index - 1
                )
            return

        if key == "up":
            self.next_block(
                1
            )
            return

        if key == "down":
            self.next_block(
                -1
            )
            return

        if key == "a":
            self.loaded.current_block = (
                "All"
            )

            self.selected_pair_id = (
                None
            )

            self.draw(
                reset_view=True
            )
            return

        if key == "r":
            self.reset_zoom()
            return

        if key == "s":
            self.save_reviews()
            return

        if key == "q":
            self.save_reviews()

            plt.close(
                self.fig
            )
            return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Interactive review of matched "
            "Events ↔ RPi mark pairs."
        )
    )

    parser.add_argument(
        "--manifest",
        type=Path,
        help=(
            "CSV with columns: "
            "events_file,rpi_file,label"
        ),
    )

    parser.add_argument(
        "--events",
        type=Path,
        help="Single Events CSV.",
    )

    parser.add_argument(
        "--rpi",
        type=Path,
        help="Single RPi CSV.",
    )

    parser.add_argument(
        "--label",
        default="",
        help=(
            "BioPac or RNS for "
            "single-pair mode."
        ),
    )

    parser.add_argument(
        "--events-dir",
        type=Path,
        help=(
            "Directory of Events CSVs."
        ),
    )

    parser.add_argument(
        "--rpi-dir",
        type=Path,
        help=(
            "Directory of RPi unified CSVs."
        ),
    )

    parser.add_argument(
        "--match-dir",
        type=Path,
        required=True,
        help=(
            "Directory containing "
            "*_mark_matches.csv and "
            "*_mark_singles.csv."
        ),
    )

    parser.add_argument(
        "--review-dir",
        type=Path,
        required=True,
        help=(
            "Directory for pair-vote CSVs."
        ),
    )

    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
    )

    parser.add_argument(
        "--skip-reviewed-pairs",
        action="store_true",
        help=(
            "Skip sessions where every active "
            "matched pair already has a 1-4 vote."
        ),
    )

    return parser.parse_args()


def build_pairs_from_args(
    args: argparse.Namespace,
) -> list[FilePair]:
    if args.manifest:
        return load_manifest(
            args.manifest
        )

    if (
        args.events_dir
        and args.rpi_dir
    ):
        return (
            build_pairs_from_directories(
                args.events_dir,
                args.rpi_dir,
            )
        )

    if (
        args.events
        and args.rpi
        and args.label.strip()
    ):
        return [
            FilePair(
                events_file=(
                    args.events.expanduser()
                ),
                rpi_file=(
                    args.rpi.expanduser()
                ),
                label=(
                    args.label.strip()
                ),
            )
        ]

    raise SystemExit(
        "Provide either --manifest, "
        "or --events-dir with --rpi-dir, "
        "or all of --events --rpi --label."
    )


def main() -> None:
    args = parse_args()

    match_dir = (
        args.match_dir.expanduser()
    )

    review_dir = (
        args.review_dir.expanduser()
    )

    review_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    pairs = build_pairs_from_args(
        args
    )

    pairs = [
        pair
        for pair in pairs
        if pair_has_match_data(
            match_dir,
            pair,
        )
    ]

    if args.skip_reviewed_pairs:
        pairs = [
            pair
            for pair in pairs
            if not pair_review_complete(
                match_dir,
                review_dir,
                pair,
            )
        ]

    if not pairs:
        raise SystemExit(
            "No matched sessions found to review."
        )

    for pair in pairs:
        if not pair.events_file.exists():
            raise FileNotFoundError(
                f"Missing Events file: {pair.events_file}"
            )

        if not pair.rpi_file.exists():
            raise FileNotFoundError(
                f"Missing RPi file: {pair.rpi_file}"
            )

    app = PairVoteReviewApp(
        pairs=pairs,
        match_dir=match_dir,
        review_dir=review_dir,
        start_index=args.start_index,
    )

    logger.info(
        "PairVoteReviewApp retained: %r",
        app,
    )

    plt.show()


if __name__ == "__main__":
    main()