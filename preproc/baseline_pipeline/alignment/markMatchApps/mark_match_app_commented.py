# alignment/mark_match_app.py

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# -----------------------------------------------------------------------------
# GUI ARCHITECTURE OVERVIEW
# -----------------------------------------------------------------------------
# This program uses Matplotlib as an interactive GUI toolkit.  The important
# pieces are:
#   1. STATE: pandas DataFrames + attributes on the app object hold the truth.
#   2. VIEW:  draw() reads that state and paints lines, points, labels, etc.
#   3. INPUT: fig.canvas.mpl_connect(...) registers mouse/keyboard callbacks.
#   4. UPDATE: a callback changes state, optionally saves CSV data, then calls
#      draw() / draw_idle() so the visible figure reflects the new state.
#
# This "state -> draw -> event -> update -> redraw" loop is the key idea to
# follow when learning how these GUIs work.
# -----------------------------------------------------------------------------

logger = logging.getLogger("mark_match_app")
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
    "RPi_Time_simple",
    "RPi_Time_verb",
    "RPi_Time_unified",
    "delta_seconds_RPi_Time_simple",
    "delta_seconds_RPi_Time_verb",
    "delta_seconds_RPi_Time_unified",
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
    "mark_time_orig",
    "RPi_Time_simple",
    "RPi_Time_verb",
    "RPi_Time_unified",
    "block",
    "matched_pair_id",
    "exclude",
    "reason",
    "reviewed_at",
]


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
    ts_col: str
    rpi_time_type: str


# Normalize filenames by repeatedly removing known suffixes so related CSV files can be matched by
# a common base name.
def strip_suffixes(stem: str, suffixes: list[str]) -> str:
    base = stem
    changed = True

    while changed:
        changed = False
        for suffix in suffixes:
            if suffix and base.endswith(suffix):
                base = base[: -len(suffix)]
                changed = True

    return base.rstrip("_-")


# Look up important Events CSV columns by name, while accepting a few alternate spellings.
def detect_events_columns(df: pd.DataFrame) -> dict[str, Optional[str]]:
    cols = {column.lower(): column for column in df.columns}

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



# Build a table of block start/end times. Later GUI views use these boundaries for block
# navigation and plotting.
def build_blocks(events: pd.DataFrame, ts_col: str, etype_col: str, block_col: Optional[str]) -> pd.DataFrame:
    ordered = events.sort_values(ts_col).copy()
    event_types = ordered[etype_col].astype(str).str.lower()

    starts = ordered[event_types.eq("blockstart")]
    ends = ordered[event_types.eq("blockend")]

    if (not starts.empty and not ends.empty and block_col and block_col in ordered.columns):
        blocks = (starts[[ts_col, block_col]].merge(ends[[ts_col, block_col]], on=block_col, how="left", suffixes=("_start", "_end")).rename(
                columns={
                    block_col: "block",
                    f"{ts_col}_start": "start",
                    f"{ts_col}_end": "end",
                }
            )
        )
    elif block_col and block_col in ordered.columns:
        grouped = ordered.groupby(block_col)[ts_col]

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

    blocks = blocks.sort_values("start").reset_index(drop=True)

    next_starts = blocks["start"].shift(-1)
    blocks["end"] = blocks["end"].where(blocks["end"].notna(), next_starts)
    blocks["end"] = blocks["end"].fillna(ordered[ts_col].max())

    return blocks


# Assign each timestamp to the block interval that contains it. This is vectorized with NumPy
# rather than looping row-by-row.
def assign_block(times: pd.Series, blocks: pd.DataFrame) -> pd.Series:
    ordered = blocks.sort_values("start")

    starts = pd.to_datetime(ordered["start"]).values.astype("datetime64[ns]")
    ends = pd.to_datetime(ordered["end"]).values.astype("datetime64[ns]")
    
    ids = ordered["block"].values

    values = pd.to_datetime(times).values.astype("datetime64[ns]")
    

    indices = (np.searchsorted(starts, values, side="right") - 1)
    indices = np.clip(indices, 0, len(ordered) - 1)

    valid = (values >= starts[indices]) & (values <= ends[indices])

    result = np.where(valid, ids[indices], np.nan)

    return pd.Series(result, index=times.index)


# Add GUI-friendly metadata to each mark: a stable ordinal/id, its stream name, and its assigned
# block.
def prepare_marks(df: pd.DataFrame, stream: str, ts_col: str, blocks: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().reset_index(drop=True)

    out["ordinal"] = np.arange(len(out), dtype=int)

    out["mark_id"] = [
        f"{stream}_{ordinal:04d}"
        for ordinal in out["ordinal"]
    ]

    out["stream"] = stream
    out["block"] = assign_block(out[ts_col], blocks)

    return out

#### right here myra
# Load one Events/RPi file pair, convert timestamp columns, isolate marks, build blocks, and
# return all data needed by the GUI.
def load_pair(pair: FilePair, rpi_time_type: str) -> LoadedPair:
    logger.info(
        "Loading pair: events=%s | rpi=%s | label=%s",
        pair.events_file,
        pair.rpi_file,
        pair.label,
    )

    events = pd.read_csv(pair.events_file)

    detected = detect_events_columns(events)

    ts_col = detected["ts"]
    event_col = detected["etype"]
    block_col = detected["block"]

    logger.debug(
        "Events columns detected: ts=%r event=%r block=%r",
        ts_col,
        event_col,
        block_col,
    )

    if not ts_col or not event_col:
        raise RuntimeError(f"Events file requires mLT_orig and an event-type column: {pair.events_file}")

    events[ts_col] = pd.to_datetime(events[ts_col], errors="coerce")

    events = events.dropna(subset=[ts_col]).copy()

    keep_types = {
        "blockstart",
        "blockend",
        "roundstart",
        "roundend",
        "mark",
    }

    events = events[events[event_col].astype(str).str.lower().isin(keep_types)].copy()

    events = events.sort_values(ts_col)

    blocks = build_blocks(events, ts_col, event_col, block_col)

    if blocks.empty:
        raise RuntimeError(f"No blocks could be constructed from {pair.events_file}")

    event_marks_raw = events[events[event_col].astype(str).str.lower().eq("mark")].copy()

    events_marks = prepare_marks(event_marks_raw, "events", ts_col, blocks)

    rpi = pd.read_csv(pair.rpi_file)

    ##### Right here myra
    if rpi_time_type not in rpi.columns:
        raise RuntimeError(
            f"Requested RPi timestamp column "
            f"{rpi_time_type!r} not found in {pair.rpi_file}. "
            f"Available columns: {list(rpi.columns)}"
        )

    rpi_time_col = rpi_time_type

    logger.debug("RPi time column selected: %r", rpi_time_col)

    if not rpi_time_col:
        raise RuntimeError(f"Could not detect an RPi timestamp column in {pair.rpi_file}")

    rpi[ts_col] = pd.to_datetime(rpi[rpi_time_col], errors="coerce")

    rpi = rpi.dropna(subset=[ts_col]).sort_values(ts_col).copy()
    

    rpi_marks = prepare_marks(rpi, "rpi", ts_col, blocks)

    logger.info(
        "Loaded pair successfully: events_marks=%d | rpi_marks=%d | blocks=%d",
        len(events_marks),
        len(rpi_marks),
        len(blocks),
    )

    return LoadedPair(
        pair=pair,
        events_marks=events_marks,
        rpi_marks=rpi_marks,
        blocks=blocks,
        ts_col=ts_col,
        rpi_time_type=rpi_time_type,
    )


# Read an explicit CSV manifest and convert each row into a FilePair object.
def load_manifest(path: Path,) -> list[FilePair]:
    manifest = pd.read_csv(path)

    required = {"events_file", "rpi_file", "label"}

    missing = required - set(manifest.columns)

    if missing:
        raise RuntimeError(f"Manifest missing columns: {sorted(missing)}")

    return [
        FilePair(
            events_file=Path(str(row["events_file"])).expanduser(),
            rpi_file=Path(str(row["rpi_file"])).expanduser(),
            label=str(row["label"]).strip(),
        )
        for _, row in manifest.iterrows()
    ]


# Automatically discover matching Events and RPi CSV files from two directories using their
# filename conventions.
def build_pairs_from_directories(events_dir: Path, rpi_dir: Path) -> list[FilePair]:
    events_dir = events_dir.expanduser()
    rpi_dir = rpi_dir.expanduser()

    if not events_dir.is_dir():
        raise NotADirectoryError(f"events_dir is not a directory: {events_dir}")

    if not rpi_dir.is_dir():
        raise NotADirectoryError(f"rpi_dir is not a directory: {rpi_dir}")

    event_map: dict[str, Path] = {}

    for path in sorted(events_dir.glob("*.csv")):
        if not path.is_file():
            continue

        base = strip_suffixes(path.stem, EVENT_SUFFIXES)

        event_map[base] = path

    rpi_map: dict[tuple[str, str], Path] = {}

    for path in sorted(rpi_dir.glob("*.csv")):
        if not path.is_file():
            continue

        for label, suffix in RPI_SUFFIXES.items():
            if not path.stem.endswith(suffix):
                continue

            base = path.stem[: -len(suffix)].rstrip("_-")

            rpi_map[(base, label)] = path
            break

    logger.info(
        "Directory scan: %d event bases | %d RPi files",
        len(event_map),
        len(rpi_map),
    )

    pairs: list[FilePair] = []

    for base, events_file in sorted(event_map.items()):
        for label in ("BioPac", "RNS"):
            rpi_file = rpi_map.get((base, label))

            if rpi_file is None:
                continue

            pairs.append(FilePair(events_file=events_file, rpi_file=rpi_file, label=label))

    logger.info(
        "Matched %d Events/RPi pairs",
        len(pairs),
    )

    return pairs


class MarkMatchApp:
    # Initialize application state and build the Matplotlib GUI. `plt.subplots()` creates the
    # window's Figure and plotting Axes; `mpl_connect()` wires GUI events to Python callback
    # methods.
    def __init__( self, pairs: list[FilePair], output_dir: Path, rpi_time_type: str, start_index: int = 0) -> None:
        if not pairs:
            raise ValueError("At least one Events/RPi pair is required.")

        self.pairs = pairs
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.rpi_time_type = rpi_time_type
        self.current_index = max(0, min(start_index, len(pairs) - 1))

        self.loaded = load_pair(self.pairs[self.current_index], self.rpi_time_type)

        self.mode = "match"
        self.current_block: str | int = "All"

        self.drag_source: Optional[tuple[str, str]] = None
        self.selected_entity: Optional[tuple[str, str]] = None

        self.matches = pd.DataFrame(columns=MATCH_COLUMNS)
        self.singles = pd.DataFrame(columns=SINGLE_COLUMNS)

        self._load_saved_state()

        # Figure = the whole GUI window; Axes = the plotting area inside that window.
        self.fig, self.ax = plt.subplots(figsize=(16, 7))

        self.default_xlim: Optional[tuple[float, float]] = None
        self.default_ylim: Optional[tuple[float, float]] = None

        logger.info("Starting MarkMatchApp")
        logger.info("Matplotlib backend: %s", matplotlib.get_backend())
        logger.info("DISPLAY=%r", os.environ.get("DISPLAY"))
        logger.info("Pairs available: %d", len(self.pairs))
        logger.info("Starting pair index: %d", self.current_index)

        logger.info(
            "Current files: events=%s | rpi=%s | label=%s",
            self.current_pair().events_file,
            self.current_pair().rpi_file,
            self.current_pair().label,
        )
        logger.info(
            "Loaded marks: events=%d | rpi=%d",
            len(self.loaded.events_marks),
            len(self.loaded.rpi_marks),
        )

        backend = str(matplotlib.get_backend()).lower()

        if "agg" in backend:
            logger.warning("Matplotlib backend is %s. An Agg backend is non-interactive, so mouse clicks/drags will not work.", matplotlib.get_backend())

        # mpl_connect registers callbacks with Matplotlib's event loop.
        # When the user clicks, Matplotlib creates a MouseEvent and passes it to on_press.
        self.press_connection_id = self.fig.canvas.mpl_connect("button_press_event", self.on_press)
        self.release_connection_id = self.fig.canvas.mpl_connect("button_release_event", self.on_release)
        self.key_connection_id = self.fig.canvas.mpl_connect("key_press_event", self.on_key)
        

        self.scroll_connection_id = self.fig.canvas.mpl_connect("scroll_event", self.on_scroll)

        logger.info(
            "Matplotlib callbacks connected: press=%s release=%s key=%s scroll=%s",
            self.press_connection_id,
            self.release_connection_id,
            self.key_connection_id,
            self.scroll_connection_id,
        )

        self.draw(reset_view=True)

    # Return the earliest block start and latest block end; this defines the x-axis range for the
    # full-session view.
    def full_session_bounds(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Return the full Events-derived block range."""
        blocks = self.loaded.blocks
        return pd.to_datetime(blocks["start"].min()), pd.to_datetime(blocks["end"].max()),
        
    # Translate the current block selection into concrete start/end timestamps used for the
    # visible x-axis.
    def current_view_bounds(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Return the currently selected block or full-session bounds."""
        if str(self.current_block).lower() == "all":
            return self.full_session_bounds()

        block_number = int(self.current_block)

        row = self.loaded.blocks[self.loaded.blocks["block"].astype(int).eq(block_number)]

        if row.empty:
            logger.warning(
                "Block %s not found; resetting to All",
                self.current_block,
            )
            self.current_block = "All"
            return self.full_session_bounds()

        block = row.iloc[0]

        return pd.to_datetime(block["start"]), pd.to_datetime(block["end"]),
        
    # Change which block is being viewed, then redraw the plot with the new time bounds.
    def next_block(self, step: int,) -> None:
        """Move forward/backward through Events-derived blocks."""
        blocks = sorted(self.loaded.blocks["block"].dropna().astype(int).unique().tolist())

        if not blocks:
            logger.warning("No blocks available for block navigation")
            return

        if str(self.current_block).lower() == "all":
            if step > 0:
                self.current_block = blocks[0]
            else:
                self.current_block = blocks[-1]
        else:
            try:
                current_index = blocks.index(int(self.current_block))
            except ValueError:
                current_index = 0

            new_index = current_index + step

            if new_index < 0 or new_index >= len(blocks):
                self.current_block = "All"
            else:
                self.current_block = blocks[new_index]

        logger.info("BLOCK VIEW: %s", self.current_block)

        self.draw(reset_view=True)

    # Switch back to the all-blocks view and redraw.
    def reset_view(self) -> None:
        """Return to the full-session view."""
        self.current_block = "All"

        logger.info("BLOCK VIEW: All")

        self.draw(reset_view=True)

    # Return the FilePair currently displayed by the GUI.
    def current_pair(self) -> FilePair:
        return self.pairs[self.current_index]

    # Matplotlib's toolbar can capture mouse input while pan/zoom is active; callbacks use this
    # check to avoid conflicting interactions.
    def toolbar_navigation_active(self) -> bool:
        """Return True while Matplotlib pan or zoom mode is active."""
        toolbar = getattr(self.fig.canvas, "toolbar", None)

        if toolbar is None:
            return False

        mode = getattr(toolbar, "mode", "")

        mode_text = str(mode).strip().lower()

        return bool(
            mode_text
            and mode_text not in {
                "none",
                "_mode.none",
            }
        )

    # Build the common filename stem used for this pair's saved match/single CSV files.
    def output_base(self) -> str:
        pair = self.current_pair()

        base = strip_suffixes(pair.events_file.stem, EVENT_SUFFIXES)

        return f"{base}_{pair.label}"

    # Return the output path for manually-created mark pairs.
    def match_path(self) -> Path:
        return self.output_dir / f"{self.output_base()}_mark_matches.csv"
        
    # Return the output path for per-mark review state.
    def single_path(self) -> Path:
        return self.output_dir / f"{self.output_base()}_mark_singles.csv"
        
    # Restore previous GUI work from CSV files so reopening the app continues where the reviewer
    # left off.
    def _load_saved_state(self) -> None:
        match_path = self.match_path()
        single_path = self.single_path()

        if match_path.exists():
            loaded = pd.read_csv(
                match_path,
                dtype=str,
            ).fillna("")

            for column in MATCH_COLUMNS:
                if column not in loaded.columns:
                    loaded[column] = ""

            loaded["exclude"] = loaded["exclude"].astype(str).str.lower().isin({"true", "1", "yes"})
            
            self.matches = loaded[MATCH_COLUMNS].copy()

            logger.info(
                "Loaded %d saved matches from %s",
                len(self.matches),
                match_path,
            )
        else:
            self.matches = pd.DataFrame(columns=MATCH_COLUMNS)

            logger.info("No saved match file found: %s", match_path)

        if single_path.exists():
            loaded = pd.read_csv(single_path, dtype=str).fillna("")

            for column in SINGLE_COLUMNS:
                if column not in loaded.columns:
                    loaded[column] = ""

            loaded["exclude"] = loaded["exclude"].astype(str).str.lower().isin({"true", "1", "yes"})

            self.singles = loaded[SINGLE_COLUMNS].copy()

            logger.info("Loaded %d saved singles from %s", len(self.singles), single_path)

        else:
            self.singles = self._build_single_table()

            logger.info("Built fresh singles table with %d rows", len(self.singles))

    # Create one bookkeeping row for every Events and RPi mark. This table stores
    # unmatched/excluded state independently of visual artists.
    def _build_single_table(self) -> pd.DataFrame:
        rows: list[dict[str, object]] = []

        pair = self.current_pair()
        ts_col = self.loaded.ts_col

        for _, row in self.loaded.events_marks.iterrows():
            event_time = pd.to_datetime(
                row[ts_col],
                errors="coerce",
            )

            rows.append(
                {
                    "events_file": str(pair.events_file),
                    "rpi_file": str(pair.rpi_file),
                    "label": pair.label,
                    "stream": "events",
                    "mark_id": str(row["mark_id"]),
                    "ordinal": int(row["ordinal"]),
                    "mark_time_orig": (
                        ""
                        if pd.isna(event_time)
                        else event_time.isoformat(sep=" ")
                    ),
                    "RPi_Time_simple": "",
                    "RPi_Time_verb": "",
                    "RPi_Time_unified": "",
                    "block": (
                        ""
                        if pd.isna(row["block"])
                        else row["block"]
                    ),
                    "matched_pair_id": "",
                    "exclude": False,
                    "reason": "",
                    "reviewed_at": "",
                }
            )
        for _, row in self.loaded.rpi_marks.iterrows():
            rpi_times = {}

            for column in (
                "RPi_Time_simple",
                "RPi_Time_verb",
                "RPi_Time_unified",
            ):
                if column in row.index:
                    value = pd.to_datetime(
                        row[column],
                        errors="coerce",
                    )
                else:
                    value = pd.NaT

                rpi_times[column] = (
                    ""
                    if pd.isna(value)
                    else value.isoformat(sep=" ")
                )

            rows.append(
                {
                    "events_file": str(pair.events_file),
                    "rpi_file": str(pair.rpi_file),
                    "label": pair.label,
                    "stream": "rpi",
                    "mark_id": str(row["mark_id"]),
                    "ordinal": int(row["ordinal"]),
                    "mark_time_orig": "",
                    **rpi_times,
                    "block": (
                        ""
                        if pd.isna(row["block"])
                        else row["block"]
                    ),
                    "matched_pair_id": "",
                    "exclude": False,
                    "reason": "",
                    "reviewed_at": "",
                }
            )

        return pd.DataFrame(rows, columns=SINGLE_COLUMNS)

    # Persist the current in-memory GUI state to CSV. Saving data separately from drawing keeps
    # the UI reproducible across sessions.
    def save(self) -> None:
        self.matches.to_csv(self.match_path(), index=False)
        self.singles.to_csv(self.single_path(), index=False)

        logger.info("Saved matches -> %s", self.match_path())
        logger.info("Saved singles -> %s", self.single_path())

    # Select the DataFrame that owns a mark based on whether it came from Events or RPi.
    def mark_dataframe(self, stream: str) -> pd.DataFrame:
        if stream == "events":
            return self.loaded.events_marks

        if stream == "rpi":
            return self.loaded.rpi_marks

        raise ValueError(f"Unknown stream: {stream}")

    # Look up one mark row by its generated mark_id.
    def find_mark(self, stream: str, mark_id: str) -> Optional[pd.Series]:
        df = self.mark_dataframe(stream)

        subset = df[df["mark_id"].astype(str).eq(str(mark_id))]

        if subset.empty:
            return None

        return subset.iloc[0]

    # Combine the two streams into one temporary hit-testing table. `_y` maps Events to y=1 and
    # RPi to y=0; `_xnum` converts datetimes to Matplotlib coordinates.
    def visible_marks(self) -> pd.DataFrame:
        ts_col = self.loaded.ts_col

        events = self.loaded.events_marks.copy()
        events["_y"] = 1.0

        rpi = self.loaded.rpi_marks.copy()
        
        rpi["_y"] = 0.0

        combined = pd.concat([events, rpi], ignore_index=True, sort=False)

        combined["_xnum"] = mdates.date2num(pd.to_datetime(combined[ts_col]))
        

        return combined

    # Convert a mouse click into a logical mark selection. Distances are measured in normalized
    # plot coordinates so hit-testing remains usable at different zoom levels.
    def find_nearest_mark(self, event) -> Optional[tuple[str, str]]:
        logger.debug(
            "find_nearest_mark: inaxes=%s xdata=%r ydata=%r",
            event.inaxes == self.ax,
            event.xdata,
            event.ydata,
        )

        if (event.inaxes != self.ax or event.xdata is None or event.ydata is None):
            logger.debug("find_nearest_mark: event outside usable axes")
            return None

        marks = self.visible_marks()

        if marks.empty:
            logger.warning("find_nearest_mark: no visible marks")
            return None

        xlim = self.ax.get_xlim()

        x_range = max(xlim[1] - xlim[0], 1e-9)

        dx = (marks["_xnum"] - event.xdata).abs() / x_range

        dy = (marks["_y"] - event.ydata).abs() / 1.2

        distance = np.sqrt(dx**2 + dy**2)

        index = distance.idxmin()
        row = marks.loc[index]

        logger.debug(
            (
                "nearest candidate: "
                "stream=%s mark_id=%s "
                "dx=%.6f dy=%.6f "
                "distance=%.6f"
            ),
            row["stream"],
            row["mark_id"],
            float(dx.loc[index]),
            float(dy.loc[index]),
            float(distance.loc[index]),
        )

        if float(dx.loc[index]) > 0.02:
            logger.debug(
                (
                    "nearest candidate rejected: "
                    "horizontal distance "
                    "%.6f > 0.02"
                ),
                float(dx.loc[index]),
            )
            return None

        if float(dy.loc[index]) > 0.25:
            logger.debug(
                (
                    "nearest candidate rejected: "
                    "vertical distance "
                    "%.6f > 0.25"
                ),
                float(dy.loc[index]),
            )
            return None

        result = (str(row["stream"]), str(row["mark_id"]))

        logger.info("MARK HIT: stream=%s mark_id=%s", result[0], result[1])

        return result

    # Check whether a mark already belongs to a saved match and return that pair_id if it does.
    def pair_for_mark(self, stream: str, mark_id: str) -> Optional[str]:
        if self.matches.empty:
            return None

        if stream == "events":
            mask = self.matches["events_mark_id"].astype(str).eq(mark_id)
            
        else:
            mask = self.matches["rpi_mark_id"].astype(str).eq(mark_id)

        subset = self.matches[mask]

        if subset.empty:
            return None

        return str(subset.iloc[-1]["pair_id"])

    # Generate the next unique identifier for a newly-created manual match.
    def next_pair_id(self) -> str:
        maximum = 0

        for value in (self.matches.get("pair_id", pd.Series(dtype=str),).astype(str)):
            if not value.startswith("pair_"):
                continue

            suffix = value[5:]

            if suffix.isdigit():
                maximum = max(maximum, int(suffix))

        return f"pair_{maximum + 1:04d}"

    # Create a logical match between one Events mark and one RPi mark, update bookkeeping tables,
    # save, and refresh the GUI.
    def add_match(self, source: tuple[str, str], target: tuple[str, str]) -> None:
        """Create a one-to-one Events ↔ RPi mark match."""
        logger.info("add_match called: source=%r target=%r", source, target)

        source_stream, source_id = (source)
        target_stream, target_id = (target)

        if (source_stream == target_stream):
            logger.warning("MATCH REJECTED: both marks are from stream=%s", source_stream)

            self.set_status("Matches must connect opposite streams.")
            return

        if source_stream == "events":
            events_id = source_id
            rpi_id = target_id
        else:
            events_id = target_id
            rpi_id = source_id

        existing_events_pair = self.pair_for_mark("events", events_id)
    

        if existing_events_pair:
            logger.warning("MATCH REJECTED: Events mark %s is already matched in %s", events_id, existing_events_pair)

            self.set_status(f"{events_id} is already matched to {existing_events_pair}.")
            return

        existing_rpi_pair = self.pair_for_mark("rpi", rpi_id)
        

        if existing_rpi_pair:
            logger.warning("MATCH REJECTED: RPi mark %s is already matched in %s", rpi_id, existing_rpi_pair)

            self.set_status(f"{rpi_id} is already matched to {existing_rpi_pair}.")
            return

        event_row = self.find_mark("events", events_id)
        rpi_row = self.find_mark("rpi", rpi_id)

        if event_row is None:
            logger.error("MATCH FAILED: Events mark %s could not be found", events_id)

            self.set_status(f"Could not find Events mark {events_id}.")
            return

        if rpi_row is None:
            logger.error("MATCH FAILED: RPi mark %s could not be found", rpi_id)

            self.set_status(f"Could not find RPi mark {rpi_id}.")
            return

        ts_col = self.loaded.ts_col

        event_time = pd.to_datetime(event_row[ts_col], errors="coerce")

        rpi_time_values = {}
        delta_values = {}

        for column in ("RPi_Time_simple", "RPi_Time_verb", "RPi_Time_unified"):
            if column in rpi_row.index:
                value = pd.to_datetime(rpi_row[column], errors="coerce")
            else:
                value = pd.NaT

            rpi_time_values[column] = (
                ""
                if pd.isna(value)
                else value.isoformat(sep=" ")
            )

            delta_values[f"delta_seconds_{column}"] = (
                np.nan
                if pd.isna(value)
                else float((value - event_time).total_seconds())
            )

        event_block = event_row.get("block", np.nan)
        

        rpi_block = rpi_row.get("block", np.nan)

        block = (
            event_block
            if not pd.isna(
                event_block
            )
            else rpi_block
        )

        pair_id = self.next_pair_id()
        
        rpi_time = pd.to_datetime(rpi_row[self.rpi_time_type], errors="coerce")

        if pd.isna(rpi_time):
            raise RuntimeError(
                f"{self.rpi_time_type!r} is missing/unparseable "
                f"for RPi mark {rpi_id}"
            )

        delta_seconds = float((rpi_time - event_time).total_seconds())

        logger.info(
            "MATCH ACCEPTED: pair_id=%s | events=%s @ %s | rpi=%s @ %s | delta=%.6f sec | block=%r",
            pair_id,
            events_id,
            event_time,
            rpi_id,
            rpi_time,
            delta_seconds,
            block,
        )

        match_row = {
            "pair_id": pair_id,
            "events_file": str(self.current_pair().events_file),
            "rpi_file": str(self.current_pair().rpi_file),
            "label": self.current_pair().label,
            "block": (
                ""
                if pd.isna(block)
                else block
            ),
            "events_mark_id": events_id,
            "rpi_mark_id": rpi_id,
            "events_time": event_time.isoformat(sep=" "),
            **rpi_time_values,
            **delta_values,
            "exclude": False,
            "reason": "",
            "reviewed_at": datetime.now().isoformat(timespec="seconds"),
        }

        self.matches = pd.concat([self.matches, pd.DataFrame([match_row])], ignore_index=True,)

        self._set_single_match("events", events_id, pair_id,)

        self._set_single_match("rpi", rpi_id, pair_id,)

        self.selected_entity = ("pair", pair_id,)

        logger.debug(
            "Match table now contains %d rows",
            len(self.matches),
        )

        self.save()

        self.set_status(
            f"Created {pair_id}: "
            f"{events_id} ↔ {rpi_id}, "
            f"Δ={delta_seconds:.3f}s"
        )

    # Update the singles table so a mark knows whether it belongs to a pair.
    def _set_single_match(
        self,
        stream: str,
        mark_id: str,
        pair_id: str,
    ) -> None:
        mask = (
            self.singles["stream"]
            .astype(str)
            .eq(stream)
            & self.singles["mark_id"]
            .astype(str)
            .eq(mark_id)
        )

        self.singles.loc[
            mask,
            "matched_pair_id",
        ] = pair_id

    # Toggle the excluded/kept state of one unpaired mark, timestamp the review action, save it,
    # and redraw.
    def toggle_single_exclusion(
        self,
        stream: str,
        mark_id: str,
    ) -> None:
        mask = (
            self.singles["stream"]
            .astype(str)
            .eq(stream)
            & self.singles["mark_id"]
            .astype(str)
            .eq(mark_id)
        )

        if not mask.any():
            return

        current = bool(
            self.singles.loc[
                mask,
                "exclude",
            ].iloc[0]
        )

        self.singles.loc[
            mask,
            "exclude",
        ] = not current

        self.singles.loc[
            mask,
            "reviewed_at",
        ] = (
            datetime.now()
            .isoformat(
                timespec="seconds"
            )
        )

        self.selected_entity = (
            "single",
            f"{stream}:{mark_id}",
        )

        logger.info(
            (
                "Single exclusion toggled: "
                "stream=%s mark_id=%s "
                "exclude=%s"
            ),
            stream,
            mark_id,
            not current,
        )

        self.save()

    # Hit-test the drawn connection between two matched marks so clicking near a line can select
    # the pair itself.
    def find_nearest_pair(
        self,
        event,
        tolerance_pixels: float = 12.0,
    ) -> Optional[str]:
        """Return the nearest connection using display-coordinate distance."""
        if (
            event.inaxes != self.ax
            or event.x is None
            or event.y is None
            or self.matches.empty
        ):
            return None

        click_point = np.array(
            [float(event.x), float(event.y)],
            dtype=float,
        )

        best_pair_id: Optional[str] = None
        best_distance = float("inf")

        for _, pair in self.matches.iterrows():
            event_time = pd.to_datetime(pair["events_time"], errors="coerce")

            rpi_time = pd.to_datetime(pair[self.rpi_time_type], errors="coerce")

            if pd.isna(event_time) or pd.isna(rpi_time):
                logger.warning(
                    "Skipping pair %s during hit-test: "
                    "missing events_time or %s",
                    pair["pair_id"],
                    self.rpi_time_type,
                )
                continue

            event_time_num = mdates.date2num(event_time)

            rpi_time_num = mdates.date2num(rpi_time)

            event_display = np.array(
                self.ax.transData.transform(
                    (
                        event_time_num,
                        1.0,
                    )
                ),
                dtype=float,
            )

            rpi_display = np.array(
                self.ax.transData.transform(
                    (
                        rpi_time_num,
                        0.0,
                    )
                ),
                dtype=float,
            )

            segment = (
                rpi_display
                - event_display
            )

            segment_length_squared = float(
                np.dot(
                    segment,
                    segment,
                )
            )

            if segment_length_squared == 0:
                distance = float(
                    np.linalg.norm(
                        click_point
                        - event_display
                    )
                )
            else:
                fraction = float(
                    np.dot(
                        click_point
                        - event_display,
                        segment,
                    )
                    / segment_length_squared
                )

                fraction = float(
                    np.clip(
                        fraction,
                        0.0,
                        1.0,
                    )
                )

                closest_point = (
                    event_display
                    + fraction * segment
                )

                distance = float(
                    np.linalg.norm(
                        click_point
                        - closest_point
                    )
                )

            logger.debug(
                "PAIR HIT TEST: pair=%s distance=%.2f px",
                pair["pair_id"],
                distance,
            )

            if distance < best_distance:
                best_distance = distance
                best_pair_id = str(
                    pair["pair_id"]
                )

        logger.info(
            "PAIR HIT RESULT: pair=%r distance=%.2f px tolerance=%.2f px",
            best_pair_id,
            best_distance,
            tolerance_pixels,
        )

        if best_distance > tolerance_pixels:
            return None

        return best_pair_id

    # Toggle whether an existing match should be excluded, then synchronize the stored review
    # state and redraw.
    def toggle_pair_exclusion(
        self,
        pair_id: str,
    ) -> None:
        mask = (
            self.matches["pair_id"]
            .astype(str)
            .eq(pair_id)
        )

        if not mask.any():
            return

        current = bool(
            self.matches.loc[
                mask,
                "exclude",
            ].iloc[0]
        )

        self.matches.loc[
            mask,
            "exclude",
        ] = not current

        self.matches.loc[
            mask,
            "reviewed_at",
        ] = (
            datetime.now()
            .isoformat(
                timespec="seconds"
            )
        )

        self.selected_entity = (
            "pair",
            pair_id,
        )

        logger.info(
            (
                "Pair exclusion toggled: "
                "pair_id=%s exclude=%s"
            ),
            pair_id,
            not current,
        )

        self.save()

    # Render the entire current application state onto the Matplotlib Axes. GUI code commonly
    # redraws from state instead of manually editing many artists one-by-one.
    def draw(
        self,
        reset_view: bool = True,
    ) -> None:
        """Redraw the matcher while optionally preserving the current zoom."""
        previous_xlim = None
        previous_ylim = None

        if not reset_view:
            previous_xlim = self.ax.get_xlim()
            previous_ylim = self.ax.get_ylim()

        logger.debug(
            (
                "DRAW START: "
                "pair_index=%d | "
                "events=%d | "
                "rpi=%d | "
                "matches=%d | "
                "singles=%d | "
                "mode=%s"
            ),
            self.current_index,
            len(
                self.loaded.events_marks
            ),
            len(
                self.loaded.rpi_marks
            ),
            len(self.matches),
            len(self.singles),
            self.mode,
        )

        self.ax.clear()

        ts_col = self.loaded.ts_col

        events = (
            self.loaded.events_marks
        )

        rpi = (
            self.loaded.rpi_marks
        )

        if not events.empty:
            event_times = (
                pd.to_datetime(
                    events[ts_col]
                )
            )

            self.ax.vlines(
                event_times,
                0.52,
                1.0,
                linewidth=1.4,
                alpha=0.8,
            )

            self.ax.scatter(
                event_times,
                np.ones(
                    len(events)
                ),
                s=28,
                zorder=4,
            )

            logger.debug(
                (
                    "DRAW: rendered %d "
                    "Events marks"
                ),
                len(events),
            )
        else:
            logger.warning(
                (
                    "DRAW: no Events "
                    "marks to render"
                )
            )

        if not rpi.empty:
            rpi_times = (
                pd.to_datetime(
                    rpi[ts_col]
                )
            )

            self.ax.vlines(
                rpi_times,
                0.0,
                0.48,
                linewidth=1.4,
                alpha=0.8,
            )

            self.ax.scatter(
                rpi_times,
                np.zeros(
                    len(rpi)
                ),
                s=28,
                zorder=4,
            )

            logger.debug(
                (
                    "DRAW: rendered %d "
                    "RPi marks"
                ),
                len(rpi),
            )
        else:
            logger.warning(
                (
                    "DRAW: no RPi "
                    "marks to render"
                )
            )

        for _, pair in (
            self.matches.iterrows()
        ):
            pair_id = str(
                pair["pair_id"]
            )

            events_mark_id = str(
                pair[
                    "events_mark_id"
                ]
            )

            rpi_mark_id = str(
                pair[
                    "rpi_mark_id"
                ]
            )

            excluded = bool(
                pair["exclude"]
            )

            event_time = (
                pd.to_datetime(
                    pair[
                        "events_time"
                    ]
                )
            )

            rpi_time = pd.to_datetime(pair[self.rpi_time_type], errors="coerce")

            if pd.isna(rpi_time):
                logger.warning(
                    "Skipping pair %s: %s is missing/unparseable",
                    pair_id,
                    self.rpi_time_type,
                )
                continue

            logger.debug(
                (
                    "DRAW CONNECTION: %s | "
                    "%s -> %s | "
                    "events_time=%s | "
                    "rpi_time=%s | "
                    "excluded=%s"
                ),
                pair_id,
                events_mark_id,
                rpi_mark_id,
                event_time,
                rpi_time,
                excluded,
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
                linewidth=(
                    1.0
                    if excluded
                    else 2.0
                ),
                linestyle=(
                    ":"
                    if excluded
                    else "-"
                ),
                alpha=(
                    0.30
                    if excluded
                    else 0.85
                ),
                zorder=2,
            )

            midpoint_time = (
                event_time
                + (
                    rpi_time
                    - event_time
                )
                / 2
            )

            self.ax.text(
                midpoint_time,
                0.5,
                pair_id,
                fontsize=7,
                rotation=90,
                ha="center",
                va="center",
                alpha=0.65,
                zorder=3,
            )

        excluded_single_count = 0

        for _, row in (
            self.singles.iterrows()
        ):
            if not bool(
                row.get(
                    "exclude",
                    False,
                )
            ):
                continue

            excluded_single_count += 1

            stream = str(
                row["stream"]
            )

            mark_time = (
                pd.to_datetime(
                    row[
                        "mark_time"
                    ]
                )
            )

            y = (
                1.0
                if stream == "events"
                else 0.0
            )

            self.ax.scatter(
                [mark_time],
                [y],
                marker="x",
                s=90,
                linewidths=2.0,
                zorder=7,
            )

        logger.debug(
            (
                "DRAW: rendered %d "
                "excluded singles"
            ),
            excluded_single_count,
        )

        for _, block in (
            self.loaded.blocks.iterrows()
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

            self.ax.axvline(
                block_start,
                linestyle="--",
                alpha=0.25,
                linewidth=0.9,
            )

            self.ax.axvline(
                block_end,
                linestyle="--",
                alpha=0.25,
                linewidth=0.9,
            )

        self.ax.set_yticks(
            [0, 1]
        )

        self.ax.set_yticklabels(
            [
                "RPi",
                "Events",
            ]
        )

        self.ax.set_ylim(
            -0.15,
            1.15,
        )

        self.ax.set_xlabel(
            "Absolute time"
        )

        active_matches = (
            0
            if self.matches.empty
            else int(
                (
                    ~self.matches[
                        "exclude"
                    ].astype(bool)
                ).sum()
            )
        )

        excluded_matches = (
            0
            if self.matches.empty
            else int(
                self.matches[
                    "exclude"
                ]
                .astype(bool)
                .sum()
            )
        )

        self.ax.set_title(
            f"Mark Matcher — "
            f"{self.current_pair().label} — "
            f"mode={self.mode.upper()} | "
            f"block={self.current_block}\n"
            f"{self.current_pair().events_file.name} | "
            f"{self.current_pair().rpi_file.name}\n"
            f"matches={len(self.matches)} "
            f"active={active_matches} "
            f"excluded={excluded_matches}"
        )

        self.ax.xaxis.set_major_formatter(
            mdates.DateFormatter(
                "%H:%M:%S"
            )
        )

        self.ax.grid(
            axis="x",
            alpha=0.12,
        )

        view_start, view_end = (
            self.current_view_bounds()
        )

        self.default_xlim = (
            mdates.date2num(view_start),
            mdates.date2num(view_end),
        )
        self.default_ylim = (
            -0.15,
            1.15,
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

        self.fig.tight_layout(
            rect=(
                0,
                0.05,
                1,
                1,
            )
        )

        self.fig.canvas.draw_idle()

        logger.debug(
            (
                "DRAW COMPLETE: "
                "connections=%d | "
                "excluded_singles=%d"
            ),
            len(self.matches),
            excluded_single_count,
        )

    # Update transient status text shown inside the figure, then request a canvas refresh.
    def set_status(
        self,
        message: str,
    ) -> None:
        logger.info(
            "STATUS: %s",
            message,
        )

    # Mouse-button press callback. It decides whether the user clicked a mark/pair and records
    # drag/selection state for the later release event.
    def on_press(
        self,
        event,
    ) -> None:
        logger.info(
            (
                "MOUSE PRESS: "
                "button=%r "
                "xdata=%r "
                "ydata=%r "
                "pixel=(%r,%r) "
                "inaxes=%s "
                "mode=%s"
            ),
            event.button,
            event.xdata,
            event.ydata,
            event.x,
            event.y,
            event.inaxes == self.ax,
            self.mode,
        )

        if (
            event.button != 1
            or event.inaxes != self.ax
        ):
            logger.debug(
                "Mouse press ignored"
            )
            return

        if self.toolbar_navigation_active():
            logger.debug(
                "Mouse press left to Matplotlib toolbar pan/zoom."
            )
            self.drag_source = None
            return

        if self.mode == "match":
            self.drag_source = (
                self.find_nearest_mark(
                    event
                )
            )

            logger.info(
                "DRAG SOURCE: %r",
                self.drag_source,
            )

            if self.drag_source is None:
                logger.warning(
                    "Match-mode press did not hit a mark."
                )

            return

        if self.mode == "unmatch":
            pair_id = (
                self.find_nearest_pair(
                    event
                )
            )

            if pair_id is None:
                logger.warning(
                    "UNMATCH CLICK: no connection found"
                )

                self.set_status(
                    "No connection found. Click closer to a match line."
                )
                return

            logger.info(
                "UNMATCH CLICK: selected %s",
                pair_id,
            )

            self.unmatch_pair(
                pair_id
            )
            return

        if self.mode == "exclude":
            mark = (
                self.find_nearest_mark(
                    event
                )
            )

            if mark is not None:
                stream, mark_id = mark

                logger.info(
                    "EXCLUDE CLICK: mark %s %s",
                    stream,
                    mark_id,
                )

                self.toggle_single_exclusion(
                    stream,
                    mark_id,
                )

                self.draw(
                    reset_view=False
                )
                return

            pair_id = (
                self.find_nearest_pair(
                    event
                )
            )

            if pair_id is not None:
                logger.info(
                    "EXCLUDE CLICK: pair %s",
                    pair_id,
                )

                self.toggle_pair_exclusion(
                    pair_id
                )

                self.draw(
                    reset_view=False
                )
                return

            logger.info(
                "EXCLUDE CLICK: no mark or pair found"
            )

    # Mouse-button release callback. Together with `on_press`, this implements drag-to-match
    # behavior between the two streams.
    def on_release(
        self,
        event,
    ) -> None:
        logger.info(
            (
                "MOUSE RELEASE: "
                "button=%r "
                "xdata=%r "
                "ydata=%r "
                "inaxes=%s "
                "mode=%s "
                "source=%r"
            ),
            event.button,
            event.xdata,
            event.ydata,
            event.inaxes == self.ax,
            self.mode,
            self.drag_source,
        )

        if self.toolbar_navigation_active():
            logger.debug(
                "Mouse release left to Matplotlib toolbar pan/zoom."
            )
            self.drag_source = None
            return

        if self.mode != "match":
            self.drag_source = None
            return

        if self.drag_source is None:
            logger.warning(
                "Release occurred but there is no drag_source."
            )
            return

        target = (
            self.find_nearest_mark(
                event
            )
        )

        source = self.drag_source
        self.drag_source = None

        logger.info(
            "MATCH ATTEMPT: source=%r target=%r",
            source,
            target,
        )

        if target is None:
            logger.warning(
                "Match rejected: release did not hit a mark."
            )
            return

        if source == target:
            logger.warning(
                "Match rejected: source and target are identical."
            )
            return

        self.add_match(
            source,
            target,
        )

        self.draw(
            reset_view=False
        )

    # Mouse-wheel callback. Matplotlib passes an event object containing the cursor position and
    # scroll direction.
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

        current_xlim = self.ax.get_xlim()

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

    # Save the current file's work, load another Events/RPi pair, reset pair-specific selection
    # state, and redraw.
    def goto_pair(
        self,
        index: int,
    ) -> None:
        self.save()

        self.current_index = max(
            0,
            min(
                index,
                len(self.pairs) - 1,
            ),
        )

        self.loaded = load_pair(self.current_pair(), self.rpi_time_type)

        self.current_block = "All"

        self._load_saved_state()

        self.drag_source = None
        self.selected_entity = None

        logger.info(
            "Moved to pair %d/%d",
            self.current_index + 1,
            len(self.pairs),
        )

        self.draw(
            reset_view=True
        )

    # Keyboard callback. This is effectively the GUI's shortcut dispatcher: inspect event.key,
    # change state, then redraw/save as needed.
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

        if key == "m":
            self.mode = "match"
            self.drag_source = None

            logger.info(
                "MODE: MATCH"
            )

            self.set_status(
                "MATCH mode: drag between Events and RPi marks."
            )

            self.draw(
                reset_view=False
            )
            return

        if key == "e":
            self.mode = "exclude"
            self.drag_source = None

            logger.info(
                "MODE: EXCLUDE"
            )

            self.set_status(
                "EXCLUDE mode: click a mark or connection."
            )

            self.draw(
                reset_view=False
            )
            return

        if key == "u":
            self.mode = "unmatch"
            self.drag_source = None

            logger.info(
                "MODE: UNMATCH"
            )

            self.set_status(
                "UNMATCH mode: click a connection to remove it."
            )

            self.draw(
                reset_view=False
            )
            return

        if key == "up":
            self.next_block(1)
            return

        if key == "down":
            self.next_block(-1)
            return

        if key == "r":
            self.reset_view()
            return

        if key == "a":
            self.reset_view()
            return

        if key == "s":
            self.save()
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

        if key == "q":
            self.save()
            plt.close(
                self.fig
            )
            return

    # Remove an existing logical match and return both marks to the singles pool.
    def unmatch_pair(
        self,
        pair_id: str,
    ) -> None:
        """Remove a match and make both marks available again."""
        mask = (
            self.matches["pair_id"]
            .astype(str)
            .eq(pair_id)
        )

        if not mask.any():
            logger.warning(
                "UNMATCH FAILED: pair %s does not exist",
                pair_id,
            )
            return

        pair = self.matches.loc[
            mask
        ].iloc[0]

        events_mark_id = str(
            pair["events_mark_id"]
        )

        rpi_mark_id = str(
            pair["rpi_mark_id"]
        )

        logger.info(
            "UNMATCHING: pair=%s events=%s rpi=%s",
            pair_id,
            events_mark_id,
            rpi_mark_id,
        )

        self.matches = (
            self.matches.loc[
                ~mask
            ]
            .reset_index(
                drop=True
            )
        )

        for stream, mark_id in (
            (
                "events",
                events_mark_id,
            ),
            (
                "rpi",
                rpi_mark_id,
            ),
        ):
            single_mask = (
                self.singles["stream"]
                .astype(str)
                .eq(stream)
                & self.singles["mark_id"]
                .astype(str)
                .eq(mark_id)
            )

            self.singles.loc[
                single_mask,
                "matched_pair_id",
            ] = ""

            self.singles.loc[
                single_mask,
                "reviewed_at",
            ] = (
                datetime.now()
                .isoformat(
                    timespec="seconds"
                )
            )

        self.selected_entity = None

        self.save()

        self.set_status(
            f"Removed {pair_id}: "
            f"{events_mark_id} ↔ {rpi_mark_id}"
        )

        logger.info(
            "UNMATCH COMPLETE: %s removed",
            pair_id,
        )

        self.draw(
            reset_view=False
        )


# Define and parse the command-line options used to launch this GUI.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Interactive Events ↔ RPi mark matcher.")

    parser.add_argument("--manifest", type=Path, help="CSV containing events_file,rpi_file,label")
    parser.add_argument("--events", type=Path, help="Single Events CSV.")
    parser.add_argument("--rpi", type=Path, help="Single RPi CSV.")
    parser.add_argument("--label", default="", help="BioPac or RNS for single-pair mode.")
    parser.add_argument("--events-dir", type=Path, help="Events CSV directory.")
    parser.add_argument("--rpi-dir", type=Path, help="RPi CSV directory.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for match/exclusion CSV files.")
    parser.add_argument("--rpi-time-type", required=True, type=str, help="RPi marks column name")

    parser.add_argument("--start-index", type=int, default=0)

    return parser.parse_args()


# Choose whether file pairs come from a manifest or automatic directory matching.
def build_pairs(args: argparse.Namespace) -> list[FilePair]:
    if args.manifest:
        return load_manifest(args.manifest)

    if (args.events_dir and args.rpi_dir):
        return (build_pairs_from_directories(args.events_dir, args.rpi_dir))
        
    if args.events and args.rpi and args.label.strip():
        return [FilePair(events_file=args.events.expanduser(), rpi_file=args.rpi.expanduser(), label=args.label.strip())]

    raise SystemExit("Provide --manifest, or --events-dir with --rpi-dir, or --events --rpi --label.")



# Program entry point: parse command-line arguments, discover file pairs, construct the GUI
# object, and hand control to Matplotlib's event loop.
def main() -> None:
    args = parse_args()
    pairs = build_pairs(args)

    if not pairs:
        raise SystemExit("No Events/RPi pairs found.")

    for pair in pairs:
        if not pair.events_file.exists():
            raise FileNotFoundError(pair.events_file)

        if not pair.rpi_file.exists():
            raise FileNotFoundError(pair.rpi_file)

    app = MarkMatchApp(pairs=pairs, output_dir=args.output_dir.expanduser(), start_index=args.start_index, rpi_time_type=args.rpi_time_type,)
    logger.info("MarkMatchApp instance retained: %r", app)
    plt.show()


if __name__ == "__main__":
    main()
