# mark_review_app.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

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

REVIEW_COLUMNS = [
    "events_file",
    "rpi_file",
    "label",
    "block",
    "stream",
    "mark_id",
    "ordinal",
    "mark_time",
    "exclude",
    "reason",
    "reviewed_at",
]

REASON_MAP = {
    "1": "false_mark",
    "2": "duplicate",
    "3": "bad_time",
    "4": "wrong_block",
    "5": "unclear",
}

EVENT_SUFFIXES = [
    "_eventsFlat",
    "_events_final",
    "_events",
    "_processed",
]

RPI_SUFFIXES = {
    "BioPac": "_BioPac_RPi_unified",
    "RNS": "_RNS_RPi_unified",
}


@dataclass
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
    current_block: str | int = "All"


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


# Build the filename used to store mark-level review decisions for one Events/RPi pair.
def review_filename_for_pair(pair: FilePair) -> str:
    base = strip_suffixes(pair.events_file.stem, EVENT_SUFFIXES)
    return f"{base}_{pair.label}_mark_review_decisions.csv"


# Join the review directory with this pair's generated review filename.
def review_path_for_pair(review_dir: Path, pair: FilePair) -> Path:
    return review_dir / review_filename_for_pair(pair)


# Use the presence/content of a saved review CSV to decide whether a pair has already been
# reviewed.
def pair_is_reviewed(review_dir: Path, pair: FilePair) -> bool:
    review_path = review_path_for_pair(review_dir, pair)
    if not review_path.exists():
        return False
    try:
        df = pd.read_csv(review_path)
        return not df.empty
    except Exception:
        return True


# Look up important Events CSV columns by name, while accepting a few alternate spellings.
def detect_events_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    cols = {c.lower(): c for c in df.columns}
    ts_col = cols.get("mlt_orig")
    etype = cols.get("lo_eventtype") or cols.get("eventtype") or cols.get("event_type") or cols.get("type")
    block = cols.get("blocknum") or cols.get("block") or cols.get("blockid") or cols.get("block_id") or cols.get("lo_block")
    rnd = cols.get("roundnum") or cols.get("round") or cols.get("roundid") or cols.get("round_id") or cols.get("lo_round")
    return {"ts": ts_col, "etype": etype, "block": block, "round": rnd}


# Choose a likely timestamp column from the RPi file, preferring the project's known timestamp
# names.
def detect_rpi_time_column(df: pd.DataFrame) -> Optional[str]:
    for name in ("RPi_Time_unified", "ML_Time_verb", "RPi_Time_simple", "Mono_Time_verb", "Mono_Time_Raw_verb"):
        if name in df.columns:
            return name
    for c in df.columns:
        if df[c].dtype == object and any(k in c.lower() for k in ("time", "timestamp", "date")):
            return c
    return None


# Build a table of block start/end times. Later GUI views use these boundaries for block
# navigation and plotting.
def build_blocks(events: pd.DataFrame, ts_col: str, etype_col: str, block_col: Optional[str]) -> pd.DataFrame:
    e = events.sort_values(ts_col).copy()
    et = e[etype_col].astype(str).str.lower()

    starts = e[et.eq("blockstart")]
    ends = e[et.eq("blockend")]

    if not starts.empty and not ends.empty and block_col and block_col in e.columns:
        blocks = starts[[ts_col, block_col]].merge(
            ends[[ts_col, block_col]],
            on=block_col,
            how="left",
            suffixes=("_start", "_end"),
        ).rename(
            columns={
                block_col: "block",
                f"{ts_col}_start": "start",
                f"{ts_col}_end": "end",
            }
        )
    elif block_col and block_col in e.columns:
        grp = e.groupby(block_col)[ts_col]
        blocks = pd.DataFrame({"block": grp.apply(lambda s: s.name).index})
        blocks["start"] = grp.min().values
        blocks["end"] = grp.max().values
    else:
        blocks = pd.DataFrame({"block": [1], "start": [e[ts_col].min()], "end": [e[ts_col].max()]})

    blocks = blocks.sort_values("start").reset_index(drop=True)
    next_starts = blocks["start"].shift(-1)
    blocks["end"] = np.where(blocks["end"].isna(), next_starts, blocks["end"])
    blocks["end"] = blocks["end"].fillna(e[ts_col].max())
    return blocks.sort_values("block").reset_index(drop=True)


# Assign each timestamp to the block interval that contains it. This is vectorized with NumPy
# rather than looping row-by-row.
def assign_block(t: pd.Series, blocks: pd.DataFrame) -> pd.Series:
    b = blocks.sort_values("start")
    starts = pd.to_datetime(b["start"]).values.astype("datetime64[ns]")
    ends = pd.to_datetime(b["end"]).values.astype("datetime64[ns]")
    ids = b["block"].values

    tv = pd.to_datetime(t).values.astype("datetime64[ns]")
    idxs = np.searchsorted(starts, tv, side="right") - 1
    idxs = np.clip(idxs, 0, len(b) - 1)
    in_range = (tv >= starts[idxs]) & (tv <= ends[idxs])
    out = np.where(in_range, ids[idxs], np.nan)
    return pd.Series(out, index=t.index, dtype="float")


# Read an explicit CSV manifest and convert each row into a FilePair object.
def load_manifest(path: Path) -> list[FilePair]:
    df = pd.read_csv(path)
    required = ["events_file", "rpi_file", "label"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Manifest missing columns: {missing}")

    pairs: list[FilePair] = []
    for _, row in df.iterrows():
        pairs.append(
            FilePair(
                events_file=Path(str(row["events_file"])).expanduser(),
                rpi_file=Path(str(row["rpi_file"])).expanduser(),
                label=str(row["label"]).strip(),
            )
        )
    return pairs


# Automatically discover matching Events and RPi CSV files from two directories using their
# filename conventions.
def build_pairs_from_directories(events_dir: Path, rpi_dir: Path) -> list[FilePair]:
    events_dir = events_dir.expanduser()
    rpi_dir = rpi_dir.expanduser()

    if not events_dir.is_dir():
        raise NotADirectoryError(f"events_dir is not a directory: {events_dir}")
    if not rpi_dir.is_dir():
        raise NotADirectoryError(f"rpi_dir is not a directory: {rpi_dir}")

    event_files = sorted(p for p in events_dir.glob("*.csv") if p.is_file())
    rpi_files = sorted(p for p in rpi_dir.glob("*.csv") if p.is_file())

    event_map: dict[str, Path] = {}
    for path in event_files:
        base = strip_suffixes(path.stem, EVENT_SUFFIXES)
        event_map[base] = path

    rpi_map: dict[tuple[str, str], Path] = {}
    for path in rpi_files:
        stem = path.stem
        for label, suffix in RPI_SUFFIXES.items():
            if stem.endswith(suffix):
                base = stem[: -len(suffix)].rstrip("_-")
                rpi_map[(base, label)] = path
                break

    pairs: list[FilePair] = []
    for base, events_file in sorted(event_map.items()):
        for label in ("BioPac", "RNS"):
            rpi_file = rpi_map.get((base, label))
            if rpi_file is not None:
                pairs.append(FilePair(events_file=events_file, rpi_file=rpi_file, label=label))

    if not pairs:
        raise RuntimeError(
            "No matching event/RPi pairs found. "
            "Expected event stems to match RPi stems after removing "
            "_eventsFlat/_processed and _BioPac_RPi_unified/_RNS_RPi_unified."
        )

    return pairs


# Load a mark-review CSV if it exists; otherwise return an empty table with the expected schema.
def load_review_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=REVIEW_COLUMNS)
    df = pd.read_csv(path, dtype="string")
    for col in REVIEW_COLUMNS:
        if col not in df.columns:
            df[col] = pd.Series(dtype="string")
    return df[REVIEW_COLUMNS].copy()


# Restore boolean types after CSV loading, because CSV files themselves do not preserve Python
# types.
def ensure_review_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "exclude" in out.columns:
        out["exclude"] = out["exclude"].astype(str).str.lower().isin({"true", "1", "yes"})
    return out


# Add GUI state columns (block/id/stream/exclude/reason/time) to the raw marks DataFrame.
def prepare_marks_df(df: pd.DataFrame, stream: str, ts_col: str, blocks: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().reset_index(drop=True)
    out["block"] = assign_block(out[ts_col], blocks)
    out["ordinal"] = np.arange(len(out), dtype=int)
    out["mark_id"] = [f"{stream}_{i:03d}" for i in out["ordinal"]]
    out["stream"] = stream
    out["exclude"] = False
    out["reason"] = ""
    out["reviewed_at"] = ""
    return out


# Merge saved decisions back onto freshly-loaded marks by mark_id so the GUI reproduces prior
# state.
def apply_reviews_to_marks(
    marks_df: pd.DataFrame,
    review_df: pd.DataFrame,
    pair: FilePair,
    stream: str,
) -> pd.DataFrame:
    if review_df.empty:
        return marks_df

    mask = (
        (review_df["events_file"] == str(pair.events_file))
        & (review_df["rpi_file"] == str(pair.rpi_file))
        & (review_df["label"] == pair.label)
        & (review_df["stream"] == stream)
    )
    sub = review_df.loc[mask].copy()
    if sub.empty:
        return marks_df

    sub = sub.drop_duplicates(subset=["mark_id"], keep="last")
    merged = marks_df.merge(
        sub[["mark_id", "exclude", "reason", "reviewed_at"]],
        on="mark_id",
        how="left",
        suffixes=("", "_review"),
    )

    for col in ("exclude", "reason", "reviewed_at"):
        review_col = f"{col}_review"
        if review_col in merged.columns:
            merged[col] = merged[review_col].combine_first(merged[col])
            merged.drop(columns=[review_col], inplace=True)

    merged["exclude"] = merged["exclude"].fillna(False).astype(bool)
    merged["reason"] = merged["reason"].fillna("")
    merged["reviewed_at"] = merged["reviewed_at"].fillna("")
    return merged


# Load and prepare one Events/RPi pair plus any saved mark-review decisions.
def load_pair(pair: FilePair, review_dir: Path) -> LoadedPair:
    review_path = review_path_for_pair(review_dir, pair)
    review_df = ensure_review_types(load_review_csv(review_path))

    events = pd.read_csv(pair.events_file)
    ev = detect_events_columns(events)
    ts_col = ev["ts"]
    et_col = ev["etype"]
    blk_col = ev["block"]

    if not ts_col or not et_col:
        raise RuntimeError(
            f"Events file must contain mLT_orig and an event type column: {pair.events_file}"
        )

    events[ts_col] = pd.to_datetime(events[ts_col], errors="coerce")
    events = events.dropna(subset=[ts_col]).copy()

    keep_types = {"blockstart", "blockend", "roundstart", "roundend", "mark"}
    events = events[events[et_col].astype(str).str.lower().isin(keep_types)].copy()
    events = events.sort_values(ts_col)

    blocks = build_blocks(events, ts_col, et_col, blk_col)
    if blocks.empty:
        raise RuntimeError(f"No blocks could be constructed: {pair.events_file}")

    events_marks = events[events[et_col].astype(str).str.lower() == "mark"].copy()
    events_marks = prepare_marks_df(events_marks, "events", ts_col, blocks)
    events_marks = apply_reviews_to_marks(events_marks, review_df, pair, "events")

    rpi = pd.read_csv(pair.rpi_file)
    rpi_time_col = detect_rpi_time_column(rpi)
    if not rpi_time_col:
        raise RuntimeError(f"Could not detect RPi datetime column: {pair.rpi_file}")

    rpi[ts_col] = pd.to_datetime(rpi[rpi_time_col], errors="coerce")
    rpi = rpi.dropna(subset=[ts_col]).sort_values(ts_col).copy()
    rpi_marks = prepare_marks_df(rpi, "rpi", ts_col, blocks)
    rpi_marks = apply_reviews_to_marks(rpi_marks, review_df, pair, "rpi")

    return LoadedPair(
        pair=pair,
        events_marks=events_marks,
        rpi_marks=rpi_marks,
        blocks=blocks,
        ts_col=ts_col,
        current_block="All",
    )


class MarkReviewApp:
    # Create the Matplotlib window and Axes, initialize application state, and register
    # mouse/keyboard callbacks with `mpl_connect()`.
    def __init__(self, pairs: list[FilePair], review_dir: Path, start_index: int = 0) -> None:
        self.pairs = pairs
        self.review_dir = review_dir
        self.review_dir.mkdir(parents=True, exist_ok=True)
        self.current_index = max(0, min(start_index, len(pairs) - 1))
        self.loaded = load_pair(self.pairs[self.current_index], self.review_dir)
        self.selected: Optional[tuple[str, int]] = None

        # Figure = the whole GUI window; Axes = the plotting area inside that window.
        self.fig, self.ax = plt.subplots(figsize=(15, 6))
        # mpl_connect registers a callback with Matplotlib's event loop.
        # When the user clicks, Matplotlib creates a MouseEvent and passes it to on_click/on_press.
        self.fig.canvas.mpl_connect("button_press_event", self.on_click)
        self.fig.canvas.mpl_connect("key_press_event", self.on_key)

        self.default_xlim = None
        self.default_ylim = None
        self._help_text_artist = None

        self.draw()

    # Return the FilePair currently displayed by the GUI.
    def current_pair(self) -> FilePair:
        return self.pairs[self.current_index]

    # Return the output CSV for the file pair currently shown.
    def current_review_path(self) -> Path:
        return review_path_for_pair(self.review_dir, self.current_pair())

    # Serialize only reviewed/excluded marks from the current in-memory DataFrames to CSV.
    def save_reviews(self) -> None:
        rows: list[dict[str, object]] = []
        pair = self.current_pair()

        for df in (self.loaded.events_marks, self.loaded.rpi_marks):
            for _, row in df.iterrows():
                if not bool(row.get("exclude", False)) and not str(row.get("reason", "")).strip():
                    continue
                rows.append(
                    {
                        "events_file": str(pair.events_file),
                        "rpi_file": str(pair.rpi_file),
                        "label": pair.label,
                        "block": "" if pd.isna(row.get("block")) else int(float(row["block"])),
                        "stream": row["stream"],
                        "mark_id": row["mark_id"],
                        "ordinal": int(row["ordinal"]),
                        "mark_time": pd.to_datetime(row[self.loaded.ts_col]).isoformat(sep=" "),
                        "exclude": bool(row["exclude"]),
                        "reason": str(row.get("reason", "") or ""),
                        "reviewed_at": str(row.get("reviewed_at", "") or ""),
                    }
                )

        review_path = self.current_review_path()
        out_df = pd.DataFrame(rows, columns=REVIEW_COLUMNS)
        review_path.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(review_path, index=False)
        print(f"[ok] saved review decisions -> {review_path}")

    # Save before leaving the current pair, load the requested pair, clear selection, and redraw.
    def goto_pair(self, idx: int) -> None:
        self.save_reviews()
        self.current_index = max(0, min(idx, len(self.pairs) - 1))
        self.loaded = load_pair(self.pairs[self.current_index], self.review_dir)
        self.selected = None
        self.draw()

    # Turn the logical current-block selection into concrete datetime bounds for plotting.
    def current_block_bounds(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        blocks = self.loaded.blocks
        if str(self.loaded.current_block).lower() == "all":
            return pd.to_datetime(blocks["start"].min()), pd.to_datetime(blocks["end"].max())

        row = blocks.loc[blocks["block"] == int(self.loaded.current_block)]
        if row.empty:
            return pd.to_datetime(blocks["start"].min()), pd.to_datetime(blocks["end"].max())
        r0 = row.iloc[0]
        return pd.to_datetime(r0["start"]), pd.to_datetime(r0["end"])

    # Create a temporary combined table of currently visible marks with plotting coordinates used
    # for mouse hit-testing.
    def visible_marks(self) -> pd.DataFrame:
        if str(self.loaded.current_block).lower() == "all":
            ev = self.loaded.events_marks.copy()
            rp = self.loaded.rpi_marks.copy()
        else:
            b = int(self.loaded.current_block)
            ev = self.loaded.events_marks[self.loaded.events_marks["block"] == b].copy()
            rp = self.loaded.rpi_marks[self.loaded.rpi_marks["block"] == b].copy()

        ev["y"] = 1.0
        rp["y"] = 0.0
        combined = pd.concat([ev, rp], ignore_index=True, sort=False)
        combined["_xnum"] = mdates.date2num(pd.to_datetime(combined[self.loaded.ts_col]))
        return combined

    # The central renderer: clear the Axes and draw the GUI entirely from current application
    # state, then ask Matplotlib to repaint the canvas.
    def draw(self) -> None:
        self.ax.clear()
        pair = self.current_pair()
        ts_col = self.loaded.ts_col
        t0, t1 = self.current_block_bounds()

        if str(self.loaded.current_block).lower() == "all":
            ev = self.loaded.events_marks.copy()
            rp = self.loaded.rpi_marks.copy()
            title_block = "All"
        else:
            b = int(self.loaded.current_block)
            ev = self.loaded.events_marks[self.loaded.events_marks["block"] == b].copy()
            rp = self.loaded.rpi_marks[self.loaded.rpi_marks["block"] == b].copy()
            title_block = str(b)

        self.ax.axvspan(t0, t1, alpha=0.05)

        self._draw_stream(ev, ts_col, y=1.0, base_color="red")
        self._draw_stream(rp, ts_col, y=0.0, base_color="blue")

        for _, row in self.loaded.blocks.iterrows():
            start = pd.to_datetime(row["start"])
            end = pd.to_datetime(row["end"])
            if end < t0 or start > t1:
                continue
            self.ax.axvline(start, linestyle="--", alpha=0.45, linewidth=0.9)
            self.ax.axvline(end, linestyle="--", alpha=0.45, linewidth=0.9)

        self._draw_selection()

        excluded_count = int(self.loaded.events_marks["exclude"].sum() + self.loaded.rpi_marks["exclude"].sum())

        self.ax.set_yticks([0, 1])
        self.ax.set_yticklabels(["RPi Mark (blue)", "Events Mark (red)"])
        self.ax.set_xlabel(f"Time ({ts_col})")
        self.ax.set_title(
            f"Mark Review — {pair.label} — Block {title_block}\n"
            f"[{self.current_index + 1}/{len(self.pairs)}] "
            f"{pair.events_file.name} | {pair.rpi_file.name}\n"
            f"excluded={excluded_count} | review_file={self.current_review_path().name}"
        )

        if self._help_text_artist is not None:
            try:
                self._help_text_artist.remove()
            except Exception:
                pass

        help_text = (
            "click=toggle exclude | left/right=file | up/down=block | a=all blocks | "
            "1..5=set reason | 0=clear reason | s=save | r=reset zoom | q=quit\n"
            "reasons: 1=false_mark 2=duplicate 3=bad_time 4=wrong_block 5=unclear"
        )
        self._help_text_artist = self.fig.text(0.01, 0.01, help_text, fontsize=9)

        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        self.fig.autofmt_xdate(rotation=0)
        self.ax.set_ylim(-0.2, 1.2)

        self.default_xlim = (mdates.date2num(t0), mdates.date2num(t1))
        self.default_ylim = (-0.2, 1.2)
        self.ax.set_xlim(*self.default_xlim)
        self.ax.set_ylim(*self.default_ylim)

        self.fig.tight_layout(rect=(0, 0.05, 1, 1))
        self.fig.canvas.draw_idle()

    # Draw one stream's marks. Kept marks use the stream color; excluded marks use a distinct
    # gray/black style.
    def _draw_stream(self, df: pd.DataFrame, ts_col: str, y: float, base_color: str) -> None:
        if df.empty:
            return

        keep_df = df[~df["exclude"]].copy()
        excl_df = df[df["exclude"]].copy()

        if not keep_df.empty:
            xs = pd.to_datetime(keep_df[ts_col])
            ys = np.full(len(keep_df), y)
            self.ax.vlines(xs, y, 0.5, color=base_color, linewidth=1.6, alpha=0.95 if y == 1.0 else 0.9)
            self.ax.scatter(xs, ys, s=18, color=base_color, zorder=3)

        if not excl_df.empty:
            xs = pd.to_datetime(excl_df[ts_col])
            ys = np.full(len(excl_df), y)
            self.ax.vlines(xs, y, 0.5, color="gray", linewidth=1.2, alpha=0.65, linestyles="dotted")
            self.ax.scatter(xs, ys, s=34, color="black", marker="x", zorder=4)

    # Overlay a highlight ring on the currently selected mark without changing the underlying
    # data.
    def _draw_selection(self) -> None:
        if self.selected is None:
            return

        stream, ordinal = self.selected
        df = self.loaded.events_marks if stream == "events" else self.loaded.rpi_marks
        sub = df[df["ordinal"] == ordinal]
        if sub.empty:
            return

        row = sub.iloc[0]
        x = pd.to_datetime(row[self.loaded.ts_col])
        y = 1.0 if stream == "events" else 0.0
        self.ax.scatter([x], [y], s=120, facecolors="none", edgecolors="gold", linewidths=2.0, zorder=5)

    # Map a mouse event to the nearest visible mark by comparing normalized x/y distances.
    def find_nearest_mark(self, event) -> Optional[tuple[str, int]]:
        if event.inaxes != self.ax or event.xdata is None or event.ydata is None:
            return None

        marks = self.visible_marks()
        if marks.empty:
            return None

        xlim = self.ax.get_xlim()
        x_range = max(xlim[1] - xlim[0], 1e-9)

        dx = (marks["_xnum"] - event.xdata).abs() / x_range
        dy = (marks["y"] - event.ydata).abs() / 1.2
        dist = np.sqrt(dx**2 + dy**2)

        idx = dist.idxmin()
        if float(dx.loc[idx]) > 0.02:
            return None

        row = marks.loc[idx]
        return str(row["stream"]), int(row["ordinal"])

    # Flip a mark's exclude flag, timestamp the action, save immediately, and redraw.
    def toggle_selected(self, selected: tuple[str, int]) -> None:
        stream, ordinal = selected
        df = self.loaded.events_marks if stream == "events" else self.loaded.rpi_marks
        mask = df["ordinal"] == ordinal
        if not mask.any():
            return

        current = bool(df.loc[mask, "exclude"].iloc[0])
        new_value = not current
        df.loc[mask, "exclude"] = new_value
        df.loc[mask, "reviewed_at"] = datetime.now().isoformat(timespec="seconds")
        if not new_value:
            df.loc[mask, "reason"] = ""
        self.selected = selected
        self.save_reviews()
        self.draw()

    # Assign a reason code to the selected mark; setting a reason also marks that item excluded.
    def set_reason_on_selected(self, reason: str) -> None:
        if self.selected is None:
            return
        stream, ordinal = self.selected
        df = self.loaded.events_marks if stream == "events" else self.loaded.rpi_marks
        mask = df["ordinal"] == ordinal
        if not mask.any():
            return
        df.loc[mask, "reason"] = reason
        df.loc[mask, "exclude"] = True
        df.loc[mask, "reviewed_at"] = datetime.now().isoformat(timespec="seconds")
        self.save_reviews()
        self.draw()

    # Clear the reason attached to the selected mark and persist the change.
    def clear_reason_on_selected(self) -> None:
        if self.selected is None:
            return
        stream, ordinal = self.selected
        df = self.loaded.events_marks if stream == "events" else self.loaded.rpi_marks
        mask = df["ordinal"] == ordinal
        if not mask.any():
            return
        df.loc[mask, "reason"] = ""
        df.loc[mask, "reviewed_at"] = datetime.now().isoformat(timespec="seconds")
        self.save_reviews()
        self.draw()

    # Mouse click callback: hit-test the cursor, toggle the nearest mark, and let the redraw
    # reflect the new state.
    def on_click(self, event) -> None:
        selected = self.find_nearest_mark(event)
        if selected is None:
            return
        self.toggle_selected(selected)

    # Navigate between block views while keeping all review data in the same loaded pair.
    def next_block(self, step: int) -> None:
        blocks = sorted(self.loaded.blocks["block"].dropna().astype(int).unique().tolist())
        if not blocks:
            return

        current = self.loaded.current_block
        if str(current).lower() == "all":
            idx = -1 if step > 0 else 0
        else:
            try:
                idx = blocks.index(int(current))
            except ValueError:
                idx = 0

        new_idx = idx + step
        if new_idx < 0 or new_idx >= len(blocks):
            self.loaded.current_block = "All"
        else:
            self.loaded.current_block = blocks[new_idx]
        self.draw()

    # Keyboard shortcut handler for file navigation, block navigation, reasons, save/reset, and
    # quit actions.
    def on_key(self, event) -> None:
        key = str(event.key).lower() if event.key else ""

        if key == "right":
            if self.current_index < len(self.pairs) - 1:
                self.goto_pair(self.current_index + 1)
            return

        if key == "left":
            if self.current_index > 0:
                self.goto_pair(self.current_index - 1)
            return

        if key == "up":
            self.next_block(1)
            return

        if key == "down":
            self.next_block(-1)
            return

        if key == "a":
            self.loaded.current_block = "All"
            self.draw()
            return

        if key == "s":
            self.save_reviews()
            return

        if key == "r":
            if self.default_xlim and self.default_ylim:
                self.ax.set_xlim(*self.default_xlim)
                self.ax.set_ylim(*self.default_ylim)
                self.fig.canvas.draw_idle()
            return

        if key == "q":
            self.save_reviews()
            plt.close(self.fig)
            return

        if key in REASON_MAP:
            self.set_reason_on_selected(REASON_MAP[key])
            return

        if key == "0":
            self.clear_reason_on_selected()
            return


# Define and parse the command-line options used to launch this GUI.
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Interactive mark reviewer for Events vs RPi timelines.")
    ap.add_argument("--manifest", type=Path, help="CSV with columns: events_file,rpi_file,label")
    ap.add_argument("--events", type=Path, help="Single Events CSV")
    ap.add_argument("--rpi", type=Path, help="Single RPi CSV")
    ap.add_argument("--label", default="", help="BioPac or RNS for single-pair mode")
    ap.add_argument("--events-dir", type=Path, help="Directory of Events CSVs")
    ap.add_argument("--rpi-dir", type=Path, help="Directory of *_RPi_unified.csv files")
    ap.add_argument("--review-dir", type=Path, required=True, help="Directory for per-pair review CSVs")
    ap.add_argument("--start-index", type=int, default=0, help="Index to start from")
    ap.add_argument("--skip-reviewed-pairs", action="store_true", help="Skip pairs that already have a saved non-empty review file")
    return ap.parse_args()


# Choose manifest-based or directory-based file discovery from parsed command-line options.
def build_pairs_from_args(args: argparse.Namespace) -> list[FilePair]:
    if args.manifest:
        return load_manifest(args.manifest)

    if args.events_dir and args.rpi_dir:
        return build_pairs_from_directories(args.events_dir, args.rpi_dir)

    if args.events and args.rpi and args.label.strip():
        return [
            FilePair(
                events_file=args.events.expanduser(),
                rpi_file=args.rpi.expanduser(),
                label=args.label.strip(),
            )
        ]

    raise SystemExit(
        "Provide either --manifest, or --events-dir with --rpi-dir, "
        "or all of --events --rpi --label."
    )


# Program entry point: parse command-line arguments, discover file pairs, construct the GUI
# object, and hand control to Matplotlib's event loop.
def main() -> None:
    args = parse_args()
    review_dir = args.review_dir.expanduser()
    review_dir.mkdir(parents=True, exist_ok=True)

    pairs = build_pairs_from_args(args)

    if args.skip_reviewed_pairs:
        pairs = [pair for pair in pairs if not pair_is_reviewed(review_dir, pair)]

    if not pairs:
        raise SystemExit("No file pairs found to review.")

    for pair in pairs:
        if not pair.events_file.exists():
            raise FileNotFoundError(f"Missing events file: {pair.events_file}")
        if not pair.rpi_file.exists():
            raise FileNotFoundError(f"Missing rpi file: {pair.rpi_file}")

    app = MarkReviewApp(
        pairs=pairs,
        review_dir=review_dir,
        start_index=args.start_index,
    )
    plt.show()


if __name__ == "__main__":
    main()
