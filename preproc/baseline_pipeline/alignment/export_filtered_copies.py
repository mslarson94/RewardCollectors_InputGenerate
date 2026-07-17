# file: export_filtered_copies.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


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

EVENT_TYPE_CANDIDATES = ["lo_eventType", "eventType", "event_type", "type"]
EVENT_TS_CANDIDATES = ["eMLT_orig"]
RPI_TIME_CANDIDATES = [
    "RPi_Time_unified",
    "ML_Time_verb",
    "RPi_Time_simple",
    "RPi_Time_verb",
    "Mono_Time_verb",
    "Mono_Time_Raw_verb",
]


@dataclass
class ExportSummary:
    review_file: str
    events_file: str
    rpi_file: str
    label: str
    events_mark_rows_total: int
    rpi_mark_rows_total: int
    events_marks_excluded: int
    rpi_marks_excluded: int
    events_rows_before: int
    events_rows_after: int
    rpi_rows_before: int
    rpi_rows_after: int
    filtered_events_file: str
    filtered_rpi_file: str
    pair_summary_csv: str
    pair_removed_log_csv: str
    status: str
    message: str


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Apply per-pair mark review CSVs to export filtered Events/RPi copies."
    )
    ap.add_argument("--review-dir", type=Path, required=True, help="Directory of per-pair *_mark_review_decisions.csv files")
    ap.add_argument("--out-dir", type=Path, required=True, help="Output directory for filtered copies")
    ap.add_argument("--only-label", choices=["BioPac", "RNS"], default="", help="Optional label subset")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing filtered copies")
    ap.add_argument("--dry-run", action="store_true", help="Report actions without writing files")
    return ap.parse_args()


def detect_events_columns(df: pd.DataFrame) -> tuple[str, str]:
    ts_col = next((c for c in EVENT_TS_CANDIDATES if c in df.columns), "")
    et_col = next((c for c in EVENT_TYPE_CANDIDATES if c in df.columns), "")
    if not ts_col:
        raise KeyError(f"Could not find Events timestamp column. Tried: {EVENT_TS_CANDIDATES}")
    if not et_col:
        raise KeyError(f"Could not find Events event-type column. Tried: {EVENT_TYPE_CANDIDATES}")
    return ts_col, et_col


def detect_rpi_time_column(df: pd.DataFrame) -> str:
    for name in RPI_TIME_CANDIDATES:
        if name in df.columns:
            return name
    for c in df.columns:
        if df[c].dtype == object and any(k in c.lower() for k in ("time", "timestamp", "date")):
            return c
    raise KeyError("Could not detect a usable RPi time column.")


def ensure_review_schema(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in REVIEW_COLUMNS:
        if col not in out.columns:
            out[col] = pd.Series(dtype="string")
    out = out[REVIEW_COLUMNS].copy()
    out["exclude"] = out["exclude"].astype(str).str.lower().isin({"true", "1", "yes"})
    out["ordinal"] = pd.to_numeric(out["ordinal"], errors="coerce").astype("Int64")
    return out


def filtered_output_name(path: Path) -> str:
    return f"{path.stem}_filtered{path.suffix}"


def reconstruct_events_marks(events_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    ts_col, et_col = detect_events_columns(events_df)
    df = events_df.copy()
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
    is_mark = df[et_col].astype(str).str.strip().str.lower().eq("mark")
    marks = df.loc[is_mark & df[ts_col].notna()].copy()
    marks = marks.sort_values(ts_col).copy()
    mark_index = pd.Series(marks.index.to_list(), index=range(len(marks)), dtype="Int64")
    return marks, mark_index


def reconstruct_rpi_marks(rpi_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, str]:
    time_col = detect_rpi_time_column(rpi_df)
    df = rpi_df.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    marks = df.loc[df[time_col].notna()].copy()
    marks = marks.sort_values(time_col).copy()
    mark_index = pd.Series(marks.index.to_list(), index=range(len(marks)), dtype="Int64")
    return marks, mark_index, time_col


def load_review_file(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype="string")
    return ensure_review_schema(df)


def get_single_value(df: pd.DataFrame, col: str) -> str:
    vals = [str(v).strip() for v in df[col].dropna().unique().tolist() if str(v).strip()]
    if not vals:
        return ""
    if len(vals) > 1:
        raise ValueError(f"Expected exactly one unique value in column '{col}', found: {vals}")
    return vals[0]


def build_removed_rows_log(
    review_df: pd.DataFrame,
    events_marks: pd.DataFrame,
    rpi_marks: pd.DataFrame,
    events_mark_index: pd.Series,
    rpi_mark_index: pd.Series,
    events_path: Path,
    rpi_path: Path,
    label: str,
    events_ts_col: str,
    rpi_time_col: str,
) -> pd.DataFrame:
    excluded = review_df.loc[review_df["exclude"]].copy()
    rows: list[dict[str, object]] = []

    event_ordinals = (
        excluded.loc[excluded["stream"] == "events", "ordinal"]
        .dropna()
        .astype(int)
        .drop_duplicates()
        .tolist()
    )
    for ordinal in event_ordinals:
        if ordinal not in events_mark_index.index:
            continue
        src_idx = int(events_mark_index.loc[ordinal])
        row = events_marks.iloc[ordinal]
        rows.append(
            {
                "review_file": "",
                "events_file": str(events_path),
                "rpi_file": str(rpi_path),
                "label": label,
                "stream": "events",
                "ordinal": ordinal,
                "source_row_index": src_idx,
                "mark_time": pd.to_datetime(row[events_ts_col]).isoformat(sep=" "),
                "mark_id": f"events_{ordinal:03d}",
                "reason": _reason_for_ordinal(excluded, "events", ordinal),
                "reviewed_at": _reviewed_at_for_ordinal(excluded, "events", ordinal),
            }
        )

    rpi_ordinals = (
        excluded.loc[excluded["stream"] == "rpi", "ordinal"]
        .dropna()
        .astype(int)
        .drop_duplicates()
        .tolist()
    )
    for ordinal in rpi_ordinals:
        if ordinal not in rpi_mark_index.index:
            continue
        src_idx = int(rpi_mark_index.loc[ordinal])
        row = rpi_marks.iloc[ordinal]
        rows.append(
            {
                "review_file": "",
                "events_file": str(events_path),
                "rpi_file": str(rpi_path),
                "label": label,
                "stream": "rpi",
                "ordinal": ordinal,
                "source_row_index": src_idx,
                "mark_time": pd.to_datetime(row[rpi_time_col]).isoformat(sep=" "),
                "mark_id": f"rpi_{ordinal:03d}",
                "reason": _reason_for_ordinal(excluded, "rpi", ordinal),
                "reviewed_at": _reviewed_at_for_ordinal(excluded, "rpi", ordinal),
            }
        )

    return pd.DataFrame(rows)


def _reason_for_ordinal(df: pd.DataFrame, stream: str, ordinal: int) -> str:
    sub = df.loc[(df["stream"] == stream) & (df["ordinal"] == ordinal), "reason"]
    vals = [str(v).strip() for v in sub.dropna().tolist() if str(v).strip()]
    return vals[-1] if vals else ""


def _reviewed_at_for_ordinal(df: pd.DataFrame, stream: str, ordinal: int) -> str:
    sub = df.loc[(df["stream"] == stream) & (df["ordinal"] == ordinal), "reviewed_at"]
    vals = [str(v).strip() for v in sub.dropna().tolist() if str(v).strip()]
    return vals[-1] if vals else ""


def export_one_review(
    review_path: Path,
    out_dir: Path,
    overwrite: bool,
    dry_run: bool,
) -> tuple[ExportSummary, pd.DataFrame]:
    review_df = load_review_file(review_path)

    events_file = get_single_value(review_df, "events_file")
    rpi_file = get_single_value(review_df, "rpi_file")
    label = get_single_value(review_df, "label")

    empty_removed = pd.DataFrame()

    if not events_file or not rpi_file or not label:
        return (
            ExportSummary(
                review_file=str(review_path),
                events_file=events_file,
                rpi_file=rpi_file,
                label=label,
                events_mark_rows_total=0,
                rpi_mark_rows_total=0,
                events_marks_excluded=0,
                rpi_marks_excluded=0,
                events_rows_before=0,
                events_rows_after=0,
                rpi_rows_before=0,
                rpi_rows_after=0,
                filtered_events_file="",
                filtered_rpi_file="",
                pair_summary_csv="",
                pair_removed_log_csv="",
                status="fail",
                message="Missing one of events_file, rpi_file, or label in review CSV.",
            ),
            empty_removed,
        )

    events_path = Path(events_file).expanduser()
    rpi_path = Path(rpi_file).expanduser()

    if not events_path.exists():
        return (
            ExportSummary(
                review_file=str(review_path),
                events_file=str(events_path),
                rpi_file=str(rpi_path),
                label=label,
                events_mark_rows_total=0,
                rpi_mark_rows_total=0,
                events_marks_excluded=0,
                rpi_marks_excluded=0,
                events_rows_before=0,
                events_rows_after=0,
                rpi_rows_before=0,
                rpi_rows_after=0,
                filtered_events_file="",
                filtered_rpi_file="",
                pair_summary_csv="",
                pair_removed_log_csv="",
                status="fail",
                message=f"Missing events file: {events_path}",
            ),
            empty_removed,
        )

    if not rpi_path.exists():
        return (
            ExportSummary(
                review_file=str(review_path),
                events_file=str(events_path),
                rpi_file=str(rpi_path),
                label=label,
                events_mark_rows_total=0,
                rpi_mark_rows_total=0,
                events_marks_excluded=0,
                rpi_marks_excluded=0,
                events_rows_before=0,
                events_rows_after=0,
                rpi_rows_before=0,
                rpi_rows_after=0,
                filtered_events_file="",
                filtered_rpi_file="",
                pair_summary_csv="",
                pair_removed_log_csv="",
                status="fail",
                message=f"Missing RPi file: {rpi_path}",
            ),
            empty_removed,
        )

    events_df = pd.read_csv(events_path)
    rpi_df = pd.read_csv(rpi_path)

    events_marks, events_mark_index = reconstruct_events_marks(events_df)
    rpi_marks, rpi_mark_index, rpi_time_col = reconstruct_rpi_marks(rpi_df)
    events_ts_col, _ = detect_events_columns(events_df)

    excluded = review_df.loc[review_df["exclude"]].copy()

    excluded_events_ord = (
        excluded.loc[excluded["stream"] == "events", "ordinal"]
        .dropna()
        .astype(int)
        .drop_duplicates()
        .tolist()
    )
    excluded_rpi_ord = (
        excluded.loc[excluded["stream"] == "rpi", "ordinal"]
        .dropna()
        .astype(int)
        .drop_duplicates()
        .tolist()
    )

    event_row_labels_to_drop = (
        events_mark_index.loc[events_mark_index.index.isin(excluded_events_ord)]
        .dropna()
        .astype(int)
        .tolist()
    )
    rpi_row_labels_to_drop = (
        rpi_mark_index.loc[rpi_mark_index.index.isin(excluded_rpi_ord)]
        .dropna()
        .astype(int)
        .tolist()
    )

    filtered_events = events_df.drop(index=event_row_labels_to_drop, errors="ignore").copy()
    filtered_rpi = rpi_df.drop(index=rpi_row_labels_to_drop, errors="ignore").copy()

    pair_out_dir = out_dir / label
    pair_out_dir.mkdir(parents=True, exist_ok=True)

    filtered_events_path = pair_out_dir / filtered_output_name(events_path)
    filtered_rpi_path = pair_out_dir / filtered_output_name(rpi_path)
    pair_summary_path = pair_out_dir / f"{events_path.stem}_{label}_filtering_summary.csv"
    pair_removed_log_path = pair_out_dir / f"{events_path.stem}_{label}_removed_marks_log.csv"

    if not overwrite:
        for p in (filtered_events_path, filtered_rpi_path, pair_summary_path, pair_removed_log_path):
            if p.exists() and not dry_run:
                return (
                    ExportSummary(
                        review_file=str(review_path),
                        events_file=str(events_path),
                        rpi_file=str(rpi_path),
                        label=label,
                        events_mark_rows_total=len(events_marks),
                        rpi_mark_rows_total=len(rpi_marks),
                        events_marks_excluded=len(event_row_labels_to_drop),
                        rpi_marks_excluded=len(rpi_row_labels_to_drop),
                        events_rows_before=len(events_df),
                        events_rows_after=len(filtered_events),
                        rpi_rows_before=len(rpi_df),
                        rpi_rows_after=len(filtered_rpi),
                        filtered_events_file=str(filtered_events_path),
                        filtered_rpi_file=str(filtered_rpi_path),
                        pair_summary_csv=str(pair_summary_path),
                        pair_removed_log_csv=str(pair_removed_log_path),
                        status="skip",
                        message=f"Output exists and --overwrite not set: {p}",
                    ),
                    empty_removed,
                )

    summary = ExportSummary(
        review_file=str(review_path),
        events_file=str(events_path),
        rpi_file=str(rpi_path),
        label=label,
        events_mark_rows_total=len(events_marks),
        rpi_mark_rows_total=len(rpi_marks),
        events_marks_excluded=len(event_row_labels_to_drop),
        rpi_marks_excluded=len(rpi_row_labels_to_drop),
        events_rows_before=len(events_df),
        events_rows_after=len(filtered_events),
        rpi_rows_before=len(rpi_df),
        rpi_rows_after=len(filtered_rpi),
        filtered_events_file=str(filtered_events_path),
        filtered_rpi_file=str(filtered_rpi_path),
        pair_summary_csv=str(pair_summary_path),
        pair_removed_log_csv=str(pair_removed_log_path),
        status="ok",
        message="Filtered copies generated.",
    )

    removed_rows_log = build_removed_rows_log(
        review_df=review_df,
        events_marks=events_marks,
        rpi_marks=rpi_marks,
        events_mark_index=events_mark_index,
        rpi_mark_index=rpi_mark_index,
        events_path=events_path,
        rpi_path=rpi_path,
        label=label,
        events_ts_col=events_ts_col,
        rpi_time_col=rpi_time_col,
    )
    if not removed_rows_log.empty:
        removed_rows_log["review_file"] = str(review_path)

    if not dry_run:
        filtered_events.to_csv(filtered_events_path, index=False)
        filtered_rpi.to_csv(filtered_rpi_path, index=False)
        pd.DataFrame([asdict(summary)]).to_csv(pair_summary_path, index=False)
        removed_rows_log.to_csv(pair_removed_log_path, index=False)

    return summary, removed_rows_log


def main() -> None:
    args = parse_args()
    review_dir = args.review_dir.expanduser()
    out_dir = args.out_dir.expanduser()

    if not review_dir.is_dir():
        raise NotADirectoryError(f"review-dir is not a directory: {review_dir}")

    review_files = sorted(review_dir.glob("*_mark_review_decisions.csv"))
    if not review_files:
        raise SystemExit(f"No review CSVs found in: {review_dir}")

    summaries: list[ExportSummary] = []
    removed_logs: list[pd.DataFrame] = []

    for review_path in review_files:
        try:
            df = load_review_file(review_path)
            label = get_single_value(df, "label")
            if args.only_label and label != args.only_label:
                continue
            summary, removed_df = export_one_review(
                review_path=review_path,
                out_dir=out_dir,
                overwrite=args.overwrite,
                dry_run=args.dry_run,
            )
        except Exception as e:
            summary = ExportSummary(
                review_file=str(review_path),
                events_file="",
                rpi_file="",
                label="",
                events_mark_rows_total=0,
                rpi_mark_rows_total=0,
                events_marks_excluded=0,
                rpi_marks_excluded=0,
                events_rows_before=0,
                events_rows_after=0,
                rpi_rows_before=0,
                rpi_rows_after=0,
                filtered_events_file="",
                filtered_rpi_file="",
                pair_summary_csv="",
                pair_removed_log_csv="",
                status="fail",
                message=str(e),
            )
            removed_df = pd.DataFrame()

        summaries.append(summary)
        if not removed_df.empty:
            removed_logs.append(removed_df)

        print(f"[{summary.status}] {Path(summary.review_file).name}: {summary.message}")

    master_summary = pd.DataFrame([asdict(s) for s in summaries])
    out_dir.mkdir(parents=True, exist_ok=True)
    master_summary_path = out_dir / "filtered_export_master_summary.csv"
    master_removed_log_path = out_dir / "removed_marks_log_master.csv"

    if not args.dry_run:
        master_summary.to_csv(master_summary_path, index=False)
        if removed_logs:
            pd.concat(removed_logs, ignore_index=True).to_csv(master_removed_log_path, index=False)
        else:
            pd.DataFrame().to_csv(master_removed_log_path, index=False)

    print(f"[ok] processed review files: {len(summaries)}")
    if not args.dry_run:
        print(f"[ok] wrote master summary -> {master_summary_path}")
        print(f"[ok] wrote master removed log -> {master_removed_log_path}")


if __name__ == "__main__":
    main()


