# mark_chunk_prototype.py
'''

python tools/mark_chunk_prototype.py \
  --input your_data.csv \
  --output-dir outputs/mark_review \
  --time-col eMLT_orig \
  --event-col lo_eventType \
  --block-col BlockNum \
  --mark-gap-seconds 60 \
  --context-window-seconds 300 \
  --boundary-pair-max-seconds 300

##### Output ####### 
mark_clusters.csv
    One row per proposed Mark cluster:

        time span
        count of Marks
        duration
        gap before/after
        soft auto-label
        mark_cluster_context.csv

mark_cluster_context.csv
    One row per cluster with nearby block evidence:

        nearby start block numbers
        nearby end block numbers
        nearest start-like event
        nearest end-like event
        event summary for quick review
        proposed_chunks.csv

proposed_chunks.csv
    Adjacent cluster pairs that look like:
        end -> start
        within your allowed boundary window
        mark_cluster_review_template.csv

mark_cluster_review_template.csv
    Editable sheet for manual review:
        label cluster
        mark split/merge actions
        notes

'''
from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


START_EVENT_TYPES = {"BlockStart", "RoundStart"}
END_EVENT_TYPES = {"BlockEnd"}
MARK_EVENT_TYPE = "Mark"


@dataclass
class MarkCluster:
    cluster_id: int
    mark_row_indices: list[int]
    start_time: pd.Timestamp
    end_time: pd.Timestamp
    duration_seconds: float
    n_marks: int
    gap_before_seconds: float | None
    gap_after_seconds: float | None
    prev_cluster_id: int | None
    next_cluster_id: int | None
    auto_label: str = "unknown"
    auto_confidence: float = 0.0
    manual_label: str = "unknown"
    review_status: str = "proposed"
    notes: str = ""
    mark_times: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["start_time"] = self.start_time.isoformat()
        record["end_time"] = self.end_time.isoformat()
        record["mark_row_indices"] = json.dumps(self.mark_row_indices)
        record["mark_times"] = json.dumps(self.mark_times)
        return record


@dataclass
class ClusterContext:
    cluster_id: int
    context_window_seconds: int
    nearby_start_blocknums: list[Any]
    nearby_end_blocknums: list[Any]
    nearest_start_time: str | None
    nearest_start_type: str | None
    nearest_start_blocknum: Any | None
    nearest_start_delta_seconds: float | None
    nearest_end_time: str | None
    nearest_end_type: str | None
    nearest_end_blocknum: Any | None
    nearest_end_delta_seconds: float | None
    nearby_event_summary: str

    def to_record(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "context_window_seconds": self.context_window_seconds,
            "nearby_start_blocknums": json.dumps(self.nearby_start_blocknums),
            "nearby_end_blocknums": json.dumps(self.nearby_end_blocknums),
            "nearest_start_time": self.nearest_start_time,
            "nearest_start_type": self.nearest_start_type,
            "nearest_start_blocknum": self.nearest_start_blocknum,
            "nearest_start_delta_seconds": self.nearest_start_delta_seconds,
            "nearest_end_time": self.nearest_end_time,
            "nearest_end_type": self.nearest_end_type,
            "nearest_end_blocknum": self.nearest_end_blocknum,
            "nearest_end_delta_seconds": self.nearest_end_delta_seconds,
            "nearby_event_summary": self.nearby_event_summary,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cluster Mark events in time and export editable candidates for manual chunk review."
    )
    parser.add_argument("--input", required=True, help="Path to input CSV.")
    parser.add_argument("--output-dir", required=True, help="Directory for exported files.")
    parser.add_argument(
        "--time-col",
        default="mLT_orig",
        help="Timestamp column used for temporal clustering.",
    )
    parser.add_argument(
        "--event-col",
        default="lo_eventType",
        help="Event type column.",
    )
    parser.add_argument(
        "--block-col",
        default="BlockNum",
        help="Block number column.",
    )
    parser.add_argument(
        "--mark-gap-seconds",
        type=int,
        default=60,
        help="New Mark cluster starts when the gap between consecutive Marks exceeds this value.",
    )
    parser.add_argument(
        "--context-window-seconds",
        type=int,
        default=300,
        help="Window around each Mark cluster used to summarize nearby block events.",
    )
    parser.add_argument(
        "--boundary-pair-max-seconds",
        type=int,
        default=300,
        help="Max gap between adjacent Mark clusters to propose an end->start boundary pair.",
    )
    parser.add_argument(
        "--timestamp-format",
        default=None,
        help="Optional explicit datetime format for parsing the time column.",
    )
    return parser.parse_args()


def load_and_prepare_dataframe(
    csv_path: str,
    time_col: str,
    event_col: str,
    block_col: str,
    timestamp_format: str | None = None,
) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    missing = [col for col in [time_col, event_col] if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if block_col not in df.columns:
        df[block_col] = pd.NA

    df = df.copy()
    df["_source_row_index"] = df.index

    if timestamp_format:
        df[time_col] = pd.to_datetime(df[time_col], format=timestamp_format, errors="coerce")
    else:
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")

    invalid_time_count = int(df[time_col].isna().sum())
    if invalid_time_count > 0:
        raise ValueError(
            f"Found {invalid_time_count} rows with unparseable timestamps in {time_col}."
        )

    df = df.sort_values(time_col).reset_index(drop=True)
    return df


def seconds_between(a: pd.Timestamp, b: pd.Timestamp) -> float:
    return float((b - a).total_seconds())


def cluster_marks(
    df: pd.DataFrame,
    time_col: str,
    event_col: str,
    mark_gap_seconds: int,
) -> list[MarkCluster]:
    mark_df = df[df[event_col] == MARK_EVENT_TYPE].copy().reset_index(drop=True)
    if mark_df.empty:
        return []

    clusters_raw: list[list[dict[str, Any]]] = []
    current_cluster: list[dict[str, Any]] = []

    previous_time: pd.Timestamp | None = None
    for _, row in mark_df.iterrows():
        row_time = row[time_col]
        row_payload = {
            "source_row_index": int(row["_source_row_index"]),
            "time": row_time,
        }

        if previous_time is None:
            current_cluster = [row_payload]
        else:
            gap_seconds = seconds_between(previous_time, row_time)
            if gap_seconds <= mark_gap_seconds:
                current_cluster.append(row_payload)
            else:
                clusters_raw.append(current_cluster)
                current_cluster = [row_payload]
        previous_time = row_time

    if current_cluster:
        clusters_raw.append(current_cluster)

    clusters: list[MarkCluster] = []
    for i, rows in enumerate(clusters_raw, start=1):
        start_time = rows[0]["time"]
        end_time = rows[-1]["time"]
        duration_seconds = seconds_between(start_time, end_time)
        cluster = MarkCluster(
            cluster_id=i,
            mark_row_indices=[r["source_row_index"] for r in rows],
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration_seconds,
            n_marks=len(rows),
            gap_before_seconds=None,
            gap_after_seconds=None,
            prev_cluster_id=i - 1 if i > 1 else None,
            next_cluster_id=i + 1 if i < len(clusters_raw) else None,
            mark_times=[r["time"].isoformat() for r in rows],
        )
        clusters.append(cluster)

    for i, cluster in enumerate(clusters):
        if i > 0:
            cluster.gap_before_seconds = seconds_between(clusters[i - 1].end_time, cluster.start_time)
        if i < len(clusters) - 1:
            cluster.gap_after_seconds = seconds_between(cluster.end_time, clusters[i + 1].start_time)

    return clusters


def nearest_event_record(
    cluster_midpoint: pd.Timestamp,
    candidate_df: pd.DataFrame,
    time_col: str,
    event_col: str,
    block_col: str,
) -> tuple[str | None, str | None, Any | None, float | None]:
    if candidate_df.empty:
        return None, None, None, None

    deltas = candidate_df[time_col].apply(
        lambda t: abs((t - cluster_midpoint).total_seconds())
    )
    nearest_idx = deltas.idxmin()
    nearest_row = candidate_df.loc[nearest_idx]
    signed_delta = float((nearest_row[time_col] - cluster_midpoint).total_seconds())
    return (
        nearest_row[time_col].isoformat(),
        str(nearest_row[event_col]),
        nearest_row[block_col],
        signed_delta,
    )


def summarize_nearby_events(
    nearby_df: pd.DataFrame,
    time_col: str,
    event_col: str,
    block_col: str,
    max_rows: int = 12,
) -> str:
    if nearby_df.empty:
        return ""

    preview = nearby_df[[time_col, event_col, block_col]].head(max_rows).copy()
    parts: list[str] = []
    for _, row in preview.iterrows():
        block_val = row[block_col]
        block_str = "NA" if pd.isna(block_val) else str(block_val)
        parts.append(f"{row[time_col].isoformat()} | {row[event_col]} | BlockNum={block_str}")
    return " || ".join(parts)


def build_cluster_context(
    df: pd.DataFrame,
    clusters: list[MarkCluster],
    time_col: str,
    event_col: str,
    block_col: str,
    context_window_seconds: int,
) -> list[ClusterContext]:
    contexts: list[ClusterContext] = []

    for cluster in clusters:
        midpoint = cluster.start_time + (cluster.end_time - cluster.start_time) / 2
        window_start = midpoint - pd.Timedelta(seconds=context_window_seconds)
        window_end = midpoint + pd.Timedelta(seconds=context_window_seconds)

        nearby_df = df[(df[time_col] >= window_start) & (df[time_col] <= window_end)].copy()
        nearby_non_mark_df = nearby_df[nearby_df[event_col] != MARK_EVENT_TYPE].copy()

        nearby_start_df = nearby_non_mark_df[nearby_non_mark_df[event_col].isin(START_EVENT_TYPES)].copy()
        nearby_end_df = nearby_non_mark_df[nearby_non_mark_df[event_col].isin(END_EVENT_TYPES)].copy()

        nearest_start = nearest_event_record(
            midpoint, nearby_start_df, time_col, event_col, block_col
        )
        nearest_end = nearest_event_record(
            midpoint, nearby_end_df, time_col, event_col, block_col
        )

        start_blocknums = sorted(
            {
                value
                for value in nearby_start_df[block_col].dropna().tolist()
            },
            key=lambda x: str(x),
        )
        end_blocknums = sorted(
            {
                value
                for value in nearby_end_df[block_col].dropna().tolist()
            },
            key=lambda x: str(x),
        )

        contexts.append(
            ClusterContext(
                cluster_id=cluster.cluster_id,
                context_window_seconds=context_window_seconds,
                nearby_start_blocknums=start_blocknums,
                nearby_end_blocknums=end_blocknums,
                nearest_start_time=nearest_start[0],
                nearest_start_type=nearest_start[1],
                nearest_start_blocknum=nearest_start[2],
                nearest_start_delta_seconds=nearest_start[3],
                nearest_end_time=nearest_end[0],
                nearest_end_type=nearest_end[1],
                nearest_end_blocknum=nearest_end[2],
                nearest_end_delta_seconds=nearest_end[3],
                nearby_event_summary=summarize_nearby_events(
                    nearby_non_mark_df.sort_values(time_col),
                    time_col,
                    event_col,
                    block_col,
                ),
            )
        )

    return contexts


def infer_soft_label(cluster: MarkCluster) -> tuple[str, float]:
    n = cluster.n_marks

    if n <= 2:
        return "pause_like", 0.45
    if n == 3:
        return "start_like", 0.5
    if n >= 4:
        return "end_like", 0.5
    return "unknown", 0.0


def apply_soft_labels(clusters: list[MarkCluster], boundary_pair_max_seconds: int) -> None:
    for cluster in clusters:
        label, confidence = infer_soft_label(cluster)
        cluster.auto_label = label
        cluster.auto_confidence = confidence

    for i in range(len(clusters) - 1):
        left = clusters[i]
        right = clusters[i + 1]
        if left.gap_after_seconds is None:
            continue
        if left.gap_after_seconds <= boundary_pair_max_seconds:
            if left.n_marks >= right.n_marks:
                left.auto_label = "end_like"
                left.auto_confidence = max(left.auto_confidence, 0.65)
                right.auto_label = "start_like"
                right.auto_confidence = max(right.auto_confidence, 0.65)
            else:
                left.auto_label = "unknown"
                right.auto_label = "unknown"


def propose_chunk_pairs(
    clusters: list[MarkCluster],
    contexts: list[ClusterContext],
    boundary_pair_max_seconds: int,
) -> pd.DataFrame:
    context_by_cluster = {c.cluster_id: c for c in contexts}
    proposals: list[dict[str, Any]] = []

    for i in range(len(clusters) - 1):
        left = clusters[i]
        right = clusters[i + 1]
        if left.gap_after_seconds is None or left.gap_after_seconds > boundary_pair_max_seconds:
            continue

        left_ctx = context_by_cluster[left.cluster_id]
        right_ctx = context_by_cluster[right.cluster_id]

        score = 0.0
        reasons: list[str] = []

        if left.auto_label == "end_like":
            score += 1.0
            reasons.append("left_end_like")
        if right.auto_label == "start_like":
            score += 1.0
            reasons.append("right_start_like")
        if left_ctx.nearest_end_type in END_EVENT_TYPES:
            score += 1.0
            reasons.append("left_near_block_end")
        if right_ctx.nearest_start_type in START_EVENT_TYPES:
            score += 1.0
            reasons.append("right_near_block_start")
        if left_ctx.nearby_end_blocknums and right_ctx.nearby_start_blocknums:
            score += 0.5
            reasons.append("both_have_block_context")

        proposals.append(
            {
                "proposal_id": len(proposals) + 1,
                "end_cluster_id": left.cluster_id,
                "start_cluster_id": right.cluster_id,
                "gap_seconds": left.gap_after_seconds,
                "score": score,
                "reasons": ",".join(reasons),
                "end_cluster_auto_label": left.auto_label,
                "start_cluster_auto_label": right.auto_label,
                "end_cluster_n_marks": left.n_marks,
                "start_cluster_n_marks": right.n_marks,
                "end_cluster_time": left.end_time.isoformat(),
                "start_cluster_time": right.start_time.isoformat(),
                "end_nearby_end_blocknums": json.dumps(left_ctx.nearby_end_blocknums),
                "start_nearby_start_blocknums": json.dumps(right_ctx.nearby_start_blocknums),
                "manual_accept": "",
                "notes": "",
            }
        )

    return pd.DataFrame(proposals).sort_values(
        ["score", "gap_seconds"], ascending=[False, True]
    ).reset_index(drop=True)


def build_cluster_dataframe(clusters: list[MarkCluster]) -> pd.DataFrame:
    if not clusters:
        return pd.DataFrame()

    return pd.DataFrame([cluster.to_record() for cluster in clusters])


def build_context_dataframe(contexts: list[ClusterContext]) -> pd.DataFrame:
    if not contexts:
        return pd.DataFrame()

    return pd.DataFrame([context.to_record() for context in contexts])


def build_review_template_df(cluster_df: pd.DataFrame) -> pd.DataFrame:
    if cluster_df.empty:
        return pd.DataFrame()

    review_df = cluster_df[
        [
            "cluster_id",
            "start_time",
            "end_time",
            "duration_seconds",
            "n_marks",
            "gap_before_seconds",
            "gap_after_seconds",
            "auto_label",
            "auto_confidence",
        ]
    ].copy()
    review_df["manual_label"] = "unknown"
    review_df["review_status"] = "proposed"
    review_df["action"] = ""
    review_df["split_after_mark_index"] = ""
    review_df["merge_with_next"] = ""
    review_df["notes"] = ""
    return review_df


def write_json_export(
    output_path: Path,
    clusters: list[MarkCluster],
    contexts: list[ClusterContext],
    proposed_chunks_df: pd.DataFrame,
) -> None:
    payload = {
        "clusters": [cluster.to_record() for cluster in clusters],
        "contexts": [context.to_record() for context in contexts],
        "proposed_chunk_pairs": proposed_chunks_df.to_dict(orient="records"),
    }
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def ensure_output_dir(path_str: str) -> Path:
    path = Path(path_str)
    path.mkdir(parents=True, exist_ok=True)
    return path


def print_summary(
    clusters: list[MarkCluster],
    proposed_chunks_df: pd.DataFrame,
    output_dir: Path,
) -> None:
    print(f"Detected Mark clusters: {len(clusters)}")
    if clusters:
        counts = pd.Series([c.n_marks for c in clusters]).value_counts().sort_index()
        print("Cluster size distribution:")
        for n_marks, count in counts.items():
            print(f"  n_marks={n_marks}: {count}")
    print(f"Proposed adjacent boundary pairs: {len(proposed_chunks_df)}")
    print(f"Exports written to: {output_dir}")


def main() -> None:
    args = parse_args()

    df = load_and_prepare_dataframe(
        csv_path=args.input,
        time_col=args.time_col,
        event_col=args.event_col,
        block_col=args.block_col,
        timestamp_format=args.timestamp_format,
    )

    clusters = cluster_marks(
        df=df,
        time_col=args.time_col,
        event_col=args.event_col,
        mark_gap_seconds=args.mark_gap_seconds,
    )

    apply_soft_labels(
        clusters=clusters,
        boundary_pair_max_seconds=args.boundary_pair_max_seconds,
    )

    contexts = build_cluster_context(
        df=df,
        clusters=clusters,
        time_col=args.time_col,
        event_col=args.event_col,
        block_col=args.block_col,
        context_window_seconds=args.context_window_seconds,
    )

    proposed_chunks_df = propose_chunk_pairs(
        clusters=clusters,
        contexts=contexts,
        boundary_pair_max_seconds=args.boundary_pair_max_seconds,
    )

    cluster_df = build_cluster_dataframe(clusters)
    context_df = build_context_dataframe(contexts)
    review_df = build_review_template_df(cluster_df)

    output_dir = ensure_output_dir(args.output_dir)

    cluster_csv = output_dir / "mark_clusters.csv"
    context_csv = output_dir / "mark_cluster_context.csv"
    proposed_csv = output_dir / "proposed_chunks.csv"
    review_csv = output_dir / "mark_cluster_review_template.csv"
    json_path = output_dir / "mark_clusters.json"

    cluster_df.to_csv(cluster_csv, index=False)
    context_df.to_csv(context_csv, index=False)
    proposed_chunks_df.to_csv(proposed_csv, index=False)
    review_df.to_csv(review_csv, index=False)
    write_json_export(json_path, clusters, contexts, proposed_chunks_df)

    print_summary(clusters, proposed_chunks_df, output_dir)


if __name__ == "__main__":
    main()