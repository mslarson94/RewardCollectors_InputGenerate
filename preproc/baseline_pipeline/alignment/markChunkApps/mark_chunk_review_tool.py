# path: tools/mark_chunk_review_tool.py

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


MARK_EVENT_TYPE = "Mark"
START_EVENT_TYPES = {"BlockStart", "RoundStart"}
END_EVENT_TYPES = {"BlockEnd"}


@dataclass
class MarkEvent:
    mark_event_id: str
    source_row_index: int
    time: pd.Timestamp

    def to_dict(self) -> dict[str, Any]:
        return {
            "mark_event_id": self.mark_event_id,
            "source_row_index": self.source_row_index,
            "time": self.time.isoformat(),
        }


@dataclass
class Cluster:
    cluster_id: str
    mark_event_ids: list[str]
    source_row_indices: list[int]
    mark_times: list[str]
    start_time: pd.Timestamp
    end_time: pd.Timestamp
    duration_seconds: float
    n_marks: int
    gap_before_seconds: float | None = None
    gap_after_seconds: float | None = None
    prev_cluster_id: str | None = None
    next_cluster_id: str | None = None
    auto_label: str = "unknown"
    auto_confidence: float = 0.0
    manual_label: str = "unknown"
    review_status: str = "proposed"
    action: str = ""
    split_after_mark_index: int | None = None
    merge_with_next: bool = False
    notes: str = ""
    parent_cluster_id: str | None = None
    edit_history: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["start_time"] = self.start_time.isoformat()
        record["end_time"] = self.end_time.isoformat()
        record["mark_event_ids"] = json.dumps(self.mark_event_ids)
        record["source_row_indices"] = json.dumps(self.source_row_indices)
        record["mark_times"] = json.dumps(self.mark_times)
        record["edit_history"] = json.dumps(self.edit_history)
        return record


@dataclass
class ClusterContext:
    cluster_id: str
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


def parse_time_series(
    series: pd.Series,
    timestamp_format: str | None = None,
) -> pd.Series:
    if timestamp_format:
        return pd.to_datetime(series, format=timestamp_format, errors="coerce")
    return pd.to_datetime(series, errors="coerce")


def parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    if pd.isna(value) or value == "":
        return []
    return json.loads(value)


def seconds_between(a: pd.Timestamp, b: pd.Timestamp) -> float:
    return float((b - a).total_seconds())


def load_source_dataframe(
    input_csv: str,
    time_col: str,
    event_col: str,
    block_col: str,
    timestamp_format: str | None = None,
) -> pd.DataFrame:
    df = pd.read_csv(input_csv)
    required = [time_col, event_col]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if block_col not in df.columns:
        df[block_col] = pd.NA

    df = df.copy()
    df["_source_row_index"] = df.index
    df[time_col] = parse_time_series(df[time_col], timestamp_format=timestamp_format)

    invalid_count = int(df[time_col].isna().sum())
    if invalid_count:
        raise ValueError(f"Found {invalid_count} rows with invalid timestamps in {time_col}.")

    df = df.sort_values([time_col, "_source_row_index"]).reset_index(drop=True)
    return df


def build_mark_events(
    df: pd.DataFrame,
    time_col: str,
    event_col: str,
) -> list[MarkEvent]:
    mark_df = df[df[event_col] == MARK_EVENT_TYPE].copy().reset_index(drop=True)
    events: list[MarkEvent] = []

    for i, row in mark_df.iterrows():
        events.append(
            MarkEvent(
                mark_event_id=f"m{i + 1}",
                source_row_index=int(row["_source_row_index"]),
                time=row[time_col],
            )
        )
    return events


def cluster_mark_events(
    mark_events: list[MarkEvent],
    mark_gap_seconds: int,
) -> list[Cluster]:
    if not mark_events:
        return []

    raw_groups: list[list[MarkEvent]] = []
    current_group: list[MarkEvent] = [mark_events[0]]

    for event in mark_events[1:]:
        previous = current_group[-1]
        gap = seconds_between(previous.time, event.time)
        if gap <= mark_gap_seconds:
            current_group.append(event)
        else:
            raw_groups.append(current_group)
            current_group = [event]
    raw_groups.append(current_group)

    clusters: list[Cluster] = []
    for i, group in enumerate(raw_groups, start=1):
        start_time = group[0].time
        end_time = group[-1].time
        clusters.append(
            Cluster(
                cluster_id=f"c{i}",
                mark_event_ids=[e.mark_event_id for e in group],
                source_row_indices=[e.source_row_index for e in group],
                mark_times=[e.time.isoformat() for e in group],
                start_time=start_time,
                end_time=end_time,
                duration_seconds=seconds_between(start_time, end_time),
                n_marks=len(group),
            )
        )

    relink_clusters(clusters)
    apply_soft_labels(clusters)
    return clusters


def relink_clusters(clusters: list[Cluster]) -> None:
    for i, cluster in enumerate(clusters):
        cluster.prev_cluster_id = clusters[i - 1].cluster_id if i > 0 else None
        cluster.next_cluster_id = clusters[i + 1].cluster_id if i < len(clusters) - 1 else None
        cluster.gap_before_seconds = (
            seconds_between(clusters[i - 1].end_time, cluster.start_time) if i > 0 else None
        )
        cluster.gap_after_seconds = (
            seconds_between(cluster.end_time, clusters[i + 1].start_time)
            if i < len(clusters) - 1
            else None
        )


def infer_soft_label(n_marks: int) -> tuple[str, float]:
    if n_marks <= 2:
        return "pause_like", 0.45
    if n_marks == 3:
        return "start_like", 0.50
    if n_marks >= 4:
        return "end_like", 0.50
    return "unknown", 0.0


def apply_soft_labels(clusters: list[Cluster]) -> None:
    for cluster in clusters:
        cluster.auto_label, cluster.auto_confidence = infer_soft_label(cluster.n_marks)


def nearest_event_record(
    midpoint: pd.Timestamp,
    candidate_df: pd.DataFrame,
    time_col: str,
    event_col: str,
    block_col: str,
) -> tuple[str | None, str | None, Any | None, float | None]:
    if candidate_df.empty:
        return None, None, None, None

    deltas = candidate_df[time_col].apply(lambda t: abs((t - midpoint).total_seconds()))
    nearest_idx = deltas.idxmin()
    row = candidate_df.loc[nearest_idx]
    signed_delta = float((row[time_col] - midpoint).total_seconds())
    return row[time_col].isoformat(), str(row[event_col]), row[block_col], signed_delta


def summarize_nearby_events(
    nearby_df: pd.DataFrame,
    time_col: str,
    event_col: str,
    block_col: str,
    limit: int = 16,
) -> str:
    if nearby_df.empty:
        return ""

    nearby_df = nearby_df.sort_values([time_col, "_source_row_index"]).head(limit)
    parts: list[str] = []
    for _, row in nearby_df.iterrows():
        block_value = "NA" if pd.isna(row[block_col]) else str(row[block_col])
        parts.append(f"{row[time_col].isoformat()} | {row[event_col]} | BlockNum={block_value}")
    return " || ".join(parts)


def build_cluster_contexts(
    df: pd.DataFrame,
    clusters: list[Cluster],
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

        nearby = df[(df[time_col] >= window_start) & (df[time_col] <= window_end)].copy()
        nearby_non_mark = nearby[nearby[event_col] != MARK_EVENT_TYPE].copy()
        nearby_start = nearby_non_mark[nearby_non_mark[event_col].isin(START_EVENT_TYPES)].copy()
        nearby_end = nearby_non_mark[nearby_non_mark[event_col].isin(END_EVENT_TYPES)].copy()

        nearest_start = nearest_event_record(midpoint, nearby_start, time_col, event_col, block_col)
        nearest_end = nearest_event_record(midpoint, nearby_end, time_col, event_col, block_col)

        start_blocknums = sorted(
            {value for value in nearby_start[block_col].dropna().tolist()},
            key=lambda x: str(x),
        )
        end_blocknums = sorted(
            {value for value in nearby_end[block_col].dropna().tolist()},
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
                    nearby_non_mark,
                    time_col=time_col,
                    event_col=event_col,
                    block_col=block_col,
                ),
            )
        )

    return contexts


def cluster_records_to_dataframe(clusters: list[Cluster]) -> pd.DataFrame:
    return pd.DataFrame([cluster.to_record() for cluster in clusters]) if clusters else pd.DataFrame()


def context_records_to_dataframe(contexts: list[ClusterContext]) -> pd.DataFrame:
    return pd.DataFrame([context.to_record() for context in contexts]) if contexts else pd.DataFrame()


def build_review_template(clusters: list[Cluster]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for cluster in clusters:
        rows.append(
            {
                "cluster_id": cluster.cluster_id,
                "manual_label": "unknown",
                "review_status": "proposed",
                "action": "",
                "split_after_mark_index": "",
                "merge_with_next": "",
                "notes": "",
            }
        )
    return pd.DataFrame(rows)


def propose_adjacent_boundaries(
    clusters: list[Cluster],
    contexts: list[ClusterContext],
    boundary_pair_max_seconds: int,
) -> pd.DataFrame:
    context_by_id = {ctx.cluster_id: ctx for ctx in contexts}
    proposals: list[dict[str, Any]] = []

    for i in range(len(clusters) - 1):
        left = clusters[i]
        right = clusters[i + 1]

        if left.gap_after_seconds is None or left.gap_after_seconds > boundary_pair_max_seconds:
            continue

        left_ctx = context_by_id[left.cluster_id]
        right_ctx = context_by_id[right.cluster_id]

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
                "proposal_id": f"p{len(proposals) + 1}",
                "end_cluster_id": left.cluster_id,
                "start_cluster_id": right.cluster_id,
                "gap_seconds": left.gap_after_seconds,
                "score": score,
                "reasons": ",".join(reasons),
                "manual_accept": "",
                "notes": "",
            }
        )

    if not proposals:
        return pd.DataFrame(
            columns=[
                "proposal_id",
                "end_cluster_id",
                "start_cluster_id",
                "gap_seconds",
                "score",
                "reasons",
                "manual_accept",
                "notes",
            ]
        )

    return pd.DataFrame(proposals).sort_values(
        ["score", "gap_seconds"],
        ascending=[False, True],
    ).reset_index(drop=True)


def ensure_dir(path_str: str | Path) -> Path:
    path = Path(path_str)
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_proposal_bundle(
    output_dir: str | Path,
    clusters: list[Cluster],
    contexts: list[ClusterContext],
    proposed_boundaries: pd.DataFrame,
    review_template: pd.DataFrame,
) -> None:
    output_path = ensure_dir(output_dir)

    cluster_df = cluster_records_to_dataframe(clusters)
    context_df = context_records_to_dataframe(contexts)

    cluster_df.to_csv(output_path / "mark_clusters.csv", index=False)
    context_df.to_csv(output_path / "mark_cluster_context.csv", index=False)
    proposed_boundaries.to_csv(output_path / "proposed_chunks.csv", index=False)
    review_template.to_csv(output_path / "mark_cluster_review_template.csv", index=False)

    bundle = {
        "clusters": cluster_df.to_dict(orient="records"),
        "contexts": context_df.to_dict(orient="records"),
        "proposed_chunks": proposed_boundaries.to_dict(orient="records"),
        "review_template": review_template.to_dict(orient="records"),
    }
    (output_path / "proposal_bundle.json").write_text(
        json.dumps(bundle, indent=2),
        encoding="utf-8",
    )


def load_clusters_from_csv(cluster_csv: str) -> list[Cluster]:
    df = pd.read_csv(cluster_csv)
    clusters: list[Cluster] = []

    for _, row in df.iterrows():
        clusters.append(
            Cluster(
                cluster_id=str(row["cluster_id"]),
                mark_event_ids=parse_json_list(row["mark_event_ids"]),
                source_row_indices=[int(v) for v in parse_json_list(row["source_row_indices"])],
                mark_times=[str(v) for v in parse_json_list(row.get("mark_times", "[]"))],
                start_time=pd.to_datetime(row["start_time"]),
                end_time=pd.to_datetime(row["end_time"]),
                duration_seconds=float(row["duration_seconds"]),
                n_marks=int(row["n_marks"]),
                gap_before_seconds=None if pd.isna(row["gap_before_seconds"]) else float(row["gap_before_seconds"]),
                gap_after_seconds=None if pd.isna(row["gap_after_seconds"]) else float(row["gap_after_seconds"]),
                prev_cluster_id=None if pd.isna(row["prev_cluster_id"]) else str(row["prev_cluster_id"]),
                next_cluster_id=None if pd.isna(row["next_cluster_id"]) else str(row["next_cluster_id"]),
                auto_label=str(row.get("auto_label", "unknown")),
                auto_confidence=float(row.get("auto_confidence", 0.0)),
                manual_label=str(row.get("manual_label", "unknown")),
                review_status=str(row.get("review_status", "proposed")),
                action="" if pd.isna(row.get("action", "")) else str(row.get("action", "")),
                split_after_mark_index=None
                if pd.isna(row.get("split_after_mark_index", ""))
                else int(row.get("split_after_mark_index")),
                merge_with_next=False
                if pd.isna(row.get("merge_with_next", ""))
                else str(row.get("merge_with_next")).strip().lower() in {"1", "true", "yes", "y"},
                notes="" if pd.isna(row.get("notes", "")) else str(row.get("notes", "")),
                parent_cluster_id=None if pd.isna(row.get("parent_cluster_id", None)) else str(row.get("parent_cluster_id")),
                edit_history=parse_json_list(row.get("edit_history", "[]")),
            )
        )

    relink_clusters(clusters)
    return clusters


def load_review_edits(review_csv: str) -> pd.DataFrame:
    review_df = pd.read_csv(review_csv, dtype=str).fillna("")
    required = {
        "cluster_id",
        "manual_label",
        "review_status",
        "action",
        "split_after_mark_index",
        "merge_with_next",
        "notes",
    }
    missing = required - set(review_df.columns)
    if missing:
        raise ValueError(f"Review CSV missing required columns: {sorted(missing)}")
    return review_df


def apply_review_values_to_clusters(
    clusters: list[Cluster],
    review_df: pd.DataFrame,
) -> list[Cluster]:
    review_by_id = {str(row["cluster_id"]): row for _, row in review_df.iterrows()}

    updated: list[Cluster] = []
    for cluster in clusters:
        clone = Cluster(**asdict(cluster))
        row = review_by_id.get(clone.cluster_id)
        if row is not None:
            clone.manual_label = row["manual_label"].strip() or "unknown"
            clone.review_status = row["review_status"].strip() or "proposed"
            clone.action = row["action"].strip()
            split_value = row["split_after_mark_index"].strip()
            clone.split_after_mark_index = int(split_value) if split_value else None
            merge_value = row["merge_with_next"].strip().lower()
            clone.merge_with_next = merge_value in {"1", "true", "yes", "y"}
            clone.notes = row["notes"].strip()
        updated.append(clone)

    relink_clusters(updated)
    return updated


def cluster_from_mark_ids(
    cluster_id: str,
    mark_event_ids: list[str],
    source_row_indices: list[int],
    mark_time_by_id: dict[str, pd.Timestamp],
    parent_cluster_id: str | None,
    history_note: str,
    template: Cluster | None = None,
) -> Cluster:
    ordered_mark_times = [mark_time_by_id[mark_id] for mark_id in mark_event_ids]
    start_time = ordered_mark_times[0]
    end_time = ordered_mark_times[-1]

    cluster = Cluster(
        cluster_id=cluster_id,
        mark_event_ids=mark_event_ids,
        source_row_indices=source_row_indices,
        mark_times=[t.isoformat() for t in ordered_mark_times],
        start_time=start_time,
        end_time=end_time,
        duration_seconds=seconds_between(start_time, end_time),
        n_marks=len(mark_event_ids),
        auto_label=template.auto_label if template else "unknown",
        auto_confidence=template.auto_confidence if template else 0.0,
        manual_label=template.manual_label if template else "unknown",
        review_status=template.review_status if template else "edited",
        action=template.action if template else "",
        split_after_mark_index=None,
        merge_with_next=False,
        notes=template.notes if template else "",
        parent_cluster_id=parent_cluster_id,
        edit_history=(template.edit_history[:] if template else []) + [history_note],
    )
    return cluster


def split_clusters(
    clusters: list[Cluster],
    mark_time_by_id: dict[str, pd.Timestamp],
) -> list[Cluster]:
    result: list[Cluster] = []
    generated_count = 1

    for cluster in clusters:
        should_split = cluster.action == "split" or cluster.split_after_mark_index is not None
        split_idx = cluster.split_after_mark_index
        valid_split = (
            should_split
            and split_idx is not None
            and 1 <= split_idx < cluster.n_marks
        )

        if not valid_split:
            cluster.edit_history.append("kept_unsplit")
            result.append(cluster)
            continue

        left_mark_ids = cluster.mark_event_ids[:split_idx]
        right_mark_ids = cluster.mark_event_ids[split_idx:]
        left_source = cluster.source_row_indices[:split_idx]
        right_source = cluster.source_row_indices[split_idx:]

        left = cluster_from_mark_ids(
            cluster_id=f"e{generated_count}",
            mark_event_ids=left_mark_ids,
            source_row_indices=left_source,
            mark_time_by_id=mark_time_by_id,
            parent_cluster_id=cluster.cluster_id,
            history_note=f"split_from:{cluster.cluster_id}:left",
            template=cluster,
        )
        generated_count += 1

        right = cluster_from_mark_ids(
            cluster_id=f"e{generated_count}",
            mark_event_ids=right_mark_ids,
            source_row_indices=right_source,
            mark_time_by_id=mark_time_by_id,
            parent_cluster_id=cluster.cluster_id,
            history_note=f"split_from:{cluster.cluster_id}:right",
            template=cluster,
        )
        generated_count += 1

        result.extend([left, right])

    relink_clusters(result)
    apply_soft_labels(result)
    return result


def merge_clusters(
    clusters: list[Cluster],
    mark_time_by_id: dict[str, pd.Timestamp],
) -> list[Cluster]:
    result: list[Cluster] = []
    i = 0
    generated_count = 100000

    while i < len(clusters):
        current = clusters[i]
        should_merge = (current.merge_with_next or current.action == "merge_next") and i < len(clusters) - 1

        if not should_merge:
            current.edit_history.append("kept_unmerged")
            result.append(current)
            i += 1
            continue

        nxt = clusters[i + 1]
        merged_mark_ids = current.mark_event_ids + nxt.mark_event_ids
        merged_source_indices = current.source_row_indices + nxt.source_row_indices

        merged = cluster_from_mark_ids(
            cluster_id=f"e{generated_count}",
            mark_event_ids=merged_mark_ids,
            source_row_indices=merged_source_indices,
            mark_time_by_id=mark_time_by_id,
            parent_cluster_id=f"{current.cluster_id}+{nxt.cluster_id}",
            history_note=f"merged_from:{current.cluster_id},{nxt.cluster_id}",
            template=current,
        )

        if current.manual_label != "unknown":
            merged.manual_label = current.manual_label
        elif nxt.manual_label != "unknown":
            merged.manual_label = nxt.manual_label
        else:
            merged.manual_label = "unknown"

        merged.notes = " | ".join([value for value in [current.notes, nxt.notes] if value]).strip()
        merged.review_status = "edited"
        generated_count += 1
        result.append(merged)
        i += 2

    relink_clusters(result)
    apply_soft_labels(result)
    return result


def build_mark_time_lookup_from_clusters(
    clusters: list[Cluster],
) -> dict[str, pd.Timestamp]:
    lookup: dict[str, pd.Timestamp] = {}
    for cluster in clusters:
        if cluster.mark_times and len(cluster.mark_times) == len(cluster.mark_event_ids):
            for mark_id, mark_time in zip(cluster.mark_event_ids, cluster.mark_times):
                lookup[mark_id] = pd.to_datetime(mark_time)
            continue

        if not cluster.mark_event_ids:
            continue

        if len(cluster.mark_event_ids) == 1:
            lookup[cluster.mark_event_ids[0]] = cluster.start_time
            continue

        for i, mark_id in enumerate(cluster.mark_event_ids):
            if i == 0:
                lookup[mark_id] = cluster.start_time
            elif i == len(cluster.mark_event_ids) - 1:
                lookup[mark_id] = cluster.end_time
            else:
                total = len(cluster.mark_event_ids) - 1
                fraction = i / total
                interpolated = cluster.start_time + (cluster.end_time - cluster.start_time) * fraction
                lookup[mark_id] = interpolated
    return lookup


def apply_review_edits(
    cluster_csv: str,
    review_csv: str,
) -> list[Cluster]:
    original_clusters = load_clusters_from_csv(cluster_csv)
    review_df = load_review_edits(review_csv)
    staged_clusters = apply_review_values_to_clusters(original_clusters, review_df)
    mark_time_by_id = build_mark_time_lookup_from_clusters(original_clusters)

    split_result = split_clusters(staged_clusters, mark_time_by_id)
    merge_result = merge_clusters(split_result, mark_time_by_id)

    for cluster in merge_result:
        cluster.gap_before_seconds = None
        cluster.gap_after_seconds = None
    relink_clusters(merge_result)
    apply_soft_labels(merge_result)
    return merge_result


def cluster_label(cluster: Cluster) -> str:
    label = (cluster.manual_label or "").strip().lower()
    if label in {"start", "end", "pause", "ignore", "unknown"}:
        return label
    return "unknown"


def parse_context_lists(context_df: pd.DataFrame) -> pd.DataFrame:
    if context_df.empty:
        return context_df
    out = context_df.copy()
    for col in ["nearby_start_blocknums", "nearby_end_blocknums"]:
        if col in out.columns:
            out[col] = out[col].apply(parse_json_list)
    return out


def build_context_lookup(context_csv: str) -> dict[str, dict[str, Any]]:
    if not Path(context_csv).exists():
        return {}
    context_df = pd.read_csv(context_csv)
    context_df = parse_context_lists(context_df)
    return {
        str(row["cluster_id"]): row.to_dict()
        for _, row in context_df.iterrows()
    }


def build_finalized_chunks(
    clusters: list[Cluster],
    context_lookup: dict[str, dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    chunk_rows: list[dict[str, Any]] = []
    chunk_event_rows: list[dict[str, Any]] = []
    anomaly_rows: list[dict[str, Any]] = []

    open_chunk: dict[str, Any] | None = None
    chunk_count = 1

    for cluster in clusters:
        label = cluster_label(cluster)
        context = context_lookup.get(cluster.cluster_id, {})
        start_blocknums = context.get("nearby_start_blocknums", [])
        end_blocknums = context.get("nearby_end_blocknums", [])

        if label in {"ignore", "unknown"}:
            anomaly_rows.append(
                {
                    "cluster_id": cluster.cluster_id,
                    "issue": "ignored_or_unknown",
                    "manual_label": label,
                    "cluster_time": cluster.start_time.isoformat(),
                    "notes": cluster.notes,
                }
            )
            continue

        if label == "start":
            if open_chunk is not None:
                anomaly_rows.append(
                    {
                        "cluster_id": cluster.cluster_id,
                        "issue": "start_while_chunk_open",
                        "manual_label": label,
                        "cluster_time": cluster.start_time.isoformat(),
                        "notes": cluster.notes,
                    }
                )

            open_chunk = {
                "chunk_id": f"chunk_{chunk_count}",
                "start_cluster_id": cluster.cluster_id,
                "start_time": cluster.start_time,
                "end_cluster_id": None,
                "end_time": None,
                "pause_cluster_ids": [],
                "start_blocknums": start_blocknums,
                "end_blocknums": [],
                "notes": cluster.notes,
            }
            chunk_event_rows.append(
                {
                    "chunk_id": open_chunk["chunk_id"],
                    "cluster_id": cluster.cluster_id,
                    "event_label": "start",
                    "time": cluster.start_time.isoformat(),
                }
            )
            chunk_count += 1
            continue

        if label == "pause":
            if open_chunk is None:
                anomaly_rows.append(
                    {
                        "cluster_id": cluster.cluster_id,
                        "issue": "pause_without_open_chunk",
                        "manual_label": label,
                        "cluster_time": cluster.start_time.isoformat(),
                        "notes": cluster.notes,
                    }
                )
            else:
                open_chunk["pause_cluster_ids"].append(cluster.cluster_id)
                chunk_event_rows.append(
                    {
                        "chunk_id": open_chunk["chunk_id"],
                        "cluster_id": cluster.cluster_id,
                        "event_label": "pause",
                        "time": cluster.start_time.isoformat(),
                    }
                )
            continue

        if label == "end":
            if open_chunk is None:
                anomaly_rows.append(
                    {
                        "cluster_id": cluster.cluster_id,
                        "issue": "end_without_open_chunk",
                        "manual_label": label,
                        "cluster_time": cluster.start_time.isoformat(),
                        "notes": cluster.notes,
                    }
                )
            else:
                open_chunk["end_cluster_id"] = cluster.cluster_id
                open_chunk["end_time"] = cluster.end_time
                open_chunk["end_blocknums"] = end_blocknums
                chunk_event_rows.append(
                    {
                        "chunk_id": open_chunk["chunk_id"],
                        "cluster_id": cluster.cluster_id,
                        "event_label": "end",
                        "time": cluster.end_time.isoformat(),
                    }
                )

                chunk_rows.append(
                    {
                        "chunk_id": open_chunk["chunk_id"],
                        "start_cluster_id": open_chunk["start_cluster_id"],
                        "end_cluster_id": open_chunk["end_cluster_id"],
                        "start_time": open_chunk["start_time"].isoformat(),
                        "end_time": open_chunk["end_time"].isoformat(),
                        "duration_seconds": seconds_between(open_chunk["start_time"], open_chunk["end_time"]),
                        "pause_cluster_ids": json.dumps(open_chunk["pause_cluster_ids"]),
                        "start_blocknums": json.dumps(open_chunk["start_blocknums"]),
                        "end_blocknums": json.dumps(open_chunk["end_blocknums"]),
                        "notes": open_chunk["notes"],
                    }
                )
                open_chunk = None

    if open_chunk is not None:
        anomaly_rows.append(
            {
                "cluster_id": open_chunk["start_cluster_id"],
                "issue": "open_chunk_without_end",
                "manual_label": "start",
                "cluster_time": open_chunk["start_time"].isoformat(),
                "notes": open_chunk["notes"],
            }
        )

    return pd.DataFrame(chunk_rows), pd.DataFrame(chunk_event_rows), pd.DataFrame(anomaly_rows)


def write_finalized_outputs(
    output_dir: str | Path,
    finalized_clusters: list[Cluster],
    finalized_chunks: pd.DataFrame,
    chunk_events: pd.DataFrame,
    anomalies: pd.DataFrame,
) -> None:
    output_path = ensure_dir(output_dir)
    finalized_cluster_df = cluster_records_to_dataframe(finalized_clusters)

    finalized_cluster_df.to_csv(output_path / "finalized_clusters.csv", index=False)
    finalized_chunks.to_csv(output_path / "finalized_chunks.csv", index=False)
    chunk_events.to_csv(output_path / "chunk_events.csv", index=False)
    anomalies.to_csv(output_path / "chunk_anomalies.csv", index=False)

    payload = {
        "finalized_clusters": finalized_cluster_df.to_dict(orient="records"),
        "finalized_chunks": finalized_chunks.to_dict(orient="records"),
        "chunk_events": chunk_events.to_dict(orient="records"),
        "anomalies": anomalies.to_dict(orient="records"),
    }
    (output_path / "finalized_bundle.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )


def run_propose_mode(args: argparse.Namespace) -> None:
    df = load_source_dataframe(
        input_csv=args.input,
        time_col=args.time_col,
        event_col=args.event_col,
        block_col=args.block_col,
        timestamp_format=args.timestamp_format,
    )

    mark_events = build_mark_events(df, time_col=args.time_col, event_col=args.event_col)
    clusters = cluster_mark_events(mark_events, mark_gap_seconds=args.mark_gap_seconds)
    contexts = build_cluster_contexts(
        df=df,
        clusters=clusters,
        time_col=args.time_col,
        event_col=args.event_col,
        block_col=args.block_col,
        context_window_seconds=args.context_window_seconds,
    )
    proposed_boundaries = propose_adjacent_boundaries(
        clusters=clusters,
        contexts=contexts,
        boundary_pair_max_seconds=args.boundary_pair_max_seconds,
    )
    review_template = build_review_template(clusters)

    write_proposal_bundle(
        output_dir=args.output_dir,
        clusters=clusters,
        contexts=contexts,
        proposed_boundaries=proposed_boundaries,
        review_template=review_template,
    )

    print(f"Mark clusters: {len(clusters)}")
    print(f"Proposal files written to: {args.output_dir}")


def run_apply_mode(args: argparse.Namespace) -> None:
    finalized_clusters = apply_review_edits(
        cluster_csv=args.cluster_csv,
        review_csv=args.review_csv,
    )
    context_lookup = build_context_lookup(args.context_csv) if args.context_csv else {}
    finalized_chunks, chunk_events, anomalies = build_finalized_chunks(
        clusters=finalized_clusters,
        context_lookup=context_lookup,
    )

    write_finalized_outputs(
        output_dir=args.output_dir,
        finalized_clusters=finalized_clusters,
        finalized_chunks=finalized_chunks,
        chunk_events=chunk_events,
        anomalies=anomalies,
    )

    print(f"Finalized clusters: {len(finalized_clusters)}")
    print(f"Finalized chunks: {len(finalized_chunks)}")
    print(f"Outputs written to: {args.output_dir}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mark cluster proposal and chunk finalization tool.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    propose = subparsers.add_parser("propose", help="Build candidate Mark clusters and review files.")
    propose.add_argument("--input", required=True, help="Input CSV path.")
    propose.add_argument("--output-dir", required=True, help="Output directory.")
    propose.add_argument("--time-col", default="mLT_orig", help="Time column.")
    propose.add_argument("--event-col", default="lo_eventType", help="Event type column.")
    propose.add_argument("--block-col", default="BlockNum", help="Block number column.")
    propose.add_argument("--mark-gap-seconds", type=int, default=60, help="Gap threshold for Mark cluster splitting.")
    propose.add_argument("--context-window-seconds", type=int, default=300, help="Context window around each cluster.")
    propose.add_argument("--boundary-pair-max-seconds", type=int, default=300, help="Max adjacent boundary pairing gap.")
    propose.add_argument("--timestamp-format", default=None, help="Optional explicit datetime format.")

    apply_mode = subparsers.add_parser("apply-edits", help="Apply review edits and emit finalized chunks.")
    apply_mode.add_argument("--cluster-csv", required=True, help="mark_clusters.csv from propose mode.")
    apply_mode.add_argument("--review-csv", required=True, help="Edited review CSV.")
    apply_mode.add_argument("--context-csv", default="", help="mark_cluster_context.csv from propose mode.")
    apply_mode.add_argument("--output-dir", required=True, help="Output directory.")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "propose":
        run_propose_mode(args)
        return

    if args.command == "apply-edits":
        run_apply_mode(args)
        return


if __name__ == "__main__":
    main()