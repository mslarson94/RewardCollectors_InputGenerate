from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv, write_json, parse_list_cell
from temporal_alignment.common.timestamps import parse_times, assert_monotonic


def _bool_series(df, col):
    if col not in df:
        return pd.Series(False, index=df.index)
    return df[col].fillna(False).astype(bool)


def _reason_series(df, col):
    if col not in df:
        return pd.Series("", index=df.index, dtype="string")
    return df[col].fillna("").astype(str)


def build_ml(args):
    src = read_csv(args.corrected_ml_marks)
    for c in [args.ml_id_col, args.ml_time_col]:
        if c not in src:
            raise KeyError(f"Corrected ML marks missing {c!r}")

    out = pd.DataFrame({
        "session_id": args.session_id,
        "label": args.label,
        "ml_mark_id": src[args.ml_id_col].astype(str),
        "ml_ordinal": (
            pd.to_numeric(src[args.ml_ordinal_col], errors="raise").astype(int)
            if args.ml_ordinal_col and args.ml_ordinal_col in src
            else np.arange(len(src), dtype=int)
        ),
        "corrected_ml_time": parse_times(src[args.ml_time_col], args.ml_time_col),
        "source_row_index": (
            pd.to_numeric(src[args.ml_source_row_col], errors="coerce")
            if args.ml_source_row_col and args.ml_source_row_col in src
            else np.nan
        ),
        "block": src[args.ml_block_col] if args.ml_block_col and args.ml_block_col in src else np.nan,
        "ml_excluded": _bool_series(src, args.ml_exclude_col),
        "ml_exclusion_reason": _reason_series(src, args.ml_reason_col),
    })

    if out["ml_mark_id"].duplicated().any():
        raise ValueError("Duplicate ml_mark_id values")
    if out["ml_ordinal"].duplicated().any():
        raise ValueError("Duplicate ml_ordinal values")
    by_ord = out.sort_values("ml_ordinal").reset_index(drop=True)
    assert_monotonic(by_ord["corrected_ml_time"], "corrected ML mark times", strict=True)
    return out


def attach_clusters(ml: pd.DataFrame, clusters_path: str | None, chunks_path: str | None):
    if not clusters_path:
        return ml, pd.DataFrame(), pd.DataFrame()

    cdf = read_csv(clusters_path)
    long_rows = []
    for _, row in cdf.iterrows():
        mark_ids = parse_list_cell(row.get("mark_event_ids"))
        source_rows = parse_list_cell(row.get("source_row_indices"))
        n = max(len(mark_ids), len(source_rows))
        if n == 0:
            continue
        if mark_ids and len(mark_ids) != n:
            raise ValueError(f"{row['cluster_id']}: mark_event_ids length mismatch")
        if source_rows and len(source_rows) != n:
            raise ValueError(f"{row['cluster_id']}: source_row_indices length mismatch")
        for pos in range(n):
            long_rows.append({
                "cluster_id": row["cluster_id"],
                "cluster_position_legacy": pos + 1,
                "cluster_mark_id": mark_ids[pos] if mark_ids else "",
                "source_row_index": source_rows[pos] if source_rows else np.nan,
                "cluster_label": row.get("manual_label", row.get("auto_label", "")),
                "cluster_review_status": row.get("review_status", ""),
            })
    clong = pd.DataFrame(long_rows)
    if clong.empty:
        return ml, clong, pd.DataFrame()

    # Prefer source-row provenance when available on both sides; otherwise use ordinal position.
    if ml["source_row_index"].notna().any() and clong["source_row_index"].notna().any():
        if clong["source_row_index"].duplicated().any():
            raise ValueError("Cluster source_row_indices are not unique")
        out = ml.merge(
            clong[["source_row_index","cluster_id","cluster_mark_id","cluster_label","cluster_review_status"]],
            on="source_row_index", how="left", validate="one_to_one"
        )
    else:
        # Fresh cluster review files normally assign m1,m2,... in mark order.
        clong = clong.reset_index(drop=True)
        clong["ml_ordinal"] = np.arange(len(clong))
        out = ml.merge(
            clong[["ml_ordinal","cluster_id","cluster_mark_id","cluster_label","cluster_review_status"]],
            on="ml_ordinal", how="left", validate="one_to_one"
        )

    out["cluster_position"] = (
        out.sort_values(["cluster_id","ml_ordinal"])
           .groupby("cluster_id", dropna=True)
           .cumcount()
           .add(1)
           .reindex(out.index)
    )

    chunk_df = read_csv(chunks_path) if chunks_path else pd.DataFrame()
    if not chunk_df.empty:
        cluster_to_chunk = {}
        cluster_to_role = {}
        for _, ch in chunk_df.iterrows():
            cid = ch["chunk_id"]
            sc, ec = ch["start_cluster_id"], ch["end_cluster_id"]
            cluster_to_chunk[sc] = cid
            cluster_to_role[sc] = "start"
            cluster_to_chunk[ec] = cid
            cluster_to_role[ec] = "end"
            for pc in parse_list_cell(ch.get("pause_cluster_ids")):
                cluster_to_chunk[pc] = cid
                cluster_to_role[pc] = "pause"
        out["chunk_id"] = out["cluster_id"].map(cluster_to_chunk)
        out["chunk_role"] = out["cluster_id"].map(cluster_to_role).fillna("none")
    else:
        out["chunk_id"] = pd.NA
        out["chunk_role"] = "none"

    return out, clong, chunk_df


def build_rpi(args):
    src = read_csv(args.rpi_marks)
    for c in [args.rpi_id_col, args.rpi_time_col]:
        if c not in src:
            raise KeyError(f"RPi marks missing {c!r}")
    out = pd.DataFrame({
        "session_id": args.session_id,
        "label": args.label,
        "rpi_mark_id": src[args.rpi_id_col].astype(str),
        "rpi_ordinal": (
            pd.to_numeric(src[args.rpi_ordinal_col], errors="raise").astype(int)
            if args.rpi_ordinal_col and args.rpi_ordinal_col in src
            else np.arange(len(src), dtype=int)
        ),
        "rpi_time": parse_times(src[args.rpi_time_col], args.rpi_time_col),
        "rpi_timestamp_source": (
            src[args.rpi_source_col].fillna("").astype(str)
            if args.rpi_source_col and args.rpi_source_col in src else ""
        ),
        "rpi_excluded": _bool_series(src, args.rpi_exclude_col),
        "rpi_exclusion_reason": _reason_series(src, args.rpi_reason_col),
    })
    if out["rpi_mark_id"].duplicated().any():
        raise ValueError("Duplicate rpi_mark_id values")
    if out["rpi_ordinal"].duplicated().any():
        raise ValueError("Duplicate rpi_ordinal values")
    assert_monotonic(out.sort_values("rpi_ordinal")["rpi_time"], "RPi mark times", strict=True)
    return out


def main():
    ap = argparse.ArgumentParser(description="Build canonical ML/RPi mark inventories.")
    ap.add_argument("--corrected-ml-marks", required=True)
    ap.add_argument("--rpi-marks", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--session-id", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--finalized-clusters", default="")
    ap.add_argument("--finalized-chunks", default="")

    ap.add_argument("--ml-id-col", default="mark_id")
    ap.add_argument("--ml-time-col", default="corrected_ml_time")
    ap.add_argument("--ml-ordinal-col", default="ordinal")
    ap.add_argument("--ml-source-row-col", default="source_row_index")
    ap.add_argument("--ml-block-col", default="block")
    ap.add_argument("--ml-exclude-col", default="exclude")
    ap.add_argument("--ml-reason-col", default="reason")

    ap.add_argument("--rpi-id-col", default="mark_id")
    ap.add_argument("--rpi-time-col", default="mark_time")
    ap.add_argument("--rpi-ordinal-col", default="ordinal")
    ap.add_argument("--rpi-source-col", default="RPi_Timestamp_Source")
    ap.add_argument("--rpi-exclude-col", default="exclude")
    ap.add_argument("--rpi-reason-col", default="reason")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    ml = build_ml(args)
    ml, cluster_long, chunks = attach_clusters(
        ml, args.finalized_clusters or None, args.finalized_chunks or None
    )
    rpi = build_rpi(args)

    write_csv(ml, out_dir / "canonical_ml_marks.csv")
    write_csv(rpi, out_dir / "canonical_rpi_marks.csv")
    if not cluster_long.empty:
        write_csv(cluster_long, out_dir / "cluster_membership_legacy.csv")
    if not chunks.empty:
        write_csv(chunks, out_dir / "chunk_structure_source.csv")

    report = {
        "session_id": args.session_id,
        "label": args.label,
        "n_ml_marks": len(ml),
        "n_rpi_marks": len(rpi),
        "ml_times_monotonic": True,
        "rpi_times_monotonic": True,
        "n_clustered_ml_marks": int(ml["cluster_id"].notna().sum()) if "cluster_id" in ml else 0,
        "n_chunks": int(chunks.shape[0]),
    }
    write_json(report, out_dir / "inventory_validation.json")
    print(f"[ok] master inventory -> {out_dir}")


if __name__ == "__main__":
    main()
