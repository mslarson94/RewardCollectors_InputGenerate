from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv, write_json
from temporal_alignment.common.timestamps import parse_times, assert_monotonic


def main():
    ap = argparse.ArgumentParser(
        description="Normalize an arbitrary corrected-ML mark export into the canonical mark input schema."
    )
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--id-col", default="")
    ap.add_argument("--time-col", required=True)
    ap.add_argument("--ordinal-col", default="")
    ap.add_argument("--source-row-col", default="")
    ap.add_argument("--block-col", default="")
    ap.add_argument("--exclude-col", default="")
    ap.add_argument("--reason-col", default="")
    ap.add_argument("--id-prefix", default="events_")
    ap.add_argument("--id-width", type=int, default=4)
    args = ap.parse_args()

    src = read_csv(args.input)
    if args.time_col not in src:
        raise KeyError(f"Input missing corrected timestamp column {args.time_col!r}")

    n = len(src)
    ordinal = (
        pd.to_numeric(src[args.ordinal_col], errors="raise").astype(int)
        if args.ordinal_col and args.ordinal_col in src
        else pd.Series(np.arange(n), index=src.index, dtype=int)
    )
    mark_id = (
        src[args.id_col].astype(str)
        if args.id_col and args.id_col in src
        else ordinal.map(lambda x: f"{args.id_prefix}{int(x):0{args.id_width}d}")
    )

    out = pd.DataFrame({
        "mark_id": mark_id,
        "ordinal": ordinal,
        "corrected_ml_time": parse_times(src[args.time_col], args.time_col),
        "source_row_index": (
            pd.to_numeric(src[args.source_row_col], errors="coerce")
            if args.source_row_col and args.source_row_col in src
            else np.nan
        ),
        "block": src[args.block_col] if args.block_col and args.block_col in src else np.nan,
        "exclude": (
            src[args.exclude_col].fillna(False).astype(bool)
            if args.exclude_col and args.exclude_col in src
            else False
        ),
        "reason": (
            src[args.reason_col].fillna("").astype(str)
            if args.reason_col and args.reason_col in src
            else ""
        ),
    })

    if out["mark_id"].duplicated().any():
        dup = out.loc[out["mark_id"].duplicated(keep=False), "mark_id"].tolist()
        raise ValueError(f"Duplicate mark IDs: {dup[:10]}")
    if out["ordinal"].duplicated().any():
        raise ValueError("Duplicate mark ordinals")

    ordered = out.sort_values("ordinal").reset_index(drop=True)
    assert_monotonic(ordered["corrected_ml_time"], "corrected ML timestamps", strict=True)

    write_csv(ordered, args.out)
    write_json({
        "n_marks": len(ordered),
        "time_col_source": args.time_col,
        "generated_ids": not bool(args.id_col),
        "generated_ordinals": not bool(args.ordinal_col),
        "has_source_row_index": bool(args.source_row_col),
        "monotonic": True,
    }, Path(args.out).with_suffix(".validation.json"))
    print(f"[ok] normalized corrected ML marks -> {args.out}")


if __name__ == "__main__":
    main()
