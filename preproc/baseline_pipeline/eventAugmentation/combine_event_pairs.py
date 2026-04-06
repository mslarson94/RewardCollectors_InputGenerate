#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import numpy as np

PAIR_SPECS = [
    ("CoinVis", "CoinVis_start", "CoinVis_end"),
    ("SwapVoteTextVis", "CoinCollect_Moment_PinDrop", "SwapVoteText_Vis_end"),
    ("VoteInstrText_Vis", "VoteInstrText_Vis_start", "VoteInstrText_Vis_end")

]

GROUP_KEYS = ["BlockNum", "BlockInstance", "effectiveRoundNum"]
TIME_COL = "AppTime"
CHEST_GROUP_KEYS = ["BlockNum", "BlockInstance", "effectiveRoundNum"]

def replace_suffix(name: str, in_suffix: str, out_suffix: str) -> str:
    if not name.endswith(in_suffix):
        raise ValueError(f"Filename does not end with expected suffix {in_suffix!r}: {name!r}")
    return name[: -len(in_suffix)] + out_suffix


def make_out_path(in_path: Path, out_dir: Path, in_suffix: str, out_suffix: str) -> Path:
    return out_dir / replace_suffix(in_path.name, in_suffix, out_suffix)


def combine_event_pairs(df: pd.DataFrame, *, drop_originals: bool = True) -> pd.DataFrame:
    df = df.copy()

    # sanity checks
    for c in ["lo_eventType", TIME_COL]:
        if c not in df.columns:
            raise KeyError(f"Missing required column: {c}")
    for k in GROUP_KEYS:
        if k not in df.columns:
            raise KeyError(f"Missing grouping key: {k}")

    # sort deterministically
    df[TIME_COL] = pd.to_numeric(df[TIME_COL], errors="coerce")
    df = df.sort_values(GROUP_KEYS + [TIME_COL]).reset_index(drop=False).rename(columns={"index": "__src_index"})

    combined_rows = []
    to_drop = set()

    for new_type, start_type, end_type in PAIR_SPECS:
        starts = df[df["lo_eventType"].eq(start_type)].copy()
        ends = df[df["lo_eventType"].eq(end_type)].copy()
        if starts.empty or ends.empty:
            continue

        # within each group, assign occurrence number by time
        starts["__pair_i"] = starts.groupby(GROUP_KEYS, sort=False).cumcount()
        ends["__pair_i"] = ends.groupby(GROUP_KEYS, sort=False).cumcount()

        # pair on (group keys + occurrence index)
        m = starts.merge(
            ends,
            on=GROUP_KEYS + ["__pair_i"],
            how="inner",
            suffixes=("_S", "_E"),
            validate="1:1",
        )
        if m.empty:
            continue

        for _, r in m.iterrows():
            out = {}

            # 1) Inherit EVERYTHING from the start marker row
            for c in m.columns:
                if c.endswith("_S"):
                    out[c[:-2]] = r[c]
            # copy merge keys (NOT suffixed)
            for k in GROUP_KEYS:
                out[k] = r[k]

            # 2) Set combined type
            out["lo_eventType"] = new_type

            # 3) Patch ONLY *_end columns from the end marker row
            for c in m.columns:
                if c.endswith("_E"):
                    base = c[:-2]
                    if base.endswith("_end"):
                        out[base] = r[c]

            # 4) End times/orig come from END marker's START fields
            #    (because end marker rows don't have end_* populated)
            # End times/orig come from END marker.
            # Prefer its start_* fields if they exist; otherwise fall back to its AppTime / eMLT_orig.
            out["end_AppTime"] = (
                r["start_AppTime_E"]
                if "start_AppTime_E" in r.index and pd.notna(r["start_AppTime_E"])
                else (r["AppTime_E"] if "AppTime_E" in r.index else pd.NA)
            )

            out["end_eMLT_orig"] = (
                r["start_eMLT_orig_E"]
                if "start_eMLT_orig_E" in r.index and pd.notna(r["start_eMLT_orig_E"])
                else (r["eMLT_orig_E"] if "eMLT_orig_E" in r.index else pd.NA)
            )

            # 5) Keep combined row anchored at start time for sorting
            if "AppTime_S" in r.index and pd.notna(r["AppTime_S"]):
                out["AppTime"] = r["AppTime_S"]
            if "eMLT_orig_S" in r.index and pd.notna(r["eMLT_orig_S"]):
                out["eMLT_orig"] = r["eMLT_orig_S"]

            combined_rows.append(out)

            if drop_originals:
                to_drop.add(int(r["__src_index_S"]))
                to_drop.add(int(r["__src_index_E"]))

    combined_df = pd.DataFrame(combined_rows)
    bad = pd.Series(False, index=combined_df.index)
    if (not combined_df.empty) and ("start_AppTime" in combined_df.columns) and ("end_AppTime" in combined_df.columns):
        bad = pd.to_numeric(combined_df["end_AppTime"], errors="coerce") < pd.to_numeric(combined_df["start_AppTime"], errors="coerce")
    if bad.any():
        print(f"⚠️ {bad.sum()} combined rows have end_AppTime < start_AppTime (pairing/order issue)", file=sys.stderr)
    # drop originals (before dropping __src_index)
    out_df = df
    if drop_originals and to_drop:
        out_df = out_df[~out_df["__src_index"].isin(to_drop)]

    out_df = out_df.drop(columns=["__src_index"], errors="ignore")

    # append combined rows
    if not combined_df.empty:
        # ensure all columns exist
        for c in out_df.columns:
            if c not in combined_df.columns:
                combined_df[c] = pd.NA
        combined_df = combined_df[out_df.columns]

        out_df = pd.concat([out_df, combined_df], ignore_index=True)
        out_df[TIME_COL] = pd.to_numeric(out_df[TIME_COL], errors="coerce")
        out_df = out_df.sort_values(GROUP_KEYS + [TIME_COL]).reset_index(drop=True)

    return out_df

def _iter_csvs(input_dir: Path, pattern: str, recursive: bool) -> list[Path]:
    if recursive:
        return sorted(input_dir.rglob(pattern))
    return sorted(input_dir.glob(pattern))


def _ensure_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def build_chestvis_intervals(
    df: pd.DataFrame,
    *,
    time_col: str = "AppTime",
    group_keys: list[str] = CHEST_GROUP_KEYS,
    drop_inputs: bool = True,
    drop_nextchestvis: bool = True,
    truecontent_event: str = "TrueContentStart",
    coincollect_chest_event: str = "CoinCollect_Moment_Chest",
    currchest_end_event: str = "CurrChestVis_end",
    nextchest_start_event: str = "NextChestVis_start",
    chestpin_col: str = "chestPin_num",
    out_event_type: str = "ChestVis",
) -> pd.DataFrame:
    """
    Create ChestVis rows with rules:
      - k=1: start=TrueContentStart -> end=CoinCollect_Moment_Chest (chestPin_num==1)
      - k>=2: start=CurrChestVis_end (chestPin_num==k-1) -> end=CoinCollect_Moment_Chest (chestPin_num==k)

    Always drop NextChestVis_start if drop_nextchestvis=True.

    IMPORTANT DROP POLICY (what you asked for):
      - NEVER drop TrueContentStart
      - NEVER drop CoinCollect_Moment_Chest
      - Optionally drop CurrChestVis_end rows that were used as starts for k>=2 (controlled by drop_inputs)
      - Always drop NextChestVis_start (controlled by drop_nextchestvis)
    """

    df = df.copy()
    for c in ["lo_eventType", time_col] + group_keys:
        if c not in df.columns:
            raise KeyError(f"Missing required column: {c}")

    df[time_col] = _ensure_num(df[time_col])
    df = df.sort_values(group_keys + [time_col]).reset_index(drop=False).rename(columns={"index": "__src_index"})

    # If chest pin column is missing, we can't build ChestVis; still drop NextChestVis_start if requested
    if chestpin_col not in df.columns:
        out_df = df
        if drop_nextchestvis:
            out_df = out_df[out_df["lo_eventType"].ne(nextchest_start_event)]
        return out_df.drop(columns=["__src_index"], errors="ignore").reset_index(drop=True)

    df[chestpin_col] = _ensure_num(df[chestpin_col])

    # Identify relevant rows
    tc = df[df["lo_eventType"].eq(truecontent_event)].copy()
    cc = df[df["lo_eventType"].eq(coincollect_chest_event)].copy()
    ce = df[df["lo_eventType"].eq(currchest_end_event)].copy()

    # If we can't form the first interval, just drop NextChestVis rows (optional) and return
    if tc.empty or cc.empty:
        out_df = df
        if drop_nextchestvis:
            out_df = out_df[out_df["lo_eventType"].ne(nextchest_start_event)]
        return out_df.drop(columns=["__src_index"], errors="ignore").reset_index(drop=True)

    # k=1: TrueContentStart -> CoinCollect (pin==1)
    cc1 = cc[cc[chestpin_col].eq(1)].copy()

    tc["__tc_i"] = tc.groupby(group_keys, sort=False).cumcount()
    cc1["__cc1_i"] = cc1.groupby(group_keys, sort=False).cumcount()

    m1 = tc.merge(
        cc1,
        left_on=group_keys + ["__tc_i"],
        right_on=group_keys + ["__cc1_i"],
        how="inner",
        suffixes=("_S", "_E"),
        validate="1:1",
    )

    # k>=2: CurrChestVis_end(pin=k-1) -> CoinCollect(pin=k)
    cc = cc.copy()
    ce = ce.copy()
    ce["__k"] = ce[chestpin_col] + 1
    cc["__k"] = cc[chestpin_col]

    ce_k = ce[ce["__k"].ge(2)].copy()
    cc_k = cc[cc["__k"].ge(2)].copy()

    m2 = ce_k.merge(
        cc_k,
        on=group_keys + ["__k"],
        how="inner",
        suffixes=("_S", "_E"),
        validate="1:1",
    )

    combined_rows: list[dict] = []
    to_drop: set[int] = set()  # will only contain CurrChestVis_end indices when drop_inputs=True

    def _inherit_start_then_patch_end(row: pd.Series) -> dict:
        out: dict = {}

        # inherit everything from start row (suffixed)
        for c in row.index:
            if c.endswith("_S"):
                out[c[:-2]] = row[c]

        # copy merge keys (NOT suffixed)
        for k in group_keys:
            out[k] = row[k]

        out["lo_eventType"] = out_event_type

        # patch *_end from end row
        for c in row.index:
            if c.endswith("_E"):
                base = c[:-2]
                if base.endswith("_end"):
                    out[base] = row[c]

        # set end time/orig from END row's start_* if present else AppTime/eMLT_orig
        if "start_AppTime_E" in row.index and pd.notna(row["start_AppTime_E"]):
            out["end_AppTime"] = row["start_AppTime_E"]
        elif "AppTime_E" in row.index:
            out["end_AppTime"] = row["AppTime_E"]

        if "start_eMLT_orig_E" in row.index and pd.notna(row["start_eMLT_orig_E"]):
            out["end_eMLT_orig"] = row["start_eMLT_orig_E"]
        elif "eMLT_orig_E" in row.index:
            out["end_eMLT_orig"] = row["eMLT_orig_E"]

        # anchor row at start time
        if "AppTime_S" in row.index and pd.notna(row["AppTime_S"]):
            out[time_col] = row["AppTime_S"]

        return out

    # Build ChestVis rows from m1 and m2
    if not m1.empty:
        for _, r in m1.iterrows():
            combined_rows.append(_inherit_start_then_patch_end(r))
            # DO NOT drop TrueContentStart (start) and DO NOT drop CoinCollect_Moment_Chest (end)

    if not m2.empty:
        for _, r in m2.iterrows():
            combined_rows.append(_inherit_start_then_patch_end(r))
            if drop_inputs:
                # Only drop CurrChestVis_end starts; keep CoinCollect_Moment_Chest ends
                if "__src_index_S" in r.index:
                    to_drop.add(int(r["__src_index_S"]))

    combined_df = pd.DataFrame(combined_rows)

    out_df = df

    # Always drop NextChestVis_start if requested
    if drop_nextchestvis:
        out_df = out_df[out_df["lo_eventType"].ne(nextchest_start_event)]

    # Drop ONLY CurrChestVis_end starts used for k>=2 if requested
    if drop_inputs and to_drop:
        out_df = out_df[~out_df["__src_index"].isin(to_drop)]

    out_df = out_df.drop(columns=["__src_index"], errors="ignore")

    # Append new ChestVis rows
    if not combined_df.empty:
        for c in out_df.columns:
            if c not in combined_df.columns:
                combined_df[c] = pd.NA
        combined_df = combined_df[out_df.columns]

        out_df = pd.concat([out_df, combined_df], ignore_index=True)
        out_df[time_col] = _ensure_num(out_df[time_col])
        out_df = out_df.sort_values(group_keys + [time_col]).reset_index(drop=True)

    return out_df


def process_one_file(
    in_path: Path,
    out_dir: Path,
    overwrite: bool,
    drop_originals: bool,
    in_suffix: str,
    out_suffix: str,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = make_out_path(in_path, out_dir, in_suffix, out_suffix)

    if out_path.exists() and not overwrite:
        raise FileExistsError(f"Output exists: {out_path} (use --overwrite)")

    df = pd.read_csv(in_path)
    df2 = combine_event_pairs(df, drop_originals=drop_originals)
    # 2) then build ChestVis with the special logic
    df3 = build_chestvis_intervals(df2, drop_inputs=True, drop_nextchestvis=True)
    df3 = df3[df3.lo_eventType != "CurrChestVis_end"]
    df3.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Combine selected lo_eventType start/end marker pairs into unified events for CSV(s)."
    )

    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--input", type=Path, help="Single input CSV")
    src.add_argument("--input-dir", type=Path, help="Directory of input CSVs")

    ap.add_argument("--output", type=Path, help="(single-file mode) Output directory (default: input parent)")
    ap.add_argument("--output-dir", type=Path, help="(dir mode) Output directory (required for --input-dir)")
    ap.add_argument("--pattern", type=str, default="*.csv", help="Glob pattern in dir mode (default: *.csv)")
    ap.add_argument("--recursive", action="store_true", help="Recurse into subdirectories in dir mode")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite outputs if they exist")
    ap.add_argument("--keep-originals", action="store_true", help="Keep original marker rows (default drops them)")

    ap.add_argument(
        "--in-suffix",
        type=str,
        default="_events_coinLabel.csv",
        help="Input filename suffix to replace (must match end of filename).",
    )
    ap.add_argument(
        "--out-suffix",
        type=str,
        default="_events_coinLabel_paired.csv",
        help="Output filename suffix.",
    )

    args = ap.parse_args()
    drop_originals = not args.keep_originals

    # Directory mode
    if args.input_dir is not None:
        in_dir: Path = args.input_dir
        out_dir: Path | None = args.output_dir
        if out_dir is None:
            ap.error("--output-dir is required when using --input-dir")

        csvs = _iter_csvs(in_dir, args.pattern, args.recursive)
        if not csvs:
            print(f"No files matched in {in_dir} with pattern={args.pattern!r}", file=sys.stderr)
            sys.exit(2)

        failures = 0
        for p in csvs:
            try:
                out_path = process_one_file(
                    p,
                    out_dir,
                    args.overwrite,
                    drop_originals,
                    args.in_suffix,
                    args.out_suffix,
                )
                print(f"✅ {p.name} -> {out_path}")
            except Exception as e:
                failures += 1
                print(f"❌ Failed: {p} :: {e}", file=sys.stderr)

        sys.exit(1 if failures else 0)

    # Single-file mode
    in_path: Path = args.input
    out_dir = args.output or in_path.parent
    out_path = process_one_file(
        in_path,
        out_dir,
        args.overwrite,
        drop_originals,
        args.in_suffix,
        args.out_suffix,
    )
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()
