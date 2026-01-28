#!/usr/bin/env python3
# 05_batch_add_startpos.py

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

from RC_utilities.reProcHelpers.startPosAssignment import compute_startpos_for_events_flexible
# put near the top of each script (below imports)

_TRAILING_TOKENS = [
    "_event_reproc_withStartPos_withEffectiveRound_startPosPropagated",
    "_event_reproc_withStartPos_withEffectiveRound",
    "_event_reproc_withStartPos",
    "_event_reproc",
    "_withStartPos",
    "_withEffectiveRound",
    "_startPosPropagated",
]

def _strip_known_trailing_tokens(stem: str) -> str:
    base = stem
    changed = True
    while changed:
        changed = False
        for tok in _TRAILING_TOKENS:
            if base.endswith(tok):
                base = base[: -len(tok)]
                changed = True
                break
    return base


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def _infer_out_path(in_path: Path, out_root: Optional[Path], suffix: str) -> Path:
    base = _strip_known_trailing_tokens(in_path.stem)
    out_name = base + suffix + in_path.suffix
    if out_root is None:
        return in_path.with_name(out_name)
    return out_root / out_name


def _infer_interval_paths(events_path: Path, interval_root: Optional[Path]) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Try BOTH:
      base_finalInterval_vert.csv / horz.csv
      base_finalInterval_vert_withStartPos.csv / horz_withStartPos.csv
    """
    stem = events_path.stem

    for token in [
        "_event_reproc_withStartPos",
        "_event_reproc",
        "_events_pre_reproc",
        "_events_final",
        "_events",
        "_withStartPos",
    ]:
        if stem.endswith(token):
            base = stem[: -len(token)]
            break
    else:
        base = stem

    candidates = [
        (base + "_finalInterval_vert.csv", base + "_finalInterval_horz.csv"),
        #(base + "_finalInterval_vert_withStartPos.csv", base + "_finalInterval_horz_withStartPos.csv"),
    ]

    for vname, hname in candidates:
        vert = events_path.with_name(vname)
        horz = events_path.with_name(hname)

        if interval_root is not None:
            vert = interval_root / vert.name
            horz = interval_root / horz.name

        if vert.exists() and horz.exists():
            return vert, horz

    return None, None


def _label_subset(
    df: pd.DataFrame,
    events_of_interest: Dict[str, str],
    *,
    role_col: str,
    x_start_col: str,
    z_start_col: str,
    x_end_col: str,
    z_end_col: str,
    add_used_xy_cols: bool,
    strict: bool,
    strict_roles: bool,
) -> pd.DataFrame:
    mask = df["lo_eventType"].astype("string").str.strip().isin(list(events_of_interest.keys()))
    if not mask.any():
        return df[["__rowid"]].assign(startPos=pd.NA, startPos_dist=pd.NA)

    sub = df.loc[mask, ["__rowid", "lo_eventType", role_col, x_start_col, z_start_col, x_end_col, z_end_col]].copy()

    labeled = compute_startpos_for_events_flexible(
        sub,
        events_of_interest,
        role_col=role_col,
        x_start_col=x_start_col,
        z_start_col=z_start_col,
        x_end_col=x_end_col,
        z_end_col=z_end_col,
        startpos_label_col="startPos",
        startpos_dist_col="startPos_dist",
        add_used_xy_cols=add_used_xy_cols,
        strict=strict,
        strict_roles=strict_roles,
    )
    return labeled[["__rowid", "startPos", "startPos_dist"]]


def _compute_effective_roundnum_for_pindrop_rows_old(
    df: pd.DataFrame,
    *,
    block_keys: Tuple[str, str] = ("BlockNum", "BlockInstance"),
    round_col: str = "RoundNum",
    sort_time_cols: Tuple[str, ...] = ("start_AppTime", "AppTime"),
) -> pd.Series:
    d = df.copy()

    sort_col = None
    for c in sort_time_cols:
        if c in d.columns:
            sort_col = c
            d[c] = pd.to_numeric(d[c], errors="coerce")
            break

    d[round_col] = pd.to_numeric(d[round_col], errors="coerce")

    sort_by = list(block_keys) + ([sort_col] if sort_col else [])
    d = d.sort_values(sort_by, kind="mergesort", na_position="last").reset_index(drop=False)

    def _future_nonnull_round(g: pd.DataFrame) -> pd.Series:
        return g[round_col].shift(-1).bfill()

    future = d.groupby(list(block_keys), dropna=False, sort=False, group_keys=False).apply(_future_nonnull_round)
    future.index = d["index"].values
    return future.reindex(df.index)


def _compute_effective_roundnum_for_pindrop_rows(
    df: pd.DataFrame,
    *,
    block_keys: Tuple[str, str] = ("BlockNum", "BlockInstance"),
    round_col: str = "RoundNum",
    sort_time_cols: Tuple[str, ...] = ("start_AppTime", "AppTime"),
) -> pd.Series:
    d = df.copy()

    # pick sort col if present
    sort_col = None
    for c in sort_time_cols:
        if c in d.columns:
            sort_col = c
            d[c] = pd.to_numeric(d[c], errors="coerce")
            break

    for k in block_keys:
        if k in d.columns:
            d[k] = pd.to_numeric(d[k], errors="coerce")
    d[round_col] = pd.to_numeric(d[round_col], errors="coerce")

    # keep original index so we can restore exact row alignment
    d = d.reset_index(drop=False).rename(columns={"index": "__orig_index"})

    sort_by = list(block_keys) + ([sort_col] if sort_col else [])
    d = d.sort_values(sort_by, kind="mergesort", na_position="last")

    # transform returns SAME LENGTH as d (never wide like apply can)
    future = d.groupby(list(block_keys), dropna=False)[round_col].transform(
        lambda s: s.shift(-1).bfill()
    )

    # put back onto original df index order
    future.index = d["__orig_index"].values
    return future.reindex(df.index)


def _build_per_round_from_definers(
    df_out: pd.DataFrame,
    *,
    max_round: int,
) -> pd.DataFrame:
    """
    Build 1 row per (BlockNum, BlockInstance, RoundNum) startPos table ONLY from defining events:
      collecting: TrueContentStart
      pindropping: InterRound_PostCylinderWalk_segment
    """
    d = df_out.copy()

    for c in ["BlockNum", "BlockInstance", "RoundNum"]:
        d[c] = pd.to_numeric(d[c], errors="coerce")

    d["lo_eventType"] = d["lo_eventType"].astype("string").str.strip()
    d["BlockType"] = d["BlockType"].astype("string").str.strip().str.lower()

    # Only those two definers
    definers = (
        ((d["BlockType"] == "collecting") & (d["lo_eventType"] == "TrueContentStart")) |
        ((d["BlockType"] == "pindropping") & (d["lo_eventType"] == "InterRound_PostCylinderWalk_segment"))
    )

    per_round = d.loc[definers & d["startPos"].notna(), ["BlockNum", "BlockInstance", "RoundNum", "startPos", "startPos_dist"]].copy()
    per_round = per_round[per_round["RoundNum"].notna() & (per_round["RoundNum"] <= max_round)].copy()

    # pick earliest by time if available
    sort_col = "start_AppTime" if "start_AppTime" in d.columns else ("AppTime" if "AppTime" in d.columns else None)
    if sort_col:
        tmp = d.loc[definers & d["startPos"].notna(), ["BlockNum", "BlockInstance", "RoundNum", sort_col, "startPos", "startPos_dist"]].copy()
        tmp[sort_col] = pd.to_numeric(tmp[sort_col], errors="coerce")
        tmp = tmp.sort_values(["BlockNum", "BlockInstance", "RoundNum", sort_col], kind="mergesort", na_position="last")
        per_round = tmp.drop_duplicates(["BlockNum", "BlockInstance", "RoundNum"], keep="first")[["BlockNum", "BlockInstance", "RoundNum", "startPos", "startPos_dist"]].copy()
    else:
        per_round = per_round.drop_duplicates(["BlockNum", "BlockInstance", "RoundNum"], keep="first")

    return per_round


def _propagate_startpos_within_round(df_out: pd.DataFrame, per_round: pd.DataFrame) -> pd.DataFrame:
    df = df_out.copy()
    keys = ["BlockNum", "BlockInstance", "RoundNum"]
    df = df.merge(per_round, on=keys, how="left", suffixes=("", "__perround"))
    df["startPos"] = df["startPos"].fillna(df["startPos__perround"])
    df["startPos_dist"] = df["startPos_dist"].fillna(df["startPos_dist__perround"])
    return df.drop(columns=["startPos__perround", "startPos_dist__perround"], errors="ignore")


def process_one_events_file(
    events_csv: Path,
    *,
    out_root: Optional[Path],
    role_col: str,
    x_start_col: str,
    z_start_col: str,
    x_end_col: str,
    z_end_col: str,
    add_used_xy_cols: bool,
    strict: bool,
    strict_roles: bool,
    also_update_intervals: bool,
    interval_root: Optional[Path],
    max_round: int,
    dry_run: bool,
) -> None:
    df = pd.read_csv(events_csv).copy()
    df["__rowid"] = np.arange(len(df), dtype=int)

    if "BlockType" not in df.columns:
        raise ValueError(f"{events_csv.name} is missing BlockType; can't split collecting vs pindropping.")
    if "lo_eventType" not in df.columns:
        raise ValueError(f"{events_csv.name} is missing lo_eventType.")
    df["BlockType"] = df["BlockType"].astype("string").str.strip().str.lower()
    df["lo_eventType"] = df["lo_eventType"].astype("string").str.strip()

    # event-type mappings
    events_collecting = {"TrueContentStart": "start"}
    events_pindropping = {"InterRound_PostCylinderWalk_segment": "start"}

    collecting_df = df[df["BlockType"] == "collecting"].copy()
    pindrop_df = df[df["BlockType"] == "pindropping"].copy()

    labeled_collecting = _label_subset(
        collecting_df,
        events_collecting,
        role_col=role_col,
        x_start_col=x_start_col,
        z_start_col=z_start_col,
        x_end_col=x_end_col,
        z_end_col=z_end_col,
        add_used_xy_cols=add_used_xy_cols,
        strict=strict,
        strict_roles=strict_roles,
    )
    labeled_pindrop = _label_subset(
        pindrop_df,
        events_pindropping,
        role_col=role_col,
        x_start_col=x_start_col,
        z_start_col=z_start_col,
        x_end_col=x_end_col,
        z_end_col=z_end_col,
        add_used_xy_cols=add_used_xy_cols,
        strict=strict,
        strict_roles=strict_roles,
    )

    labeled_subset = pd.concat([labeled_collecting, labeled_pindrop], ignore_index=True)

    # attach sparse labels
    df_out = df.merge(labeled_subset[["__rowid", "startPos", "startPos_dist"]], on="__rowid", how="left")

    # LOOKAHEAD only for InterRound rows
    is_inter = df_out["lo_eventType"] == "InterRound_PostCylinderWalk_segment"
    if is_inter.any():
        future_round = _compute_effective_roundnum_for_pindrop_rows(df_out)
        df_out["RoundNum"] = pd.to_numeric(df_out["RoundNum"], errors="coerce")
        needs = is_inter & df_out["RoundNum"].isna()
        df_out.loc[needs, "RoundNum"] = future_round.loc[needs]

    # Build per-round ONLY from defining rows, then propagate
    per_round = _build_per_round_from_definers(df_out, max_round=max_round)
    df_out = _propagate_startpos_within_round(df_out, per_round)

    df_out = df_out.drop(columns=["__rowid"], errors="ignore")

    out_path = _infer_out_path(events_csv, out_root, suffix="_withStartPos")
    _ensure_parent(out_path)

    if dry_run:
        print(f"[dry-run] would write events: {out_path}")
    else:
        df_out.to_csv(out_path, index=False)
        print(f"[ok] wrote events: {out_path}")

    if not also_update_intervals:
        return

    vert_path, horz_path = _infer_interval_paths(events_csv, interval_root)
    if vert_path is None or horz_path is None:
        print(f"[skip] intervals not found for {events_csv.name}")
        return

    vert = pd.read_csv(vert_path)
    horz = pd.read_csv(horz_path)

    # --- SANITY: preserve pin info ---
    vert_in_rows = len(vert)
    horz_in_rows = len(horz)
    has_pin_long = "chestPin_num" in vert.columns
    has_pin_wide = any(c.startswith("pin") for c in horz.columns) or any("pin" in c.lower() for c in horz.columns)

    # merge per_round into intervals
    keys = ["BlockNum", "BlockInstance", "RoundNum"]

    vert_out = vert.merge(per_round, on=keys, how="left", validate="m:1")
    horz_out = horz.merge(per_round, on=keys, how="left", validate="1:1")

    # rowcount sanity
    if len(vert_out) != vert_in_rows:
        raise RuntimeError(f"{events_csv.name}: interval_vert rowcount changed {vert_in_rows} -> {len(vert_out)} (should never happen)")
    if len(horz_out) != horz_in_rows:
        raise RuntimeError(f"{events_csv.name}: interval_horz rowcount changed {horz_in_rows} -> {len(horz_out)} (should never happen)")

    # pin-column sanity
    if has_pin_long and "chestPin_num" not in vert_out.columns:
        raise RuntimeError(f"{events_csv.name}: interval_vert lost chestPin_num column")
    if has_pin_wide:
        # just ensure we didn’t accidentally end up writing a minimal table
        if sum(("pin" in c.lower()) for c in horz_out.columns) < sum(("pin" in c.lower()) for c in horz.columns):
            raise RuntimeError(f"{events_csv.name}: interval_horz appears to have lost pin columns")

    vert_out_path = _infer_out_path(events_csv, interval_root, suffix="_withStartPos")
    horz_out_path = _infer_out_path(events_csv, interval_root, suffix="_withStartPos")
    _ensure_parent(vert_out_path)
    _ensure_parent(horz_out_path)

    if dry_run:
        print(f"[dry-run] would write intervals: {vert_out_path}")
        print(f"[dry-run] would write intervals: {horz_out_path}")
    else:
        vert_out.to_csv(vert_out_path, index=False)
        horz_out.to_csv(horz_out_path, index=False)
        print(f"[ok] wrote intervals: {vert_out_path}")
        print(f"[ok] wrote intervals: {horz_out_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Add startPos + optionally update interval tables (with pin-safety checks).")
    ap.add_argument("--root", required=True)
    ap.add_argument("--pattern", default="*_event_reproc.csv", help="Recommend: *_event_reproc.csv")
    ap.add_argument("--out-root", default="")
    ap.add_argument("--interval-root", default="")

    ap.add_argument("--role-col", default="currentRole")
    ap.add_argument("--x-start-col", default="HeadPosAnchored_x_start")
    ap.add_argument("--z-start-col", default="HeadPosAnchored_z_start")
    ap.add_argument("--x-end-col", default="HeadPosAnchored_x_end")
    ap.add_argument("--z-end-col", default="HeadPosAnchored_z_end")

    ap.add_argument("--add-used-xy-cols", action="store_true")
    ap.add_argument("--no-strict", action="store_true")
    ap.add_argument("--no-strict-roles", action="store_true")

    ap.add_argument("--also-update-intervals", action="store_true")
    ap.add_argument("--max-round", type=int, default=100)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = Path(args.root).expanduser()
    out_root = Path(args.out_root).expanduser() if args.out_root else None
    interval_root = Path(args.interval_root).expanduser() if args.interval_root else None

    strict = not args.no_strict
    strict_roles = not args.no_strict_roles

    files = sorted(root.rglob(args.pattern))
    print(f"[scan] found {len(files)} files under {root} matching {args.pattern}")

    for f in files:
        try:
            process_one_events_file(
                f,
                out_root=out_root,
                role_col=args.role_col,
                x_start_col=args.x_start_col,
                z_start_col=args.z_start_col,
                x_end_col=args.x_end_col,
                z_end_col=args.z_end_col,
                add_used_xy_cols=args.add_used_xy_cols,
                strict=strict,
                strict_roles=strict_roles,
                also_update_intervals=args.also_update_intervals,
                interval_root=interval_root,
                max_round=args.max_round,
                dry_run=args.dry_run,
            )
        except Exception as e:
            print(f"[fail] {f}: {e}")

    print("[done]")


if __name__ == "__main__":
    main()
