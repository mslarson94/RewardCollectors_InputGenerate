#!/usr/bin/env python3
# 06a_make_effective_roundnum.py
#
# Step A:
#   1) Create effectiveRoundNum = RoundNum, except sentinel/out-of-range -> NA
#   2) For InterRound_PostCylinderWalk_segment rows with effectiveRoundNum NA,
#      assign the RoundNum of the NEXT RoundStart in the same (BlockNum, BlockInstance) by time.
#   3) Save the updated events file (and optional audit table).

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


INTER_EVENT = "InterRound_PostCylinderWalk_segment"
ROUNDSTART_EVENT = "TrueContentStart"

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

def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _infer_out_path(in_path: Path, out_root: Optional[Path], suffix: str) -> Path:
    base = _strip_known_trailing_tokens(in_path.stem)
    out_name = base + suffix + in_path.suffix
    if out_root is None:
        return in_path.with_name(out_name)
    return out_root / out_name



def _pick_time_col(df: pd.DataFrame) -> str:
    for c in ("start_AppTime", "AppTime", "end_AppTime"):
        if c in df.columns:
            return c
    raise ValueError("No usable time column found (need one of start_AppTime/AppTime/end_AppTime).")


def _coerce_numeric(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def _make_effective_roundnum(
    df: pd.DataFrame,
    *,
    round_col: str = "RoundNum",
    out_col: str = "effectiveRoundNum",
    max_round: int = 100,
    sentinel: int = 8888,
) -> pd.Series:
    r = pd.to_numeric(df[round_col], errors="coerce")
    eff = r.copy()
    eff[(eff > max_round) | (eff == sentinel)] = np.nan
    return eff.astype("Float64")


def _assign_next_roundstart_roundnum_v1(
    df: pd.DataFrame,
    *,
    time_col: str,
    block_keys: tuple[str, str] = ("BlockNum", "BlockInstance"),
    inter_event: str = INTER_EVENT,
    roundstart_event: str = ROUNDSTART_EVENT,
    round_col: str = "RoundNum",
    max_round: int = 100,
) -> pd.DataFrame:
    """
    Returns a table indexed by InterRound __rowid with the matched next RoundStart RoundNum.
    """
    d = df.copy()

    # stable row id (local to this step; kept in output for auditing)
    if "__rowid" not in d.columns:
        d["__rowid"] = np.arange(len(d), dtype=int)

    # normalize strings
    d["lo_eventType"] = d["lo_eventType"].astype("string").str.strip()

    # numeric coercions
    _coerce_numeric(d, [*block_keys, round_col, time_col])

    # sort required for merge_asof
    d = d.sort_values([*block_keys, time_col], kind="mergesort", na_position="last")

    # RoundStart candidates with valid round numbers
    rs = d[d["lo_eventType"] == roundstart_event][
        ["__rowid", *block_keys, time_col, round_col]
    ].dropna(subset=[*block_keys, time_col]).copy()

    rs = rs.rename(columns={"__rowid": "__rs_rowid", time_col: "__rs_time", round_col: "__rs_round"})
    rs = rs[rs["__rs_round"].notna() & (rs["__rs_round"] <= max_round)].copy()
    rs = rs.sort_values([*block_keys, "__rs_time"], kind="mergesort", na_position="last")

    # InterRound rows
    inter = d[d["lo_eventType"] == inter_event][
        ["__rowid", *block_keys, time_col]
    ].dropna(subset=[*block_keys, time_col]).copy()

    inter = inter.rename(columns={"__rowid": "__inter_rowid", time_col: "__inter_time"})
    inter = inter.sort_values([*block_keys, "__inter_time"], kind="mergesort", na_position="last")

    if inter.empty or rs.empty:
        return pd.DataFrame(columns=["__inter_rowid", "__rs_round", "__rs_time"])

    linked = pd.merge_asof(
        inter,
        rs,
        left_on="__inter_time",
        right_on="__rs_time",
        by=list(block_keys),
        direction="forward",
        allow_exact_matches=False,  # must be after inter time
    )

    # Keep only successful matches
    linked = linked[linked["__rs_round"].notna()].copy()
    return linked[["__inter_rowid", "__rs_round", "__rs_time"]]

def _assign_next_roundstart_roundnum(
    df: pd.DataFrame,
    *,
    inter_time_col: str,
    rs_time_col: str,
    block_keys: tuple[str, str] = ("BlockNum", "BlockInstance"),
    inter_event: str = INTER_EVENT,
    roundstart_event: str = ROUNDSTART_EVENT,
    round_col: str = "RoundNum",
    max_round: int = 100,
) -> pd.DataFrame:
    """
    Returns a table indexed by InterRound __rowid with the matched next RoundStart RoundNum.
    """
    d = df.copy()

    if "__rowid" not in d.columns:
        d["__rowid"] = np.arange(len(d), dtype=int)

    d["lo_eventType"] = d["lo_eventType"].astype("string").str.strip()

    _coerce_numeric(d, [*block_keys, round_col, inter_time_col, rs_time_col])

    # Sort by a "master" time for stable ordering; merge_asof will use left/right times below
    # We just need both sides sorted by their respective time columns.
    d = d.sort_values([*block_keys, rs_time_col], kind="mergesort", na_position="last")

    rs = d[d["lo_eventType"] == roundstart_event][
        ["__rowid", *block_keys, rs_time_col, round_col]
    ].dropna(subset=[*block_keys, rs_time_col]).copy()

    rs = rs.rename(
        columns={
            "__rowid": "__rs_rowid",
            rs_time_col: "__rs_time",
            round_col: "__rs_round",
        }
    )
    rs = rs[rs["__rs_round"].notna() & (rs["__rs_round"] <= max_round)].copy()
    rs = rs.sort_values([*block_keys, "__rs_time"], kind="mergesort", na_position="last")

    inter = d[d["lo_eventType"] == inter_event][
        ["__rowid", *block_keys, inter_time_col]
    ].dropna(subset=[*block_keys, inter_time_col]).copy()

    inter = inter.rename(
        columns={
            "__rowid": "__inter_rowid",
            inter_time_col: "__inter_time",
        }
    )
    inter = inter.sort_values([*block_keys, "__inter_time"], kind="mergesort", na_position="last")

    if inter.empty or rs.empty:
        return pd.DataFrame(columns=["__inter_rowid", "__rs_round", "__rs_time"])

    linked = pd.merge_asof(
        inter,
        rs,
        left_on="__inter_time",
        right_on="__rs_time",
        by=list(block_keys),
        direction="forward",
        allow_exact_matches=False,
    )

    linked = linked[linked["__rs_round"].notna()].copy()
    return linked[["__inter_rowid", "__inter_time", "__rs_round", "__rs_time"]]



def process_one_file(
    in_events: Path,
    *,
    out_root: Optional[Path],
    max_round: int,
    sentinel: int,
    dry_run: bool,
    write_audit: bool,
) -> None:
    df = pd.read_csv(in_events).copy()

    required = {"lo_eventType", "BlockNum", "BlockInstance", "RoundNum"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{in_events.name} missing required columns: {missing}")

    # Normalize event type early
    df["lo_eventType"] = df["lo_eventType"].astype("string").str.strip()

    # --- Pick time anchors explicitly ---
    # InterRound should look ahead starting from the END of the segment if possible
    if "end_AppTime" in df.columns:
        inter_time_col = "end_AppTime"
    elif "AppTime" in df.columns:
        inter_time_col = "AppTime"
    elif "start_AppTime" in df.columns:
        inter_time_col = "start_AppTime"
    else:
        raise ValueError("No usable time column for InterRound (need end_AppTime/AppTime/start_AppTime).")

    # RoundStart time should use its event time (AppTime preferred)
    if "AppTime" in df.columns:
        rs_time_col = "AppTime"
    elif "start_AppTime" in df.columns:
        rs_time_col = "start_AppTime"
    elif "end_AppTime" in df.columns:
        rs_time_col = "end_AppTime"
    else:
        raise ValueError("No usable time column for RoundStart (need AppTime/start_AppTime/end_AppTime).")

    _coerce_numeric(df, ["BlockNum", "BlockInstance", "RoundNum", inter_time_col, rs_time_col])

    # Stable row id (handy for auditing/debugging)
    if "__rowid" not in df.columns:
        df["__rowid"] = np.arange(len(df), dtype=int)

    # 1) Baseline effectiveRoundNum = RoundNum (with sentinel/out-of-range -> NA)
    df["effectiveRoundNum"] = _make_effective_roundnum(df, max_round=max_round, sentinel=sentinel)

    # 2) InterRound effectiveRoundNum = RoundNum of the NEXT TrueContentStart in same BlockNum/BlockInstance
    is_inter = df["lo_eventType"] == INTER_EVENT
    is_rs = df["lo_eventType"] == ROUNDSTART_EVENT


    # Build a match-time used only for ordering / lookahead:
    # - InterRound rows: use inter_time_col (prefer end_AppTime)
    # - all other rows: use rs_time_col (AppTime preferred)
    df["__match_time"] = df[rs_time_col]
    df.loc[is_inter, "__match_time"] = df.loc[is_inter, inter_time_col]

    # Sort within block for lookahead logic
    df = df.sort_values(["BlockNum", "BlockInstance", "__match_time"], kind="mergesort", na_position="last")

    # "Look ahead" to next TrueContentStart round number within each (BlockNum, BlockInstance)
    next_rs_round = (
        df["RoundNum"]
        .where(is_rs)
        .groupby([df["BlockNum"], df["BlockInstance"]])
        .bfill()
        .astype("Float64")
    )
    # sanity: InterRound rows should usually find a next TrueContentStart (except maybe last one)
    unmatched = (is_inter & next_rs_round.isna()).sum()
    if unmatched:
        print(f"[warn] {in_events.name}: {int(unmatched)} InterRound rows had no next TrueContentStart")

        # Assign only where a next TrueContentStart exists
    df.loc[is_inter & next_rs_round.notna(), "effectiveRoundNum"] = next_rs_round[is_inter & next_rs_round.notna()]

    audit_link = None
    if write_audit:
        # Minimal audit: InterRound rows with their matched next TrueContentStart round number
        audit_link = df.loc[is_inter, ["__rowid", "BlockNum", "BlockInstance", "RoundNum", "__match_time"]].copy()
        audit_link["effectiveRoundNum_assigned"] = df.loc[is_inter, "effectiveRoundNum"].values

    # Cleanup helper column
    df = df.drop(columns=["__match_time"])

    # Output
    out_path = _infer_out_path(in_events, out_root, suffix="_withEffectiveRound")
    _ensure_parent(out_path)

    # Sort output for stable inspection (use rs_time_col if present)
    df = df.sort_values(["BlockNum", "BlockInstance", rs_time_col], kind="mergesort", na_position="last")

    if dry_run:
        print(f"[dry-run] would write: {out_path}")
        return

    df.to_csv(out_path, index=False)
    print(f"[ok] wrote: {out_path}")

    if write_audit and (audit_link is not None):
        audit_path = _infer_out_path(in_events, out_root, suffix="_audit_interround_to_roundstart").with_suffix(".csv")
        audit_link.to_csv(audit_path, index=False)
        print(f"[ok] wrote audit: {audit_path}")



def main() -> None:
    ap = argparse.ArgumentParser(description="Step A: create effectiveRoundNum and assign it for InterRound rows using next RoundStart.")
    ap.add_argument("--root", required=True, help="Root directory to search under")
    ap.add_argument("--pattern", default="*_withStartPos.csv", help="Glob for events files")
    ap.add_argument("--out-root", default="", help="Optional flat output dir; otherwise next to inputs")
    ap.add_argument("--max-round", type=int, default=100)
    ap.add_argument("--sentinel", type=int, default=8888)
    ap.add_argument("--write-audit", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = Path(args.root).expanduser()
    out_root = Path(args.out_root).expanduser() if args.out_root else None

    files = sorted(root.rglob(args.pattern))
    print(f"[scan] found {len(files)} files under {root} matching {args.pattern}")

    for f in files:
        try:
            process_one_file(
                f,
                out_root=out_root,
                max_round=args.max_round,
                sentinel=args.sentinel,
                dry_run=args.dry_run,
                write_audit=args.write_audit,
            )
        except Exception as e:
            print(f"[fail] {f}: {e}")

    print("[done]")


if __name__ == "__main__":
    main()
