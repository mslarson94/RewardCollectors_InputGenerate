#!/usr/bin/env python3
from __future__ import annotations
import sys
import argparse
from pathlib import Path
import pandas as pd

KEYS = ["BlockNum", "BlockInstance", "RoundNum"]

# Always pulled from RoundStart (all BlockTypes)
ROUNDSTART_ALWAYS_COLS = [
    "CoinSetID",
    "coinSet",
    "path_order_round",
    "startPos",
]

# Only pulled for pindropping from Adjusted_1st_Walk_PinDrop
PINDROP_ONLY_COLS = [
    "totDistRound",
]

def _iter_files(input_dir: Path, pattern: str, recursive: bool) -> list[Path]:
    return sorted(input_dir.rglob(pattern)) if recursive else sorted(input_dir.glob(pattern))

def _replace_suffix(name: str, in_suffix: str, out_suffix: str) -> str:
    if not name.endswith(in_suffix):
        raise ValueError(f"Filename does not end with expected suffix {in_suffix!r}: {name!r}")
    return name[: -len(in_suffix)] + out_suffix

def _default_out_path(in_path: Path, out_dir: Path, in_suffix: str, out_suffix: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / _replace_suffix(in_path.name, in_suffix, out_suffix)

def _require_cols(df: pd.DataFrame, cols: list[str], name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"{name} missing required columns: {missing}")


def _coerce_keys_numeric(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for k in KEYS:
        df[k] = pd.to_numeric(df[k], errors="coerce")
    return df


def _dedupe_keep_last(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_index().drop_duplicates(subset=KEYS, keep="last")

def build_interval_from_events_split(
    interval_df: pd.DataFrame,
    events_df: pd.DataFrame,
    *,
    debug: bool = True,
) -> pd.DataFrame:
    """
    Create/update interval columns from events using (BlockNum, BlockInstance, RoundNum):

    - Always fill CoinSetID, coinSet, startPos, path_order_round from lo_eventType == 'RoundStart'
    - Also (if present) fill BlockType from RoundStart
    - If resulting BlockType == 'pindropping', fill totalDistRound and adj_totalDistRound
      from lo_eventType == 'Adjusted_1st_Walk_PinDrop'

    No requirement that interval has BlockType initially.
    """
    if interval_df is None or interval_df.empty:
        raise ValueError("interval_df is empty.")
    if events_df is None or events_df.empty:
        raise ValueError("events_df is empty.")

    _require_cols(interval_df, KEYS, "interval_df")
    _require_cols(events_df, KEYS + ["lo_eventType"], "events_df")

    interval = _coerce_keys_numeric(interval_df)
    events = _coerce_keys_numeric(events_df)

    # Ensure destination columns exist in interval
    for c in ROUNDSTART_ALWAYS_COLS + PINDROP_ONLY_COLS + ["BlockType"]:
        if c not in interval.columns:
            interval[c] = pd.NA

    # -------------------------
    # Phase A: RoundStart mapping (always cols + BlockType if available)
    # -------------------------
    rs = events.loc[events["lo_eventType"].eq("TrueContentStart")].dropna(subset=KEYS).copy()
    rs = _dedupe_keep_last(rs)

    rs_cols = [c for c in ROUNDSTART_ALWAYS_COLS if c in rs.columns]
    rs_missing = [c for c in ROUNDSTART_ALWAYS_COLS if c not in rs.columns]

    bt_available = "BlockType" in rs.columns

    if debug:
        print("\n=== BuildIntervals DEBUG ===")
        print(f"interval rows: {len(interval):,}")
        print(f"events rows:   {len(events):,}")
        print(f"RoundStart rows (deduped): {len(rs):,}")
        if rs_missing:
            print("⚠️ Missing in RoundStart (won't be filled):", rs_missing)
        print("BlockType available in RoundStart:", bt_available)

    rs_map_cols = KEYS + rs_cols + (["BlockType"] if bt_available else [])
    rs_map = rs[rs_map_cols].copy()

    if bt_available:
        rs_map["BlockType"] = rs_map["BlockType"].astype(str).str.lower()

    merged = interval.merge(rs_map, on=KEYS, how="left", suffixes=("", "__rs"), validate="m:1")

    # Fill roundstart-always cols from RoundStart where present
    updated = {}
    for c in rs_cols:
        src = merged[c + "__rs"]
        mask = src.notna()
        updated[c] = int(mask.sum())
        merged.loc[mask, c] = src.loc[mask].values
        merged = merged.drop(columns=[c + "__rs"])

    # Fill BlockType from RoundStart if available
    if bt_available and "BlockType__rs" in merged.columns:
        src = merged["BlockType__rs"]
        mask = merged["BlockType"].isna() & src.notna()
        updated["BlockType"] = int(mask.sum())
        merged.loc[mask, "BlockType"] = src.loc[mask].values
        merged = merged.drop(columns=["BlockType__rs"])
    else:
        # if it wasn't merged, ensure no leftover
        merged = merged.drop(columns=["BlockType__rs"], errors="ignore")

    if debug:
        print("\nFilled from RoundStart:")
        for k, v in updated.items():
            print(f"  {k}: {v:,}")
        print("BlockType distribution after RoundStart fill (incl NA):")
        print(merged["BlockType"].value_counts(dropna=False))

    # -------------------------
    # Phase B: Pindropping-only cols from Adjusted_1st_Walk_PinDrop
    # -------------------------
    # Only keys where BlockType == pindropping
    pindrop_keys = merged.loc[merged["BlockType"].astype(str).str.lower().eq("pindropping"), KEYS].dropna(subset=KEYS)
    if pindrop_keys.empty:
        if debug:
            print("\nNo pindropping rows detected in interval (BlockType != 'pindropping' or missing). Done.")
            print("=== end DEBUG ===\n")
        return merged

    ap = events.loc[events["lo_eventType"].eq("Adjusted_1st_Walk_PinDrop")].dropna(subset=KEYS).copy()
    # Restrict to keys we need
    ap = ap.merge(pindrop_keys.drop_duplicates(), on=KEYS, how="inner")
    ap = _dedupe_keep_last(ap)

    ap_cols = [c for c in PINDROP_ONLY_COLS if c in ap.columns]
    ap_missing = [c for c in PINDROP_ONLY_COLS if c not in ap.columns]
    if debug and ap_missing:
        print("⚠️ Missing in Adjusted_1st_Walk_PinDrop (won't be filled):", ap_missing)

    ap_map = ap[KEYS + ap_cols].copy()

    merged2 = merged.merge(ap_map, on=KEYS, how="left", suffixes=("", "__ap"), validate="m:1")

    updated2 = {}
    for c in ap_cols:
        src = merged2[c + "__ap"]
        mask = src.notna()
        updated2[c] = int(mask.sum())
        merged2.loc[mask, c] = src.loc[mask].values
        merged2 = merged2.drop(columns=[c + "__ap"])

    if debug:
        print("\nFilled pindropping-only cols from Adjusted_1st_Walk_PinDrop:")
        for k, v in updated2.items():
            print(f"  {k}: {v:,}")
        print("=== end DEBUG ===\n")
    #merged2 = merged2.rename(columns={"startPos__final": "startPos"})
    merged2["effectiveRoundNum"] = merged2["RoundNum"]

    return merged2


def main():
    ap = argparse.ArgumentParser(
        description="Build/update interval CSV(s) from events."
    )

    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--interval", type=Path, help="Single interval CSV")
    src.add_argument("--interval-dir", type=Path, help="Directory of interval CSVs")

    ap.add_argument("--events", type=Path, help="(single-file mode) Events CSV path")
    ap.add_argument("--events-dir", type=Path, help="(dir mode) Directory of events CSVs")

    ap.add_argument("--out", type=Path, help="(single-file mode) Output CSV path")
    ap.add_argument("--out-dir", type=Path, help="(dir mode) Output directory (required with --interval-dir)")

    ap.add_argument("--pattern", type=str, default="*_finalInterval_vert.csv", help="(dir mode) interval glob pattern")
    ap.add_argument("--events-suffix", type=str, default="_eventsWalks_filled.csv",
                    help="(dir mode) expected events suffix used to find matching events file by prefix")
    ap.add_argument("--interval-in-suffix", type=str, default="_finalInterval_vert.csv",
                    help="(dir mode) interval suffix to replace")
    ap.add_argument("--interval-out-suffix", type=str, default="_interval_fromEvents.csv",
                    help="(dir mode) output suffix")
    ap.add_argument("--recursive", action="store_true")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--no-debug", action="store_true")

    args = ap.parse_args()
    debug = not args.no_debug

    # ---- directory mode ----
    if args.interval_dir is not None:
        if args.out_dir is None:
            ap.error("--out-dir is required when using --interval-dir")
        if args.events_dir is None:
            ap.error("--events-dir is required when using --interval-dir")

        interval_files = sorted(args.interval_dir.rglob(args.pattern)) if args.recursive else sorted(args.interval_dir.glob(args.pattern))
        if not interval_files:
            print(f"No interval files matched in {args.interval_dir} with pattern={args.pattern!r}", file=sys.stderr)
            sys.exit(2)

        failures = 0
        for interval_path in interval_files:
            try:
                if not interval_path.name.endswith(args.interval_in_suffix):
                    raise ValueError(f"Interval filename does not end with {args.interval_in_suffix!r}: {interval_path.name}")

                prefix = interval_path.name[: -len(args.interval_in_suffix)]
                events_path = args.events_dir / f"{prefix}{args.events_suffix}"
                if not events_path.exists():
                    raise FileNotFoundError(f"Missing matching events file: {events_path}")

                out_path = args.out_dir / f"{prefix}{args.interval_out_suffix}"
                out_path.parent.mkdir(parents=True, exist_ok=True)

                if out_path.exists() and not args.overwrite:
                    raise FileExistsError(f"Output exists: {out_path} (use --overwrite)")

                interval_df = pd.read_csv(interval_path)
                events_df = pd.read_csv(events_path)

                out_df = build_interval_from_events_split(interval_df, events_df, debug=debug)
                out_df.to_csv(out_path, index=False)

                print(f"✅ {interval_path.name} -> {out_path}")
            except Exception as e:
                failures += 1
                print(f"❌ Failed: {interval_path} :: {e}", file=sys.stderr)

        sys.exit(1 if failures else 0)

    # ---- single-file mode ----
    if args.events is None or args.out is None:
        ap.error("--events and --out are required in single-file mode (--interval)")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    interval_df = pd.read_csv(args.interval)
    events_df = pd.read_csv(args.events)
    out_df = build_interval_from_events_split(interval_df, events_df, debug=debug)
    out_df.to_csv(args.out, index=False)
    print(f"✅ Wrote: {args.out}")


if __name__ == "__main__":
    main()
