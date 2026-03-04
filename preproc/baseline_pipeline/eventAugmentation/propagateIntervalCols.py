from __future__ import annotations
import sys
import pandas as pd
import numpy as np
from pathlib import Path
import argparse


# Columns to propagate from interval -> events
colsThatNeedPropagation_wEvents = [
    "round_start_AppTime", "round_start_origRow", "round_end_AppTime", "round_end_origRow",
    "round_dur_s", "round_index_in_block",
    "block_start_AppTime", "block_start_origRow", "block_end_AppTime", "block_end_origRow",
    "block_dur_s", "path_order_round", "path_step_in_round", "totDistRound", "totDistBlock"
]

SKIP_LO_EVENTTYPES = {"PinDrop_Moment", "Adjusted_1st_Walk_PinDrop"}
KEYS = ["BlockNum", "BlockInstance", "effectiveRoundNum"]



def _iter_files(input_dir: Path, pattern: str, recursive: bool) -> list[Path]:
    return sorted(input_dir.rglob(pattern)) if recursive else sorted(input_dir.glob(pattern))

def _replace_suffix(name: str, in_suffix: str, out_suffix: str) -> str:
    if not name.endswith(in_suffix):
        raise ValueError(f"Filename does not end with expected suffix {in_suffix!r}: {name!r}")
    return name[: -len(in_suffix)] + out_suffix

def _default_out_path(in_path: Path, out_dir: Path, in_suffix: str, out_suffix: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / _replace_suffix(in_path.name, in_suffix, out_suffix)

def _coerce_key_cols(df: pd.DataFrame, keys=KEYS) -> pd.DataFrame:
    df = df.copy()
    for k in keys:
        if k not in df.columns:
            raise KeyError(f"Missing key column: {k}")
        df[k] = pd.to_numeric(df[k], errors="coerce")
    return df


def propagate_interval_cols_into_events(
    events_df: pd.DataFrame,
    interval_df: pd.DataFrame,
    *,
    cols_to_propagate=colsThatNeedPropagation_wEvents,
    keys=KEYS,
    skip_eventtypes=SKIP_LO_EVENTTYPES,
    overwrite: bool = True,
    debug: bool = True
) -> pd.DataFrame:
    """
    Propagate interval-level columns onto events rows by matching on (BlockNum, BlockInstance, effectiveRoundNum).

    - Applies to rows where lo_eventType NOT in skip_eventtypes
    - Skips rows where any key is NaN
    - If overwrite=True, always write interval values (when present) into events
      If overwrite=False, only fills events cells that are NA
    """
    if events_df is None or events_df.empty:
        if debug:
            print("Events df is empty; nothing to do.")
        return events_df
    if interval_df is None or interval_df.empty:
        raise ValueError("Interval df is empty; cannot propagate.")

    if "lo_eventType" not in events_df.columns:
        raise KeyError("events_df must contain 'lo_eventType'.")

    events = _coerce_key_cols(events_df, keys)
    interval = _coerce_key_cols(interval_df, keys)

    # Ensure propagation columns exist in events (create if missing)
    for c in cols_to_propagate:
        if c not in events.columns:
            events[c] = pd.NA

    # Interval must contain the cols you want to copy; keep only those that exist
    available_cols = [c for c in cols_to_propagate if c in interval.columns]
    missing_in_interval = [c for c in cols_to_propagate if c not in interval.columns]
    if debug and missing_in_interval:
        print("⚠️ Columns not found in interval file (will be skipped):", missing_in_interval)

    # Deduplicate interval on keys (keep last) to avoid ambiguous joins
    interval_small = (
        interval[keys + available_cols]
        .dropna(subset=keys)
        .drop_duplicates(subset=keys, keep="last")
    )

    # Build mask: eligible rows in events
    keys_notna = events[keys].notna().all(axis=1)
    not_skipped = ~events["lo_eventType"].isin(skip_eventtypes)
    eligible = keys_notna & not_skipped

    if debug:
        print("\n=== Interval Propagation DEBUG ===")
        print(f"events rows: {len(events):,}")
        print(f"interval rows: {len(interval):,} (unique by keys: {len(interval_small):,})")
        print(f"eligible rows (keys present & lo_eventType not skipped): {int(eligible.sum()):,}")

    # Merge just for eligible rows to keep it clean
    merged = events.loc[eligible, keys + available_cols].merge(
        interval_small,
        on=keys,
        how="left",
        suffixes=("", "__interval"),
        validate="m:1",  # many events -> one interval row
    )

    # Write back into events
    updated_rows = 0
    updated_cells = 0
    per_col_updates = {c: 0 for c in available_cols}

    # Determine which eligible rows actually matched an interval record:
    # if at least one propagated col is non-NA on the interval side OR keys exist in interval.
    # We'll use presence of any non-NA in interval columns as a proxy for match.
    any_interval_values = merged[[c + "__interval" for c in available_cols]].notna().any(axis=1)
    matched_idx = merged.index[any_interval_values]

    # Apply column updates
    for c in available_cols:
        src = merged[c + "__interval"]
        if overwrite:
            # overwrite only where interval has a value
            to_set = src.notna()
        else:
            # fill only where events is NA and interval has value
            to_set = merged[c].isna() & src.notna()

        if to_set.any():
            # write into original events df at the eligible row positions
            target_event_idx = events.loc[eligible].index[to_set]
            events.loc[target_event_idx, c] = src[to_set].values

            per_col_updates[c] = int(to_set.sum())
            updated_cells += int(to_set.sum())

    # Count rows touched (at least one col updated)
    if updated_cells > 0:
        # recompute row touch count: any column changed among eligible
        # approximate via union of updated target indices:
        touched = set()
        for c in available_cols:
            # rows where interval had a value and we set it (same to_set logic)
            src = merged[c + "__interval"]
            if overwrite:
                to_set = src.notna()
            else:
                to_set = merged[c].isna() & src.notna()
            if to_set.any():
                target_event_idx = events.loc[eligible].index[to_set]
                touched.update(target_event_idx.tolist())
        updated_rows = len(touched)

    if debug:
        print(f"rows updated (>=1 col): {updated_rows:,}")
        print(f"cells updated: {updated_cells:,}")
        print("per-column updates:")
        for k, v in per_col_updates.items():
            print(f"  {k}: {v:,}")
        print("=== end DEBUG ===\n")

    return events


def main():
    p = argparse.ArgumentParser(
        description="Propagate interval-level columns into events by matching on BlockNum, BlockInstance, effectiveRoundNum."
    )

    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--filled", type=Path, help="Single *_filled.csv events file")
    src.add_argument("--filled-dir", type=Path, help="Directory of *_filled.csv events files")

    p.add_argument("--interval", type=Path, help="(single-file mode) interval CSV path")
    p.add_argument("--interval-dir", type=Path, help="(dir mode) directory of interval CSVs")

    p.add_argument("--out", type=Path, help="(single-file mode) output CSV path")
    p.add_argument("--out-dir", type=Path, help="(dir mode) output directory (required with --filled-dir)")

    p.add_argument("--pattern", type=str, default="*_eventsWalks_filled.csv", help="(dir mode) glob for filled files")
    p.add_argument("--filled-in-suffix", type=str, default="_eventsWalks_filled.csv", help="(dir mode) suffix to replace")
    p.add_argument("--filled-out-suffix", type=str, default="_filled_intervalProps.csv", help="(dir mode) output suffix")

    p.add_argument("--interval-suffix", type=str, default="_interval_fromEvents.csv",
                   help="(dir mode) interval suffix used to find matching interval by prefix")

    p.add_argument("--recursive", action="store_true")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--no-overwrite", action="store_true", help="Only fill NA cells instead of overwriting")
    args = p.parse_args()

    overwrite_vals = not args.no_overwrite

    # ---- directory mode ----
    if args.filled_dir is not None:
        if args.out_dir is None:
            p.error("--out-dir is required when using --filled-dir")
        if args.interval_dir is None:
            p.error("--interval-dir is required when using --filled-dir")

        filled_files = sorted(args.filled_dir.rglob(args.pattern)) if args.recursive else sorted(args.filled_dir.glob(args.pattern))
        if not filled_files:
            print(f"No filled files matched in {args.filled_dir} with pattern={args.pattern!r}", file=sys.stderr)
            sys.exit(2)

        failures = 0
        for filled_path in filled_files:
            try:
                if not filled_path.name.endswith(args.filled_in_suffix):
                    raise ValueError(f"Filled filename does not end with {args.filled_in_suffix!r}: {filled_path.name}")

                prefix = filled_path.name[: -len(args.filled_in_suffix)]
                interval_path = args.interval_dir / f"{prefix}{args.interval_suffix}"
                if not interval_path.exists():
                    raise FileNotFoundError(f"Missing matching interval file: {interval_path}")

                out_path = args.out_dir / f"{prefix}{args.filled_out_suffix}"
                out_path.parent.mkdir(parents=True, exist_ok=True)
                if out_path.exists() and not args.overwrite:
                    raise FileExistsError(f"Output exists: {out_path} (use --overwrite)")

                events_df = pd.read_csv(filled_path)
                interval_df = pd.read_csv(interval_path)

                out_df = propagate_interval_cols_into_events(
                    events_df,
                    interval_df,
                    overwrite=overwrite_vals,
                    debug=True,
                )
                out_df.to_csv(out_path, index=False)
                print(f"✅ {filled_path.name} -> {out_path}")
            except Exception as e:
                failures += 1
                print(f"❌ Failed: {filled_path} :: {e}", file=sys.stderr)

        sys.exit(1 if failures else 0)

    # ---- single-file mode ----
    if args.interval is None or args.out is None:
        p.error("--interval and --out are required in single-file mode (--filled)")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    events_df = pd.read_csv(args.filled)
    interval_df = pd.read_csv(args.interval)

    out_df = propagate_interval_cols_into_events(
        events_df,
        interval_df,
        overwrite=overwrite_vals,
        debug=True
    )
    out_df1 = out_df[out_df["lo_eventType"] != "CurrChestVis_end"]
    out_df1.to_csv(args.out, index=False)
    print(f"✅ Wrote: {args.out}")



if __name__ == "__main__":
    main()
