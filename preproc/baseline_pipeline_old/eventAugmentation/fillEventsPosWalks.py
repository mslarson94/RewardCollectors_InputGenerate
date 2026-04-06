from __future__ import annotations
import sys
import argparse
import pandas as pd
import json
import numpy as np
from pathlib import Path
import os
### 
'''
you can't use totRoundFrac or roundFrac in general on the adj_walk2Pin event. It spans over 2 rounds therefore the value in those fields is nonsense. 
'''

######## New Coding
colsThatNeedPropagation_wEvents = ["round_start_AppTime", "round_start_origRow", "round_end_AppTime", "round_end_origRow", "round_dur_s", "round_index_in_block", 
        "block_start_AppTime", "block_start_origRow", "block_end_AppTime", "block_end_origRow", "block_dur_s", "path_order_round", "path_step_in_round", "totDistRound", "totDistBlock"]
# loEventsThatNeedPositionTimeDistInfo = [
# {"Adjusted_1st_Walk_PinDrop": ["HeadForthAnchored_yaw_at_start"    "HeadForthAnchored_pitch_at_start"    "HeadForthAnchored_roll_at_start"]}, ## needs
# ]
MISSING_POSTIMECOLS = ["stepDist", "totDistBlock_current", "totDistRound_current", "currSpeed"]

GOOD_LO_EVENTS = []


def _iter_files(input_dir: Path, pattern: str, recursive: bool) -> list[Path]:
    return sorted(input_dir.rglob(pattern)) if recursive else sorted(input_dir.glob(pattern))

def _replace_suffix(name: str, in_suffix: str, out_suffix: str) -> str:
    if not name.endswith(in_suffix):
        raise ValueError(f"Filename does not end with expected suffix {in_suffix!r}: {name!r}")
    return name[: -len(in_suffix)] + out_suffix

def _default_out_path(in_path: Path, out_dir: Path, in_suffix: str, out_suffix: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / _replace_suffix(in_path.name, in_suffix, out_suffix)


##### 
def build_end_events_Walks(row, startEnd):
    suffix = "_start" if startEnd == "start" else "_end"
    missingcolsDict = {}
    for col in MISSING_POSTIMECOLS:
        missingcolsDict[col + suffix] = row.get(col, pd.NA)
    return missingcolsDict

def fixEventsFiles(df: pd.DataFrame, reproc_df: pd.DataFrame, *, debug: bool = True) -> pd.DataFrame:
    """
    Fill *_start / *_end columns for MISSING_POSTIMECOLS in eventsWalks rows
    whose lo_eventType is NOT in GOOD_LO_EVENTS.

    IMPORTANT: Uses the closest non-Event row DIRECTLY BEFORE (or at) each origRow_start/end.
    Concretely:
      - start values come from prev non-Event at/before origRow_start
      - end   values come from prev non-Event at/before origRow_end

    This matches "closest to stimulus/feedback moment" when the marker row itself is Type=='Event'.
    """
    if df is None or df.empty:
        if debug:
            print("fixEventsFiles: events df empty; nothing to do.")
        return df
    if reproc_df is None or reproc_df.empty:
        raise ValueError("reproc_df is empty; cannot propagate position/time/dist columns.")

    required_events_cols = {"origRow_start", "origRow_end", "lo_eventType"}
    missing_required = required_events_cols - set(df.columns)
    if missing_required:
        raise KeyError(f"eventsWalks df missing required columns: {sorted(missing_required)}")

    if "origRow" not in reproc_df.columns:
        raise KeyError("reproc_df must contain an 'origRow' column.")

    # If Type missing, treat all rows as usable samples
    reproc = reproc_df.copy()
    if "Type" not in reproc.columns:
        reproc["Type"] = "RTdata"

    # Ensure destination columns exist
    dst_cols = []
    for base in MISSING_POSTIMECOLS:
        for suffix in ("_start", "_end"):
            colname = f"{base}{suffix}"
            dst_cols.append(colname)
            if colname not in df.columns:
                df[colname] = pd.NA

    # ---- DEBUG: snapshot NA counts before ----
    if debug:
        print("\n=== fillEventsPosWalks DEBUG ===")
        print(f"events rows: {len(df):,}")
        print(f"reproc rows: {len(reproc):,}")
        na_before = {c: int(df[c].isna().sum()) for c in dst_cols}
        print("NA counts BEFORE:", na_before)

    # Sort reproc by origRow and normalize origRow to int
    reproc = reproc.sort_values("origRow").reset_index(drop=True)
    reproc["origRow"] = pd.to_numeric(reproc["origRow"], errors="coerce")
    reproc = reproc.dropna(subset=["origRow"])
    reproc["origRow"] = reproc["origRow"].astype(int)

    # Keep only non-Event rows as sources (your file uses Type=='RTdata' for sample rows)
    usable_mask = reproc["Type"].ne("Event")
    usable_vals = reproc[MISSING_POSTIMECOLS].where(usable_mask)

    # Build "next non-Event" table (bfill) indexed by origRow
    nextpos = usable_vals.bfill()
    nextpos.index = reproc["origRow"].values

    # Normalize start/end origRows in events df
    s_num = pd.to_numeric(df["origRow_start"], errors="coerce")
    e_num = pd.to_numeric(df["origRow_end"], errors="coerce")

    needs_fix_mask = ~df["lo_eventType"].isin(GOOD_LO_EVENTS)

    # ---- DEBUG counters ----
    eligible_rows = int(needs_fix_mask.sum())
    valid_origrows = 0
    start_key_present = 0
    end_key_present = 0
    rows_any_fill = 0
    filled_cells = 0
    filled_by_col = {c: 0 for c in dst_cols}

    for idx in df.index[needs_fix_mask]:
        s = s_num.at[idx]
        e = e_num.at[idx]
        if pd.isna(s) or pd.isna(e):
            continue

        try:
            s_key = int(s)
            e_key = int(e)
        except Exception:
            continue

        valid_origrows += 1

        has_start = s_key in nextpos.index
        has_end = e_key in nextpos.index
        start_key_present += int(has_start)
        end_key_present += int(has_end)

        row_filled = False

        if has_start:
            for base in MISSING_POSTIMECOLS:
                dst = f"{base}_start"
                if pd.isna(df.at[idx, dst]):
                    val = nextpos.at[s_key, base]
                    if pd.notna(val):
                        df.at[idx, dst] = val
                        filled_cells += 1
                        filled_by_col[dst] += 1
                        row_filled = True

        if has_end:
            for base in MISSING_POSTIMECOLS:
                dst = f"{base}_end"
                if pd.isna(df.at[idx, dst]):
                    val = nextpos.at[e_key, base]
                    if pd.notna(val):
                        df.at[idx, dst] = val
                        filled_cells += 1
                        filled_by_col[dst] += 1
                        row_filled = True

        if row_filled:
            rows_any_fill += 1

    # ---- DEBUG: snapshot NA counts after ----
    if debug:
        na_after = {c: int(df[c].isna().sum()) for c in dst_cols}
        print(f"\nEligible rows (lo_eventType not in GOOD_LO_EVENTS): {eligible_rows:,}")
        print(f"Eligible rows with valid origRow_start & origRow_end numbers: {valid_origrows:,}")
        print(f"Start keys found in reproc index: {start_key_present:,}")
        print(f"End keys found in reproc index:   {end_key_present:,}")
        print(f"Rows where at least one cell was filled: {rows_any_fill:,}")
        print(f"Total non-NA cells filled: {filled_cells:,}")

        print("\nFilled cells by column:")
        for k, v in filled_by_col.items():
            print(f"  {k}: {v:,}")

        print("\nNA counts AFTER:", na_after)

        # quick sanity: show delta
        print("\nNA reduction (BEFORE - AFTER):")
        for c in dst_cols:
            print(f"  {c}: {na_before[c] - na_after[c]:,}")

        print("=== end DEBUG ===\n")

    return df

def infer_reprocessed_path(events_walks_path: Path) -> Path:
    """Given /.../*_eventsWalks.csv infer matching *_reprocessed_with_dist.csv in same dir."""
    name = events_walks_path.name
    if not name.endswith("_eventsWalks.csv"):
        raise ValueError(f"Expected *_eventsWalks.csv, got: {name}")
    base = name[: -len("_eventsWalks.csv")]
    candidate = events_walks_path.with_name(f"{base}_reprocessed_with_dist.csv")
    return candidate

def delta(df: pd.DataFrame, end_col: str, start_col: str) -> pd.Series:
    return pd.to_numeric(df[end_col], errors="coerce") - pd.to_numeric(df[start_col], errors="coerce")


def process_events_walks_file(events_walks_csv: str, reprocessed_csv: str | None = None, out_csv: str | None = None, debug: bool=True) -> str:
    events_path = Path(events_walks_csv)
    reproc_path = Path(reprocessed_csv) if reprocessed_csv else infer_reprocessed_path(events_path)

    if not events_path.exists():
        raise FileNotFoundError(f"eventsWalks file not found: {events_path}")
    if not reproc_path.exists():
        raise FileNotFoundError(f"reprocessed_with_dist file not found: {reproc_path}")

    df_events = pd.read_csv(events_path)
    df_reproc = pd.read_csv(reproc_path)

    #df_fixed = fixEventsFiles(df_events, df_reproc)
    df_fixed = fixEventsFiles(df_events, df_reproc, debug=debug)

    if out_csv:
        out_path = Path(out_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        out_path = events_path.with_name(events_path.name.replace("_eventsWalks.csv", "_eventsWalks_filled.csv"))
    dropCols = ["HeadPosAnchored_x_start", "HeadPosAnchored_x_end", "HeadPosAnchored_y_start", "HeadPosAnchored_y_end", "HeadPosAnchored_z_start", "HeadPosAnchored_z_end"]
    #print(set(dropCols).issubset(df_fixed.columns))
    #print(df_fixed.columns)
    df_fixed = df_fixed.drop(columns=dropCols)
    print(set(dropCols).issubset(df_fixed.columns))

    df_fixed["totEventDur"]       = delta(df_fixed, "end_AppTime", "start_AppTime")
    df_fixed["totEventDist"]      = delta(df_fixed, "totDistBlock_current_end", "totDistBlock_current_start")
    df_fixed["totEventRoundFrac"] = delta(df_fixed, "roundFrac_end", "roundFrac_start")
    df_fixed["totEventBlockFrac"] = delta(df_fixed, "blockFrac_end", "blockFrac_start")

    df_fixed["avgEventSpeed"] = df_fixed["totEventDist"] / df_fixed["totEventDur"]
    df_fixed.loc[df_fixed["totEventDur"].isna() | (df_fixed["totEventDur"] == 0), "avgEventSpeed"] = np.nan

    bad_type = df_fixed["lo_eventType"].eq("Adjusted_1st_Walk_PinDrop") ## you can't have anything related to roundFrac for this event type because it actually spans 2 round types - the post cylinder walk idle period & the first part of the actual true round. So, I'm blanking these fields out. 

    df_fixed.loc[bad_type, ["roundFrac_start", "roundFrac_end", "totEventRoundFrac"]] = np.nan

    df_fixed.to_csv(out_path, index=False)

    return str(out_path)

def _cli_v1():
    import argparse
    p = argparse.ArgumentParser(description="Fill missing *_start/_end pos/time/dist cols in *_eventsWalks.csv using *_reprocessed_with_dist.csv origRow lookups.")
    p.add_argument("--events-walks", required=True, type=str, help="Path to *_eventsWalks.csv")
    p.add_argument("--reprocessed", default=None, type=str, help="Optional explicit path to *_reprocessed_with_dist.csv")
    p.add_argument("--out", default=None, type=str, help="Output CSV path (default: *_eventsWalks_filled.csv)")
    p.add_argument("--debug", type=bool, default= True)
    args = p.parse_args()

    out_path = process_events_walks_file(args.events_walks, args.reprocessed, args.out)
    print(f"✅ Wrote: {out_path}")

def _cli():
    import argparse, sys
    from pathlib import Path

    p = argparse.ArgumentParser(
        description="Fill missing *_start/_end pos/time/dist cols in *_eventsWalks.csv using *_reprocessed_with_dist.csv origRow lookups."
    )

    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--events-walks", type=str, help="Path to a single *_eventsWalks.csv")
    src.add_argument("--events-walks-dir", type=Path, help="Directory containing *_eventsWalks.csv files")

    p.add_argument("--reprocessed", default=None, type=str,
                   help="(single-file mode) Optional explicit path to *_reprocessed_with_dist.csv")
    p.add_argument("--reprocessed-dir", type=Path, default=None,
                   help="(dir mode) Directory containing *_reprocessed_with_dist.csv files (optional; inferred if None)")
    p.add_argument("--pattern", type=str, default="*_eventsWalks.csv", help="(dir mode) Glob pattern")
    p.add_argument("--recursive", action="store_true", help="(dir mode) Recurse")
    p.add_argument("--out", default=None, type=str,
                   help="(single-file mode) Output file path. If omitted, writes *_eventsWalks_filled.csv next to input.")
    p.add_argument("--out-dir", type=Path, default=None,
                   help="(dir mode) Output directory (required for --events-walks-dir)")
    p.add_argument("--in-suffix", type=str, default="_eventsWalks.csv",
                   help="(dir mode) Input suffix to replace.")
    p.add_argument("--out-suffix", type=str, default="_eventsWalks_filled.csv",
                   help="(dir mode) Output suffix.")
    p.add_argument("--overwrite", action="store_true", help="(dir mode) Overwrite outputs if exist")
    p.add_argument("--debug", action="store_true", help="Enable debug printing")

    args = p.parse_args()

    # ---- directory mode ----
    if args.events_walks_dir is not None:
        if args.out_dir is None:
            p.error("--out-dir is required when using --events-walks-dir")

        in_dir = args.events_walks_dir
        files = sorted(in_dir.rglob(args.pattern)) if args.recursive else sorted(in_dir.glob(args.pattern))
        if not files:
            print(f"No files matched in {in_dir} with pattern={args.pattern!r}", file=sys.stderr)
            sys.exit(2)

        failures = 0
        for f in files:
            try:
                # infer or find matching reproc file
                if args.reprocessed_dir is not None:
                    # match by replacing suffix
                    base = f.name[:-len(args.in_suffix)] if f.name.endswith(args.in_suffix) else f.stem
                    reproc = args.reprocessed_dir / f"{base}_reprocessed_with_dist.csv"
                    reproc_path = str(reproc)
                else:
                    reproc_path = None  # let your infer_reprocessed_path handle it

                out_path = args.out_dir / (
                    (f.name[:-len(args.in_suffix)] + args.out_suffix)
                    if f.name.endswith(args.in_suffix)
                    else (f.stem + args.out_suffix)
                )

                if out_path.exists() and not args.overwrite:
                    raise FileExistsError(f"Output exists: {out_path} (use --overwrite)")

                wrote = process_events_walks_file(
                    str(f),
                    reproc_path,
                    str(out_path),
                    debug=args.debug
                )
                print(f"✅ {f.name} -> {wrote}")
            except Exception as e:
                failures += 1
                print(f"❌ Failed: {f} :: {e}", file=sys.stderr)

        sys.exit(1 if failures else 0)

    # ---- single-file mode ----
    wrote = process_events_walks_file(args.events_walks, args.reprocessed, args.out, debug=args.debug)
    print(f"✅ Wrote: {wrote}")

if __name__ == "__main__":
    _cli()