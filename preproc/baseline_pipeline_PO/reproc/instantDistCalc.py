#!/usr/bin/env python3
"""
Reprocess *_processed.csv files by adding distance columns to:
- fixed POSITIONS targets
- reward targets (HV/LV/NV) determined dynamically via:
    events CSV -> coinSet -> CoinSets.csv lookup

Matching:
- input file is .../<NAME>_processed.csv
- base name is <NAME> (strip "_processed" from stem)
- events file is .../<NAME><events_suffix> (suffix provided via CLI)
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


# 8 labeled positions
POSITIONS: dict[str, tuple[float, float]] = {
    "pos1": (0.0, 5.0),
    "pos2": (3.5, 3.5),
    "pos3": (5.0, 0.0),
    "pos4": (3.5, -3.5),
    "pos5": (0.0, -5.0),
    "pos6": (-3.5, -3.5),
    "pos7": (-5.0, 0.0),
    "pos8": (-3.5, 3.5),
    "pos888": (0.0, -5.0),
    "pos999": (2.0, -5.0),
}

# Fallback reward positions (only if you choose to allow fallback)
DEFAULT_REWARDS: dict[str, tuple[float, float]] = {
    "HV": (1.36, -3.04),
    "LV": (-3.76, -0.1),
    "NV": (-0.57, 2.4),
}

X_COL = "HeadPosAnchored_x"
Z_COL = "HeadPosAnchored_z"

EVENTS_COINSET_COL = "coinSet"   # where to read coinSet from the events file
INPUT_SUFFIX_TO_STRIP = "_reprocessed"

COINSETS_COINSET_COL = "coinSet"
COINSETS_REQUIRED_COLS = {
    COINSETS_COINSET_COL,
    "HV_x", "HV_z",
    "LV_x", "LV_z",
    "NV_x", "NV_z",
}


def load_coinsets_table(coinsets_path: Path) -> pd.DataFrame:
    cs = pd.read_csv(coinsets_path)

    missing = COINSETS_REQUIRED_COLS - set(cs.columns)
    if missing:
        raise KeyError(f"CoinSets file missing required columns: {sorted(missing)}")

    cs = cs.copy()
    cs["CoinSet_norm"] = cs[COINSETS_COINSET_COL].astype(str).str.strip().str.lower()
    return cs


def compute_base_name_from_processed(input_path: Path) -> str:
    """
    For input like 'subject01_processed.csv' -> 'subject01'
    For 'subject01_processed_something.csv' (unexpected), only strips trailing '_processed' if present.
    """
    stem = input_path.stem  # no extension
    if stem.endswith(INPUT_SUFFIX_TO_STRIP):
        base = stem[: -len(INPUT_SUFFIX_TO_STRIP)]
        # If it ends with underscore due to naming like "abc__processed" or "abc_processed" slicing
        return base.rstrip("_")
    # If someone passes a file not matching pattern, we still treat full stem as base
    return stem


def find_matching_events_file(
    input_processed_path: Path,
    events_dir: Path | None,
    events_suffix: str,
) -> Path:
    base = compute_base_name_from_processed(input_processed_path)
    search_dir = events_dir if events_dir is not None else input_processed_path.parent

    # events_suffix should include extension (e.g., "_events.csv")
    candidate = search_dir / f"{base}{events_suffix}"
    if candidate.exists():
        return candidate

    # If not found, provide a helpful error
    # Also offer a simple glob suggestion
    glob_hits = sorted(search_dir.glob(f"{base}*"))
    hint = f"Tried: {candidate}"
    if glob_hits:
        hint += f" | Nearby matches: {', '.join(p.name for p in glob_hits[:10])}"
    raise FileNotFoundError(f"Matching events file not found. {hint}")


def detect_coinset_from_events(events_path: Path) -> str:
    ev = pd.read_csv(events_path)
    if EVENTS_COINSET_COL not in ev.columns:
        raise KeyError(f"Events file missing required column {EVENTS_COINSET_COL!r}: {events_path}")

    s = ev[EVENTS_COINSET_COL].dropna()
    if s.empty:
        raise ValueError(f"Events file has no non-null {EVENTS_COINSET_COL!r} values: {events_path}")

    return str(s.iloc[0]).strip()


def rewards_for_coinset(coinsets_df: pd.DataFrame, coinset_value: str) -> dict[str, tuple[float, float]]:
    key = str(coinset_value).strip().lower()
    hit = coinsets_df.loc[coinsets_df["CoinSet_norm"] == key]
    if hit.empty:
        known = ", ".join(sorted(coinsets_df[COINSETS_COINSET_COL].astype(str).unique()))
        raise KeyError(f"coinSet {coinset_value!r} not found in CoinSets. Known: {known}")

    row = hit.iloc[0]
    return {
        "HV": (float(row["HV_x"]), float(row["HV_z"])),
        "LV": (float(row["LV_x"]), float(row["LV_z"])),
        "NV": (float(row["NV_x"]), float(row["NV_z"])),
    }


def add_distance_columns(df: pd.DataFrame, rewards: dict[str, tuple[float, float]]) -> pd.DataFrame:
    if X_COL not in df.columns or Z_COL not in df.columns:
        raise KeyError(f"Missing required columns: {X_COL!r} and/or {Z_COL!r}")

    x = pd.to_numeric(df[X_COL], errors="coerce")
    z = pd.to_numeric(df[Z_COL], errors="coerce")

    targets: dict[str, tuple[float, float]] = {}
    targets.update(POSITIONS)
    targets.update(rewards)

    for label, (tx, tz) in targets.items():
        df[f"dist_{label}"] = ((x - tx) ** 2 + (z - tz) ** 2) ** 0.5

    return df


def process_one_file(
    in_path: Path,
    out_dir: Path,
    overwrite: bool,
    coinsets_df: pd.DataFrame,
    events_dir: Path | None,
    events_suffix: str,
    allow_fallback_rewards: bool,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{in_path.stem}_with_dist.csv"

    if out_path.exists() and not overwrite:
        raise FileExistsError(f"Output exists: {out_path}. Use --overwrite to replace it.")

    df = pd.read_csv(in_path)

    # Find events, detect coinSet, map to rewards
    try:
        events_path = find_matching_events_file(in_path, events_dir, events_suffix)
        coinset_value = detect_coinset_from_events(events_path)
        rewards = rewards_for_coinset(coinsets_df, coinset_value)
    except Exception as e:
        if not allow_fallback_rewards:
            raise
        rewards = DEFAULT_REWARDS
        print(f"⚠️  {in_path.name}: {e} -> using DEFAULT_REWARDS", file=sys.stderr)

    df = add_distance_columns(df, rewards)

    drop_cols = ["BlockInstance_int"]
    df = df.drop(columns=drop_cols, errors="ignore")

    df.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description="Add distance-to-target columns to *_processed.csv using events+CoinSets lookup.")

    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--input", type=Path, help="Path to a single input *_processed.csv")
    src.add_argument("--input-dir", type=Path, help="Directory containing input *_processed.csv files")

    ap.add_argument(
        "--output",
        type=Path,
        help="(single-file mode) Output directory. If omitted, writes alongside input with _with_dist suffix.",
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        help="(dir mode) Output directory (required when using --input-dir)",
    )
    ap.add_argument("--overwrite", action="store_true", help="Overwrite output file(s) if they exist")

    ap.add_argument(
        "--coinsets",
        type=Path,
        default=Path("CoinSets.csv"),
        help="Path to CoinSets.csv (default: ./CoinSets.csv)",
    )

    ap.add_argument(
        "--events-dir",
        type=Path,
        default=None,
        help="Directory containing events CSVs. If omitted, uses the input file's directory.",
    )
    ap.add_argument(
        "--events-suffix",
        type=str,
        required=True,
        help="Suffix pattern for matching events file, e.g. '_events.csv' so base+suffix is the events filename.",
    )

    ap.add_argument(
        "--allow-fallback-rewards",
        action="store_true",
        help="If matching/lookup fails, use DEFAULT_REWARDS instead of erroring.",
    )

    args = ap.parse_args()

    coinsets_df = load_coinsets_table(args.coinsets)

    # ---- directory mode ----
    if args.input_dir is not None:
        in_dir: Path = args.input_dir
        out_dir: Path | None = args.output_dir
        if out_dir is None:
            ap.error("--output-dir is required when using --input-dir")

        # Only process *_processed.csv by default
        csvs = sorted(in_dir.glob(f"*{INPUT_SUFFIX_TO_STRIP}.csv"))
        if not csvs:
            print(f"No '*{INPUT_SUFFIX_TO_STRIP}.csv' files found in: {in_dir}", file=sys.stderr)
            sys.exit(2)

        failures = 0
        for in_path in csvs:
            try:
                out_path = process_one_file(
                    in_path=in_path,
                    out_dir=out_dir,
                    overwrite=args.overwrite,
                    coinsets_df=coinsets_df,
                    events_dir=args.events_dir,
                    events_suffix=args.events_suffix,
                    allow_fallback_rewards=args.allow_fallback_rewards,
                )
                print(f"✅ {in_path.name} -> {out_path}")
            except Exception as e:
                failures += 1
                print(f"❌ Failed: {in_path} :: {e}", file=sys.stderr)

        if failures:
            sys.exit(1)
        return

    # ---- single-file mode ----
    in_path: Path = args.input
    if args.output is None:
        out_dir = in_path.parent
    else:
        out_dir = Path(args.output)
        out_dir.mkdir(parents=True, exist_ok=True)

    out_path = process_one_file(
        in_path=in_path,
        out_dir=out_dir,
        overwrite=args.overwrite,
        coinsets_df=coinsets_df,
        events_dir=args.events_dir,
        events_suffix=args.events_suffix,
        allow_fallback_rewards=args.allow_fallback_rewards,
    )
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()
