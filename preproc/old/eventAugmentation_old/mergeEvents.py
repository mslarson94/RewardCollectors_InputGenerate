#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import argparse
from pathlib import Path
from typing import Dict, List, Sequence

import pandas as pd


# -------------------------------
# I/O helpers
# -------------------------------
def load_metadata(base_path: Path, sheet: str = "MagicLeapFiles") -> pd.DataFrame:
    path = base_path / "collatedData.xlsx"
    return pd.read_excel(path, sheet_name=sheet)


def get_file_paths(
    base_dir: Path,
    proc_dir: Path,
    file_stem: str,
    events_dir: str = "Events_AugFinal_withWalks",
    event_suffix: str = "_events_final.csv",
    breaker: str = "_processed",
    *,
    events_subdir: str = "augmented",
    #events_subdir: str = "",
) -> Dict[str, Path]:
    """
    Build paths for a given cleaned file stem.
    - Events CSV assumed under <proc>/<events_dir>/<events_subdir>/<stem><event_suffix>
    - Meta JSON under <proc>/MetaData_Flat/<stem>_meta.json
    """
    #events_stem = file_stem.replace("_processed", "")
    events_stem = file_stem.split(breaker)[0]
    csv_path = base_dir / proc_dir / events_dir / events_subdir / f"{events_stem}{event_suffix}"
    meta_path = base_dir / proc_dir / "MetaData_Flat" / f"{file_stem}_meta.json"
    return {"csv": csv_path, "meta": meta_path}


def collapse_meta_files(
    meta_paths: Sequence[Path], group_df: pd.DataFrame, out_event_suffix: str = "_events.csv"
) -> dict:
    combined = {"CoinRegistry": {}, "BlockStructureSummary": [], "SourceFiles": []}

    for path in meta_paths:
        with path.open("r", encoding="utf-8") as f:
            meta = json.load(f)
        for k, v in meta.get("CoinRegistry", {}).items():
            if k not in combined["CoinRegistry"]:
                combined["CoinRegistry"][k] = v
            else:
                combined["CoinRegistry"][k].update(v)
        combined["BlockStructureSummary"].extend(meta.get("BlockStructureSummary", []))

    combined["SourceFiles"] = [
        f"{Path(row['cleanedFile']).stem}{out_event_suffix}" for _, row in group_df.iterrows()
    ]
    return combined


# -------------------------------
# Core merge logic
# -------------------------------
def merge_group_files(
    group_df: pd.DataFrame,
    base_dir: Path,
    proc_dir: Path,
    group_key_fields: Sequence[str],
    events_dir: str = "Events_AugFinal_withWalks",
    event_suffix: str = "_events_final.csv",
    breaker: str = "_processed"
) -> dict:
    merged_csvs: List[pd.DataFrame] = []
    meta_paths: List[Path] = []

    for _, row in group_df.iterrows():
        file_stem = Path(row["cleanedFile"]).stem
        paths = get_file_paths(
            base_dir=base_dir,
            proc_dir=proc_dir,
            file_stem=file_stem,
            events_dir=events_dir,
            event_suffix=event_suffix,
            breaker=breaker,
        )
        print(paths["csv"])

        if paths["csv"].exists():
            merged_csvs.append(pd.read_csv(paths["csv"]))
        if paths["meta"].exists():
            meta_paths.append(paths["meta"])

    final_csv = pd.concat(merged_csvs, ignore_index=True) if merged_csvs else None
    final_meta = collapse_meta_files(meta_paths, group_df, "_events.csv") if meta_paths else None

    return {
        "csv": final_csv,
        "meta": final_meta,
        "group_info": group_df.iloc[0][list(group_key_fields)].to_dict(),
    }


def group_metadata(
    df: pd.DataFrame,
    keys: Sequence[str],
    base_dir: Path,
    proc_dir: Path,
    events_dir: str = "Events_AugFinal_withWalks",
    event_suffix: str = "_events_final.csv",
    breaker: str = "_processed",
):
    df = df.dropna(subset=keys)
    df = df[df["cleanedFile"].notna()]

    def file_exists(file_stem: str) -> bool:
        paths = get_file_paths(
            base_dir=base_dir,
            proc_dir=proc_dir,
            file_stem=file_stem,
            events_dir=events_dir,
            event_suffix=event_suffix,
            breaker=breaker
        )
        return any(p.exists() for p in paths.values())

    df = df.assign(file_stem=df["cleanedFile"].apply(lambda f: Path(f).stem))
    df = df[df["file_stem"].apply(file_exists)]
    df = df.assign(group_key=df[list(keys)].astype(str).agg("_".join, axis=1))
    return df.groupby("group_key")


def merge_all_versions(
    base_dir: Path,
    proc_dir: Path,
    group_key_fields: Sequence[str],
    events_dir: str = "Events_AugFinal_withWalks",
    event_suffix: str = "_events_final.csv",
    breaker: str = "_processed",
):
    metadata = load_metadata(base_dir)
    grouped = group_metadata(
        df=metadata,
        keys=group_key_fields,
        base_dir=base_dir,
        proc_dir=proc_dir,
        events_dir=events_dir,
        event_suffix=event_suffix,
        breaker=breaker,
    )

    merged_data = []
    for _, group_df in grouped:
        merged = merge_group_files(
            group_df=group_df,
            base_dir=base_dir,
            proc_dir=proc_dir,
            group_key_fields=group_key_fields,
            events_dir=events_dir,
            event_suffix=event_suffix,
            breaker=breaker,
        )
        merged_data.append(merged)
    return merged_data


# -------------------------------
# Export
# -------------------------------
def export_merged_data_v1(
    merged_data: Sequence[dict],
    export_dir: Path,
    group_key_fields: Sequence[str],
    *,
    out_suffix: str = "_events.csv",
):
    export_dir.mkdir(parents=True, exist_ok=True)
    flat_dir_csv = Path(str(export_dir) + "_Flat_csv")
    flat_dir_meta = Path(str(export_dir) + "_Flat_metaJson")
    print(str(export_dir))
    print(str(flat_dir_csv))
    flat_dir_csv.mkdir(parents=True, exist_ok=True)
    flat_dir_meta.mkdir(parents=True, exist_ok=True)

    for item in merged_data:
        group_info = item["group_info"]
        group_key = "_".join(str(group_info.get(k, "NA")) for k in group_key_fields)
        print(group_key, type(group_key))
        group_folder = export_dir / group_key
        group_folder.mkdir(parents=True, exist_ok=True)

        if item["csv"] is not None:
            nested_csv = group_folder / f"{group_key}{out_suffix}"
            item["csv"].to_csv(nested_csv, index=False)
            flat_csv = flat_dir_csv / f"{group_key}{out_suffix}"
            print("the flat directory", flat_csv)
            item["csv"].to_csv(flat_csv, index=False)

        if item["meta"] is not None:
            nested_meta = group_folder / f"{group_key}_meta.json"
            flat_meta = flat_dir_meta / f"{group_key}_meta.json"
            with nested_meta.open("w", encoding="utf-8") as f:
                json.dump(item["meta"], f, indent=2)
            with flat_meta.open("w", encoding="utf-8") as f:
                json.dump(item["meta"], f, indent=2)


# -------------------------------
# CLI
# -------------------------------
def cli() -> None:
    parser = argparse.ArgumentParser(
        prog="mergeEventsV2",
        description="Merge final events files by specific keys",
    )
    parser.add_argument(
        "--root-dir",
        required=True,
        type=Path,
        help="Base project directory (e.g., '/Users/you/RC_TestingNotes').",
    )
    parser.add_argument(
        "--proc-dir",
        required=True,
        type=Path,
        help="Dataset subdirectory under --root-dir (e.g., 'FreshStart'). "
        "If absolute, --root-dir is ignored. 'full' will be appended.",
    )
    parser.add_argument(
        "--event-dir",
        default="Events_AugFinal_withWalks",
        help="Subdirectory under <root/proc/full> that holds source event CSVs.",
    )
    parser.add_argument(
        "--eventEnding",
        default="_events_final.csv",
        help="File suffix for source event CSV files (e.g., '_events_final.csv').",
    )
    parser.add_argument(
        "--outEnding",
        default="_events.csv",
        help="File suffix for merged CSV files (e.g., '_events.csv').",
    )
    parser.add_argument(
        "--output-dir",
        default="MergedEvents",
        help="Output directory name under <root/proc/full>.",
    )
    parser.add_argument(
        "--group-key-fields",
        dest="group_key_fields",
        action="append",
        choices=[
            "participantID",
            "pairID",
            "testingDate",
            "currentRole",
            "ptIsAorB",
            "coinSet",
            "sessionType",
            "device",
            "main_RR",
            "BlockNum",
            "BlockType",
            "coinLabel",
            "actualClosestCoinLabel",
        ],
        default=["participantID", "testingDate", "currentRole", "coinSet", "device", "main_RR"],
        help="Key fields used to merge event files. Repeat flag to add multiple keys.",
    )
    parser.add_argument(
        "--breaker",
        dest="breaker",
        default="_processed",
        help="the breaking point in the file naming scheme that we can use to index back into the collatedData file")
    args = parser.parse_args()

    root = args.root_dir.expanduser()
    proc = args.proc_dir
    proc_dir = (proc if proc.is_absolute() else (root / proc)) / "full"
    breaker = args.breaker

    events_dir_path = proc_dir / args.event_dir / "augmented"
    #events_dir_path = proc_dir / args.event_dir 
    meta_dir_path = proc_dir / "MetaData_Flat"
    output_dir = proc_dir / args.output_dir

    for p, label in (
        (proc_dir, "proc-dir"),
        (events_dir_path, "events-dir"),
        (meta_dir_path, "meta-dir"),
    ):
        if not p.exists():
            parser.error(f"{label} not found: {p}")

    output_dir.mkdir(parents=True, exist_ok=True)

    merged_data = merge_all_versions(
        base_dir=root,
        proc_dir=proc_dir,
        breaker=breaker,
        group_key_fields=args.group_key_fields,
        events_dir=args.event_dir,
        event_suffix=args.eventEnding,
    )

    export_merged_data_v1(
        merged_data=merged_data,
        export_dir=output_dir,
        group_key_fields=args.group_key_fields,
        out_suffix=args.outEnding,
    )


if __name__ == "__main__":
    cli()

## Usage if for some reason you want all possible group key fields (not recommended, but left it so you can copy/paste what you want specifically)

# python "${CODE_DIR}/eventAugmentation/mergeEventsV3.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --event-dir "Events_AugFinal_withWalks" \
#   --eventEnding "_events_final.csv" \
#   --outEnding "_events.csv" \
#   --output-dir "MergedEvents" \
#   --group-key-fields participantID \
#   --group-key-fields pairID \
#   --group-key-fields testingDate \
#   --group-key-fields currentRole \
#   --group-key-fields ptIsAorB \
#   --group-key-fields coinSet \
#   --group-key-fields sessionType \
#   --group-key-fields device \
#   --group-key-fields main_RR \
#   --group-key-fields BlockNum \
#   --group-key-fields BlockType \
#   --group-key-fields coinLabel \
#   --group-key-fields actualClosestCoinLabel
