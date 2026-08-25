# computeEarliestRoundStart.py

import argparse
from pathlib import Path

import pandas as pd


SOURCE_EVENT_TYPE = "InterRound_PostCylinderWalk_segment"
NEW_EVENT_TYPE = "earliestRoundStart"


def create_earliest_round_start_rows_v1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Duplicate InterRound_PostCylinderWalk_segment rows as earliestRoundStart.

    All source-row values are preserved except:
    - lo_eventType becomes earliestRoundStart.
    - Every matching *_end field is replaced by its *_start value.
    - end_AppTime becomes start_AppTime.
    - end_eMLT_orig becomes start_eMLT_orig.
    """
    if "lo_eventType" not in df.columns:
        raise ValueError("Missing required column: lo_eventType")

    source_mask = (
        df["lo_eventType"]
        .astype("string")
        .str.strip()
        .eq(SOURCE_EVENT_TYPE)
    )

    new_rows = df.loc[source_mask].copy()

    if new_rows.empty:
        return new_rows

    new_rows["lo_eventType"] = NEW_EVENT_TYPE

    if "source" in new_rows.columns:
        new_rows["source"] = "synthetic"

    for end_column in df.columns:
        if not end_column.endswith("_end"):
            continue

        start_column = f"{end_column[:-4]}_start"

        if start_column in df.columns:
            new_rows[end_column] = new_rows[start_column]

    explicit_pairs = {
        "end_AppTime": "start_AppTime",
        "end_eMLT_orig": "start_eMLT_orig",
    }

    for end_column, start_column in explicit_pairs.items():
        if end_column in df.columns and start_column in df.columns:
            new_rows[end_column] = new_rows[start_column]

    return new_rows

def find_next_round_reference_event(
    df: pd.DataFrame,
    source_row: pd.Series,
) -> pd.Series | None:
    """
    Find the next real event that defines the RoundNum and chestPin_num
    for an earliestRoundStart row.

    collecting:
        next RoundStart

    pindropping:
        next PinDrop_Moment

    Matching is restricted to the same block, but intentionally does not
    use RoundNum because RoundNum is the field being corrected.
    """
    block_type = str(source_row.get("BlockType", "")).strip().lower()

    reference_event_types = {
        "collecting": "RoundStart",
        "pindropping": "PinDrop_Moment",
    }

    target_event_type = reference_event_types.get(block_type)

    if target_event_type is None:
        return None

    source_time = pd.to_numeric(
        source_row.get("start_AppTime", pd.NA),
        errors="coerce",
    )

    if pd.isna(source_time):
        return None

    candidates = df.copy()

    candidates["_match_start_AppTime"] = pd.to_numeric(
        candidates["start_AppTime"],
        errors="coerce",
    )

    candidates = candidates[
        candidates["lo_eventType"]
        .astype("string")
        .str.strip()
        .eq(target_event_type)
    ]

    # Stay inside the same block.
    for block_column in ("BlockInstance", "BlockNum"):
        if block_column not in df.columns:
            continue

        source_value = source_row.get(block_column, pd.NA)

        if pd.isna(source_value):
            continue

        candidates = candidates[
            candidates[block_column].eq(source_value)
        ]

    # Only events occurring after the InterRound segment begins.
    candidates = candidates[
        candidates["_match_start_AppTime"] > source_time
    ]

    if candidates.empty:
        return None

    # "Real" event preference. The event-type filter is already restrictive,
    # but this protects against future synthetic RoundStart/PinDrop rows.
    if "source" in candidates.columns:
        real_candidates = candidates[
            ~candidates["source"]
            .astype("string")
            .str.lower()
            .eq("synthetic")
        ]

        if not real_candidates.empty:
            candidates = real_candidates

    candidates = candidates.sort_values(
        "_match_start_AppTime",
        kind="stable",
    )

    return candidates.iloc[0]


def create_earliest_round_start_rows(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Create earliestRoundStart rows from InterRound_PostCylinderWalk_segment.

    Each generated row:
    - inherits the source InterRound_PostCylinderWalk_segment values;
    - changes lo_eventType to earliestRoundStart;
    - collapses each available end value onto its corresponding start value;
    - inherits RoundNum and chestPin_num from the next relevant real event:
        collecting   -> next RoundStart
        pindropping  -> next PinDrop_Moment
    """
    if "lo_eventType" not in df.columns:
        raise ValueError(
            "Input dataframe is missing required column: lo_eventType"
        )

    if "start_AppTime" not in df.columns:
        raise ValueError(
            "Input dataframe is missing required column: start_AppTime"
        )

    source_mask = (
        df["lo_eventType"]
        .astype("string")
        .str.strip()
        .eq("InterRound_PostCylinderWalk_segment")
    )

    source_rows = df.loc[source_mask].copy()

    if source_rows.empty:
        return source_rows

    generated_rows = []

    for source_index, source_row in source_rows.iterrows():
        new_row = source_row.copy()

        new_row["lo_eventType"] = "earliestRoundStart"

        if "source" in df.columns:
            new_row["source"] = "synthetic"

        # Collapse suffix-style end fields onto their matching start fields.
        for end_column in df.columns:
            if not end_column.endswith("_end"):
                continue

            start_column = f"{end_column[:-4]}_start"

            if start_column in df.columns:
                new_row[end_column] = source_row.get(
                    start_column,
                    pd.NA,
                )

        # Handle fields using end_* / start_* naming.
        explicit_pairs = {
            "end_AppTime": "start_AppTime",
            "end_eMLT_orig": "start_eMLT_orig",
        }

        for end_column, start_column in explicit_pairs.items():
            if (
                end_column in df.columns
                and start_column in df.columns
            ):
                new_row[end_column] = source_row.get(
                    start_column,
                    pd.NA,
                )

        reference_event = find_next_round_reference_event(
            df=df,
            source_row=source_row,
        )

        if reference_event is None:
            block_type = source_row.get("BlockType", pd.NA)
            block_num = source_row.get("BlockNum", pd.NA)
            block_instance = source_row.get(
                "BlockInstance",
                pd.NA,
            )
            start_time = source_row.get(
                "start_AppTime",
                pd.NA,
            )

            print(
                "⚠️ Could not find next round-reference event for "
                f"row {source_index}: "
                f"BlockType={block_type}, "
                f"BlockNum={block_num}, "
                f"BlockInstance={block_instance}, "
                f"start_AppTime={start_time}"
            )

            # Do not retain potentially incorrect inherited round coding.
            if "RoundNum" in df.columns:
                new_row["RoundNum"] = 5555


        else:
            if "RoundNum" in df.columns:
                new_row["RoundNum"] = reference_event.get(
                    "RoundNum",
                    pd.NA,
                )


        generated_rows.append(new_row)

    new_rows = pd.DataFrame(
        generated_rows,
        columns=df.columns,
    )

    return new_rows

def validate_earliest_round_start_rows(
    rows: pd.DataFrame,
) -> list[str]:
    """
    Validate that all generated earliestRoundStart rows have matching
    start/end values wherever corresponding fields exist.
    """
    errors = []

    if rows.empty:
        return errors

    explicit_pairs = {
        "end_AppTime": "start_AppTime",
        "end_eMLT_orig": "start_eMLT_orig",
    }

    comparison_pairs = []

    for end_column in rows.columns:
        if not end_column.endswith("_end"):
            continue

        start_column = f"{end_column[:-4]}_start"

        if start_column in rows.columns:
            comparison_pairs.append((start_column, end_column))

    for end_column, start_column in explicit_pairs.items():
        if start_column in rows.columns and end_column in rows.columns:
            comparison_pairs.append((start_column, end_column))

    for start_column, end_column in comparison_pairs:
        start_values = rows[start_column]
        end_values = rows[end_column]

        equal_mask = (
            start_values.eq(end_values)
            | (start_values.isna() & end_values.isna())
        )

        mismatch_count = int((~equal_mask).sum())

        if mismatch_count:
            errors.append(
                f"{start_column} != {end_column}: "
                f"{mismatch_count} mismatched row(s)"
            )

    return errors


def build_validation_dataframe(
    source_rows: pd.DataFrame,
    generated_rows: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a compact debug file containing source rows and generated rows.
    """
    source_debug = source_rows.copy()
    generated_debug = generated_rows.copy()

    source_debug["_validation_role"] = "source"
    generated_debug["_validation_role"] = "generated"

    validation_df = pd.concat(
        [source_debug, generated_debug],
        ignore_index=True,
    )

    sort_columns = []

    for column in (
        "BlockInstance",
        "BlockNum",
        "effectiveRoundNum",
        "RoundNum",
        "start_AppTime",
        "_validation_role",
    ):
        if column in validation_df.columns:
            sort_columns.append(column)

    if sort_columns:
        validation_df = (
            validation_df
            .sort_values(sort_columns, kind="stable")
            .reset_index(drop=True)
        )

    return validation_df


def process_event_file(
    input_path: Path,
    merged_output_path: Path,
    validation_output_path: Path | None = None,
) -> int:
    """
    Add earliestRoundStart rows to one event CSV.

    Returns the number of generated rows.
    """
    df = pd.read_csv(input_path)

    source_mask = (
        df["lo_eventType"]
        .astype("string")
        .str.strip()
        .eq(SOURCE_EVENT_TYPE)
    )

    source_rows = df.loc[source_mask].copy()
    new_rows = create_earliest_round_start_rows(df)

    if new_rows.empty:
        print(
            f"⚠️ {input_path.name}: no "
            f"'{SOURCE_EVENT_TYPE}' rows found."
        )
        return 0

    validation_errors = validate_earliest_round_start_rows(new_rows)

    if validation_errors:
        error_text = "\n".join(
            f"  - {error}"
            for error in validation_errors
        )

        raise ValueError(
            f"Validation failed for {input_path.name}:\n"
            f"{error_text}"
        )

    new_rows = new_rows.reindex(columns=df.columns)

    merged_df = pd.concat(
        [df, new_rows],
        ignore_index=True,
    )

    if "start_AppTime" in merged_df.columns:
        merged_df["start_AppTime"] = pd.to_numeric(
            merged_df["start_AppTime"],
            errors="coerce",
        )

        merged_df = (
            merged_df
            .sort_values("start_AppTime", kind="stable")
            .reset_index(drop=True)
        )

    merged_output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    merged_df.to_csv(
        merged_output_path,
        index=False,
    )

    if validation_output_path is not None:
        validation_df = build_validation_dataframe(
            source_rows=source_rows,
            generated_rows=new_rows,
        )

        validation_output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        validation_df.to_csv(
            validation_output_path,
            index=False,
        )

    print(
        f"✅ {input_path.name}: "
        f"created {len(new_rows)} '{NEW_EVENT_TYPE}' rows"
    )

    return len(new_rows)


def batch_compute_earliest_round_start(
    events_dir: Path,
    output_dir: Path,
    events_ending: str = "events_flat",
    debug_validation: bool = False,
) -> None:
    """
    Batch-create earliestRoundStart events for matching CSV files.
    """
    events_dir = Path(events_dir)
    output_dir = Path(output_dir)

    if not events_dir.exists():
        raise FileNotFoundError(
            f"Events directory not found: {events_dir}"
        )

    event_files = {
        file.stem.removesuffix(f"_{events_ending}"): file
        for file in events_dir.glob(f"*_{events_ending}.csv")
    }

    if not event_files:
        print(
            f"⚠️ No files matching '*_{events_ending}.csv' "
            f"found in {events_dir}"
        )
        return

    merged_output_dir = (
        output_dir 
    )

    validation_dir = (
        output_dir / "EarliestRoundStartValidation"
    )

    merged_output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    if debug_validation:
        validation_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

    total_new_rows = 0
    processed_files = 0

    for key in sorted(event_files):
        event_file = event_files[key]

        merged_output_file = (
            merged_output_dir
            / f"{key}_earliestRoundStart.csv"
        )

        validation_output_file = None

        if debug_validation:
            validation_output_file = (
                validation_dir
                / f"{key}_validation.csv"
            )

        print(
            f"\n➡️ Computing earliestRoundStart for: "
            f"{event_file.name}"
        )

        generated_count = process_event_file(
            input_path=event_file,
            merged_output_path=merged_output_file,
            validation_output_path=validation_output_file,
        )

        if generated_count == 0:
            continue

        processed_files += 1
        total_new_rows += generated_count

        print(
            f"   → {merged_output_file.name}"
        )

        if validation_output_file is not None:
            print(
                f"   → validation: "
                f"{validation_output_file.name}"
            )

    print(
        f"\n✅ Batch complete."
        f"\n   Files processed: {processed_files}"
        f"\n   New rows:        {total_new_rows}"
    )


def cli() -> None:
    parser = argparse.ArgumentParser(
        prog="computeEarliestRoundStart",
        description=(
            "Create earliestRoundStart events from "
            "InterRound_PostCylinderWalk_segment rows."
        ),
    )

    parser.add_argument(
        "--root-dir",
        required=True,
        type=Path,
        help=(
            "Base project directory "
            "(e.g. '/Users/you/RC_TestingNotes')."
        ),
    )

    parser.add_argument(
        "--proc-dir",
        required=True,
        type=Path,
        help=(
            "Dataset subdirectory under --root-dir. "
            "If absolute, --root-dir is ignored."
        ),
    )

    parser.add_argument(
        "--events-dir-name",
        default="Events_AugPart1",
        help=(
            "Subdirectory under EventSegmentation "
            "containing input event CSV files."
        ),
    )

    parser.add_argument(
        "--output-dir-name",
        default="Events_ComputedEarliestRoundStart",
        help=(
            "Output subdirectory under EventSegmentation."
        ),
    )

    parser.add_argument(
        "--eventsEnding",
        dest="events_ending",
        default="events_flat",
        help=(
            "Input filename suffix before '.csv'. "
            "Examples: events_flat, eventsWalks."
        ),
    )

    parser.add_argument(
        "--debug-validation",
        action="store_true",
        help=(
            "Write source InterRound_PostCylinderWalk_segment "
            "rows and generated earliestRoundStart rows "
            "to a validation CSV."
        ),
    )

    args = parser.parse_args()

    root = args.root_dir.expanduser()
    proc = args.proc_dir.expanduser()

    base_dir = (
        proc
        if proc.is_absolute()
        else root / proc
    ) / "EventSegmentation"

    events_dir = base_dir / args.events_dir_name
    output_dir = base_dir / args.output_dir_name

    if not base_dir.exists():
        parser.error(
            f"base-dir not found: {base_dir}"
        )

    if not events_dir.exists():
        parser.error(
            f"events-dir not found: {events_dir}"
        )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    print(
        "🚀 Starting batch earliestRoundStart creation..."
    )
    print(f"Input:  {events_dir}")
    print(f"Output: {output_dir}")
    print(f"Ending: _{args.events_ending}.csv")

    if args.debug_validation:
        print("Debug validation: ON")

    batch_compute_earliest_round_start(
        events_dir=events_dir,
        output_dir=output_dir,
        events_ending=args.events_ending,
        debug_validation=args.debug_validation,
    )


if __name__ == "__main__":
    cli()