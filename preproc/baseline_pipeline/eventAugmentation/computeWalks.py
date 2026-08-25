# computeWalks.py
#
# These are the specific edits I recommend.
# Existing unrelated functions can remain unchanged.

import argparse
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd


EVENT_TYPE_CONFIG = {
    "AN": {
        "file_prefix": "ObsReward_A",
        "round_start_event": "TrueContentStart",
    },
    "PO": {
        "file_prefix": "ObsReward_B",
        "round_start_event": "earliestRoundStart",
    },
}


def build_end_events_Walks(row):
    return {
        "path_step_in_round": row.get("path_step_in_round", pd.NA),

        "end_eMLT_orig": row.get("start_eMLT_orig"),
        "end_AppTime": row.get("start_AppTime", pd.NA),
        "origRow_end": row.get("origRow_end", pd.NA),

        "HeadPosAnchored_x_at_end": row.get("HeadPosAnchored_x_at_start", pd.NA),
        "HeadPosAnchored_y_at_end": row.get("HeadPosAnchored_y_at_start", pd.NA),
        "HeadPosAnchored_z_at_end": row.get("HeadPosAnchored_z_at_start", pd.NA),
        "HeadForthAnchored_yaw_at_end": row.get("HeadForthAnchored_yaw_at_start", pd.NA),
        "HeadForthAnchored_pitch_at_end": row.get("HeadForthAnchored_pitch_at_start", pd.NA),
        "HeadForthAnchored_roll_at_end": row.get("HeadForthAnchored_roll_at_start", pd.NA),

        "currSpeed_end": row.get("currSpeed_start", pd.NA),
        "dt_end": row.get("dt_start", pd.NA),

        "roundElapsed_s_end": row.get("roundElapsed_s_start", pd.NA),
        "blockElapsed_s_end": row.get("blockElapsed_s_start", pd.NA),
        "totalSessionElapsed_s_end": row.get("totalSessionElapsed_s_start", pd.NA),
        "roundFrac_end": row.get("roundFrac_start", pd.NA),
        "blockFrac_end": row.get("blockFrac_start", pd.NA),

        "stepDist_end": row.get("stepDist_start", pd.NA),
        "totDistBlock_current_end": row.get("totDistBlock_current_start", pd.NA),
        "totDistRound_current_end": row.get("totDistRound_current_start", pd.NA),
    }


def compute_collecting_walks(group: pd.DataFrame):
    rows = []
    g = group.sort_values("start_AppTime").copy()

    g["start_AppTime"] = pd.to_numeric(g["start_AppTime"],errors="coerce",)
    if "end_AppTime" in g.columns:
        g["end_AppTime"] = pd.to_numeric(g["end_AppTime"],errors="coerce",)

    chests = g[g["lo_eventType"].astype("string").str.strip().eq("ChestOpen_Moment")].copy()

    starts = g[g["lo_eventType"].astype("string").str.strip().eq("TrueContentStart")].copy()

    coins_end = g[g["lo_eventType"].astype("string").str.strip().eq("CoinVis_end")].copy()

    chests = chests.dropna(subset=["start_AppTime"])
    starts = starts.dropna(subset=["start_AppTime"])
    coins_end = coins_end.dropna(subset=["start_AppTime"])

    if chests.empty:
        return rows

    chest1 = chests.iloc[0]
    prev_starts = starts[starts["start_AppTime"] <= chest1["start_AppTime"]]

    if not prev_starts.empty:
        start = prev_starts.iloc[-1]
        walk_time = (chest1["start_AppTime"] - start["start_AppTime"])

        end_event_fields = build_end_events_Walks(chest1)
        row = start.to_dict()
        row.update(
            {
                "lo_eventType": "Walk_ChestOpen",
                "med_eventType": "RewardMemoryDrivenNav",
                "hi_eventType": "WalkingPeriod",
                "hiMeta_eventType": "PreBlockActivity",
                "source": "synthetic",
                "walkTime": walk_time,
                "chestPin_num": chest1.get("chestPin_num",pd.NA,),
                **end_event_fields,
            }
        )
        rows.append(row)

    for i in range(1, len(chests)):
        chest = chests.iloc[i]
        prev_chest = chests.iloc[i - 1]
        anchor = prev_chest.get("start_AppTime",pd.NA,)

        if pd.isna(anchor) or pd.isna(chest.get("start_AppTime", pd.NA)):
            continue

        walk_time = chest["start_AppTime"] - anchor
        end_event_fields = build_end_events_Walks(chest)

        row = prev_chest.to_dict()
        row.update(
            {
                "lo_eventType": "Walk_ChestOpen",
                "med_eventType": "RewardMemoryDrivenNav",
                "hi_eventType": "WalkingPeriod",
                "hiMeta_eventType": "PreBlockActivity",
                "source": "synthetic",
                "walkTime": walk_time,
                "chestPin_num": chest.get("chestPin_num", pd.NA),
                **end_event_fields,
            }
        )
        rows.append(row)

    return rows


def compute_pindrop_walks(
    group: pd.DataFrame,
    phase_label: str,
    *,
    round_start_event: str,
):
    """
    Compute PinDrop walking segments.

    The first pin is anchored to the most recent configured round-start
    event. Later pins use the existing coin-collection / CoinVis logic.
    """
    rows = []
    g = group.sort_values("start_AppTime")

    pins = g[g["lo_eventType"] == "PinDrop_Moment"].copy()

    starts = g[g["lo_eventType"] == round_start_event].copy()

    coin_collect = g[g["lo_eventType"] == "CoinCollect_Moment_PinDrop"].copy()

    coin_vis = g[g["lo_eventType"] == "CoinVis"].copy()

    for d in (pins, starts, coin_collect, coin_vis):

        d["start_AppTime"] = pd.to_numeric(d["start_AppTime"], errors="coerce")

    coin_vis["end_AppTime"] = pd.to_numeric(coin_vis.get("end_AppTime"), errors="coerce")

    pins = pins.dropna(subset=["start_AppTime"])
    starts = starts.dropna(subset=["start_AppTime"])
    coin_collect = coin_collect.dropna(subset=["start_AppTime"])
    coin_vis = coin_vis.dropna(subset=["start_AppTime"])

    if pins.empty:
        return rows

    for _, pin in pins.iterrows():
        pin_time = pin["start_AppTime"]
        pin_num = pin.get("chestPin_num", pd.NA)

        if pd.notna(pin_num) and int(float(pin_num)) == 1:

            prev_starts = starts[starts["start_AppTime"] <= pin_time]

            if prev_starts.empty:
                continue

            start = prev_starts.iloc[-1]
            anchor_time = start["start_AppTime"]

            walk_time = pin_time - anchor_time
            end_event_fields = build_end_events_Walks(pin)

            row = start.to_dict()
            row.update(
                {
                    "lo_eventType": "Walk_PinDrop",
                    "med_eventType": phase_label,
                    "hi_eventType": "WalkingPeriod",
                    "hiMeta_eventType": "BlockActivity",
                    "source": "synthetic",
                    "walkTime": walk_time,
                    "chestPin_num": pin.get("chestPin_num", pd.NA),
                    **end_event_fields,
                }
            )
            rows.append(row)
            continue

        prev_collect = coin_collect[coin_collect["start_AppTime"] < pin_time]

        if not prev_collect.empty:
            anchor_row = prev_collect.iloc[-1]
            anchor_time = anchor_row["start_AppTime"]
        else:
            prev_vis = coin_vis[coin_vis["start_AppTime"] < pin_time]

            if prev_vis.empty:
                continue

            anchor_row = prev_vis.iloc[-1]
            anchor_time = anchor_row.get("end_AppTime",pd.NA)

            if pd.isna(anchor_time):
                anchor_time = anchor_row.get("start_AppTime",pd.NA)

            if pd.isna(anchor_time):
                continue

        walk_time = pin_time - anchor_time
        end_event_fields = build_end_events_Walks(pin)

        row = anchor_row.to_dict()
        row.update(
            {
                "lo_eventType": "Walk_PinDrop",
                "med_eventType": phase_label,
                "hi_eventType": "WalkingPeriod",
                "hiMeta_eventType": "BlockActivity",
                "source": "synthetic",
                "walkTime": walk_time,
                "chestPin_num": pin.get("chestPin_num", pd.NA),
                **end_event_fields,
            }
        )
        rows.append(row)

    return rows


def compute_walk_rows(
    flat_path,
    meta_path,
    out_path,
    merge_outpath,
    *,
    round_start_event: str,
):
    with open(meta_path, "r") as f:
        meta = json.load(f)

    df = pd.read_csv(flat_path)

    df["start_AppTime"] = pd.to_numeric(df["start_AppTime"], errors="coerce")
    
    df["end_AppTime"] = pd.to_numeric(df["end_AppTime"], errors="coerce")

    all_rows = []

    for (block_num, block_instance, round_num,), group in df.groupby(["BlockNum","BlockInstance","RoundNum"]):

        block_type = (str(group["BlockType"].iloc[0]).lower()
            if "BlockType" in group.columns and len(group)
            else "")

        if "totalRounds" in group.columns:
            tr = pd.to_numeric(group["totalRounds"], errors="coerce").dropna()

            total_rounds = (int(tr.max()) if not tr.empty else 0)

        elif "RoundNum" in group.columns:
            rn = pd.to_numeric(group["RoundNum"], errors="coerce").dropna()

            total_rounds = (int(rn.max()) if not rn.empty else 0)

        else:
            total_rounds = 0

        if block_type == "collecting":
            all_rows.extend(compute_collecting_walks(group))

        elif block_type == "pindropping":
            if total_rounds <= 1:
                all_rows.extend(
                    compute_pindrop_walks(
                        group,
                        phase_label="Pindropping_TP2",
                        round_start_event=round_start_event,
                    )
                )
            else:
                all_rows.extend(
                    compute_pindrop_walks(
                        group,
                        phase_label="Pindropping_TP1",
                        round_start_event=round_start_event,
                    )
                )

    walk_df = pd.DataFrame(all_rows)

    if walk_df.empty:
        print(f"⚠️ No walks detected — skipping creation of {Path(out_path).name}")
        return

    if "start_AppTime" in walk_df.columns:
        walk_df = walk_df.sort_values("start_AppTime").reset_index(drop=True)

    cols = df.columns

    df_merged1 = pd.concat([df, walk_df.reindex(columns=cols)], ignore_index=True)

    extra_walk_rows = compute_pindrop_walksXL(df_merged1)

    extra_walks = pd.DataFrame(extra_walk_rows)

    df_merged = pd.concat([df_merged1, extra_walks.reindex(columns=cols)], ignore_index=True)

    walk_df1 = pd.concat([walk_df, extra_walks.reindex(columns=cols)], ignore_index=True)

    drop_cols = [
        "totDistRound_end",
        "totDistBlock_end",
        "avgRoundSpeed_end",
        "avgBlockSpeed_end",
        "__rowid"
        ]

    walk_df1 = walk_df1.drop(columns=drop_cols,errors="ignore")

    walk_df1 = walk_df1.rename(
        columns={
            "totDistRound_start": "totDistRound",
            "totDistBlock_start": "totDistBlock",
            "avgRoundSpeed_start": "avgRoundSpeed",
            "avgBlockSpeed_start": "avgBlockSpeed"
        }
    )

    df_merged = df_merged.drop(columns=drop_cols,errors="ignore")

    df_merged = df_merged.rename(
        columns={
            "totDistRound_start": "totDistRound",
            "totDistBlock_start": "totDistBlock",
            "avgRoundSpeed_start": "avgRoundSpeed",
            "avgBlockSpeed_start": "avgBlockSpeed"
        }
    )

    walk_df1.to_csv(out_path,index=False,)

    df_merged = df_merged.sort_values("source").reset_index(drop=True)

    df_merged = df_merged.sort_values("start_AppTime").reset_index(drop=True)

    print(df_merged[df_merged.lo_eventType.eq("Walk_PinDrop")]["chestPin_num"].value_counts(dropna=False))

    df_merged.to_csv(merge_outpath, index=False)

    print(f"✅ Walk rows written to {out_path}")


def batch_compute_walks(
    events_dir,
    meta_dir,
    output_dir,
    *,
    events_ending="events_flat",
    event_type="AN",
):
    """
    Process only the files belonging to the selected event dataset.

    AN:
      ObsReward_A*
      first PinDrop anchor = TrueContentStart

    PO:
      ObsReward_B*
      first PinDrop anchor = earliestRoundStart
    """
    events_dir = Path(events_dir)
    meta_dir = Path(meta_dir)
    output_dir = Path(output_dir)

    config = EVENT_TYPE_CONFIG[event_type]
    file_prefix = config["file_prefix"]
    round_start_event = config["round_start_event"]

    print(f"[config] event_type: {event_type}")
    print(f"[config] file_prefix: {file_prefix}")
    print(f"[config] round_start_event: {round_start_event}")
    print(f"[config] events_ending: {events_ending}")
    print(f"[config] events_dir: {events_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    meta_files = {
        f.stem.replace("_processed_meta", "",): f
        for f in meta_dir.glob(f"{file_prefix}*_processed_meta.json")
    }

    event_pattern = (f"{file_prefix}*_{events_ending}.csv")

    event_files = {
        f.stem.replace(f"_{events_ending}", "",): f
        for f in events_dir.glob(event_pattern)
    }

    print(f"[scan] event pattern: {event_pattern}")

    print(f"[scan] matching event files: {len(event_files)}")
    print(f"[scan] matching metadata files: {len(meta_files)}")

    matched_keys = (set(event_files) & set(meta_files))

    print(f"[scan] matched event/meta pairs: {len(matched_keys)}")

    for key in sorted(matched_keys):
        flat_file = event_files[key]
        meta_file = meta_files[key]

        out_dir = output_dir / "WalksOnly"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / f"{key}_walks.csv"

        merged_out_dir = output_dir / "EventsMergedWalks"

        merged_out_dir.mkdir(parents=True, exist_ok=True)

        merged_out_file = merged_out_dir / f"{key}_eventsWalks.csv"

        print(f"➡️ Computing walks for: {flat_file.name}")
        print(f"   round start marker: {round_start_event}")
        print(f"   walks output: {out_file.name}")
        print(f"   merged output: {merged_out_file.name}")

        compute_walk_rows(
            flat_file,
            meta_file,
            out_file,
            merged_out_file,
            round_start_event=round_start_event,
        )

def compute_pindrop_walksXL(df: pd.DataFrame):
    rows = []
    keys = ["BlockInstance", "BlockNum", "effectiveRoundNum"]

    start_type = "InterRound_PostCylinderWalk_segment"
    end_type   = "PinDrop_Moment"

    starts = df[df["lo_eventType"].eq(start_type)].copy()
    ends   = df[df["lo_eventType"].eq(end_type) & df["chestPin_num"].eq(1)].copy()

    if starts.empty or ends.empty:
        return rows

    # Ensure numeric times
    starts["start_AppTime"] = pd.to_numeric(starts["start_AppTime"], errors="coerce")
    ends["start_AppTime"]   = pd.to_numeric(ends["start_AppTime"], errors="coerce")

    starts = starts.dropna(subset=keys + ["start_AppTime"])
    ends   = ends.dropna(subset=keys + ["start_AppTime"])

    if starts.empty or ends.empty:
        return rows

    # 1-to-1 match on keys
    m = starts.merge(ends, on=keys, suffixes=("_start", "_end"), how="inner")
    if m.empty:
        return rows

    for _, r in m.iterrows():
        # Base row inherits END-side (so most columns come from the pin drop marker row)
        end_dict = (
            r.filter(like="_end")
             .rename(lambda c: c[:-4])   # strip "_end"
             .to_dict()
        )

        start_time = r.get("start_AppTime_start", pd.NA)
        end_time   = r.get("start_AppTime_end", pd.NA)  # end marker only has start_*

        adjwalk_time = (
            end_time - start_time
            if pd.notna(start_time) and pd.notna(end_time)
            else pd.NA
        )

        row = end_dict

        # ✅ keys come from unsuffixed merge keys
        for k in keys:
            row[k] = r.get(k, pd.NA)

        # Set / force core identity fields
        row.update({
            "lo_eventType": "Adjusted_1st_Walk_PinDrop",
            "hi_eventType": "WalkingPeriod",
            "hiMeta_eventType": "BlockActivity",
            "source": "synthetic",
            "adjwalkTime": adjwalk_time,

            # force pin/step = 1
            "chestPin_num": 1,
            "path_step_in_round": 1,

            # end timing comes from end marker's start_*
            "end_AppTime": end_time,
            "end_eMLT_orig": r.get("start_eMLT_orig_end", r.get("eMLT_orig_end", pd.NA)),

            # preserve correct start timing from start marker
            "AppTime": r.get("AppTime_start", pd.NA),
            "eMLT_orig": r.get("eMLT_orig_start", pd.NA),
            "mLT_orig": r.get("mLT_orig_start", pd.NA),
            "mLT_raw": r.get("mLT_raw_start", pd.NA),
            "origRow_start": r.get("origRow_start_start", pd.NA),
            "start_AppTime": start_time,
            "start_eMLT_orig": r.get("start_eMLT_orig_start", pd.NA),
        })

        # Keep origRow_end from END marker if present
        row["origRow_end"] = r.get("origRow_end_end", r.get("origRow_end", pd.NA))

        # If you want med_eventType to inherit the phase label you set earlier
        # (make sure "phase_label" exists in your df)
        if "phase_label_end" in r.index:
            row["med_eventType"] = r.get("phase_label_end", pd.NA)

        rows.append(row)

    return rows
    
def cli() -> None:
    parser = argparse.ArgumentParser(
        prog="computeWalks",
        description=("Compute walk durations for AN or PO event files."),
    )

    parser.add_argument(
        "--root-dir",
        required=True,
        type=Path,
        help="Base project directory e.g., '/Users/you/RC_TestingNotes').",
    )

    parser.add_argument(
        "--proc-dir",
        required=True,
        type=Path,
        help="Dataset subdirectory under --root-dir. If absolute, --root-dir is ignored.",
    )

    parser.add_argument(
        "--events-dir-name",
        default="Events_AugPart1",
        help="Subdirectory under <root/proc/EventSegmentation> containing input event CSVs.",
    )

    parser.add_argument(
        "--meta-dir-name",
        default="MetaData_Flat",
        help="Subdirectory under <root/proc/EventSegmentation> containing metadata files.",
    )

    parser.add_argument(
        "--output-dir-name",
        default="Events_ComputedWalks",
        help="Subdirectory under <root/proc/EventSegmentation> for output.",
    )

    parser.add_argument(
        "--eventsEnding",
        default="events_flat",
        help="Input event filename suffix before '.csv', for example 'startPosPropagated'.",
    )

    parser.add_argument(
        "--event-type",
        required=True,
        choices=["AN", "PO"],
        help="AN = ObsReward_A files using TrueContentStart; PO = ObsReward_B files using earliestRoundStart.",
    )

    args = parser.parse_args()

    root = args.root_dir.expanduser()
    proc = args.proc_dir

    base_dir = (proc if proc.is_absolute() else root / proc) / "EventSegmentation"

    events_dir = base_dir / args.events_dir_name
    
    meta_dir = base_dir / args.meta_dir_name
    
    output_dir = base_dir / args.output_dir_name
    

    for path, label in (
        (base_dir, "base-dir"),
        (events_dir, "events-dir"),
        (meta_dir, "meta-dir"),
    ):
        if not path.exists():parser.error(f"{label} not found: {path}")

    output_dir.mkdir(parents=True, exist_ok=True,)

    config = EVENT_TYPE_CONFIG[args.event_type]

    print("🚀 Starting batch compute walks")
    print(f"   dataset: {args.event_type}")
    
    print(f"   files: {config['file_prefix']}*")
    print(f"   first-pin start event: {config['round_start_event']}")

    batch_compute_walks(
        events_dir,
        meta_dir,
        output_dir,
        events_ending=args.eventsEnding,
        event_type=args.event_type,
    )


if __name__ == "__main__":
    cli()