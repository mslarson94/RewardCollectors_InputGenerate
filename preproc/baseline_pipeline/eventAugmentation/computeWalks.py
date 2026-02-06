import pandas as pd
import json
import numpy as np
from pathlib import Path
import os


########

def build_end_events_Walks(row):
    return {
        ## current coin collection number
        "chestPin_num": row.get("chestPin_num", pd.NA),
        "path_step_in_round": row.get("path_step_in_round", pd.NA),
        
        ## time & orig rows
        "end_eMLT_orig": row.get('start_eMLT_orig'),
        "end_AppTime": row.get("start_AppTime", pd.NA),
        "origRow_end": row.get("origRow_end", pd.NA),
        
        ## position
        "HeadPosAnchored_x_at_end": row.get("HeadPosAnchored_x_at_start", pd.NA),
        "HeadPosAnchored_y_at_end": row.get("HeadPosAnchored_y_at_start", pd.NA),
        "HeadPosAnchored_z_at_end": row.get("HeadPosAnchored_z_at_start", pd.NA),
        "HeadForthAnchored_yaw_at_end": row.get("HeadForthAnchored_yaw_at_start", pd.NA),
        "HeadForthAnchored_pitch_at_end": row.get("HeadForthAnchored_pitch_at_start", pd.NA),
        "HeadForthAnchored_roll_at_end": row.get("HeadForthAnchored_roll_at_start", pd.NA),

        ## speed
        "currSpeed_end": row.get("currSpeed_start", pd.NA),
        "dt_end": row.get("dt_start", pd.NA),

        ## elapsed time
        "roundElapsed_s_end": row.get("roundElapsed_s_start", pd.NA),
        "blockElapsed_s_end": row.get("blockElapsed_s_start", pd.NA),
        "totalSessionElapsed_s_end": row.get("totalSessionElapsed_s_start", pd.NA),
        "roundFrac_end": row.get("roundFrac_start", pd.NA),
        "blockFrac_end": row.get("blockFrac_start", pd.NA),

        ## distance
        "stepDist_end": row.get("stepDist_start", pd.NA),
        "totDistBlock_current_end": row.get("totDistBlock_current_start", pd.NA),
        "totDistRound_current_end": row.get("totDistRound_current_start", pd.NA),

    }


def compute_collecting_walks(group):
    """Collecting phase (initial encoding): block-start→first chest, then chest→chest segments."""
    rows = []
    g = group.sort_values("start_AppTime")

    chests = g[g['lo_eventType'] == 'ChestOpen_Moment']
    coins  = g[g['lo_eventType'] == 'CoinVis_end']      # optional anchor
    start_event = g[g['lo_eventType'] == 'TrueContentStart']

    # Block start → first chest
    if not start_event.empty and not chests.empty:
        start = start_event.iloc[0]
        end   = chests.iloc[0]
        walk_time = end['start_AppTime'] - start['start_AppTime']
        endEventFields = build_end_events_Walks(end)
        row = start.to_dict() ## i'm pretty sure that I didn't need to grab all of the start columns - just build the end events I think 
        row.update({
            "lo_eventType": "Walk_ChestOpen",
            "med_eventType": "RewardMemoryDrivenNav",
            "hi_eventType": "WalkingPeriod",
            "hiMeta_eventType": "PreBlockActivity",
            "source": "synthetic",
            "walkTime": walk_time,
            **endEventFields
        })
        rows.append(row)

    # chest → chest (via last coin visibility end as anchor)
    for i in range(1, len(chests)):
        chest = chests.iloc[i]
        prev_coins = coins[coins['start_AppTime'] < chest['start_AppTime']]
        if not prev_coins.empty:
            last_coin = prev_coins.iloc[-1]
            walk_time = chest['start_AppTime'] - last_coin['end_AppTime']
            endEventFields = build_end_events_Walks(chest)

            row = last_coin.to_dict()
            row.update({
                "lo_eventType": "Walk_ChestOpen",
                "med_eventType": "RewardMemoryDrivenNav",
                "hi_eventType": "WalkingPeriod",
                "hiMeta_eventType": "PreBlockActivity",
                "source": "synthetic",
                "walkTime": walk_time,
                **endEventFields
            })
            rows.append(row)

    return rows

#def compute_collecting_wait4coin(group):
    """Collecting phase (initial encoding): block-start→first chest, then chest→chest segments."""
    rows = []
    g = group.sort_values("start_AppTime")

    chests = g[g['lo_eventType'] == 'ChestOpen_Moment']
    coins  = g[g['lo_eventType'] == 'CoinVis_end']      # optional anchor

    for i in range(1, len(chests)):
        chest = chests.iloc[i]
        prev_coins = coins[coins['start_AppTime'] < chest['start_AppTime']]
        if not prev_coins.empty:
            last_coin = prev_coins.iloc[-1]
            walk_time = chest['start_AppTime'] - last_coin['end_AppTime']
            endEventFields = build_end_events_Walks(chest)

            row = last_coin.to_dict()
            row.update({
                "lo_eventType": "Wait4Coin",
                "med_eventType": "RewardMemoryDrivenNav",
                "hi_eventType": "WalkingPeriod",
                "hiMeta_eventType": "PreBlockActivity",
                "source": "synthetic",
                "walkTime": walk_time,
                **endEventFields
            })
            rows.append(row)

    return rows


def compute_pindrop_walks_v1(group, phase_label):
    # Pindropping phase: walks from block start to first Pin, and pin-to-pin via last coin visibility end
    rows = []
    g = group.sort_values("start_AppTime")

    pins = g[g['lo_eventType'] == 'PinDrop_Moment']
    coins = g[g['lo_eventType'] == 'CoinVis_end']
    start_event = g[g['lo_eventType'] == 'TrueContentStart']

    if not start_event.empty and not pins.empty:
        start = start_event.iloc[0]
        end = pins.iloc[0]
        walk_time = end['start_AppTime'] - start['start_AppTime']
        endEventFields = build_end_events_Walks(end)
        row = start.to_dict()
        row.update({
            "lo_eventType": "Walk_PinDrop",
            "med_eventType": phase_label,  # e.g., "Pindropping_Rounds1" or "Pindropping_RoundsGT1"
            "hi_eventType": "WalkingPeriod",
            "hiMeta_eventType": "BlockActivity",
            "source": "synthetic",
            "walkTime": walk_time,
            **endEventFields
        })
        rows.append(row)

    for i in range(1, len(pins)):
        pin = pins.iloc[i]
        prev_coins = coins[coins['start_AppTime'] < pin['start_AppTime']]
        if not prev_coins.empty:
            last_coin = prev_coins.iloc[-1]
            walk_time = pin['start_AppTime'] - last_coin['start_AppTime']
            row = last_coin.to_dict()
            endEventFields = build_end_events_Walks(pin)
            row.update({
                "lo_eventType": "Walk_PinDrop",
                "med_eventType": phase_label,
                "hi_eventType": "WalkingPeriod",
                "hiMeta_eventType": "BlockActivity",
                "source": "synthetic",
                "walkTime": walk_time,
                **endEventFields
            })
            rows.append(row)

    return rows

def compute_pindrop_walks(group, phase_label):
    rows = []
    g = group.sort_values("start_AppTime")

    pins = g[g["lo_eventType"] == "PinDrop_Moment"].copy()
    coins = g[g["lo_eventType"] == "CoinVis_end"].copy()
    starts = g[g["lo_eventType"] == "TrueContentStart"].copy()

    # safety: ensure numeric so comparisons work
    pins["start_AppTime"] = pd.to_numeric(pins["start_AppTime"], errors="coerce")
    coins["start_AppTime"] = pd.to_numeric(coins["start_AppTime"], errors="coerce")
    coins["end_AppTime"] = pd.to_numeric(coins["end_AppTime"], errors="coerce")
    starts["start_AppTime"] = pd.to_numeric(starts["start_AppTime"], errors="coerce")

    pins = pins.dropna(subset=["start_AppTime"])
    coins = coins.dropna(subset=["start_AppTime"])
    starts = starts.dropna(subset=["start_AppTime"])

    if pins.empty:
        return rows

    for _, pin in pins.iterrows():
        pin_num = pin.get("chestPin_num", pd.NA)

        # --- RESET POINT: pin1 uses the most recent TrueContentStart before the pin ---
        if pd.notna(pin_num) and int(pin_num) == 1:
            prev_starts = starts[starts["start_AppTime"] <= pin["start_AppTime"]]
            if prev_starts.empty:
                continue  # can't compute a "start->pin1" walk without a start marker

            start = prev_starts.iloc[-1]
            walk_time = pin["start_AppTime"] - start["start_AppTime"]

            endEventFields = build_end_events_Walks(pin)
            row = start.to_dict()
            row.update({
                "lo_eventType": "Walk_PinDrop",
                "med_eventType": phase_label,
                "hi_eventType": "WalkingPeriod",
                "hiMeta_eventType": "BlockActivity",
                "source": "synthetic",
                "walkTime": walk_time,
                **endEventFields
            })
            rows.append(row)
            continue

        # --- For pin2/pin3: anchor from last coin before the pin (use end_AppTime if you prefer) ---
        prev_coins = coins[coins["start_AppTime"] < pin["start_AppTime"]]
        if prev_coins.empty:
            continue

        last_coin = prev_coins.iloc[-1]

        # If your coin "end_AppTime" is trustworthy, use it; otherwise use start_AppTime
        anchor = last_coin.get("end_AppTime", pd.NA)
        if pd.isna(anchor):
            anchor = last_coin.get("start_AppTime", pd.NA)

        if pd.isna(anchor):
            continue

        walk_time = pin["start_AppTime"] - anchor

        endEventFields = build_end_events_Walks(pin)
        row = last_coin.to_dict()
        row.update({
            "lo_eventType": "Walk_PinDrop",
            "med_eventType": phase_label,
            "hi_eventType": "WalkingPeriod",
            "hiMeta_eventType": "BlockActivity",
            "source": "synthetic",
            "walkTime": walk_time,
            **endEventFields
        })
        rows.append(row)

    return rows

import pandas as pd

def compute_pindrop_walksXL(df: pd.DataFrame):
    rows = []
    keys = ["BlockInstance", "BlockNum", "effectiveRoundNum"]

    start_type = "InterRound_PostCylinderWalk_segment"
    end_type   = "PinDrop_Moment"

    # Filter starts / ends
    starts = df[df["lo_eventType"].eq(start_type)].copy()
    ends   = df[df["lo_eventType"].eq(end_type) & df["chestPin_num"].eq(1)].copy()

    if starts.empty or ends.empty:
        return rows

    # Ensure numeric times (so subtraction works)
    starts["start_AppTime"] = pd.to_numeric(starts["start_AppTime"], errors="coerce")
    ends["end_AppTime"]     = pd.to_numeric(ends.get("start_AppTime"), errors="coerce")
    ends["start_AppTime"]   = pd.to_numeric(ends.get("start_AppTime"), errors="coerce")

    # Merge 1-to-1 on keys
    m = starts.merge(ends, on=keys, suffixes=("_start", "_end"), how="inner")
    if m.empty:
        return rows

    # Sanity: if your assumption is true, these should match
    # (You can remove these checks once you trust it.)
    # if m.duplicated(keys).any():
    #     raise ValueError("Non-unique match on keys; expected 1-to-1 per key.")

    for _, r in m.iterrows():
        # Base row inherits END values
        end_dict = r.filter(like="_end").rename(lambda c: c[:-4]).to_dict()
        start = r  # access via *_start columns below

        # End time fallback if end_AppTime is missing
        end_time = r.get("start_AppTime_end", pd.NA)
        if pd.isna(end_time):
            end_time = r.get("start_AppTime_end", pd.NA)

        start_time = r.get("start_AppTime_start", pd.NA)
        adjwalk_time = end_time - start_time if (pd.notna(end_time) and pd.notna(start_time)) else pd.NA

        row = end_dict
        row.update({
            "lo_eventType": "Adjusted_1st_Walk_PinDrop",
            "hi_eventType": "WalkingPeriod",
            "hiMeta_eventType": "BlockActivity",
            "source": "synthetic",
            "adjwalkTime": adjwalk_time,

            # force pin=1
            "chestPin_num": r.get("chestPin_num_end", pd.NA),

            # ensure end fields are set how you want
            "end_AppTime": end_time,
            "origRow_end":  r.get("origRow_end_end", pd.NA),
        })

        # Overwrite ONLY the start-derived fields you listed
        row.update({
            "AppTime": r.get("AppTime_start", pd.NA),
            "eMLT_orig": r.get("eMLT_orig_start", pd.NA),
            "mLT_orig": r.get("mLT_orig_start", pd.NA),
            "mLT_raw": r.get("mLT_raw_start", pd.NA),
            "origRow_start": r.get("origRow_start_start", pd.NA),
            "start_AppTime": r.get("start_AppTime_start", pd.NA),
            "start_eMLT_orig": r.get("start_eMLT_orig_start", pd.NA),
            "med_eventType": r.get("phase_label_end", pd.NA),

            "currSpeed_start": r.get("currSpeed_start_start", pd.NA),
            "dt_start": r.get("dt_start_start", pd.NA),

            "HeadForthAnchored_pitch_at_start": r.get("HeadPosAnchored_pitch_at_start_start", pd.NA),
            "HeadForthAnchored_roll_at_start":  r.get("HeadPosAnchored_roll_at_start_start", pd.NA),
            "HeadForthAnchored_yaw_at_start":   r.get("HeadPosAnchored_yaw_at_start_start", pd.NA),
            "HeadPosAnchored_x_at_start":       r.get("HeadPosAnchored_x_at_start_start", pd.NA),

            "HeadPosAnchored_y_at_start":       r.get("HeadPosAnchored_y_at_start_start", pd.NA),

            "HeadPosAnchored_z_at_start":       r.get("HeadPosAnchored_z_at_start_start", pd.NA),


            "blockElapsed_s_start": r.get("blockElapsed_s_start_start", pd.NA),
            "blockFrac_start":      r.get("blockFrac_start_start", pd.NA),
            "roundElapsed_s_start": r.get("roundElapsed_s_start_start", pd.NA),
            "roundFrac_start":      r.get("roundFrac_start_start", pd.NA),

            "stepDist_start":              r.get("stepDist_start_start", pd.NA),
            "distFromCoinPos_HV":          r.get("distFromCoinPos_HV_start", pd.NA),
            "distFromCoinPos_LV":          r.get("distFromCoinPos_LV_start", pd.NA),
            "distFromCoinPos_NV":          r.get("distFromCoinPos_NV_start", pd.NA),
            "distToPin_HV":                r.get("distToPin_HV_start", pd.NA),
            "distToPin_LV":                r.get("distToPin_LV_start", pd.NA),
            "distToPin_NV":                r.get("distToPin_NV_start", pd.NA),
            "totDistBlock_current_start":  r.get("totDistBlock_current_start_start", pd.NA),
            "totDistRound_current_start":  r.get("totDistRound_current_start_start", pd.NA),
        })

        rows.append(row)

    return rows


def compute_walk_rows(flat_path, meta_path, out_path, merge_outpath):
    with open(meta_path, 'r') as f:
        meta = json.load(f)
    #session_date = pd.to_datetime(meta.get("testingDate", "01_01_1970"), format="%m_%d_%Y")

    df = pd.read_csv(flat_path)
    # Parse mLTs once
    #df['start_eMLT_orig'] = df['start_eMLT_orig'].apply(lambda ts: fix_time_str(ts, session_date))
    #df['end_eMLT_orig'] = df['end_eMLT_orig'].apply(lambda ts: fix_time_str(ts, session_date))
    df["start_AppTime"] = pd.to_numeric(df["start_AppTime"], errors="coerce")
    df["end_AppTime"]   = pd.to_numeric(df["end_AppTime"], errors="coerce")
    all_rows = []

    for block_num, group in df.groupby("BlockNum"):
        # Derive block type (lowercased string); default '' if missing
        block_type = str(group['BlockType'].iloc[0]).lower() if 'BlockType' in group.columns and len(group) else ''

        # Derive totalRounds for the block; try column totalRounds else fallback to RoundNum max
        if 'totalRounds' in group.columns:
            tr = pd.to_numeric(group['totalRounds'], errors='coerce').dropna()
            total_rounds = int(tr.max()) if not tr.empty else 0
        elif 'RoundNum' in group.columns:
            rn = pd.to_numeric(group['RoundNum'], errors='coerce').dropna()
            total_rounds = int(rn.max()) if not rn.empty else 0
        else:
            total_rounds = 0

        if block_type == 'collecting':
            all_rows.extend(compute_collecting_walks(group))
        elif block_type == 'pindropping':
            if total_rounds <= 1:
                all_rows.extend(compute_pindrop_walks(group, phase_label="Pindropping_TP2"))
            else:
                all_rows.extend(compute_pindrop_walks(group, phase_label="Pindropping_TP1"))
        else:
            # Unknown/other block types: skip or handle if you want
            continue

    walk_df = pd.DataFrame(all_rows)
    if walk_df.empty:
        print(f"⚠️ No walks detected — skipping creation of {Path(out_path).name}")
    else:
        # Ensure chronological order
        if 'start_AppTime' in walk_df.columns:
            walk_df = walk_df.sort_values("start_AppTime").reset_index(drop=True)
        cols = df.columns
        df_merged1 = pd.concat([df, walk_df.reindex(columns=cols)], ignore_index=True)
        extraWalkRows = compute_pindrop_walksXL(df_merged1)
        extraWalks = pd.DataFrame(extraWalkRows)
        df_merged = pd.concat([df_merged1, extraWalks.reindex(columns=cols)], ignore_index=True)
        walk_df1 = pd.concat([walk_df, extraWalks.reindex(columns=cols)], ignore_index=True)
        dropCols = [
            "totDistRound_end",
            "totDistBlock_end",
            "__rowid",
            ]
        walk_df1 = walk_df1.drop(columns=dropCols, errors="ignore")
        walk_df1 = walk_df1.rename(columns={
        "totDistRound_start": "totDistRound",
        "totDistBlock_start": "totDistBlock",
        })
        df_merged = df_merged.drop(columns=dropCols, errors="ignore")
        df_merged = df_merged.rename(columns={
        "totDistRound_start": "totDistRound",
        "totDistBlock_start": "totDistBlock",
        })
        
        walk_df1.to_csv(out_path, index=False)
        df_merged = df_merged.sort_values("source").reset_index(drop=True)
        df_merged = df_merged.sort_values("start_AppTime").reset_index(drop=True)
        df_merged.to_csv(merge_outpath, index=False)
        print(f"✅ Walk rows written to {out_path}")


def batch_compute_walks(events_dir, meta_dir, output_dir, eventsEnding='events_flat'):
    events_dir = Path(events_dir)
    meta_dir = Path(meta_dir)
    output_dir = Path(output_dir)
    print(events_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    meta_files = {f.stem.replace("_processed_meta", ""): f for f in meta_dir.glob("*_processed_meta.json")}
    event_files = {f.stem.replace(f"_{eventsEnding}", ""): f for f in events_dir.glob(f"*_{eventsEnding}.csv")}

    matched_keys = set(event_files) & set(meta_files)

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
        compute_walk_rows(flat_file, meta_file, out_file, merged_out_file)


import argparse
from pathlib import Path

def cli() -> pd.NA:
    parser = argparse.ArgumentParser(
        prog="computeWalks",
        description="Compute walk durations for AN event files."
    )
    parser.add_argument(
        "--root-dir", required=True, type=Path,
        help="Base project directory (e.g., '/Users/you/RC_TestingNotes')."
    )
    parser.add_argument(
        "--proc-dir", required=True, type=Path,
        help="Dataset subdirectory under --root-dir (e.g., 'FreshStart'). "
             "If absolute, --root-dir is ignored."
    )
    parser.add_argument(
        "--events-dir-name", default="Events_AugPart1",
        help="Subdirectory under <root/proc/full> containing input event CSVs."
    )
    parser.add_argument(
        "--meta-dir-name", default="MetaData_Flat",
        help="Subdirectory under <root/proc/full> containing *_meta.json files."
    )
    parser.add_argument(
        "--output-dir-name", default="Events_ComputedWalks",
        help="Subdirectory under <root/proc/full> for output."
    )

    parser.add_argument(
        "--eventsEnding", default="events_flat",
        help="Subdirectory under <root/proc/full> for output."
    )

    args = parser.parse_args()

    root = args.root_dir.expanduser()
    proc = args.proc_dir
    base_dir = (proc if proc.is_absolute() else (root / proc)) / "EventSegmentation"

    events_dir = base_dir / args.events_dir_name
    meta_dir = base_dir / args.meta_dir_name
    output_dir = base_dir

    for p, label in ((base_dir, "base-dir"),
                     (events_dir, "events-dir"),
                     (meta_dir, "meta-dir")):
        if not p.exists():
            parser.error(f"{label} not found: {p}")

    output_dir.mkdir(parents=True, exist_ok=True)

    print("🚀 Starting batch compute walks..")
    batch_compute_walks(
        str(events_dir),
        str(meta_dir),
        str(output_dir),
        str(args.eventsEnding)
    )

if __name__ == "__main__":
    cli()

