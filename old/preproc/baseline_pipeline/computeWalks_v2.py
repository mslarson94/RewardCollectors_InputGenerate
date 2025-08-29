import pandas as pd
import json
import numpy as np
from pathlib import Path
import os


# def fix_time_str_v1(ts, session_date):
#     if not isinstance(ts, str):
#         return pd.NaT
#     if ts.count(":") == 3:
#         ts = ".".join(ts.rsplit(":", 1))
#     try:
#         return pd.to_datetime(f"{session_date.strftime('%Y-%m-%d')} {ts}", format="%Y-%m-%d %H:%M:%S.%f")
#     except Exception:
#         return pd.NaT


# def compute_walk_rows_v1(flat_path, meta_path, out_path):
#     with open(meta_path, 'r') as f:
#         meta = json.load(f)
#     session_date = pd.to_datetime(meta.get("testingDate", "01_01_1970"), format="%m_%d_%Y")

#     df = pd.read_csv(flat_path)
#     df['start_Timestamp'] = df['start_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))
#     df['end_Timestamp'] = df['end_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))

#     walk_rows = []

#     for block_num, group in df.groupby("BlockNum"):
#         if block_num < 2 or block_num == 3:
#             continue

#         group = group.sort_values("start_Timestamp")
#         pins = group[group['lo_eventType'] == 'PinDrop_Moment']
#         coins = group[group['lo_eventType'] == 'CoinVis_end']
#         start_event = group[group['lo_eventType'] == 'TrueBlockStart']

#         if not start_event.empty and not pins.empty:
#             start = start_event.iloc[0]
#             end = pins.iloc[0]
#             walk_time = (end['start_Timestamp'] - start['start_Timestamp']).total_seconds()
#             row = start.to_dict()
#             row.update({
#                 "lo_eventType": "Walk_PinDrop",
#                 "med_eventType": "RewardMemoryDrivenNav",
#                 "hi_eventType": "WalkingPeriod",
#                 "hiMeta_eventType": "PreBlockActivity",
#                 "source": "synthetic",
#                 "start_Timestamp": start['start_Timestamp'],
#                 "end_Timestamp": end['start_Timestamp'],
#                 "start_AppTime": start.get("start_AppTime", pd.NA),
#                 "end_AppTime": end.get("start_AppTime", pd.NA),
#                 "AppTime": start.get("start_AppTime", pd.NA),
#                 "original_row_start": start.get("original_row_start", pd.NA),
#                 "original_row_end": end.get("original_row_end", pd.NA),
#                 "details": f"{{walkTime= {int(walk_time)} seconds}}"
#             })
#             walk_rows.append(row)

#         for i in range(1, len(pins)):
#             pin = pins.iloc[i]
#             prev_coins = coins[coins['end_Timestamp'] < pin['start_Timestamp']]
#             if not prev_coins.empty:
#                 last_coin = prev_coins.iloc[-1]
#                 walk_time = (pin['start_Timestamp'] - last_coin['end_Timestamp']).total_seconds()
#                 row = last_coin.to_dict()
#                 row.update({
#                     "lo_eventType": "Walk_PinDrop",
#                     "med_eventType": "RewardMemoryDrivenNav",
#                     "hi_eventType": "WalkingPeriod",
#                     "hiMeta_eventType": "PreBlockActivity",
#                     "source": "synthetic",
#                     "start_Timestamp": last_coin['end_Timestamp'],
#                     "end_Timestamp": pin['start_Timestamp'],
#                     "start_AppTime": last_coin.get("end_AppTime", pd.NA),
#                     "end_AppTime": pin.get("start_AppTime", pd.NA),
#                     "AppTime": last_coin.get("end_AppTime", pd.NA),
#                     "original_row_start": last_coin.get("original_row_start", pd.NA),
#                     "original_row_end": pin.get("original_row_end", pd.NA),
#                     "details": f"{{walkTime= {int(walk_time)} seconds}}"
#                 })
#                 walk_rows.append(row)

#     walk_df = pd.DataFrame(walk_rows)
#     if walk_df.empty:
#         print(f"⚠️ No walks detected — skipping creation of {out_path.name}")
#     else:
#         walk_df.to_csv(out_path, index=False)
#         print(f"✅ Walk rows written to {out_path}")


def fix_time_str(ts, session_date):
    if not isinstance(ts, str):
        return pd.NaT
    if ts.count(":") == 3:
        ts = ".".join(ts.rsplit(":", 1))
    try:
        return pd.to_datetime(f"{session_date.strftime('%Y-%m-%d')} {ts}", format="%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        return pd.NaT

def compute_walk_rows(flat_path, meta_path, out_path):
    with open(meta_path, 'r') as f:
        meta = json.load(f)
    session_date = pd.to_datetime(meta.get("testingDate", "01_01_1970"), format="%m_%d_%Y")

    df = pd.read_csv(flat_path)
    df['start_Timestamp'] = df['start_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))
    df['end_Timestamp'] = df['end_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))

    walk_rows = []

    for block_num, group in df.groupby("BlockNum"):
        # if block_num < 2 or block_num == 3:
        #     continue  # Skip special blocks
        if block_num == 2 or block_num == 0:
            # check that the blockType is 'collecting' and that totalRounds = 1 
            continue  # Skip special blocks
        elif block_num == 1 or block_num == 3:
            # check that the blockType is 'pindropping' and that totalRounds > 1
            continue

        group = group.sort_values("start_Timestamp")
        pins = group[group['lo_eventType'] == 'PinDrop_Moment']
        coins = group[group['lo_eventType'] == 'CoinVis_end']
        start_event = group[group['lo_eventType'] == 'TrueBlockStart']

        # First pin relative to block start
        if not start_event.empty and not pins.empty:
            start = start_event.iloc[0]
            end = pins.iloc[0]
            walk_time = (end['start_Timestamp'] - start['start_Timestamp']).total_seconds()
            row = start.to_dict()
            row.update({
                "lo_eventType": "Walk_PinDrop",
                "med_eventType": "RewardMemoryDrivenNav",
                "hi_eventType": "WalkingPeriod",
                "hiMeta_eventType": "PreBlockActivity",
                "source": "synthetic",
                "start_Timestamp": start['start_Timestamp'].strftime("%H:%M:%S.%f") if pd.notnull(start['start_Timestamp']) else pd.NA,
                "end_Timestamp": end['start_Timestamp'].strftime("%H:%M:%S.%f") if pd.notnull(end['start_Timestamp']) else pd.NA,
                "start_AppTime": start.get("start_AppTime", pd.NA),
                "end_AppTime": end.get("start_AppTime", pd.NA),
                "AppTime": start.get("AppTime", pd.NA),
                "original_row_start": start.get("original_row_start", pd.NA),
                "original_row_end": end.get("original_row_end", pd.NA),
                "walkTime": walk_time
                #"details": f"{{walkTime= {int(walk_time)} seconds}}"
            })
            walk_rows.append(row)

        # Pin-to-pin walks
        for i in range(1, len(pins)):
            pin = pins.iloc[i]
            prev_coins = coins[coins['end_Timestamp'] < pin['start_Timestamp']]
            if not prev_coins.empty:
                last_coin = prev_coins.iloc[-1]
                walk_time = (pin['start_Timestamp'] - last_coin['end_Timestamp']).total_seconds()
                row = last_coin.to_dict()
                row.update({
                    "lo_eventType": "Walk_PinDrop",
                    "med_eventType": "RewardMemoryDrivenNav",
                    "hi_eventType": "WalkingPeriod",
                    "hiMeta_eventType": "PreBlockActivity",
                    "source": "synthetic",
                    "start_Timestamp": last_coin['end_Timestamp'].strftime("%H:%M:%S.%f") if pd.notnull(last_coin['end_Timestamp']) else pd.NA,
                    "end_Timestamp": pin['start_Timestamp'].strftime("%H:%M:%S.%f") if pd.notnull(pin['start_Timestamp']) else pd.NA,
                    "start_AppTime": last_coin.get("end_AppTime", pd.NA),
                    "end_AppTime": pin.get("start_AppTime", pd.NA),
                    "AppTime": pin.get("AppTime", pd.NA),
                    "original_row_start": last_coin.get("original_row_start", pd.NA),
                    "original_row_end": pin.get("original_row_end", pd.NA),
                    "walkTime": walk_time
                    #"details": f"{{walkTime= {int(walk_time)} seconds}}"
                })
                walk_rows.append(row)

    walk_df = pd.DataFrame(walk_rows)
    if walk_df.empty:
        print(f"⚠️ No walks detected — skipping creation of {out_path.name}")
    else:
        walk_df.to_csv(out_path, index=False)
        print(f"✅ Walk rows written to {out_path}")


def compute_initial_encoding_walks(df, group, block_num):
    """
    Return a list of rows describing 'encoding walks' before the first TrueBlockStart.
    Customize logic as needed (e.g., use hiMeta_eventType or BlockType tags if present).
    """
    walk_rows = []
    #df = df.sort_values("start_Timestamp")
    #pre = df[df['lo_eventType'] != 'TrueBlockStart']  # or a tighter filter
    group = group.sort_values("start_Timestamp")
    #pins = group[group['lo_eventType'] == 'PinDrop_Moment']
    coins = group[group['lo_eventType'] == 'CoinVis_end']
    start_event = group[group['lo_eventType'] == 'TrueBlockStart']
    # Example: earliest activity to first ChestOpen_Moment
    chests = df[df['lo_eventType'] == 'ChestOpen_Moment']
    
    # First chest relative to block start
    if not start_event.empty and not chests.empty:
        start = start_event.iloc[0]
        end = chests.iloc[0]
        walk_time = (end['start_Timestamp'] - start['start_Timestamp']).total_seconds()
        row = start.to_dict()
        row.update({
            "lo_eventType": "Walk_ChestOpen",
            "med_eventType": "RewardMemoryDrivenNav",
            "hi_eventType": "WalkingPeriod",
            "hiMeta_eventType": "PreBlockActivity",
            "source": "synthetic",
            "start_Timestamp": start['start_Timestamp'].strftime("%H:%M:%S.%f") if pd.notnull(start['start_Timestamp']) else pd.NA,
            "end_Timestamp": end['start_Timestamp'].strftime("%H:%M:%S.%f") if pd.notnull(end['start_Timestamp']) else pd.NA,
            "start_AppTime": start.get("start_AppTime", pd.NA),
            "end_AppTime": end.get("start_AppTime", pd.NA),
            "AppTime": start.get("AppTime", pd.NA),
            "original_row_start": start.get("original_row_start", pd.NA),
            "original_row_end": end.get("original_row_end", pd.NA),
            "walkTime": walk_time
            #"details": f"{{walkTime= {int(walk_time)} seconds}}"
        })
        walk_rows.append(row)

    # chest-to-chest walks
    for i in range(1, len(chests)):
        chest = chests.iloc[i]
        prev_coins = coins[coins['end_Timestamp'] < chest['start_Timestamp']]
        if not prev_coins.empty:
            last_coin = prev_coins.iloc[-1]
            walk_time = (chest['start_Timestamp'] - last_coin['end_Timestamp']).total_seconds()
            row = last_coin.to_dict()
            row.update({
                "lo_eventType": "Walk_ChestOpen",
                "med_eventType": "RewardMemoryDrivenNav",
                "hi_eventType": "WalkingPeriod",
                "hiMeta_eventType": "PreBlockActivity",
                "source": "synthetic",
                "start_Timestamp": last_coin['end_Timestamp'].strftime("%H:%M:%S.%f") if pd.notnull(last_coin['end_Timestamp']) else pd.NA,
                "end_Timestamp": chest['start_Timestamp'].strftime("%H:%M:%S.%f") if pd.notnull(chest['start_Timestamp']) else pd.NA,
                "start_AppTime": last_coin.get("end_AppTime", pd.NA),
                "end_AppTime": chest.get("start_AppTime", pd.NA),
                "AppTime": chest.get("AppTime", pd.NA),
                "original_row_start": last_coin.get("original_row_start", pd.NA),
                "original_row_end": chest.get("original_row_end", pd.NA),
                "walkTime": walk_time
                #"details": f"{{walkTime= {int(walk_time)} seconds}}"
            })
            walk_rows.append(row)
    return walk_rows

def batch_compute_walks(events_dir, meta_dir, output_dir):
    events_dir = Path(events_dir)
    meta_dir =  Path(meta_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all meta and event files
    meta_files = {f.stem.replace("_processed_meta", ""): f for f in meta_dir.glob("*_meta.json")}
    event_files = {f.stem.replace("_events_flat", ""): f for f in events_dir.glob("*_events_flat.csv")}

    matched_keys = set(event_files) & set(meta_files)

    for key in sorted(matched_keys):
        flat_file = event_files[key]
        meta_file = meta_files[key]
        out_file = output_dir / f"{key}_walks.csv"
        print(f"➡️ Computing walks for: {flat_file.name}")
        compute_walk_rows(flat_file, meta_file, out_file)


# # Example usage
# morning = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day/pair_08/02_17_2025/Morning/MagicLeaps/ML2A'
# afternoon = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day/pair_08/02_17_2025/Afternoon/MagicLeaps/ML2G'
# output_dir = afternoon + "/flattened"
# batch_compute_walks(output_dir, output_dir)

if __name__ == "__main__":
    base_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/ResurrectedData"
    #base_dir = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day'
    events_dir = os.path.join(base_dir, "Events_AugmentedPart3")
    meta_dir = os.path.join(base_dir, "MetaData_Flat")
    output_dir = os.path.join(base_dir, "Events_ComputedWalks")
    print("🚀 Starting batch compute walks..")
    batch_compute_walks(events_dir, meta_dir, output_dir)
