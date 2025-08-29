import pandas as pd
from pathlib import Path
import json

'''def compute_walk_rows_v1(flat_path, out_path):
    df = pd.read_csv(flat_path, parse_dates=["start_Timestamp", "end_Timestamp"], infer_datetime_format=True)
    walk_rows = []

    for block_num, group in df.groupby("BlockNum"):
        if block_num < 2 or block_num == 3:
            continue  # Skip special blocks

        group = group.sort_values("start_Timestamp")
        pins = group[group['lo_eventType'] == 'PinDrop_Moment']
        coins = group[group['lo_eventType'] == 'CoinVis_end']
        start_event = group[group['lo_eventType'] == 'TrueBlockStart']

        # First pin relative to block start
        if not start_event.empty and not pins.empty:
            start_ts = start_event.iloc[0]['start_Timestamp']
            end_ts = pins.iloc[0]['start_Timestamp']
            walk_time = (end_ts - start_ts).total_seconds() if pd.notnull(start_ts) and pd.notnull(end_ts) else None
            if walk_time is not None:
                walk_rows.append({
                    **pins.iloc[0].to_dict(),
                    "lo_eventType": "Walk_PinDrop",
                    "med_eventType": "Walks",
                    "start_Timestamp": start_ts,
                    "end_Timestamp": end_ts,
                    "details": f"{{walkTime= {int(walk_time)} seconds}}"
                })

        # Pin-to-pin walks
        for i in range(1, len(pins)):
            pin = pins.iloc[i]
            prev_coins = coins[coins['end_Timestamp'] < pin['start_Timestamp']]
            if not prev_coins.empty:
                last_coin = prev_coins.iloc[-1]
                walk_time = (pin['start_Timestamp'] - last_coin['end_Timestamp']).total_seconds()
                walk_rows.append({
                    **pin.to_dict(),
                    "lo_eventType": "Walk_PinDrop",
                    "med_eventType": "Walks",
                    "start_Timestamp": last_coin['end_Timestamp'],
                    "end_Timestamp": pin['start_Timestamp'],
                    "details": f"{{walkTime= {int(walk_time)} seconds}}"
                })

    walk_df = pd.DataFrame(walk_rows)
    walk_df.to_csv(out_path, index=False)
    print(f"✅ Computed Walk_PinDrop rows written to {out_path}")


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
    # Extract session date from metadata
    with open(meta_path, 'r') as f:
        meta = json.load(f)
    session_date = pd.to_datetime(meta.get("testingDate", "01_01_1970"), format="%m_%d_%Y")

    # Load and fix timestamps
    df = pd.read_csv(flat_path)
    df['start_Timestamp'] = df['start_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))
    df['end_Timestamp'] = df['end_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))

    walk_rows = []

    for block_num, group in df.groupby("BlockNum"):
        if block_num < 2 or block_num == 3:
            continue  # Skip special blocks

        group = group.sort_values("start_Timestamp")
        pins = group[group['lo_eventType'] == 'PinDrop_Moment']
        coins = group[group['lo_eventType'] == 'CoinVis_end']
        start_event = group[group['lo_eventType'] == 'TrueBlockStart']

        if not start_event.empty and not pins.empty:
            start_ts = start_event.iloc[0]['start_Timestamp']
            end_ts = pins.iloc[0]['start_Timestamp']
            if pd.notnull(start_ts) and pd.notnull(end_ts):
                walk_time = (end_ts - start_ts).total_seconds()
                walk_rows.append({
                    **pins.iloc[0].to_dict(),
                    "lo_eventType": "Walk_PinDrop",
                    "med_eventType": "Walks",
                    "start_Timestamp": start_ts,
                    "end_Timestamp": end_ts,
                    "details": f"{{walkTime= {int(walk_time)} seconds}}"
                })

        for i in range(1, len(pins)):
            pin = pins.iloc[i]
            prev_coins = coins[coins['end_Timestamp'] < pin['start_Timestamp']]
            if not prev_coins.empty:
                last_coin = prev_coins.iloc[-1]
                walk_time = (pin['start_Timestamp'] - last_coin['end_Timestamp']).total_seconds()
                walk_rows.append({
                    **pin.to_dict(),
                    "lo_eventType": "Walk_PinDrop",
                    "med_eventType": "Walks",
                    "start_Timestamp": last_coin['end_Timestamp'],
                    "end_Timestamp": pin['start_Timestamp'],
                    "details": f"{{walkTime= {int(walk_time)} seconds}}"
                })

    walk_df = pd.DataFrame(walk_rows)
    walk_df.to_csv(out_path, index=False)
    print(f"✅ Computed Walk_PinDrop rows written to {out_path}")

# Example usage:
# compute_walk_rows("./outputs/events_flattened.csv", "./outputs/walk_rows.csv")
def batch_computeWalks_v1(input_dir, output_dir):
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    event_files = {f.stem.replace("_events", ""): f for f in input_dir.glob("*_events.csv")}

    for event in event_files:
        events_path = event_files[event]
        walk_path = output_dir / f"{event}_walks.csv"
        print(f"➡️ Processing: {events_path.name}")
        compute_walk_rows(events_path, walk_path)

def merge_events_and_walks(events_path, walks_path, meta_path, out_path):
    # Load session date
    with open(meta_path, 'r') as f:
        meta = json.load(f)
    session_date = pd.to_datetime(meta.get("testingDate", "01_01_1970"), format="%m_%d_%Y")

    df_events = pd.read_csv(events_path)
    df_walks = pd.read_csv(walks_path)

    df_events['start_Timestamp'] = df_events['start_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))
    df_walks['start_Timestamp'] = df_walks['start_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))

    df_combined = pd.concat([df_events, df_walks], ignore_index=True)
    df_combined = df_combined.sort_values("start_Timestamp").reset_index(drop=True)
    df_combined.to_csv(out_path, index=False)
    print(f"✅ Final merged file written to {out_path}")'''

import pandas as pd
import json
import numpy as np
from pathlib import Path


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
        if block_num < 2 or block_num == 3:
            continue

        group = group.sort_values("start_Timestamp")
        pins = group[group['lo_eventType'] == 'PinDrop_Moment']
        coins = group[group['lo_eventType'] == 'CoinVis_end']
        start_event = group[group['lo_eventType'] == 'TrueBlockStart']

        if not start_event.empty and not pins.empty:
            start_ts = start_event.iloc[0]['start_Timestamp']
            end_ts = pins.iloc[0]['start_Timestamp']
            if pd.notnull(start_ts) and pd.notnull(end_ts):
                walk_time = (end_ts - start_ts).total_seconds()
                walk_rows.append({
                    **pins.iloc[0].to_dict(),
                    "lo_eventType": "Walk_PinDrop",
                    "med_eventType": "Walks",
                    "start_Timestamp": start_ts,
                    "end_Timestamp": end_ts,
                    "details": f"{{walkTime= {int(walk_time)} seconds}}"
                })

        for i in range(1, len(pins)):
            pin = pins.iloc[i]
            prev_coins = coins[coins['end_Timestamp'] < pin['start_Timestamp']]
            if not prev_coins.empty:
                last_coin = prev_coins.iloc[-1]
                walk_time = (pin['start_Timestamp'] - last_coin['end_Timestamp']).total_seconds()
                walk_rows.append({
                    **pin.to_dict(),
                    "lo_eventType": "Walk_PinDrop",
                    "med_eventType": "Walks",
                    "start_Timestamp": last_coin['end_Timestamp'],
                    "end_Timestamp": pin['start_Timestamp'],
                    "details": f"{{walkTime= {int(walk_time)} seconds}}"
                })

    walk_df = pd.DataFrame(walk_rows)
    if walk_df.empty:
        print(f"⚠️ No walks detected — skipping creation of {out_path.name}")
    else:
        walk_df.to_csv(out_path, index=False)
        print(f"✅ Computed Walk_PinDrop rows written to {out_path}")


def batch_compute_walks(input_dir, output_dir):
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    flat_files = {f.stem.replace("_events", ""): f for f in input_dir.glob("*_events.csv")}
    meta_files = {f.stem.replace("_meta", ""): f for f in input_dir.glob("*_meta.json")}

    for key in set(flat_files) & set(meta_files):
        flat_path = flat_files[key]
        meta_path = meta_files[key]
        out_path = output_dir / f"{key}_walks.csv"
        print(f"➡️ Computing walks for: {flat_path.name}")
        compute_walk_rows(flat_path, meta_path, out_path)


# Example usage:
input_dir = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day/pair_08/02_17_2025/Morning/MagicLeaps/ML2A'
output_dir = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day/pair_08/02_17_2025/Morning/MagicLeaps/ML2A/flattened'
batch_compute_walks(input_dir, output_dir)