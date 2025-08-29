import pandas as pd
import json
import numpy as np
from pathlib import Path
import os

#########
def fix_time_str(ts, session_date):
    """Robustly combine a session date (string 'MM_DD_%Y' or datetime) with a time string.
       Supports 'HH:MM:SS:ms' and 'HH:MM:SS.ms'."""
    if not isinstance(ts, str):
        return pd.NaT

    # Ensure session_date is a datetime
    if isinstance(session_date, str):
        try:
            session_date = pd.to_datetime(session_date, format="%m_%d_%Y")
        except Exception:
            return pd.NaT

    # Convert HH:MM:SS:ms → HH:MM:SS.ms
    if ts.count(":") == 3:
        ts = ".".join(ts.rsplit(":", 1))

    try:
        return pd.to_datetime(f"{session_date.strftime('%Y-%m-%d')} {ts}",
                              format="%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        return pd.NaT

def _fmt(ts):
    """Format pandas Timestamp to 'HH:MM:SS.mmmmmm' or NA if null."""
    return ts.strftime("%H:%M:%S.%f") if pd.notnull(ts) else pd.NA

########

def compute_collecting_walks(group):
    """Collecting phase (initial encoding): block-start→first chest, then chest→chest segments."""
    rows = []
    g = group.sort_values("start_Timestamp")

    chests = g[g['lo_eventType'] == 'ChestOpen_Moment']
    coins  = g[g['lo_eventType'] == 'CoinVis_end']      # optional anchor
    start_event = g[g['lo_eventType'] == 'TrueBlockStart']

    # Block start → first chest
    if not start_event.empty and not chests.empty:
        start = start_event.iloc[0]
        end   = chests.iloc[0]
        walk_time = (end['start_Timestamp'] - start['start_Timestamp']).total_seconds()

        row = start.to_dict()
        row.update({
            "lo_eventType": "Walk_ChestOpen",
            "med_eventType": "RewardMemoryDrivenNav",
            "hi_eventType": "WalkingPeriod",
            "hiMeta_eventType": "PreBlockActivity",
            "source": "synthetic",
            # NOTE: timestamps already parsed in compute_walk_rows; just format:
            "start_Timestamp": _fmt(start['start_Timestamp']),
            "end_Timestamp":   _fmt(end['start_Timestamp']),
            "start_AppTime": start.get("start_AppTime", pd.NA),
            "end_AppTime":   end.get("start_AppTime", pd.NA),
            "AppTime": start.get("AppTime", pd.NA),
            "original_row_start": start.get("original_row_start", pd.NA),
            "original_row_end":   end.get("original_row_end", pd.NA),
            "walkTime": walk_time,
        })
        rows.append(row)

    # chest → chest (via last coin visibility end as anchor)
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
                "start_Timestamp": _fmt(last_coin['end_Timestamp']),
                "end_Timestamp":   _fmt(chest['start_Timestamp']),
                "start_AppTime": last_coin.get("end_AppTime", pd.NA),
                "end_AppTime":   chest.get("start_AppTime", pd.NA),
                "AppTime": chest.get("AppTime", pd.NA),
                "original_row_start": last_coin.get("original_row_start", pd.NA),
                "original_row_end":   chest.get("original_row_end", pd.NA),
                "walkTime": walk_time,
            })
            rows.append(row)

    return rows


def compute_pindrop_walks(group, phase_label):
    # Pindropping phase: walks from block start to first Pin, and pin-to-pin via last coin visibility end
    rows = []
    g = group.sort_values("start_Timestamp")

    pins = g[g['lo_eventType'] == 'PinDrop_Moment']
    coins = g[g['lo_eventType'] == 'CoinVis_end']
    start_event = g[g['lo_eventType'] == 'TrueBlockStart']

    if not start_event.empty and not pins.empty:
        start = start_event.iloc[0]
        end = pins.iloc[0]
        walk_time = (end['start_Timestamp'] - start['start_Timestamp']).total_seconds()
        row = start.to_dict()
        row.update({
            "lo_eventType": "Walk_PinDrop",
            "med_eventType": phase_label,  # e.g., "Pindropping_Rounds1" or "Pindropping_RoundsGT1"
            "hi_eventType": "WalkingPeriod",
            "hiMeta_eventType": "InBlock",
            "source": "synthetic",
            "start_Timestamp": _fmt(start['start_Timestamp']),
            "end_Timestamp": _fmt(end['start_Timestamp']),
            "start_AppTime": start.get("start_AppTime", pd.NA),
            "end_AppTime": end.get("start_AppTime", pd.NA),
            "AppTime": start.get("AppTime", pd.NA),
            "original_row_start": start.get("original_row_start", pd.NA),
            "original_row_end": end.get("original_row_end", pd.NA),
            "walkTime": walk_time
        })
        rows.append(row)

    for i in range(1, len(pins)):
        pin = pins.iloc[i]
        prev_coins = coins[coins['end_Timestamp'] < pin['start_Timestamp']]
        if not prev_coins.empty:
            last_coin = prev_coins.iloc[-1]
            walk_time = (pin['start_Timestamp'] - last_coin['end_Timestamp']).total_seconds()
            row = last_coin.to_dict()
            row.update({
                "lo_eventType": "Walk_PinDrop",
                "med_eventType": phase_label,
                "hi_eventType": "WalkingPeriod",
                "hiMeta_eventType": "InBlock",
                "source": "synthetic",
                "start_Timestamp": _fmt(last_coin['end_Timestamp']),
                "end_Timestamp": _fmt(pin['start_Timestamp']),
                "start_AppTime": last_coin.get("end_AppTime", pd.NA),
                "end_AppTime": pin.get("start_AppTime", pd.NA),
                "AppTime": pin.get("AppTime", pd.NA),
                "original_row_start": last_coin.get("original_row_start", pd.NA),
                "original_row_end": pin.get("original_row_end", pd.NA),
                "walkTime": walk_time
            })
            rows.append(row)

    return rows


def compute_walk_rows(flat_path, meta_path, out_path):
    with open(meta_path, 'r') as f:
        meta = json.load(f)
    session_date = pd.to_datetime(meta.get("testingDate", "01_01_1970"), format="%m_%d_%Y")

    df = pd.read_csv(flat_path)
    # Parse timestamps once
    df['start_Timestamp'] = df['start_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))
    df['end_Timestamp'] = df['end_Timestamp'].apply(lambda ts: fix_time_str(ts, session_date))

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
        if 'start_Timestamp' in walk_df.columns:
            walk_df = walk_df.sort_values("start_Timestamp").reset_index(drop=True)
        walk_df.to_csv(out_path, index=False)
        print(f"✅ Walk rows written to {out_path}")


def batch_compute_walks(events_dir, meta_dir, output_dir):
    events_dir = Path(events_dir)
    meta_dir = Path(meta_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    meta_files = {f.stem.replace("_processed_meta", ""): f for f in meta_dir.glob("*_meta.json")}
    event_files = {f.stem.replace("_events_flat", ""): f for f in events_dir.glob("*_events_flat.csv")}

    matched_keys = set(event_files) & set(meta_files)

    for key in sorted(matched_keys):
        flat_file = event_files[key]
        meta_file = meta_files[key]
        out_file = output_dir / f"{key}_walks.csv"
        print(f"➡️ Computing walks for: {flat_file.name}")
        compute_walk_rows(flat_file, meta_file, out_file)


if __name__ == "__main__":
    base_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/ResurrectedData"
    events_dir = os.path.join(base_dir, "Events_AugmentedPart3")   # *_events_flat.csv
    meta_dir = os.path.join(base_dir, "MetaData_Flat")             # *_meta.json
    output_dir = os.path.join(base_dir, "Events_ComputedWalks")    # output
    print("🚀 Starting batch compute walks..")
    batch_compute_walks(events_dir, meta_dir, output_dir)
