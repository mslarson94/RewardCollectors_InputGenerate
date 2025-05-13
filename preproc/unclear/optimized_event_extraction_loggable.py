

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

import pandas as pd
import re
from datetime import datetime, timedelta

SKIP_OFFSET = 0
ALLOWED_STATUSES = {"complete", "done", "finished"}

def classify_coin_type(CoinSetID, idvCoinID):
    if CoinSetID == 2 and idvCoinID == 2:
        return "PPE"
    elif CoinSetID == 3 and idvCoinID == 0:
        return "NPE"
    elif CoinSetID == 1:
        return "Normal"
    elif CoinSetID == 2 and idvCoinID in [0, 1]:
        return "Normal"
    elif CoinSetID == 3 and idvCoinID in [1, 2]:
        return "Normal"
    elif CoinSetID == 4:
        return "TutorialNorm"
    elif CoinSetID == 5 and idvCoinID == 1:
        return "TutorialNorm"
    elif CoinSetID == 5 and idvCoinID in [0, 2]:
        return "TutorialRPE"
    return "Unknown"

def enrich_with_metadata(events, source_file, metadata_df):
    matched = metadata_df[metadata_df["source_file"] == source_file]
    if matched.empty:
        return events
    metadata = matched.iloc[0].to_dict()
    for event in events:
        event.update(metadata)
    return events

def fast_process_pin_drop(df):
    logging.info('Entering fast_process_pin_drop')
    events = []
    cascade_id = 0
    filtered = df[
        df["Type"].eq("Event") &
        df["Message"].str.contains("Other participant just dropped a new pin at", na=False)
    ]
    for idx, row in filtered.iterrows():
        block_status = row.get("BlockStatus", "unknown")
        if pd.notna(row.get("BlockNum")) and block_status not in ALLOWED_STATUSES:
            continue
        cascade_id += 1
        start_time = row["AppTime"]
        timestamp = row["Timestamp"]
        common_info = {
            "BlockNum": row.get("BlockNum", None),
            "RoundNum": row.get("RoundNum", None),
            "CoinSetID": row.get("CoinSetID", None),
            "BlockStatus": block_status,
            "original_row": idx + SKIP_OFFSET
        }
        event = {
            "AppTime": start_time,
            "Timestamp": timestamp,
            "cascade_id": cascade_id,
            "event_type": "PinDrop",
            "details": {},
            "source": "logged",
            **common_info
        }
        match = re.search(r'at ([\d\.\-]+) ([\d\.\-]+) ([\d\.\-]+)', row["Message"])
        if match:
            event["details"].update({
                "pin_local_x": float(match.group(1)),
                "pin_local_y": float(match.group(2)),
                "pin_local_z": float(match.group(3)),
            })
        for j in range(idx + 1, min(idx + 10, len(df))):
            msg = df.at[j, "Message"] if isinstance(df.at[j, "Message"], str) else ""
            if "Dropped pin was dropped at " in msg:
                match = re.search(
                    r'Dropped pin was dropped at ([\d\.]+) from chest (\d+) originally at \(([-\d\.]+), ([-\d\.]+), ([-\d\.]+)\):(INCORRECT|CORRECT)', msg
                )
                if match:
                    event["details"].update({
                        "pinDist": match.group(1),
                        "coinIdvID": match.group(2),
                        "coin_IdvPosition": (match.group(3), match.group(4), match.group(5)),
                        "pinDropQuality": match.group(6)
                    })
            elif "Observer chose" in msg and "pindrop from the navigator" in msg:
                match = re.search(r'Observer chose (CORRECT|INCORRECT)', msg)
                if match:
                    event["details"].update({
                        "pinDropVote": match.group(1)
                    })
        events.append(event)
        try:
            synthetic_time = datetime.strptime(timestamp, '%H:%M:%S:%f')
            for offset, evt in [
                (0.000, "PinDrop_Sound_start"), (0.000, "GrayPin_Visible_start"),
                (0.000, "textInstr_Visible_start"), (0.000, "voteWindow_start"),
                (0.654, "PinDrop_Sound_end"), (1.000, "textInstr_Visible_end"),
                (2.000, "GrayPin_Visible_end"), (2.000, "voteWindow_end"),
                (2.000, "Feedback_Sound_start"), (2.000, "Feedback_textNcolor_Visible_start"),
                (3.000, "Feedback_textNcolor_Visible_end"), (3.000, "Coin_Visible_start")
            ]:
                synth_time = synthetic_time + timedelta(seconds=offset)
                events.append({
                    "AppTime": start_time + offset,
                    "Timestamp": synth_time.strftime('%H:%M:%S:%f'),
                    "cascade_id": cascade_id,
                    "event_type": evt,
                    "details": {},
                    "source": "synthetic",
                    **{k: event[k] for k in ("BlockNum", "RoundNum", "CoinSetID", "BlockStatus")}
                })
        except ValueError:
            continue
    return events

def fast_process_feedback_collect_fixed(df):
    logging.info('Entering fast_process_feedback_collect_fixed')
    events = []
    cached_idvCoinID = {}
    event_rows = df[(df["Type"] == "Event") & df["Message"].notna()]
    for row in event_rows.itertuples():
        block_status = getattr(row, "BlockStatus", "unknown")
        if pd.notna(row.BlockNum) and block_status not in ALLOWED_STATUSES:
            continue
        msg = row.Message.strip()
        if msg.startswith("A.N. collected coin:") and "round reward" in msg:
            try:
                body = msg.replace("A.N. collected coin:", "").replace("round reward:", "").strip()
                id_str, reward_str = body.split()
                idv_id = int(id_str)
                round_total = float(reward_str)
                cached_idvCoinID[(row.BlockNum, row.RoundNum, row.CoinSetID)] = idv_id
                coin_type = classify_coin_type(row.CoinSetID, idv_id)
                event = {
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "cascade_id": None,
                    "event_type": "Feedback_CoinCollect",
                    "details": {
                        "runningRoundTotal": round_total,
                        "idvCoinID": idv_id,
                        "CoinType": coin_type
                    },
                    "source": "logged",
                    "original_row": row.Index + SKIP_OFFSET,
                    "BlockNum": row.BlockNum,
                    "RoundNum": row.RoundNum,
                    "CoinSetID": row.CoinSetID,
                }
                events.append(event)
            except ValueError:
                continue
        elif msg.startswith("A.N. collected coin:") and "round reward" not in msg:
            id_str = msg.replace("A.N. collected coin:", "").strip()
            if id_str.isdigit():
                cached_idvCoinID[(row.BlockNum, row.RoundNum, row.CoinSetID)] = int(id_str)
    return events

def fast_process_ie_events(df):
    logging.info('Entering fast_process_ie_events')
    events = []
    filtered = df[
        (df["Type"] == "Event") &
        (df["Message"].notna()) &
        (df["Message"].str.contains("Other participant just collected coin:", na=False) |
         df["Message"].str.contains("Chest opened:", na=False))
    ]
    for row in filtered.itertuples():
        block_status = getattr(row, "BlockStatus", "unknown")
        if pd.notna(row.BlockNum) and block_status not in ALLOWED_STATUSES:
            continue
        msg = row.Message.strip()
        if msg.startswith("Chest opened:"):
            try:
                parts = msg.replace("Chest opened:", "").strip()
                details = {"idvCoinID": int(parts)}
                event = {
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "cascade_id": None,
                    "event_type": "IE_ChestOpen",
                    "details": details,
                    "source": "logged",
                    "original_row": row.Index + SKIP_OFFSET,
                    "BlockNum": row.BlockNum,
                    "RoundNum": row.RoundNum,
                    "CoinSetID": row.CoinSetID,
                }
                events.append(event)
            except ValueError:
                continue
        elif msg.startswith("Other participant just collected coin:"):
            try:
                coin_id = int(msg.split(":")[1].strip())
                details = {"idvCoinID": coin_id}
                event = {
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "cascade_id": None,
                    "event_type": "IE_CoinCollected",
                    "details": details,
                    "source": "logged",
                    "original_row": row.Index + SKIP_OFFSET,
                    "BlockNum": row.BlockNum,
                    "RoundNum": row.RoundNum,
                    "CoinSetID": row.CoinSetID,
                }
                events.append(event)
            except (IndexError, ValueError):
                continue
    return events

def extract_all_events(df, metadata_df, source_file):
    logging.info(f'Extracting events from: {source_file}')
    all_events = []
    all_events += fast_process_pin_drop(df)
    all_events += fast_process_feedback_collect_fixed(df)
    all_events += fast_process_ie_events(df)
    logging.info(f'Total events extracted before enrichment: {len(all_events)}')
    enriched = enrich_with_metadata(all_events, source_file, metadata_df)
    return enriched
