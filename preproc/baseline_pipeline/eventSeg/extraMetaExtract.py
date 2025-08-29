# Filename: extraMetaExtract.py
import pandas as pd
import json
import re
from collections import defaultdict
from datetime import datetime

def extract_coin_registry_deltas(data):
    coin_registry = defaultdict(dict)
    current_coinset_id = None

    for _, row in data.iterrows():
        message = str(row.get("Message", ""))
        coinset_match = re.search(r"coinsetID:(\d+)", message)
        if coinset_match:
            current_coinset_id = int(coinset_match.group(1))

        coinpoint_match = re.search(
            r"coinpoint(\d+):.*?deltax:([-+]?[0-9]*\.?[0-9]+)\s+deltay:([-+]?[0-9]*\.?[0-9]+)\s+deltaz:([-+]?[0-9]*\.?[0-9]+)",
            message
        )
        if coinpoint_match and current_coinset_id is not None:
            coin_index = int(coinpoint_match.group(1))
            dx = float(coinpoint_match.group(2))
            dy = float(coinpoint_match.group(3))
            dz = float(coinpoint_match.group(4))
            coin_registry[current_coinset_id][coin_index] = {"deltax": dx, "deltay": dy, "deltaz": dz}

    return dict(coin_registry)

def generate_meta_json(data, meta_df, target_file, role):
    coin_registry_deltas = extract_coin_registry_deltas(data)
    meta_df["cleanedFile"] = meta_df["cleanedFile"].astype(str).str.strip().str.lower()
    matched_meta = meta_df[meta_df["cleanedFile"] == target_file.lower()]
    meta_row = matched_meta.iloc[0].to_dict() if not matched_meta.empty else {}

    block_structure_summary = summarize_block_structure_with_durations(data, role)

    meta_json = {
        "file": target_file,
        "participantID": meta_row.get("participantID", "unknown"),
        "pairID": meta_row.get("pairID", "unknown"),
        "testingDate": str(meta_row.get("testingDate", "unknown")),
        "device": meta_row.get("device", "unknown"),
        "coinSet": meta_row.get("coinSet", "unknown"),
        "sessionType": meta_row.get("sessionType", "unknown"),
        "main_RR": meta_row.get("main_RR", "unknown"),
        "CoinRegistry": coin_registry_deltas,
        "BlockStructureSummary": block_structure_summary
    }

    return meta_json


def summarize_block_structure_with_durations(df, role):
    df = df.copy()
    df["is_special"] = df["RoundNum"].isin([0, 7777, 8888, 9999])
    #df["parsed_Timestamp"] = df["Timestamp"].apply(safe_parse_timestamp)
    timestamp_col = "mLTimestamp" if role == "AN" else "mLTimestamp_orig"
    print(timestamp_col)
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")

    block_summaries = []

    for block_num, block_df in df.groupby("BlockNum"):
        round_set = set(block_df["RoundNum"].dropna().unique())
        round_nums = sorted(round_set)

        block_status = block_df["BlockStatus"].iloc[0] if not block_df["BlockStatus"].isna().all() else "unknown"

        block_df = block_df.copy()
        block_df["is_7777"] = block_df["RoundNum"] == 7777
        block_df["transition"] = block_df["is_7777"].ne(block_df["is_7777"].shift()).cumsum()
        grouped = block_df.groupby(["transition", "is_7777"])
        unique_7777_spans = grouped.size().reset_index(name="count")
        num_7777_segments = unique_7777_spans[unique_7777_spans["is_7777"]].shape[0]

        ts_min = block_df[timestamp_col].min()
        ts_max = block_df[timestamp_col].max()
        duration_sec = (ts_max - ts_min).total_seconds() if ts_min and ts_max else None

        block_summaries.append({
            "BlockNum": int(block_num) if pd.notna(block_num) else None,
            "BlockStatus": block_status,
            "AllRoundNums": round_nums,
            "SpecialRoundNums": sorted(round_set.intersection({0, 7777, 8888, 9999})),
            "NumTrueRounds": len([r for r in round_nums if 1 <= r <= 20]),
            "Num7777Segments": int(num_7777_segments),
            "BlockDuration_sec": duration_sec
        })

    return block_summaries
