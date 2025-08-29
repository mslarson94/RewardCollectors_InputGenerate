# eventParser_PO.py
# Extracted and modularized PO event parsing functions.

import re
import pandas as pd

def process_pin_drop(df, allowed_statuses):
    pattern = "Other participant just dropped a new pin"
    return df[df["Messages"].str.contains(pattern, na=False)]

def process_pin_drop_vote(df, allowed_statuses):
    return df[df["Messages"].str.contains("Observer says", na=False)]

def process_feedback_collect(df, allowed_statuses):
    return df[df["Messages"].str.contains("A.N. collected coin", na=False)]

def process_ie_events(df, allowed_statuses):
    return df[df["Messages"].str.contains("A.N. opened a chest", na=False)]

def process_marks(df, allowed_statuses):
    return df[df["Messages"].str.contains("Mark should happen", na=False)]

def process_swap_votes(df, allowed_statuses):
    return df[df["Messages"].str.contains("Observer says", na=False)]

def process_block_periods(df, allowed_statuses):
    return df[df["Messages"].str.contains("block period", na=False)]

def extract_walking_periods(df, cascades, allowed_statuses):
    # Stub for future walking periods logic
    return []

## not yet supported 
def process_chest_opened(df, allowed_statuses):
    chest_events = []
    for row in df.itertuples():
        if pd.notna(row.BlockNum) and getattr(row, "BlockStatus", "unknown") not in allowed_statuses:
            continue
        if isinstance(row.Message, str) and "Chest opened:" in row.Message:
            try:
                coin_id = int(row.Message.replace("AN opened chest: ", "").strip())
                chest_events.append({
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "event_type": "ChestOpened",
                    "idvCoinID": coin_id,
                    "BlockNum": row.BlockNum,
                    "RoundNum": row.RoundNum,
                    "CoinSetID": row.CoinSetID,
                    "original_row_start": df.at[row.Index, "original_index"],
                    "original_row_end": df.at[row.Index, "original_index"]

                })
            except ValueError:
                continue
    return chest_events

## not yet supported
def process_IE_coin_collected(df, chest_events, allowed_statuses):
    collected_events = []
    chest_lookup = {(e["BlockNum"], e["RoundNum"]): e for e in chest_events}

    for row in df.itertuples():
        if pd.notna(row.BlockNum) and getattr(row, "BlockStatus", "unknown") not in allowed_statuses:
            continue
        #if isinstance(row.Message, str) and ("Collected feedback coin" in row.Message or "coin collected" in row.Message):
        if isinstance(row.Message, str) and "coin collected" in row.Message:
            match_key = (row.BlockNum, row.RoundNum)
            linked_chest = chest_lookup.get(match_key)

            event_data = {
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "event_type": "CoinCollect",
                "BlockNum": row.BlockNum,
                "RoundNum": row.RoundNum,
                "CoinSetID": row.CoinSetID,
                "original_row_start": df.at[row.Index, "original_index"],
                "original_row_end": df.at[row.Index, "original_index"]
            }

            if linked_chest:
                event_data["idvCoinID"] = linked_chest.get("idvCoinID")

            collected_events.append(event_data)
    return collected_events