
"""
extraction_AN_v2.py

Enhanced extraction for RewardCollectors data:
- Summary row generation
- Walking time computation
- Detail-based table extraction
"""

import pandas as pd
import json
from pathlib import Path
import re
import ast

def safe_parse_details_column(df):
    def clean_and_parse(val):
        if isinstance(val, str) and val.startswith("{") and "'" in val:
            try:
                val_dict = ast.literal_eval(val)
                return val_dict
            except (ValueError, SyntaxError):
                return parse_details_field(val)
        return parse_details_field(val)

    parsed_details = df['details'].apply(clean_and_parse)
    return pd.concat([df.drop(columns=['details']), parsed_details.apply(pd.Series)], axis=1)

def parse_details_field(details_str):
    result = {}
    if not isinstance(details_str, str):
        return result

    pairs = [seg.strip() for seg in details_str.split('|') if ':' in seg]
    for pair in pairs:
        key, val = pair.split(':', 1)
        key = key.strip()
        val = val.strip()

        if re.match(r"\(.*\)", val):
            nums = re.findall(r"-?\d+\.?\d*", val)
            if len(nums) == 3:
                result[f"{key}_x"] = float(nums[0])
                result[f"{key}_y"] = float(nums[1])
                result[f"{key}_z"] = float(nums[2])
        else:
            try:
                result[key] = float(val) if '.' in val else int(val)
            except ValueError:
                result[key] = val
    return result

def extract_summary_data(meta_path, events_path, out_dir="."):
    with open(meta_path, 'r') as f:
        meta = json.load(f)

    df = pd.read_csv(events_path)
    session_date_str = meta.get("testingDate", "01_01_1970")
    session_date = pd.to_datetime(session_date_str, format="%m_%d_%Y")

    def fix_time_str(ts):
        if not isinstance(ts, str):
            return pd.NaT
        if ts.count(":") == 3:
            ts = ".".join(ts.rsplit(":", 1))
        try:
            return pd.to_datetime(f"{session_date.strftime('%Y-%m-%d')} {ts}", format="%Y-%m-%d %H:%M:%S.%f")
        except Exception:
            return pd.NaT

    df['start_Timestamp'] = df['start_Timestamp'].apply(fix_time_str)
    df['end_Timestamp'] = df['end_Timestamp'].apply(fix_time_str)
    df = df.sort_values('start_Timestamp').reset_index(drop=True)

    blocks = meta['BlockStructureSummary']
    summary = {
        "file": meta["file"].replace(".csv", ""),
        "participant_id": meta.get("participantID"),
        "pairID": meta.get("pairID"),
        "testingDate": meta.get("testingDate"),
        "sessionType": meta.get("sessionType"),
        "ptIsAorB": meta.get("ptIsAorB"),
        "coinSet": meta.get("coinSet"),
        "device": meta.get("device"),
        "main_RR": meta.get("main_RR"),
        "currentRole": "AN",
        "total_blocks": len(blocks),
        "complete_blocks": sum(1 for b in blocks if b["BlockStatus"] == "complete"),
        "truncated_blocks": sum(1 for b in blocks if b["BlockStatus"] == "truncated"),
        "total_duration_sec": sum(b["BlockDuration_sec"] for b in blocks),
        "total_true_rounds": sum(b["NumTrueRounds"] for b in blocks),
        "avg_block_duration_sec": round(sum(b["BlockDuration_sec"] for b in blocks) / len(blocks), 4),
    }

    baseFileInfo = ['BlockNum', 'RoundNum', 'CoinSetID', 'BlockStatus', 'BlockType', 'chestPin_num', 'start_Timestamp']
    marks = df[df['lo_eventType'] == 'Mark'][baseFileInfo + ['details']]
    pinDropDist = df[df['lo_eventType'] == 'PinDrop_Moment'][baseFileInfo + ['details']]
    swapVotes = df[df['lo_eventType'] == 'SwapVoteMoment'][baseFileInfo + ['details']]

    marks_df = safe_parse_details_column(marks)
    pinDropDist_df = safe_parse_details_column(pinDropDist)
    swapVotes_df = safe_parse_details_column(swapVotes)

    walk_rows = []
    for block_num, group in df.groupby("BlockNum"):
        group = group.sort_values("start_Timestamp")
        start_event = group[group["lo_eventType"] == "TrueBlockStart"]
        pin_drops = group[group["lo_eventType"] == "PinDrop_Moment"]
        coin_ends = group[group["lo_eventType"] == "CoinVis_end"]

        if not start_event.empty and not pin_drops.empty:
            first_pin = pin_drops.iloc[0]
            walk_time = (first_pin["start_Timestamp"] - start_event.iloc[0]["start_Timestamp"]).total_seconds()
            walk_rows.append({
                "BlockNum": block_num,
                "start_Timestamp": first_pin["start_Timestamp"],
                "walk_time_sec": walk_time,
                "chestPin_num": first_pin["chestPin_num"],
                "RoundNum": first_pin.get("RoundNum"),
                "BlockType": first_pin.get("BlockType"),
                "lo_eventType": "Walk_PinDrop",
                "med_eventType": "Walks"
            })

        for i in range(1, len(pin_drops)):
            curr_pin = pin_drops.iloc[i]
            prev_coin = coin_ends[coin_ends["end_Timestamp"] < curr_pin["start_Timestamp"]]
            if not prev_coin.empty:
                last_coin = prev_coin.iloc[-1]
                walk_time = (curr_pin["start_Timestamp"] - last_coin["end_Timestamp"]).total_seconds()
                walk_rows.append({
                    "BlockNum": block_num,
                    "start_Timestamp": curr_pin["start_Timestamp"],
                    "walk_time_sec": walk_time,
                    "chestPin_num": curr_pin["chestPin_num"],
                    "RoundNum": curr_pin.get("RoundNum"),
                    "BlockType": curr_pin.get("BlockType"),
                    "lo_eventType": "Walk_PinDrop",
                    "med_eventType": "Walks"
                })

    walk_df = pd.DataFrame(walk_rows)
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    pd.DataFrame([summary]).to_csv(Path(out_dir) / "summary_extracted.csv", index=False)
    walk_df.to_csv(Path(out_dir) / "pinDropWalks_extracted.csv", index=False)
    marks_df.to_csv(Path(out_dir) / "marks_extracted.csv", index=False)
    pinDropDist_df.to_csv(Path(out_dir) / "pinDropDist_extracted.csv", index=False)
    swapVotes_df.to_csv(Path(out_dir) / "swapVotes_extracted.csv", index=False)

    print(f"✅ Saved outputs to: {out_dir}")
    return df, walk_df, summary

def build_full_walk_rows(events_df, walk_df, output_path):
    full_walk_rows = []

    for _, walk in walk_df.iterrows():
        block = walk["BlockNum"]
        pin_num = walk["chestPin_num"]

        start_event = events_df[
            (events_df["BlockNum"] == block) &
            (events_df["chestPin_num"] == pin_num) &
            (events_df["lo_eventType"] == "CoinVis_end")
        ].sort_values("start_Timestamp").head(1)

        end_event = events_df[
            (events_df["BlockNum"] == block) &
            (events_df["chestPin_num"] == pin_num + 1) &
            (events_df["lo_eventType"] == "PinDrop_Moment")
        ].sort_values("start_Timestamp").head(1)

        if not start_event.empty and not end_event.empty:
            start = start_event.iloc[0]
            end = end_event.iloc[0]

            combined_row = start.to_dict()
            combined_row.update({
                "lo_eventType": "Walk_PinDrop",
                "med_eventType": "Walks",
                "hi_eventType": end.get("hi_eventType", "PinDrop"),
                "hiMeta_eventType": end.get("hiMeta_eventType", "logged"),
                "start_Timestamp": start["start_Timestamp"],
                "end_Timestamp": end["start_Timestamp"],
                "start_AppTime": start["start_AppTime"],
                "end_AppTime": end["start_AppTime"],
                "AppTime": start["start_AppTime"],
                "original_row_start": start["original_row_start"],
                "original_row_end": end["original_row_end"],
                "details": f"{{walkTime= {int(walk['walk_time_sec'])} seconds}}"
            })
            full_walk_rows.append(combined_row)

    walk_full_df = pd.DataFrame(full_walk_rows)
    walk_full_df.to_csv(output_path, index=False)
    print(f"✅ Saved detailed walk rows to: {output_path}")


df, walk_df, summary = extract_summary_data("ObsReward_A_02_17_2025_15_11_processed_meta.json", "ObsReward_A_02_17_2025_15_11_processed_events.csv", out_dir="./outputs")
build_full_walk_rows(df, walk_df, output_path="./outputs/pinDropWalks_full.csv")
