import os
import pandas as pd
import numpy as np
import re
from datetime import datetime
from typing import Optional, Any

# preprocHelpers.py

cols_2D = ['EyeLeft', 'EyeRight']
cols_3D = ["HeadPosAnchored", "EyeDirectionAnchored", "FixationPointAnchored"]
cols_rotation = ["HeadForthAnchored"]
cols2Drop = [
    "GlobalBlock", "HeadPos", "HeadRot(pitch yaw roll)", "EyeDirection",
    "optiRbodyposA", "optiRbodyposB", "optiRbodyrotA", "optiRbodyrotB",
    "Messages_filled"
]
'''
    RPTimestamp = RobustParsedTimestamp --> just that the original Timestamp column is full of strings that needed to be parsed to be pandas friendly
    eRPTimestamp = enhanced RobustParsedTimestamp --> enhancing the RPTimestamp with AppTime's microsecond precision 

    "RPTimestamp", "eRPTimestamp", "origRow", ---> Search for uses of RPT eRPT, origRow 
    "RPTimestamp_orig" &  "eRPTimestamp_orig", are called that way in the PO version because 
'''

last_chunk_column_order = [
    "AppTime", "mLT_raw", 
    "origRow", "BlockNum", "RoundNum", "BlockInstance", "Type",
    "HeadPosAnchored_x", "HeadPosAnchored_y", "HeadPosAnchored_z", 
    "HeadForthAnchored_yaw", "HeadForthAnchored_pitch", "HeadForthAnchored_roll", 
    "EyeDirectionAnchored_x", "EyeDirectionAnchored_y", "EyeDirectionAnchored_z", 
    "FixationPointAnchored_x", "FixationPointAnchored_y", "FixationPointAnchored_z", 
    "LeftEyeOpen", "RightEyeOpen", "EyeLeft_x", "EyeLeft_y", 
    "EyeRight_x", "EyeRight_y", "EyeTarget", "AmplitudeDeg", 
    "DirectionRadial", "VelocityDegps", "Message", 
    "BlockType", "CoinSetID", "BlockStatus", 
    "chestPin_num", "totalRounds"
]


final_column_order_AN = ["mLT", "eMLT"] + last_chunk_column_order
final_column_order_PO = ["mLT_orig", "eMLT_orig"] + last_chunk_column_order
final_column_order = ["mLT_orig", "eMLT_orig"] + last_chunk_column_order
def detect_and_tag_blocks(data, role):
    block_start_idx = None
    block_num = None
    coinset_id = None
    block_type = None
    last_seen_coinset = None
    round_num = None
    block_instance = 0

    data["RoundNum"] = np.nan
    data["BlockNum"] = np.nan
    data["CoinSetID"] = np.nan
    data["BlockType"] = np.nan
    data["BlockInstance"] = np.nan  # Add BlockInstance column

    for idx, row in data.iterrows():
        message = row.get("Message", "")

        if isinstance(message, str):

            if message == "Mark should happen if checked on terminal.":
                block_start_idx = idx
                round_num = 0  # round starts counting from this row forward
                block_instance += 1  # Increment BlockInstance
                # ✅ assign block instance on the Mark row itself
                data.at[idx, "BlockInstance"] = block_instance
                continue  # don't set RoundNum here

            if message.startswith("coinsetID:"):
                match = re.search(r"coinsetID:(\d+)", message)
                if match:
                    last_seen_coinset = int(match.group(1))

            # Unified regex for both PO and AN participants
            block_match = re.search(
                r"Started (?:watching other participant's )?(collecting|pin dropping|pindropping)\. Block:\s*(\d+)",
                message,
                re.IGNORECASE
            )

            if block_match:
                block_type_raw = block_match.group(1)
                block_num = int(block_match.group(2))
                coinset_id = last_seen_coinset
                block_type = 'collecting' if 'collecting' in block_type_raw else 'pindropping'
                if role.upper() == "PO":
                    start = block_start_idx + 1 if block_start_idx is not None else idx
                else:
                    start = block_start_idx if block_start_idx is not None else idx

                for j in range(start, idx + 1):
                    data.at[j, "BlockNum"] = block_num
                    data.at[j, "CoinSetID"] = coinset_id
                    data.at[j, "BlockType"] = block_type
                    data.at[j, "BlockInstance"] = block_instance
                block_start_idx = None

            if "Repositioned and ready to start block or round" in message:
                if round_num is not None:
                    round_num += 1
                    data.at[idx, "RoundNum"] = round_num

    return data

def forward_fill_block_info(data):
    """
    Generic forward-fill function.
    """
    current_block_num = None
    current_coinset_id = None
    current_block_type = None
    # current_round_num = None
    current_block_instance = None

    for idx in range(len(data)):
        row = data.iloc[idx]
        # round_num = row["RoundNum"]

        if not pd.isna(row["BlockNum"]):
            current_block_num = row["BlockNum"]
            current_coinset_id = row["CoinSetID"]
            current_block_type = row["BlockType"]

        if not pd.isna(row["BlockInstance"]):
            current_block_instance = row["BlockInstance"]

        # if not pd.isna(round_num):
        #     current_round_num = round_num

        if current_block_num is not None:
            data.at[idx, "BlockNum"] = current_block_num
            data.at[idx, "CoinSetID"] = current_coinset_id
            data.at[idx, "BlockType"] = current_block_type
            data.at[idx, "BlockInstance"] = current_block_instance

        # if current_round_num is not None and pd.isna(round_num):
        #     data.at[idx, "RoundNum"] = current_round_num

    return data

def detect_block_completeness(block_rows):
    messages = block_rows["Message"].dropna().str.lower()
    has_start = any("mark should happen" in m for m in messages)
    has_end = any("finished current task" in m or "finished watching other participant's" in m for m in messages)

    if has_start and has_end:
        return "complete"
    elif has_start:
        return "truncated"
    return "incomplete"

def fix_collecting_block_coinsetids(data):
    for block_id in data["BlockNum"].dropna().unique():
        block_rows = data[data["BlockNum"] == block_id]
        if block_rows["BlockType"].iloc[0] != "collecting":
            continue  # Only fix collecting blocks

        # Look within this block for the correct coinsetID
        correct_coinset = None
        for msg in block_rows["Message"].dropna():
            match = re.search(r"coinsetID:(\d+)", msg)
            if match:
                correct_coinset = int(match.group(1))
                break

        if correct_coinset is not None:
            data.loc[data["BlockNum"] == block_id, "CoinSetID"] = correct_coinset

    return data

## Handling Coodinates
def split_coordinates(val):
    if isinstance(val, str):
        parts = val.strip().split()
        if len(parts) == 2:
            try:
                return float(parts[0]), float(parts[1])
            except ValueError:
                return None, None
        elif len(parts) == 3:
            try:
                return float(parts[0]), float(parts[1]), float(parts[2])
            except ValueError:
                return None, None, None
    return None

def parse_2D_coords(df, coords2DList):
    for col in coords2DList:
        df[[f"{col}_x", f"{col}_y"]] = df[col].apply(lambda x: pd.Series(split_coordinates(x)))
    df.drop(columns=coords2DList, inplace=True)
    return df

def parse_3D_coords(df, coords3DList):
    for col in coords3DList:
        df[[f"{col}_x", f"{col}_y", f"{col}_z"]] = df[col].apply(lambda x: pd.Series(split_coordinates(x)))
    df.drop(columns=coords3DList, inplace=True)
    return df

def parse_rotation(df, rotationList):
    for col in rotationList:
        df[[f"{col}_yaw", f"{col}_pitch", f"{col}_roll"]] = df[col].apply(lambda x: pd.Series(split_coordinates(x)))
    df.drop(columns=rotationList, inplace=True)
    return df

def drop_dead_cols(df, cols2Drop):
    df.drop(columns=cols2Drop, inplace=True, errors='ignore')
    return df

## Prelim Timestamp Handling
def safe_parse_timestamp(ts):
    try:
        if isinstance(ts, str) and ts.count(':') == 3:
            hh, mm, ss, ms = ts.split(':')
            ms = (ms + '000')[:6]
            return datetime.strptime(f"{hh}:{mm}:{ss}:{ms}", "%H:%M:%S:%f")
        return datetime.strptime(ts, "%H:%M:%S:%f")
    except Exception:
        return None

from pandas import to_datetime, NaT
from datetime import datetime

def robust_parse_timestamp(ts, session_date=None):
    """
    Drop-in replacement for safe_parse_timestamp, using fix_time_str logic.
    Accepts optional session_date (string 'MM_DD_YYYY' or datetime).
    """
    if not isinstance(ts, str):
        return NaT

    # Auto-convert session_date string if needed
    if isinstance(session_date, str):
        try:
            session_date = to_datetime(session_date, format="%m_%d_%Y")
        except Exception:
            return NaT

    # Fallback to a dummy date if none provided
    if session_date is None:
        session_date = datetime(1970, 1, 1)

    # Fix malformed 3-colon time like HH:MM:SS:ms
    if ts.count(":") == 3:
        ts = ".".join(ts.rsplit(":", 1))

    try:
        return to_datetime(f"{session_date.strftime('%Y-%m-%d')} {ts}", format="%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        return NaT

'''
# Example session date (replace with real source)
session_date = "02_17_2025"

df['ParsedTimestamp'] = df['Timestamp'].apply(lambda ts: robust_parse_timestamp(ts, session_date))

'''

#def enhance_timestamp_with_apptime(df, role):
def enhance_timestamp_with_apptime(df):
    """
    Enhances 'RobustParsedTimestamp' by injecting the microsecond precision from 'AppTime'.
    Adds a new column 'EnhancedTimestamp' with the modified datetime.
    """
    # if role == "AN":
    #     timestamps = pd.to_datetime(df["mLT"], errors="coerce")
    # elif role == "PO": 
    #     timestamps = pd.to_datetime(df["mLT_orig"], errors="coerce")
    # else:
    #     print("You broke it! You need to provide a role - either AN or PO!")
    timestamps = pd.to_datetime(df["mLT_orig"], errors="coerce")
    app_times = pd.to_numeric(df["AppTime"], errors="coerce")

    enhanced_ts = []
    for ts, at in zip(timestamps, app_times):
        if pd.isnull(ts) or pd.isnull(at):
            enhanced_ts.append(pd.NaT)
            continue
        fractional = at % 1
        ts = ts.replace(microsecond=int(round(fractional * 1_000_000)))
        enhanced_ts.append(ts)

    # if role == "AN": 
    #     df["eMLT"] = enhanced_ts
    # elif role == "PO":
    #     df["eMLT_orig"] = enhanced_ts
    # return df
    df["eMLT_orig"] = enhanced_ts
    return df

## Tying Nearly Everything Together
def process_obsreward_file(data, role):
    data["BlockNum"] = None
    data["RoundNum"] = None
    data["BlockType"] = None
    data["CoinSetID"] = None
    data["BlockStatus"] = None

    detect_and_tag_blocks(data, role)
    forward_fill_block_info(data)
    fix_collecting_block_coinsetids(data)

    data["Messages_filled"] = data["Message"].fillna(method='ffill')

    for block_instance in data["BlockInstance"].dropna().unique():
        block_mask = data["BlockInstance"] == block_instance
        block_rows = data[block_mask]
        status = detect_block_completeness(block_rows)
        data.loc[block_mask, "BlockStatus"] = status

    data = drop_dead_cols(data, cols2Drop)
    data = parse_2D_coords(data, cols_2D)
    data = parse_3D_coords(data, cols_3D)
    data = parse_rotation(data, cols_rotation)
    return data


def check_monotonic_apptime(
    df: pd.DataFrame,
    *,
    col: str = "AppTime",
    context: str = "",
    allow_equal: bool = True,
    logger: Optional[Any] = None,
    max_examples: int = 10,) -> bool:
    """
    Warning-only sanity check: verifies AppTime is non-decreasing (or strictly increasing).
    Does NOT reorder, drop, or modify your dataframe.

    Returns True if monotonic (per allow_equal), else False.

    - allow_equal=True: checks dt >= 0
    - allow_equal=False: checks dt > 0

    Logs warnings to `logger.log(...)` if provided, else prints.
    """

    def _log(msg: str) -> None:
        if logger is not None and hasattr(logger, "log"):
            logger.log(msg)
        else:
            print(msg)

    if col not in df.columns:
        _log(f"⚠️ AppTime monotonic check skipped: column '{col}' not found. {context}".strip())
        return True

    s = pd.to_numeric(df[col], errors="coerce")

    n_total = len(s)
    n_valid = int(s.notna().sum())
    if n_valid <= 1:
        _log(f"⚠️ AppTime monotonic check skipped: not enough valid '{col}' values "
             f"({n_valid}/{n_total}). {context}".strip())
        return True

    dt = s.diff()
    bad_mask = dt < 0 if allow_equal else dt <= 0
    bad_mask = bad_mask.fillna(False)

    n_bad = int(bad_mask.sum())
    if n_bad == 0:
        return True

    # summarize
    dt_bad = dt[bad_mask]
    min_dt = float(dt_bad.min())
    mean_dt = float(dt_bad.mean())

    _log(
        f"⚠️ Non-monotonic {col} detected ({n_bad} violations out of {n_valid-1} diffs). "
        f"min_dt={min_dt:.6g}, mean_dt={mean_dt:.6g}. "
        f"{context}".strip()
    )

    # example rows
    bad_idxs = dt_bad.index[:max_examples].tolist()
    ex_rows = []
    for idx in bad_idxs:
        prev_idx = idx - 1 if isinstance(idx, int) else None
        # use iloc for safety if index is RangeIndex-like
        try:
            i = int(idx)
            prev_val = s.iloc[i - 1] if i - 1 >= 0 else pd.NA
            cur_val = s.iloc[i]
        except Exception:
            prev_val = pd.NA
            cur_val = pd.NA
        ex_rows.append((idx, prev_val, cur_val, dt.loc[idx]))

    ex_str = "\n".join(
        [f"  idx={i} prev={pv} cur={cv} dt={d}" for (i, pv, cv, d) in ex_rows]
    )
    _log("⚠️ Examples of violations:\n" + ex_str)

    return False

def drop_malformed_trailing_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop ONLY trailing malformed/truncated rows before saving *_processed.csv.

    Assumptions (per your pipeline):
      - columns 'Type', 'Message', 'EyeTarget' always exist.
      - Event rows should have EyeTarget (often "none") AND a non-empty Message.
      - RTdata rows should have EyeTarget AND (optionally) non-null HeadPosAnchored_*.
    """
    out = df.copy()

    required = ["Type", "Message", "EyeRight_y"]
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise ValueError(f"drop_malformed_trailing_rows: missing required columns: {missing}")

    pos_cols = ["HeadPosAnchored_x", "HeadPosAnchored_y", "HeadPosAnchored_z"]
    has_pos = all(c in out.columns for c in pos_cols)

    def _is_blank(v) -> bool:
        return pd.isna(v) or str(v).strip() == ""

    while len(out):
        last = out.iloc[-1]

        # missing/blank Type => definitely truncated
        if _is_blank(last["Type"]):
            out = out.iloc[:-1]
            continue

        # # EyeTarget must exist & be non-blank for ALL row types ("none" is OK)
        # if _is_blank(last["EyeTarget"]):
        #     out = out.iloc[:-1]
        #     continue

        t = str(last["Type"]).strip().lower()

        if t == "event":
            # Event rows must have Message
            if _is_blank(last["Message"]):
                out = out.iloc[:-1]
                continue
            break
        elif t == 'rtdata':
            # RT data rows must have EyeRight_y values
            if _is_blank(last["EyeRight_y"]):
                out = out.iloc[:-1]
                continue
            break

        # Unknown Type: be conservative and stop (don’t drop)
        break

    return out
