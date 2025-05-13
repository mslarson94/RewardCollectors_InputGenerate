import pandas as pd
import re
import os
import json
from datetime import datetime, timedelta
from io import StringIO


# --- Load Metadata ---
collated_path = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
MAGIC_LEAP_METADATA = pd.read_excel(collated_path, sheet_name='MagicLeapFiles')
MAGIC_LEAP_METADATA = MAGIC_LEAP_METADATA.dropna(subset=['cleanedFile'])
MAGIC_LEAP_METADATA = MAGIC_LEAP_METADATA[MAGIC_LEAP_METADATA['currentRole'] == 'PO']
MAGIC_LEAP_METADATA = MAGIC_LEAP_METADATA.rename(columns={"cleanedFile": "source_file"})

# --- Utilities ---

SKIP_OFFSET = 6  # we have to skip the first 5 rows after the header row, so we need to declare this global variable so we can use it when later reporting an original row number  
ALLOWED_STATUSES = {"complete"}
def flatten_event(event, source_file):
    flat = event.copy()
    flat["source_file"] = source_file
    if isinstance(flat.get("details"), dict):
        for k, v in flat["details"].items():
            flat[f"details_{k}"] = v
        flat.pop("details", None)
    return flat

def enrich_with_metadata(events, source_file, metadata_df):
    matched = metadata_df[metadata_df["source_file"] == source_file]
    if matched.empty:
        return [flatten_event(event, source_file) for event in events]
    metadata = matched.iloc[0].to_dict()
    return [flatten_event(event, source_file) | metadata for event in events]

def save_event_summary(events, source_file, output_dir):
    df = pd.DataFrame([flatten_event(e, source_file) for e in events])
    df = df.sort_values(by=["AppTime", "Timestamp"])
    output_path = os.path.join(output_dir, os.path.splitext(source_file)[0] + "_events.csv")
    df.to_csv(output_path, index=False)
    return output_path

# --- Coin Type Classification ---
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

# --- Swap Vote Classification ---
def classify_swap_vote(CoinSetID, swapvote):
    if CoinSetID in [2, 3] and swapvote == "NEW":
        return "Correct"
    elif CoinSetID == 1 and swapvote == "OLD":
        return "Correct"
    elif CoinSetID == 1 and swapvote == "NEW":
        return "Incorrect"
    elif CoinSetID in [2, 3] and swapvote == "OLD":
        return "Incorrect"
    return "Unknown"

# --- Event Extraction Functions ---

def extract_walking_periods(df, cascade_events, allowed_statuses):
    seen_rounds = set()
    walking_periods = []
    df = df.sort_values("AppTime").reset_index(drop=True)

    pin_cascades = {
        round(e['AppTime'], 3): e['cascade_id']
        for e in cascade_events
        if e['event_type'] == "PinDrop" and 'cascade_id' in e
    }

    # Extract cylinder visibility intervals
    cylinder_windows = []
    for e in cascade_events:
        if e["event_type"] == "PreBlock_BlueCylinderVisible_start":
            current = {"start": e["AppTime"]}
        elif e["event_type"] == "PreBlock_BlueCylinderVisible_end" and "current" in locals():
            current["end"] = e["AppTime"]
            cylinder_windows.append(current)
            del current

    trigger_rows = []
    for i, row in df.iterrows():
        msg = row.Message if isinstance(row.Message, str) else ""
        round_key = (row.get("BlockNum"), row.get("RoundNum"), row.get("CoinSetID"))

        if round_key not in seen_rounds:
            seen_rounds.add(round_key)
            trigger_rows.append((i, "Round start"))
        elif isinstance(msg, str) and ("A.N. collected coin:" in msg or "Other participant just collected coin:" in msg):
            trigger_rows.append((i, "Post_coin_collect"))

    for idx, trigger_type in trigger_rows:
        row = df.iloc[idx]
        block = row.get("BlockNum")
        block_status = row.get("BlockStatus", "unknown")

        # Skip if not an allowed block
        if pd.notna(block) and block_status not in allowed_statuses:
            continue

        start_time = row["AppTime"]
        timestamp = row["Timestamp"]
        roundnum = row.get("RoundNum")
        coinset = row.get("CoinSetID")

        walk_type = "WithinRoundStanding"
        if roundnum == 9999:
            walk_type = "InterBlockIdle"
        elif roundnum == 0:
            walk_type = "PreBlock_CylinderWalk"
        elif roundnum == 7777:
            walk_type = "InterRound_CylinderWalk"
        elif roundnum == 8888:
            walk_type = "InterRound_PostCylinderWalk"

        end_time = None
        cascade_id = None
        for j in range(idx + 1, len(df)):
            msg = df.at[j, "Message"] if "Message" in df.columns else None
            if isinstance(msg, str) and (
                "Other participant just dropped a new pin at" in msg or "Chest opened" in msg
            ):
                ##update later with chest opened string
                end_time = df.at[j, "AppTime"]
                cascade_id = pin_cascades.get(round(end_time, 3))
                break

        if end_time is not None:
            walking_periods.append({
                "AppTime": start_time,
                "Timestamp": timestamp,
                "event_type": "WalkingPeriod",
                "start_AppTime": start_time,
                "end_AppTime": end_time,
                "duration": end_time - start_time,
                "cascade_id": cascade_id,
                "BlockNum": block,
                "RoundNum": roundnum,
                "CoinSetID": coinset,
                "BlockStatus": block_status,  # include this for metadata tracing
                "details": {"trigger": trigger_type, "walk_type": walk_type},
                "source": "synthetic",
                "original_row_start": idx + SKIP_OFFSET,
                "original_row_end": j - 1
            })

    return walking_periods

def process_pin_drop(df, allowed_statuses):
    events = []
    cascade_id = 0
    i = 0

    while i < len(df):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        # ✅ Skip if block is not marked complete
        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            i += 1
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and "Other participant just dropped a new pin at" in row["Message"]:
            cascade_id += 1
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            common_info = {
                "BlockNum": row.get("BlockNum", None),
                "RoundNum": row.get("RoundNum", None),
                "CoinSetID": row.get("CoinSetID", None),
                "BlockStatus": block_status,
                "original_row": i + SKIP_OFFSET
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

            j = i + 1
            while j < len(df):
                next_row = df.iloc[j]
                if next_row["Type"] != "Event" or not isinstance(next_row["Message"], str):
                    break
                msg = next_row["Message"]

                if "Other participant just dropped a new pin at " in msg:
                    match = re.search(
                        r'at ([\d\.\-]+) ([\d\.\-]+) ([\d\.\-]+)', msg
                    )
                    if match:
                        try:
                            event["details"].update({
                                "pin_local_x": float(match.group(1)),
                                "pin_local_y": float(match.group(2)),
                                "pin_local_z": float(match.group(3)),
                            })
                        except ValueError:
                            print(f"⚠️ Float conversion failed in pin location at row {j}: {msg}")
                    else:
                        print(f"⚠️ Regex mismatch for pin location at row {j}: {msg}")


                elif "Dropped pin was dropped at " in msg:
                    match = re.search(
                        r'Dropped pin was dropped at ([\d\.]+) from chest (\d+) originally at \(([-\d\.]+), ([-\d\.]+), ([-\d\.]+)\):(INCORRECT|CORRECT)', msg
                    )
                    if match:
                        try:
                            event["details"].update({
                                "pinDist": match.group(1),
                                "coinIdvID": match.group(2),
                                "coin_IdvPosition": (match.group(3), match.group(4), match.group(5)),
                                "pinDropQuality": match.group(6)
                            })
                        except ValueError:
                            print(f"⚠️ Drop analysis parsing error at row {j}: {msg}")
                    else:
                        print(f"⚠️ Regex mismatch in drop analysis at row {j}: {msg}")

            events.append(event)

            # Add synthetic events
            for offset, evt in [
                (0.000, "PinDrop_Sound_start"),
                (0.000, "GrayPin_Visible_start"),
                (0.000, "textInstr_Visible_start"),
                (0.000, "voteWindow_start"),
                (0.654, "PinDrop_Sound_end"),
                (1.000, "textInstr_Visible_end"),
                (2.000, "GrayPin_Visible_end"),
                (2.000, "voteWindow_end"),
                (2.000, "Feedback_Sound_start"),
                (2.000, "Feedback_textNcolor_Visible_start"),
                (3.000, "Feedback_textNcolor_Visible_end"),
                (3.000, "Coin_Visible_start"),
            ]:
                try:
                    synthetic_time = datetime.strptime(timestamp, '%H:%M:%S:%f') + timedelta(seconds=offset)
                    events.append({
                        "AppTime": start_time + offset,
                        "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                        "cascade_id": cascade_id,
                        "event_type": evt,
                        "details": {},
                        "source": "synthetic",
                        **{k: event[k] for k in ("BlockNum", "RoundNum", "CoinSetID", "BlockStatus")}
                    })
                except Exception as e:
                    print(f"⚠️ Failed to create synthetic event {evt} at row {i}: {e}")

            i = j
        else:
            i += 1
    return events

def process_pin_drop_vote(df, allowed_statuses):
    events = []
    cascade_id = 0
    i = 0

    while i < len(df):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        # ✅ Skip if block is not marked complete
        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            i += 1
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and "Observer chose " in row["Message"]:
            cascade_id += 1
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            common_info = {
                "BlockNum": row.get("BlockNum", None),
                "RoundNum": row.get("RoundNum", None),
                "CoinSetID": row.get("CoinSetID", None),
                "BlockStatus": block_status,
                "original_row": i + SKIP_OFFSET
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

            j = i + 1
            while j < len(df):
                next_row = df.iloc[j]
                if next_row["Type"] != "Event" or not isinstance(next_row["Message"], str):
                    break
                msg = next_row["Message"]

                if "Observer chose " in msg:
                    match = re.search(r'Observer chose (CORRECT|INCORRECT)', msg)
                    if match:
                        try:
                            event["details"].update({
                                "pinDropVote": match.group(1)
                            })
                        except ValueError:
                            print(f"⚠️ Score part conversion failed at row {j}: {msg}")
                    else:
                        print(f"⚠️ Unexpected parts format in score line at row {j}: {msg}")

                j += 1

            events.append(event)

            # Add synthetic events
            for offset, evt in [
                (0.000, "textInstr_Visible_end"),
                (0.000, "textSubmission_start"),
                (1.000, "textSubmission_start"),
            ]:
                try:
                    synthetic_time = datetime.strptime(timestamp, '%H:%M:%S:%f') + timedelta(seconds=offset)
                    events.append({
                        "AppTime": start_time + offset,
                        "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                        "cascade_id": cascade_id,
                        "event_type": evt,
                        "details": {},
                        "source": "synthetic",
                        **{k: event[k] for k in ("BlockNum", "RoundNum", "CoinSetID", "BlockStatus")}
                    })
                except Exception as e:
                    print(f"⚠️ Failed to create synthetic event {evt} at row {i}: {e}")

            i = j
        else:
            i += 1

    return events

def process_feedback_collect(df, allowed_statuses):
    events = []
    cached_idvCoinID = {}

    for row in df.itertuples():
        block_status = getattr(row, "BlockStatus", "unknown")
        if pd.notna(row.BlockNum) and block_status not in allowed_statuses:
            continue

        msg = row.Message
        if not isinstance(msg, str):
            continue

        # Full feedback with reward
        if "round reward" in msg and msg.startswith("A.N. collected coin:"):
            msg_body = msg.replace("A.N. collected coin:", "").replace(" round reward:", "")
            parts = msg_body.split(":")
            if len(parts) == 2:
                try:
                    id_str = parts[0].strip()
                    round_total = float(parts[1].strip())
                    idv_id = int(id_str) if id_str.isdigit() else None

                    # Cache ID in case needed
                    cached_idvCoinID[(row.BlockNum, row.RoundNum, row.CoinSetID)] = idv_id

                    coin_type = classify_coin_type(row.CoinSetID, idv_id) if idv_id is not None else "Unknown"

                    # Try to link to latest PinDrop
                    cascade_id = None
                    for e in reversed(events):
                        if e["event_type"] == "PinDrop" and all(
                            e.get(k) == getattr(row, k, None) for k in ("BlockNum", "RoundNum", "CoinSetID")
                        ):
                            cascade_id = e["cascade_id"]
                            break

                    event = {
                        "AppTime": row.AppTime,
                        "Timestamp": row.Timestamp,
                        "cascade_id": cascade_id,
                        "event_type": "Feedback_CoinCollect",
                        "details": {
                            "runningRoundTotal": round_total,
                            "idvCoinID": idv_id,
                            "CoinType": coin_type
                        },
                        "source": "logged",
                        "original_row": row.Index + SKIP_OFFSET,
                        "BlockNum": getattr(row, "BlockNum", None),
                        "RoundNum": getattr(row, "RoundNum", None),
                        "CoinSetID": getattr(row, "CoinSetID", None)
                    }

                    events.append(event)

                except ValueError:
                    print(f"⚠️ Malformed numeric data in Feedback Coin at row {row.Index}: {msg}")

        # Cache-only case
        elif msg.startswith("A.N. collected coin:") and "round reward" not in msg:
            id_str = msg.replace("A.N. collected coin:", "").strip()
            if id_str.isdigit():
                cached_idvCoinID[(row.BlockNum, row.RoundNum, row.CoinSetID)] = int(id_str)

    return events

def process_ie_events(df, allowed_statuses):
    events = []
    for row in df.itertuples():
        block_status = getattr(row, "BlockStatus", "unknown")
        if pd.notna(row.BlockNum) and block_status not in allowed_statuses:
            continue
        ## This currently isn't supported for PO files
        if isinstance(row.Message, str) and "Chest opened:" in row.Message:
            parts = row.Message.replace("Chest opened: ", "")
            details = {"idvCoinID": int(parts.strip())}

            event = {
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "cascade_id": None,
                "event_type": "IE_ChestOpen",
                "details": details,
                "source": "logged",
                "original_row": row.Index + SKIP_OFFSET,
                "BlockNum": getattr(row, "BlockNum", None),
                "RoundNum": getattr(row, "RoundNum", None),
                "CoinSetID": getattr(row, "CoinSetID", None)
            }

            events.append(event)

            cascade_id = None
            for e in reversed(events):
                if e["event_type"] == "IE_ChestOpen" and all(
                    e.get(k) == event.get(k) for k in ("BlockNum", "RoundNum", "CoinSetID")
                ):
                    cascade_id = e["cascade_id"]
                    break

            start_time = row.AppTime
            timestamp = row.Timestamp

            for offset, evt in [
                (0.000, "ChestOpening_visNsound_start"),
                (0.400, "ChestOpening_visNsound_end"),
                (0.400, "ChestOpen_and_empty_start"),
                (2.000, "ChestOpen_and_empty_end"),
                (2.000, "Coin_Visible_start"),
                (2.000, "Coin_soundPres_start"),
                (2.650, "Coin_soundPres_end"),
                (3.000, "Coin_Released")
            ]:
                synthetic_time = datetime.strptime(timestamp, '%H:%M:%S:%f') + timedelta(seconds=offset)
                events.append({
                    "AppTime": start_time + offset,
                    "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                    "cascade_id": cascade_id,
                    "event_type": evt,
                    "details": {},
                    "source": "synthetic",
                    "BlockNum": getattr(row, "BlockNum", None),
                    "RoundNum": getattr(row, "RoundNum", None),
                    "CoinSetID": getattr(row, "CoinSetID", None)
                })

        elif isinstance(row.Message, str) and "Other participant just collected coin:" in row.Message:
            parts = row.Message.split("Other participant just collected coin:")
            if len(parts) == 2 and parts[1].strip().isdigit():
                details = {"idvCoinID": int(parts[1].strip())}

                event = {
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "cascade_id": None,
                    "event_type": "IE_CoinCollected",
                    "details": details,
                    "source": "logged",
                    "original_row": row.Index,
                    "BlockNum": getattr(row, "BlockNum", None),
                    "RoundNum": getattr(row, "RoundNum", None),
                    "CoinSetID": getattr(row, "CoinSetID", None)
                }

                events.append(event)

                cascade_id = None
                for e in reversed(events):
                    if e["event_type"] == "IE_ChestOpen" and all(
                        e.get(k) == event.get(k) for k in ("BlockNum", "RoundNum", "CoinSetID")
                    ):
                        cascade_id = e["cascade_id"]
                        break

                start_time = row.AppTime
                timestamp = row.Timestamp

                for offset, evt in [
                    (0.000, "Coin_Visible_end"),
                    (0.000, "Coin_Sound_start"),
                    (0.650, "Coin_Sound_end"),
                    (2.000, "Coin_Released")
                ]:
                    synthetic_time = datetime.strptime(timestamp, '%H:%M:%S:%f') + timedelta(seconds=offset)
                    events.append({
                        "AppTime": start_time + offset,
                        "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                        "cascade_id": cascade_id,
                        "event_type": evt,
                        "details": {},
                        "source": "synthetic",
                        "BlockNum": getattr(row, "BlockNum", None),
                        "RoundNum": getattr(row, "RoundNum", None),
                        "CoinSetID": getattr(row, "CoinSetID", None)
                    })
            else:
                print(f"⚠️ Malformed coin collected message at row {row.Index}: {row.Message}")
                continue

    return events

def process_marks(df, allowed_statuses):
    events = []
    for row in df.itertuples():
        if isinstance(row.Message, str) and "Sending Headset mark" in row.Message:
            events.append({
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "cascade_id": None,
                "event_type": "Mark",
                "details": {"mark": "A"},
                "source": "logged",
                "original_row": row.Index + SKIP_OFFSET,
                "BlockNum": getattr(row, "BlockNum", None),
                "RoundNum": getattr(row, "RoundNum", None),
                "CoinSetID": getattr(row, "CoinSetID", None)
            })
    return events

def process_swap_votes(df):
    events = []
    for row in df.itertuples():
        if isinstance(row.Message, str) and row.Message.startswith("Observer says it was a "):
            swapvote = row.Message.replace("Observer says it was a ", "").strip().upper()
            score = classify_swap_vote(row.CoinSetID, swapvote)

            events.append({
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "cascade_id": None,
                "event_type": "SwapVote",
                "details": {
                    "SwapVote": swapvote,
                    "SwapVoteScore": score
                },
                "source": "logged",
                "original_row": row.Index + SKIP_OFFSET,
                "BlockNum": getattr(row, "BlockNum", None),
                "RoundNum": getattr(row, "RoundNum", None),
                "CoinSetID": getattr(row, "CoinSetID", None)
            })
    return events

def process_block_periods(df):
    events = []
    df = df.reset_index(drop=True)

    # Mapping of round codes to event cascade tags
    round_event_map = {
        0: "PreBlock_CylinderWalk",
        7777: "InterRound_CylinderWalk",
        8888: "InterRound_PostCylinderWalk",
        9999: "InterBlock_Idle"
    }

    previous_round = None
    start_idx = None

    for idx, row in df.iterrows():
        round_code = row.get("RoundNum")
        msg = row.Message if isinstance(row.Message, str) else ""

        # Track and emit events for round-based mini-cascades
        if round_code in round_event_map:
            if round_code != previous_round:
                # End previous temporal cascade
                if previous_round in round_event_map and start_idx is not None:
                    events.append({
                        "event_type": f"{round_event_map[previous_round]}_end",
                        "AppTime": df.at[idx - 1, "AppTime"],
                        "Timestamp": df.at[idx - 1, "Timestamp"],
                        "details": {},
                        "source": "synthetic",
                        "original_row": idx - 1 + SKIP_OFFSET,
                        "BlockNum": df.at[idx - 1, "BlockNum"],
                        "RoundNum": df.at[idx - 1, "RoundNum"],
                        "CoinSetID": df.at[idx - 1, "CoinSetID"]
                    })
                # Start new one
                start_idx = idx
                events.append({
                    "event_type": f"{round_event_map[round_code]}_start",
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "details": {},
                    "source": "synthetic",
                    "original_row": idx + SKIP_OFFSET,
                    "BlockNum": row.BlockNum,
                    "RoundNum": row.RoundNum,
                    "CoinSetID": row.CoinSetID
                })

        # End the final cascade if we're at the last row
        if idx == len(df) - 1 and round_code in round_event_map and start_idx is not None:
            events.append({
                "event_type": f"{round_event_map[round_code]}_end",
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "details": {},
                "source": "synthetic",
                "original_row": idx + SKIP_OFFSET,
                "BlockNum": row.BlockNum,
                "RoundNum": row.RoundNum,
                "CoinSetID": row.CoinSetID
            })

        previous_round = round_code

        # Original static structural events
        shared_fields = {
            "AppTime": row.AppTime,
            "Timestamp": row.Timestamp,
            "source": "synthetic",
            "original_row": idx + SKIP_OFFSET,
            "BlockNum": row.BlockNum,
            "RoundNum": row.RoundNum,
            "CoinSetID": row.CoinSetID
        }

        if msg == "Mark should happen if checked on terminal.":
            events.append({
                "event_type": "PreBlock_BlueCylinderVisible_start",
                "details": {},
                **shared_fields
            })

        elif msg == "Repositioned and ready to start block or round":
            events.extend([
                {
                    "event_type": "PreBlock_BlueCylinderVisible_end",
                    "details": {},
                    **shared_fields
                },
                {
                    "event_type": "StartRoundText_visible_start",
                    "details": {},
                    **shared_fields
                }
            ])

        elif msg.startswith("Started"):
            events.extend([
                {
                    "event_type": "StartRoundText_visible_end",
                    "details": {},
                    **shared_fields
                },
                {
                    "event_type": "RoundInstructionText_visible_start",
                    "details": {},
                    **shared_fields
                },
                {
                    "event_type": "RoundInstructionText_visible_end",
                    "AppTime": row.AppTime + 2.0,
                    "Timestamp": row.Timestamp,
                    "details": {},
                    "source": "synthetic",
                    "original_row": idx + SKIP_OFFSET,
                    "BlockNum": row.BlockNum,
                    "RoundNum": row.RoundNum,
                    "CoinSetID": row.CoinSetID
                }
            ])

    return events


def build_timeline_from_processed(file_path, output_path):
    df = pd.read_csv(file_path, skiprows=range(1, 7))
    all_events = (
        process_pin_drop(df) +
        process_pin_drop_vote(df) +
        process_feedback_collect(df) +  # not yet status-filtered
        process_ie_events(df)           # not yet status-filtered
    )
    timeline_df = pd.DataFrame(all_events).sort_values(by="AppTime")
    timeline_df.to_csv(output_path, index=False)

# --- Main Processor ---

def process_all_obsreward_files(root_dir, output_dir="EventCascades"):
    pattern = re.compile(r"ObsReward_B_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_processed\.csv$")
    summary_rows = []
    os.makedirs(output_dir, exist_ok=True)

    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if pattern.match(fname):
                full_path = os.path.join(dirpath, fname)
                try:
                    df = pd.read_csv(full_path, skiprows=range(1, 7))

                    cascades = (
                        process_pin_drop(df) +
                        process_pin_drop_vote(df) +
                        process_feedback_collect(df) +
                        process_ie_events(df) +
                        process_marks(df) + 
                        process_swap_votes(df) +
                        process_block_periods(df)
                    )
                    valid_blocks = df[df['BlockStatus'] == 'complete']['BlockNum'].dropna().unique()
                    walking_periods = extract_walking_periods(df, cascades)

                    all_events = cascades + walking_periods

                    # Save individual summary
                    save_event_summary(all_events, fname, dirpath)

                    # Enrich for final summary
                    enriched = enrich_with_metadata(all_events, fname, MAGIC_LEAP_METADATA)
                    summary_rows.extend(enriched)

                    print(f"✓ Processed: {fname}")

                except Exception as e:
                    print(f"✗ Failed to process {fname}: {e}")

    if summary_rows:
        summary_df = pd.DataFrame(summary_rows).sort_values(by=["AppTime", "Timestamp"])
        summary_df.to_csv(os.path.join(output_dir, "event_summary.csv"), index=False)
        print(f"\n📄 Summary saved to: {os.path.join(output_dir, 'event_summary.csv')}")
    else:
        print("⚠️ No events found to summarize.")

# --- Execute ---

# # Execution block
# root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData"
# output_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/ProcessedData"

# Single Test File 
root_directory = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile/ProcessedData"
output_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile/Summary"


process_all_obsreward_files(root_directory, output_dir)