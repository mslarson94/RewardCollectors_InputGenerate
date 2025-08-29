# Utility functions for cascade ID assignment and walking period extraction

def find_cascade_windows_from_events(events):
    """
    Identifies the time windows of pinDrop or chestOpen cascades based on event sequences.
    """
    windows = []
    cascade_id = 0
    start_time, group_type = None, None

    for event in sorted(events, key=lambda e: e["AppTime"]):
        if event["event_type"] in {"PinDrop", "ChestOpen"}:
            cascade_id += 1
            start_time = event["AppTime"]
            group_type = "pinDrop" if event["event_type"] == "PinDrop" else "chestOpen"
        elif event["event_type"] in {"Feedback_textNcolor_Visible_end", "Coin_Collect"} and start_time is not None:
            windows.append({
                "cascade_id": cascade_id,
                "group_type": group_type,
                "start": start_time,
                "end": event["AppTime"]
            })
            start_time = None
            group_type = None

    return windows

def assign_cascade_id(event, cascade_windows, debug=False):
    """
    Given an event, assigns it the correct cascade_id if it falls within a known cascade window.
    Optionally logs unmatched events if `debug` is True.
    """
    time = event.get("AppTime")
    for window in cascade_windows:
        if window["start"] <= time <= window["end"]:
            return window["cascade_id"]
    
    if debug:
        print(f"⚠️ No cascade match for event at AppTime {time} — type: {event.get('event_type')}")
    return None

def extract_walking_periods_with_cascade_ids(df, cascade_windows):
    """
    Creates walking periods from log data and assigns cascade_id based on timing relative to cascade windows.
    """
    walking_periods = []
    seen_rounds = set()
    df = df.sort_values("AppTime").reset_index(drop=True)

    for i, row in df.iterrows():
        msg = row.Message if isinstance(row.Message, str) else ""
        round_key = (row.get("BlockNum"), row.get("RoundNum"), row.get("CoinSetID"))

        if round_key not in seen_rounds:
            seen_rounds.add(round_key)
            trigger_type = "Round start"
        elif "collected" in msg.lower():
            trigger_type = "Post_coin_collect"
        else:
            continue

        start_time = row["AppTime"]
        end_time, matched_cascade_id = None, None

        for j in range(i + 1, len(df)):
            msg_j = df.at[j, "Message"] if "Message" in df.columns else ""
            if any(term in msg_j.lower() for term in ["dropped a pin", "chest opened", "coin collected"]):
                end_time = df.at[j, "AppTime"]
                break

        if end_time:
            matched_cascade_id = assign_cascade_id({"AppTime": end_time}, cascade_windows, debug=True)
            walking_periods.append({
                "AppTime": start_time,
                "Timestamp": row["Timestamp"],
                "event_type": "WalkingPeriod",
                "start_AppTime": start_time,
                "end_AppTime": end_time,
                "duration": end_time - start_time,
                "cascade_id": matched_cascade_id,
                "BlockNum": row.get("BlockNum"),
                "RoundNum": row.get("RoundNum"),
                "CoinSetID": row.get("CoinSetID"),
                "BlockStatus": row.get("BlockStatus"),
                "details": {"trigger": trigger_type},
                "source": "synthetic",
                "original_row_start": row.get("original_index", i),
                "original_row_end": j - 1
            })

    return walking_periods


def build_common_event_fields(row, index=None):
    """
    Constructs a standardized dictionary of shared event fields from a row.
    
    Parameters:
        row (pd.Series): A row from the dataframe representing an event.
        index (int or None): The row index for original_row_start/end. If None, infer from row name.
    
    Returns:
        dict: Common fields used across event definitions.
    """
    idx = index if index is not None else row.name
    return {
        "BlockNum": row.get("BlockNum", None),
        "RoundNum": row.get("RoundNum", None),
        "CoinSetID": row.get("CoinSetID", None),
        "BlockStatus": row.get("BlockStatus", "unknown"),
        "chestPin_num": row.get("chestPin_num", None),
        "original_row_start": row.get("original_index", idx),
        "original_row_end": row.get("original_index", idx),
        "cascade_id": None
    }

def generate_synthetic_events(base_time, timestamp_str, offsets_events, base_info, event_meta):
    """
    Generate synthetic events with specified time offsets.

    Parameters:
        base_time (float): Base AppTime from original event.
        timestamp_str (str): Timestamp string (e.g., '13:45:23:123').
        offsets_events (list of tuples): (offset_seconds, lo_eventType) pairs.
        base_info (dict): Shared event data (e.g., common_info).
        event_meta (dict): Keys like 'med_eventType', 'hi_eventType'.

    Returns:
        List[dict]: List of synthetic events.
    """
    synthetic_events = []
    try:
        base_timestamp = safe_parse_timestamp(timestamp_str)
        for offset, lo_evt in offsets_events:
            synthetic_time = base_timestamp + timedelta(seconds=offset)
            synthetic_events.append({
                "AppTime": base_time + offset,
                "Timestamp": synthetic_time.strftime('%H:%M:%S:%f'),
                "lo_eventType": lo_evt,
                "details": {},
                "source": "synthetic",
                **event_meta,
                **base_info
            })
    except Exception as e:
        print(f"⚠️ Failed to create synthetic event at {timestamp_str}: {e}")
    return synthetic_events


def safe_parse_timestamp(ts):
    try:
        return datetime.strptime(ts, '%H:%M:%S:%f')
    except Exception:
        return None


