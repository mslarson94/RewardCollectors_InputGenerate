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
