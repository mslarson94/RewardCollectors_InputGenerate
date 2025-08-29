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




def process_swap_votes_v3(df, allowed_statuses):
    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")

        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row["Type"] == "Event" and isinstance(row["Message"], str) and row.Message.startswith("Active Navigator says it was a "):
            swapvote = row.Message.replace("Active Navigator says it was a ", "").strip().upper()
            try:
                start_time = row["AppTime"]
                timestamp = row["Timestamp"]
                common_info = build_common_event_fields(row, i)

                events.append({
                    "AppTime": start_time,
                    "Timestamp": timestamp,
                    "lo_eventType": "SwapVoteMoment",
                    "med_eventType": "SwapVote",
                    "hi_eventType": "SwapVote",
                    "details": {"SwapVote": swapvote},
                    "source": "logged",
                    **common_info
                })

                offsets_events = [
                    (0.000, "SwapVoteText_end"),
                    (0.000, "BlockScoreText_start"),
                    (2.000, "BlockScoreText_start")
                ]
                event_meta = {
                    "med_eventType": "FullPostSwapVoteEvents",
                    "hi_eventType": "SwapVote"
                }
                synthetic = generate_synthetic_events(start_time, timestamp, offsets_events, common_info, event_meta)
                events.extend(synthetic)

            except Exception as e:
                print(f"⚠️ Failed to process swap vote at row {i}: {e}")

    return events

def process_block_periods_v2(df, allowed_statuses):
    events = []
    df = df.reset_index(drop=True)

    round_event_map = {
        0: "PreBlock_CylinderWalk",
        7777: "InterRound_CylinderWalk",
        8888: "InterRound_PostCylinderWalk",
        9999: "InterBlock_Idle"
    }

    previous_round = None

    for idx, row in df.iterrows():
        round_code = row.get("RoundNum")

        if round_code in round_event_map and round_code != previous_round:
            common_info = build_common_event_fields(row, idx)
            synthetic = generate_synthetic_events(
                row.AppTime,
                row.Timestamp,
                [(0.0, f"{round_event_map[round_code]}_start")],
                common_info,
                {
                    "med_eventType": "NonRewardDrivenNavigation",
                    "hi_eventType": "WalkingPeriod"
                }
            )
            events.extend(synthetic)

        previous_round = round_code

def process_marks(df, allowed_statuses):
    events = []
    for idx, row in df.iterrows():
        if isinstance(row.Message, str) and "Sending Headset mark" in row.Message:
            common_info = build_common_event_fields(row, idx)
            events.append({
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "lo_eventType": "Mark",  # Aligning naming convention with other event types
                "med_eventType": "SystemSignal",
                "hi_eventType": "Infrastructure",
                "details": {"mark": "A"},
                "source": "logged",
                **common_info
            })
    return events


    return events
