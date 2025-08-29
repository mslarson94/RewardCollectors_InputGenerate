
def extract_walking_periods(df, cascade_events):
    seen_rounds = set()
    walking_periods = []
    df = df.sort_values("AppTime").reset_index(drop=True)

    pin_cascades = {
        round(e['AppTime'], 3): e['cascade_id']
        for e in cascade_events
        if e['event_type'] == "PinDrop" and 'cascade_id' in e
    }

    preblock_active = False
    interblock_active = False
    last_preblock_row = None
    last_interblock_row = None

    trigger_rows = []
    for i, row in df.iterrows():
        msg = row.Message if isinstance(row.Message, str) else ""
        round_key = (row.get("BlockNum"), row.get("RoundNum"), row.get("CoinSetID"))

        if msg == "Mark should happen if checked on terminal.":
            preblock_active = True
            last_preblock_row = i

        elif msg == "Repositioned and ready to start block or round":
            preblock_active = False
            last_preblock_row = None

        elif msg.startswith("Finished pindrop round:0"):
            interblock_active = True
            last_interblock_row = i

        elif "Started" in msg:
            interblock_active = False
            last_interblock_row = None

        elif round_key not in seen_rounds:
            seen_rounds.add(round_key)
            trigger_rows.append((i, "Round start"))
        elif isinstance(msg, str) and ("Collected feedback coin" in msg or "coin collected" in msg):
            trigger_rows.append((i, "Post_coin_collect"))

    for idx, trigger_type in trigger_rows:
        start_time = df.at[idx, "AppTime"]
        timestamp = df.at[idx, "Timestamp"]
        block = df.at[idx, "BlockNum"]
        roundnum = df.at[idx, "RoundNum"]
        coinset = df.at[idx, "CoinSetID"]

        walk_type = "GoalDirected"
        if preblock_active and roundnum == 0:
            walk_type = "PreBlockWalk"
        elif interblock_active:
            walk_type = "InterBlockWalk"

        end_time = None
        cascade_id = None
        for j in range(idx + 1, len(df)):
            msg = df.at[j, "Message"] if "Message" in df.columns else None
            if isinstance(msg, str) and (
                "Just dropped a pin" in msg or "Chest opened" in msg
            ):
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
                "details": {"trigger": trigger_type, "walk_type": walk_type},
                "source": "synthetic",
                "original_row_start": idx,
                "original_row_end": (j-1)
            })

    return walking_periods
