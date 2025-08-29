
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

# --- Walking Periods with AppTime/Timestamp Top-Level ---
def extract_walking_periods(df, cascade_events):
    seen_rounds = set()
    walking_periods = []
    df = df.sort_values("AppTime").reset_index(drop=True)

    pin_cascades = {
        round(e['AppTime'], 3): e['cascade_id']
        for e in cascade_events
        if e['event_type'] == "PinDrop" and 'cascade_id' in e
    }

    trigger_rows = []
    for i, row in df.iterrows():
        round_key = (row.get("BlockNum"), row.get("RoundNum"), row.get("CoinSetID"))
        if round_key not in seen_rounds:
            seen_rounds.add(round_key)
            trigger_rows.append((i, "Round start"))
        elif isinstance(row.get("Message"), str) and "Collected feedback coin" in row["Message"] or isinstance(row.get("Message"), str) and "coin collected" in row["Message"]:
            trigger_rows.append((i, "Post_coin_collect"))

    for idx, trigger_type in trigger_rows:
        start_time = df.at[idx, "AppTime"]
        timestamp = df.at[idx, "Timestamp"]
        block = df.at[idx, "BlockNum"]
        roundnum = df.at[idx, "RoundNum"]
        coinset = df.at[idx, "CoinSetID"]

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
                "details": {"trigger": trigger_type},
                "source": "synthetic",
                "original_row_start": idx,
                "original_row_end": (j-1)
            })

    return walking_periods
