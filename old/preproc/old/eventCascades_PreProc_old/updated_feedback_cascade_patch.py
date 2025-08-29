
def process_feedback_collect(df):
    events = []
    cached_idvCoinID = {}

    for row in df.itertuples():
        # Cache individual coin ID
        if isinstance(row.Message, str) and row.Message.startswith("Collected pin feedback coin:"):
            id_str = row.Message.replace("Collected pin feedback coin: ", "").strip()
            if id_str.isdigit():
                cached_idvCoinID[(row.BlockNum, row.RoundNum, row.CoinSetID)] = int(id_str)

        # Main Feedback Coin Collect parser
        elif isinstance(row.Message, str) and row.Message.startswith("Collected feedback coin:"):
            msg_body = row.Message.replace("Collected feedback coin:", "").replace(" round reward", "")
            parts = msg_body.split(":")
            if len(parts) == 2:
                try:
                    value_earned = float(parts[0].strip())
                    round_total = float(parts[1].strip())
                    idv_id = cached_idvCoinID.get((row.BlockNum, row.RoundNum, row.CoinSetID))
                    coin_type = classify_coin_type(row.CoinSetID, idv_id) if idv_id is not None else "Unknown"

                    # Attempt to attach to most recent PinDrop
                    cascade_id = None
                    for e in reversed(events):
                        if e["event_type"] == "PinDrop" and all(
                            e.get(k) == getattr(row, k, None) for k in ("BlockNum", "RoundNum", "CoinSetID")
                        ):
                            cascade_id = e["cascade_id"]
                            break

                    details = {
                        "valueEarned": value_earned,
                        "runningRoundTotal": round_total,
                        "idvCoinID": idv_id,
                        "CoinType": coin_type
                    }

                    event = {
                        "AppTime": row.AppTime,
                        "Timestamp": row.Timestamp,
                        "cascade_id": cascade_id,
                        "event_type": "Feedback_CoinCollect",
                        "details": details,
                        "source": "logged",
                        "original_row": row.Index,
                        "BlockNum": getattr(row, "BlockNum", None),
                        "RoundNum": getattr(row, "RoundNum", None),
                        "CoinSetID": getattr(row, "CoinSetID", None)
                    }

                    events.append(event)

                except ValueError:
                    print(f"⚠️ Malformed numeric data in Feedback Coin at row {row.Index}: {row.Message}")
                    continue

    return events
