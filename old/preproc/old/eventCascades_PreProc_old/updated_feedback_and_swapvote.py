
def process_feedback_collect(df):
    events = []
    cached_idvCoinID = {}

    for row in df.itertuples():
        if isinstance(row.Message, str) and row.Message.startswith("Collected pin feedback coin:"):
            id_str = row.Message.replace("Collected pin feedback coin: ", "").strip()
            if id_str.isdigit():
                cached_idvCoinID[(row.BlockNum, row.RoundNum, row.CoinSetID)] = int(id_str)

        elif isinstance(row.Message, str) and row.Message.startswith("Collected feedback coin:"):
            msg_body = row.Message.replace("Collected feedback coin:", "").replace(" round reward", "")
            parts = msg_body.split(":")
            if len(parts) == 2:
                try:
                    value_earned = float(parts[0].strip())
                    round_total = float(parts[1].strip())
                    idv_id = cached_idvCoinID.get((row.BlockNum, row.RoundNum, row.CoinSetID))
                    coin_type = classify_coin_type(row.CoinSetID, idv_id) if idv_id is not None else "Unknown"

                    details = {
                        "valueEarned": value_earned,
                        "runningRoundTotal": round_total,
                        "idvCoinID": idv_id,
                        "CoinType": coin_type
                    }

                    event = {
                        "AppTime": row.AppTime,
                        "Timestamp": row.Timestamp,
                        "cascade_id": None,
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


def process_swap_votes(df):
    events = []
    for row in df.itertuples():
        if isinstance(row.Message, str) and row.Message.startswith("Active Navigator says it was a "):
            swapvote = row.Message.replace("Active Navigator says it was a ", "").strip().upper()
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
                "original_row": row.Index,
                "BlockNum": getattr(row, "BlockNum", None),
                "RoundNum": getattr(row, "RoundNum", None),
                "CoinSetID": getattr(row, "CoinSetID", None)
            })
    return events
