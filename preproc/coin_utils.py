
# --- Utility: Coin and Swap Vote Classification ---
def classify_coin_type(CoinSetID, idvCoinID):
    if CoinSetID == 2 and idvCoinID == 2:
        return "PPE"
    elif CoinSetID == 3 and idvCoinID == 0:
        return "NPE"
    elif CoinSetID == 1 or (CoinSetID in [2, 3] and idvCoinID in [0, 1]):
        return "Normal"
    elif CoinSetID == 4 or (CoinSetID == 5 and idvCoinID == 1):
        return "TutorialNorm"
    elif CoinSetID == 5 and idvCoinID in [0, 2]:
        return "TutorialRPE"
    return "Unknown"

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

# --- Marks  ---
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
                "original_row_start": df.at[row.Index, "original_index"],
                "original_row_end": df.at[row.Index, "original_index"],
                "BlockNum": getattr(row, "BlockNum", None),
                "RoundNum": getattr(row, "RoundNum", None),
                "CoinSetID": getattr(row, "CoinSetID", None)
            })
    return events



### Might need to be Role coded
# --- Walking Periods ---
def process_block_periods(df, allowed_statuses):
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
                        "original_row_start": df.at[idx - 1, "original_index"],
                        "original_row_end": df.at[idx - 1, "original_index"],
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
                    "original_row_start": df.at[idx, "original_index"],
                    "original_row_end": df.at[idx, "original_index"],
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
                "original_row_start": df.at[idx, "original_index"],
                "original_row_end": df.at[idx, "original_index"],
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
            "original_row_start": df.at[idx, "original_index"],
            "original_row_end": df.at[idx, "original_index"],
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
                    "original_row_start": df.at[idx, "original_index"],
                    "original_row_end": df.at[idx, "original_index"],
                    "BlockNum": row.BlockNum,
                    "RoundNum": row.RoundNum,
                    "CoinSetID": row.CoinSetID
                }
            ])

    return events