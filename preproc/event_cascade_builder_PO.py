
import pandas as pd
import re

def extract_pin_drop_votes(df):
    events = []
    cascade_id = 0
    pin_drop_dist = None
    good_drop = None

    for i, row in df.iterrows():
        msg = str(row.get("Message", ""))
        if "Dropped pin was dropped at" in msg:
            # Extract pinDropDist
            match = re.search(r'dropped at ([\d\.]+) from chest', msg)
            if match:
                pin_drop_dist = float(match.group(1))
            if "CORRECT" in msg:
                good_drop = "CORRECT"
            elif "INCORRECT" in msg:
                good_drop = "INCORRECT"

        if "Observer chose" in msg:
            cascade_id += 1
            povote = "did not vote"
            if "CORRECT" in msg:
                povote = "CORRECT"
            elif "INCORRECT" in msg:
                povote = "INCORRECT"

            score = None
            if good_drop and povote != "did not vote":
                score = "POisCorrect" if good_drop == povote else "POisIncorrect"
            elif povote == "did not vote":
                score = "POdidntVote"

            details = {
                "POVote": povote,
                "GoodDrop": good_drop,
                "Score": score,
                "pinDropDist": pin_drop_dist
            }
            events.append({
                "AppTime": row["AppTime"],
                "Timestamp": row["Timestamp"],
                "cascade_id": cascade_id,
                "event_type": "PO Pin Vote",
                "details": details,
                "source": "logged"
            })
            pin_drop_dist = None
            good_drop = None
    return events

def extract_chest_events(df):
    events = []
    for row in df.itertuples():
        msg = str(row.Message)
        if "Chest opened" in msg:
            match = re.search(r'Chest opened: (\d+)', msg)
            if match:
                events.append({
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "cascade_id": None,
                    "event_type": "IE Chest Open",
                    "details": {"ChestID": int(match.group(1))},
                    "source": "logged"
                })
        elif "coin collected" in msg:
            match = re.search(r'coin collected: (\d+)', msg)
            if match:
                events.append({
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "cascade_id": None,
                    "event_type": "IE Coin Collect",
                    "details": {"CoinPointID": int(match.group(1))},
                    "source": "logged"
                })
    return events

def build_timeline_from_processed(file_path, output_path):
    df = pd.read_csv(file_path)
    all_events = extract_pin_drop_votes(df) + extract_chest_events(df)
    timeline_df = pd.DataFrame(all_events).sort_values(by="AppTime")
    timeline_df.to_csv(output_path, index=False)
