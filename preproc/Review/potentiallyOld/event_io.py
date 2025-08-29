import pandas as pd
import json
import os

def save_event_summary(events, csv_path, json_path):
    df = pd.DataFrame(events).sort_values(by=["AppTime", "Timestamp"])
    df.to_csv(csv_path, index=False)
    with open(json_path, "w") as jf:
        for record in df.to_dict(orient="records"):
            jf.write(json.dumps(record) + "\n")
