
import pandas as pd
import json
import ast
import numpy as np

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

def parse_details(val):
    if pd.isna(val) or val == "":
        return {}
    if isinstance(val, dict):
        return val
    try:
        return ast.literal_eval(val)
    except Exception:
        return {}

def annotate_events_with_coin_info(events_path, meta_path, output_path):
    events_df = pd.read_csv(events_path)
    with open(meta_path, "r") as f:
        meta = json.load(f)

    coin_registry = meta["CoinRegistry"]
    flat_registry = {
        (int(cid), int(idv)): np.array([entry["deltax"], entry["deltay"], entry["deltaz"]])
        for cid, coins in coin_registry.items()
        for idv, entry in coins.items()
    }

    targets = events_df[
        events_df["lo_eventType"].isin(["PinDrop_Moment", "Feedback_CoinCollect", "CoinCollect_Moment_PinDrop"])
    ].copy()

    updates = []

    for idx, row in targets.iterrows():
        try:
            details = parse_details(row["details"])
            key = row["CoinSetID"]
            if pd.isna(key) or str(int(key)) not in coin_registry:
                continue
            csid = int(key)

            vec = None
            if "coinPos_x" in details and "coinPos_y" in details and "coinPos_z" in details:
                vec = np.array([details["coinPos_x"], details["coinPos_y"], details["coinPos_z"]])
            elif "deltax" in details and "deltay" in details and "deltaz" in details:
                vec = np.array([details["deltax"], details["deltay"], details["deltaz"]])
            else:
                continue

            min_dist, best_idv = float("inf"), None
            for (cs, idv), coord in flat_registry.items():
                if cs != csid:
                    continue
                dist = np.linalg.norm(coord - vec)
                if dist < min_dist:
                    min_dist = dist
                    best_idv = idv

            if best_idv is not None:
                details["idvCoinID"] = best_idv
                details["CoinType"] = classify_coin_type(csid, best_idv)
                updates.append((idx, details))
        except Exception:
            continue

    for idx, new_details in updates:
        events_df.at[idx, "details"] = str(new_details)

    events_df.to_csv(output_path, index=False)
