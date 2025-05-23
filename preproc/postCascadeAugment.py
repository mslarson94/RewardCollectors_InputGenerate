import json
import re
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple

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

def extract_coin_registry(df: pd.DataFrame) -> Dict[int, List[Dict[str, float]]]:
    registry = {}
    current_set_id = None
    pattern = re.compile(r"coinpoint(\d+):.*?deltax:(-?\d+).*?deltay:(-?\d+).*?deltaz:(-?\d+)", re.DOTALL)

    for msg in df["Message"].dropna():
        if isinstance(msg, str):
            if msg.startswith("coinsetID:"):
                match = re.search(r"coinsetID:(\d+)", msg)
                if match:
                    current_set_id = int(match.group(1))
                    registry[current_set_id] = []
            elif msg.startswith("coinpoint") and current_set_id is not None:
                m = pattern.search(msg)
                if m:
                    _, dx, dy, dz = m.groups()
                    registry[current_set_id].append({
                        "deltax": float(dx),
                        "deltay": float(dy),
                        "deltaz": float(dz)
                    })

    return registry

def find_closest_coin(pindrop_event: Dict, coinpoints: List[Dict[str, float]]) -> Tuple[int, Dict[str, float]]:
    if "details" not in pindrop_event or "delta_position" not in pindrop_event["details"]:
        return -1, {}

    pdx, pdy, pdz = pindrop_event["details"]["delta_position"]
    distances = [
        np.sqrt((pdx - c["deltax"]) ** 2 + (pdy - c["deltay"]) ** 2 + (pdz - c["deltaz"]) ** 2)
        for c in coinpoints
    ]

    if not distances:
        return -1, {}

    min_idx = int(np.argmin(distances))
    return min_idx, coinpoints[min_idx]

def augment_events_with_coin_proximity(json_path: Path, csv_path: Path, output_path: Path):
    df = pd.read_csv(csv_path)
    events = json.loads(Path(json_path).read_text())
    registry = extract_coin_registry(df)
    ## Double check that you are specificing the coinset ID correctly 
    for evt in events:
        if evt.get("lo_eventType") == "PinDropped":
            coinset_id = evt.get("CoinSetID")
            if coinset_id in registry:
                idx, coin = find_closest_coin(evt, registry[coinset_id])
                coinType = classify_coin_type(idx, coinset_id)
                evt["details"]["idvCoinID"] = idx
                evt["details"]["coinType"] = coinType
                evt["details"]["drop_distance_manual"] = coin

    with open(output_path, "w") as f:
        json.dump(events, f, indent=2)

# Example usage
augment_events_with_coin_proximity(
    json_path=Path("example_processed_cascades.json"),
    csv_path=Path("example_processed.csv"),
    output_path=Path("example_augmented_cascades.json")
)