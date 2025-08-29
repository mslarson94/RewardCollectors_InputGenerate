# Re-extract CoinRegistry but using delta values instead of absolute (x, y, z)
def extract_coin_registry_deltas(data):
    """
    Extracts CoinRegistry using delta positions from coinpoint lines in a DataFrame.

    Returns:
        dict: CoinRegistry structure mapping CoinSetID -> coin index -> {deltax, deltay, deltaz}
    """
    coin_registry = defaultdict(dict)
    current_coinset_id = None

    for _, row in data.iterrows():
        message = str(row.get("Message", ""))
        coinset_match = re.search(r"coinsetID:(\d+)", message)
        if coinset_match:
            current_coinset_id = int(coinset_match.group(1))

        coinpoint_match = re.search(
            r"coinpoint(\d+):.*?deltax:([-+]?[0-9]*\.?[0-9]+)\s+deltay:([-+]?[0-9]*\.?[0-9]+)\s+deltaz:([-+]?[0-9]*\.?[0-9]+)",
            message
        )
        if coinpoint_match and current_coinset_id is not None:
            coin_index = int(coinpoint_match.group(1))
            dx = float(coinpoint_match.group(2))
            dy = float(coinpoint_match.group(3))
            dz = float(coinpoint_match.group(4))
            coin_registry[current_coinset_id][coin_index] = {"deltax": dx, "deltay": dy, "deltaz": dz}

    return dict(coin_registry)

# Extract and save the updated CoinRegistry with delta coordinates
coin_registry_deltas = extract_coin_registry_deltas(data_processed)
coin_registry_path_deltas = "/mnt/data/ObsReward_A_02_17_2025_15_11_coinregistry_deltas.json"
with open(coin_registry_path_deltas, "w") as f:
    json.dump(coin_registry_deltas, f, indent=2)

coin_registry_path_deltas



import pandas as pd
import json

# Load the metadata sheet
meta_path = "/mnt/data/collatedData.xlsx"
meta_df = pd.read_excel(meta_path, sheet_name="MagicLeapFiles")

# Normalize the filename for lookup
target_file = "obsreward_a_02_17_2025_15_11_processed.csv"
meta_df["cleanedFile"] = meta_df["cleanedFile"].astype(str).str.strip().str.lower()

# Extract the metadata row
matched_meta = meta_df[meta_df["cleanedFile"] == target_file]
if not matched_meta.empty:
    meta_row = matched_meta.iloc[0].to_dict()
else:
    meta_row = {}

# Build metadata dictionary
meta_json = {
    "file": target_file,
    "participantID": meta_row.get("participantID", "unknown"),
    "pairID": meta_row.get("pairID", "unknown"),
    "testingDate": str(meta_row.get("testingDate", "unknown")),
    "device": meta_row.get("AorB", "unknown"),
    "coinSet": meta_row.get("coinSet", "unknown"),
    "sessionType": meta_row.get("sessionType", meta_row.get("testingDate", "unknown")),
    "CoinRegistry": coin_registry_deltas
}

# Save to file
meta_output_path = "/mnt/data/ObsReward_A_02_17_2025_15_11_meta.json"
with open(meta_output_path, "w") as f:
    json.dump(meta_json, f, indent=2)

meta_output_path
