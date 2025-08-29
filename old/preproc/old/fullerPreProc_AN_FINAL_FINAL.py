
import os
import pandas as pd
import numpy as np
import re

def process_obsreward_file(data, file_path):
    data['BlockNum'] = None
    data['RoundNum'] = None
    data['BlockType'] = None
    data['CoinSetID'] = None

    current_block_num = None
    current_block_type = None
    current_coinset_id = None
    round_num = 1

    block_start_idx = None
    temp_indices = []

    for idx, row in data.iterrows():
        message = row.get("Message", "")

        if isinstance(message, str):
            # Start new block at "Mark..."
            if message == "Mark should happen if checked on terminal.":
                block_start_idx = idx
                temp_indices = [idx]
                current_block_num = None
                current_block_type = None
                current_coinset_id = None
                round_num = 1
                continue

            if block_start_idx is not None:
                temp_indices.append(idx)

                # Check for "Started ... Block:X"
                block_match = re.search(r"Started.*Block:\s*(\d+)", message)
                if block_match:
                    current_block_num = int(block_match.group(1))
                    if "pindropping" in message.lower():
                        current_block_type = "pindropping"
                    elif "collecting" in message.lower():
                        current_block_type = "collection"

                # Check for CoinSetID
                coinset_match = re.search(r"coinsetID:(\d+)", message)
                if coinset_match:
                    current_coinset_id = int(coinset_match.group(1))

                # If we have all we need, backfill
                if current_block_num is not None and current_coinset_id is not None:
                    for j in temp_indices:
                        m = data.at[j, "Message"]
                        data.at[j, "BlockNum"] = current_block_num
                        data.at[j, "BlockType"] = current_block_type
                        data.at[j, "CoinSetID"] = current_coinset_id
                        data.at[j, "RoundNum"] = round_num
                        if isinstance(m, str) and "Repositioned and ready to start block or round" in m:
                            round_num += 1
                    # Reset
                    block_start_idx = None
                    temp_indices = []

    data["Messages_filled"] = data["Message"].fillna(method='ffill')
    data.to_csv(file_path, index=False)
    print(f"Processed and saved: {file_path}")


