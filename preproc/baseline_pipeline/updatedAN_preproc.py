# Complete updated script with correct block tagging, temporal interval assignment, and augmentation
def detect_and_tag_blocks_deferred_coinset(data):
    data["RoundNum"] = np.nan
    data["BlockNum"] = np.nan
    data["CoinSetID"] = np.nan
    data["BlockType"] = np.nan

    current_block = {
        "start_idx": None,
        "block_num": None,
        "block_type": None,
        "coinset_id": None,
    }

    for idx in range(len(data)):
        message = str(data.at[idx, "Message"]).strip()

        if message == "Mark should happen if checked on terminal.":
            current_block = {
                "start_idx": idx,
                "block_num": None,
                "block_type": None,
                "coinset_id": None,
            }
            data.at[idx, "RoundNum"] = 0

        block_match = re.search(r"Started (collecting|pindropping)\. Block:(\d+)", message)
        if block_match:
            current_block["block_type"] = block_match.group(1)
            current_block["block_num"] = int(block_match.group(2))

        if message.startswith("coinsetID:"):
            match = re.search(r"coinsetID:(\d+)", message)
            if match:
                current_block["coinset_id"] = int(match.group(1))

        if message == "finished current task":
            if all(v is not None for v in current_block.values()):
                for j in range(current_block["start_idx"], idx + 1):
                    data.at[j, "BlockNum"] = current_block["block_num"]
                    data.at[j, "CoinSetID"] = current_block["coinset_id"]
                    data.at[j, "BlockType"] = current_block["block_type"]
            else:
                print(f"⚠️ Incomplete metadata for block ending at idx {idx}: {current_block}")
            current_block = {
                "start_idx": None,
                "block_num": None,
                "block_type": None,
                "coinset_id": None,
            }

        if "Repositioned and ready to start block or round" in message:
            if pd.isna(data.at[idx, "RoundNum"]):
                prev = data["RoundNum"].loc[:idx].dropna()
                data.at[idx, "RoundNum"] = (prev.iloc[-1] + 1) if not prev.empty else 1

    return data

def augment_with_chestpin_and_totalrounds(data):
    data["chestPin_num"] = np.nan
    data["totalRounds"] = np.nan

    data["RoundNum_filled"] = data["RoundNum"].ffill()
    data["BlockNum_filled"] = data["BlockNum"].ffill()

    valid_rounds = data[~data["RoundNum_filled"].isin([0, 7777, 8888, 9999])]
    round_counts = valid_rounds.groupby("BlockNum_filled")["RoundNum_filled"].nunique()
    data["totalRounds"] = data["BlockNum_filled"].map(round_counts)

    chest_pin_count = 0
    for idx, row in data.iterrows():
        message = row.get("Message", "")
        if isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
            chest_pin_count = 0
        if isinstance(message, str) and (message.startswith("Chest opened: ") or message.startswith("Just dropped a pin.")):
            chest_pin_count += 1
        data.at[idx, "chestPin_num"] = chest_pin_count

    data.drop(columns=["RoundNum_filled", "BlockNum_filled"], inplace=True)
    return data

# Load data
data = pd.read_csv("/mnt/data/ObsReward_A_02_17_2025_15_11.csv")

# Run full processing
data = detect_and_tag_blocks_deferred_coinset(data)
data = forward_fill_block_info(data)

# assign_temporal_intervals already defined in preproc_AN.py, we replicate here for consistency
def assign_temporal_intervals(data):
    all_indices = data.index.tolist()
    idx_limit = len(data)
    block_starts = data[data["Message"] == "Mark should happen if checked on terminal."].index

    for i, block_start in enumerate(block_starts):
        next_block_start = block_starts[i + 1] if i + 1 < len(block_starts) else idx_limit
        block_idxs = list(range(block_start, next_block_start))

        last_reposition = None
        last_finished_round = None

        for idx in block_idxs:
            message = data.at[idx, "Message"]

            if isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
                last_reposition = idx

            if last_finished_round is not None and isinstance(message, str) and message.startswith("Repositioned and ready to start block or round"):
                for j in range(last_finished_round, idx):
                    data.at[j, "RoundNum"] = 7777
                last_finished_round = None

            if isinstance(message, str) and message.startswith("Finished pindrop round:"):
                last_finished_round = idx

            if last_reposition is not None and isinstance(message, str) and re.match(r"Started (?:collecting|pindropping)\. Block:\d+", message):
                for j in range(last_reposition, idx):
                    data.at[j, "RoundNum"] = 8888
                last_reposition = None

            if isinstance(message, str) and message.lower().strip() == "finished current task":
                block_id = data.at[idx, "BlockNum"]
                post_rows = data[(data.index > idx-1) & (data["BlockNum"] == block_id)]
                for j in post_rows.index:
                    if data.at[j, "RoundNum"] not in [7777, 8888]:
                        data.at[j, "RoundNum"] = 9999

    return data

# Apply temporal intervals and augmentation
data = assign_temporal_intervals(data)

# Assign block completeness
data['BlockStatus'] = None
for block_num in data['BlockNum'].dropna().unique():
    block_mask = data['BlockNum'] == block_num
    msgs = data[block_mask]["Message"].dropna().str.lower()
    has_start = any("mark should happen" in m for m in msgs)
    has_end = any("finished current task" in m for m in msgs)
    status = "complete" if has_start and has_end else "truncated" if has_start else "incomplete"
    data.loc[block_mask, 'BlockStatus'] = status

# Save mid-stage output
basic_output = "/mnt/data/ObsReward_A_02_17_2025_15_11_processed_with_specials.csv"
data.to_csv(basic_output, index=False)

# Apply chest/pin and total rounds augmentation
augmented_data = augment_with_chestpin_and_totalrounds(data)
final_output = "/mnt/data/ObsReward_A_02_17_2025_15_11_processed_FULL.csv"
augmented_data.to_csv(final_output, index=False)

basic_output, final_output 
