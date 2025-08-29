
import pandas as pd
import json

# ===== USER INPUTS =====
# Update these paths with your local filenames
events_file = "ObsReward_A_02_17_2025_15_11_events_with_walks.csv"
meta_file = "ObsReward_B_02_17_2025_15_11_processed_meta.json"
collated_file = "collatedData.xlsx"

# ===== LOAD DATA =====
events_df = pd.read_csv(events_file)
with open(meta_file) as f:
    meta_data = json.load(f)
collated_df = pd.read_excel(collated_file)

# ===== METRICS COMPUTATION =====

# Total blocks completed
num_blocks_completed = len([b for b in meta_data['BlockStructureSummary'] if b['BlockStatus'] == 'complete'])

# Files generated (meta + event implies 1)
num_files_generated = 1

# CoinSet and Role
coin_set = meta_data.get("coinSet")
role = meta_data.get("device")
blocks_coin_role = {f"{coin_set}_{role}": num_blocks_completed}

# Total points earned
total_points_earned = events_df["currGrandTotal"].dropna().iloc[-1]

# Total session time
total_session_time = events_df["SessionElapsedTime"].dropna().iloc[-1]

# Average round time
events_filtered = events_df[
    ((events_df["lo_eventType"] == "RoundEnd") & (events_df["BlockNum"].isin([1, 3]))) |
    ((events_df["lo_eventType"] == "TrueBlockEnd") & (~events_df["BlockNum"].isin([1, 3])))
]
round_times = events_filtered.apply(
    lambda row: row["RoundElapsedTime"] if row["lo_eventType"] == "RoundEnd" else row["BlockElapsedTime"],
    axis=1
)
average_round_time = round_times.mean()

# % correct swap votes
swap_votes = events_df[events_df["SwapVote"].notna()]
pct_correct_swap_votes = (
    (swap_votes["SwapVoteScore"] == 1).sum() / len(swap_votes) * 100
) if len(swap_votes) > 0 else None

# # rounds to criterion (from block 3)
rounds_criterion_df = events_df[(events_df["BlockNum"] == 3) & (events_df["totalRounds"].notna())]
rounds_to_criterion_count = rounds_criterion_df["totalRounds"].iloc[0] if not rounds_criterion_df.empty else None

# Final summary dictionary
summary = {
    "Total Blocks Completed": num_blocks_completed,
    "Files Generated": num_files_generated,
    "CoinSet_Role": f"{coin_set}_{role}",
    "Blocks per CoinSet_Role": blocks_coin_role,
    "Total Points Earned": total_points_earned,
    "Total Session Time (sec)": total_session_time,
    "Average Round Time (sec)": average_round_time,
    "% Correct Swap Votes": pct_correct_swap_votes,
    "Rounds to Criterion": rounds_to_criterion_count
}

# Output summary
print("=== Subject Summary ===")
for k, v in summary.items():
    print(f"{k}: {v}")
