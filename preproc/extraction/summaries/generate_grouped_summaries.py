
import pandas as pd
from pathlib import Path

# ===== USER INPUTS =====
root_dir = "/Users/mairahmac/Desktop/RC_TestingNotes"
summary_file = Path(root_dir) / "ResurrectedData" / "Summary" / "all_subjects_summary.csv"
output_dir = Path(root_dir) / "ResurrectedData" / "Summary"
output_dir.mkdir(parents=True, exist_ok=True)

# ===== LOAD DATA =====
df = pd.read_csv(summary_file)

# Ensure numeric-only columns are coerced safely
numeric_cols = [
    "Total Blocks Completed",
    "Total Points Earned",
    "Total Session Time (sec)",
    "Average Round Time (sec)",
    "Number of Total Swap Votes",
    "Number of Correct Swap Votes",
    'Number of PinDropVotes',
    "Number of Correct PinDropVotes"
]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# ===== 1. Per-Participant Summary =====
group1 = df.groupby("participantID").agg({
    "Total Blocks Completed": "sum",
    "Total Points Earned": "sum",
    "Total Session Time (sec)": "sum",
    "Average Round Time (sec)": "mean",
    "Number of Total Swap Votes": "sum",
    "Number of Correct Swap Votes": "sum",
    'Number of PinDropVotes': "sum",
    "Number of Correct PinDropVotes": "sum"
}).reset_index()
group1["% Correct Swap Votes"] = (
    group1["Number of Correct Swap Votes"] / group1["Number of Total Swap Votes"] * 100
)
group1.to_csv(output_dir / "summary_by_participant.csv", index=False)

# ===== 2. Per-Participant + Role Summary =====
group2 = df.groupby(["participantID", "currentRole"]).agg({
    "Total Blocks Completed": "sum",
    "Total Points Earned": "sum",
    "Total Session Time (sec)": "sum",
    "Average Round Time (sec)": "mean",
    "Number of Total Swap Votes": "sum",
    "Number of Correct Swap Votes": "sum",
    'Number of PinDropVotes': "sum",
    "Number of Correct PinDropVotes": "sum"
}).reset_index()
group2["% Correct Swap Votes"] = (
    group2["Number of Correct Swap Votes"] / group2["Number of Total Swap Votes"] * 100
)
group2.to_csv(output_dir / "summary_by_participant_and_role.csv", index=False)

# ===== 3. Per-Participant + Role + CoinSet Summary =====
group3 = df.groupby(["participantID", "currentRole", "coinSet"]).agg({
    "Total Blocks Completed": "sum",
    "Total Points Earned": "sum",
    "Total Session Time (sec)": "sum",
    "Average Round Time (sec)": "mean",
    "Number of Total Swap Votes": "sum",
    "Number of Correct Swap Votes": "sum",
    'Number of PinDropVotes': "sum",
    "Number of Correct PinDropVotes": "sum"
}).reset_index()
group3["% Correct Swap Votes"] = (
    group3["Number of Correct Swap Votes"] / group3["Number of Total Swap Votes"] * 100
)
group3.to_csv(output_dir / "summary_by_participant_role_coinset.csv", index=False)

print("✅ Aggregated summaries saved.")
