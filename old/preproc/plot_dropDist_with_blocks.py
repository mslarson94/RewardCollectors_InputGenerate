
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ===== USER INPUTS =====
root_dir = "/Users/mairahmac/Desktop/RC_TestingNotes"
input_dir = Path(root_dir) / "ResurrectedData" / "pin_drops"
output_dir = Path(root_dir) / "ResurrectedData" / "pin_drops_plots"
output_dir.mkdir(parents=True, exist_ok=True)

# ===== PROCESS EACH PARTICIPANT FILE =====
for file in input_dir.glob("*_pin_drops.csv"):
    df = pd.read_csv(file, parse_dates=["ParsedTimestamp"])
    participant = df["participantID"].iloc[0]

    if "BlockNum" not in df.columns or "ParsedTimestamp" not in df.columns:
        continue

    for coinSet in df["coinSet"].dropna().unique():
        subset = df[df["coinSet"] == coinSet].copy()
        if subset.empty:
            continue

        # Sort by time and compute relative time in minutes
        subset = subset.sort_values("ParsedTimestamp")
        t0 = subset["ParsedTimestamp"].iloc[0]
        subset["TimeSinceStart_min"] = (subset["ParsedTimestamp"] - t0).dt.total_seconds() / 60.0

        # Track block transitions
        block_changes = subset[["BlockNum", "TimeSinceStart_min"]].dropna().drop_duplicates("BlockNum")

        # Plot
        plt.figure(figsize=(12, 6))
        for qual, color in [("good", "blue"), ("bad", "red")]:
            qsubset = subset[subset["dropQual"] == qual]
            plt.scatter(qsubset["TimeSinceStart_min"], qsubset["dropDist"], label=f"{qual} drop", color=color, alpha=0.6)

        # Block transition lines
        for _, row in block_changes.iterrows():
            plt.axvline(row["TimeSinceStart_min"], linestyle="--", color="gray", alpha=0.4)
            plt.text(row["TimeSinceStart_min"], plt.ylim()[0], f'Block {int(row["BlockNum"])}',
                     rotation=90, verticalalignment='bottom', horizontalalignment='right', fontsize=8, color='gray')

        plt.title(f"Pin Drop Distance Over Time (relative)
Participant: {participant}, CoinSet: {coinSet}")
        plt.xlabel("Minutes Since First Drop")
        plt.ylabel("Pin Distance (dropDist)")
        plt.legend()
        plt.tight_layout()

        out_path = output_dir / f"{participant}_CoinSet{coinSet}_dropDist_plot_blocks.png"
        plt.savefig(out_path)
        plt.close()
        print(f"✅ Saved block-labeled plot for {participant}, CoinSet {coinSet} → {out_path}")
