
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
    df = pd.read_csv(file, parse_dates=["ParsedTimestamp"], infer_datetime_format=True)
    participant = df["participantID"].iloc[0]

    for coinSet in df["coinSet"].dropna().unique():
        subset = df[df["coinSet"] == coinSet]
        if subset.empty:
            continue

        plt.figure(figsize=(12, 6))
        for qual, color in [("good", "blue"), ("bad", "red")]:
            qsubset = subset[subset["dropQual"] == qual]
            plt.scatter(qsubset["ParsedTimestamp"], qsubset["dropDist"], label=f"{qual} drop", color=color, alpha=0.6)

        plt.title(f"Pin Drop Distance Over Time \nParticipant: {participant}, CoinSet: {coinSet}")
        plt.xlabel("Time")
        plt.ylabel("Pin Distance (dropDist)")
        plt.legend()
        plt.tight_layout()

        out_path = output_dir / f"{participant}_CoinSet{coinSet}_dropDist_plot.png"
        plt.savefig(out_path)
        plt.close()
        print(f"✅ Saved plot for {participant}, CoinSet {coinSet} → {out_path}")
