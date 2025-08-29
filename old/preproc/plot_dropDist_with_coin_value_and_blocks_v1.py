
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ===== USER INPUTS =====
root_dir = "/Users/mairahmac/Desktop/RC_TestingNotes"
input_dir = Path(root_dir) / "ResurrectedData" / "pin_drops"
output_dir = Path(root_dir) / "ResurrectedData" / "pin_drops_plots"
output_dir.mkdir(parents=True, exist_ok=True)

# Marker styles by valueTag
value_marker_map = {
    "HV": "*",
    "LV": "o",
    "NV": "o"
}

# Fill style: unfilled for NV
value_fill_map = {
    "HV": True,
    "LV": True,
    "NV": False
}

# Color by dropQual
qual_color_map = {
    "good": "blue",
    "bad": "red"
}

# ===== PROCESS EACH PARTICIPANT FILE =====
for file in input_dir.glob("*_pin_drops.csv"):
    df = pd.read_csv(file, parse_dates=["ParsedTimestamp"])
    participant = df["participantID"].iloc[0]

    if "BlockNum" not in df.columns or "ParsedTimestamp" not in df.columns or "valueTag" not in df.columns:
        continue

    for coinSet in df["coinSet"].dropna().unique():
        subset = df[df["coinSet"] == coinSet].copy()
        if subset.empty:
            continue

        # Sort by time and compute relative time
        subset = subset.sort_values("ParsedTimestamp")
        t0 = subset["ParsedTimestamp"].iloc[0]
        subset["TimeSinceStart_min"] = (subset["ParsedTimestamp"] - t0).dt.total_seconds() / 60.0

        # Filter for every 5th block only for vertical lines
        block_changes = subset[["BlockNum", "TimeSinceStart_min"]].dropna().drop_duplicates("BlockNum")
        block_changes = block_changes[block_changes["BlockNum"] % 5 == 0]

        # Plot setup
        plt.figure(figsize=(12, 6))

        # Plot by valueTag and dropQual
        for value in ["HV", "LV", "NV"]:
            for qual in ["good", "bad"]:
                filt = (subset["valueTag"] == value) & (subset["dropQual"] == qual)
                qdata = subset[filt]
                if qdata.empty:
                    continue
                marker = value_marker_map[value]
                color = qual_color_map[qual]
                fill = value_fill_map[value]
                edge = color if not fill else None
                plt.scatter(qdata["TimeSinceStart_min"], qdata["dropDist"],
                            label=f"{value} / {qual}", color=color if fill else "none",
                            edgecolors=edge, marker=marker, alpha=0.6)

        # Add vertical block lines every 5th block
        for _, row in block_changes.iterrows():
            plt.axvline(row["TimeSinceStart_min"], linestyle="--", color="gray", alpha=0.4)
            plt.text(row["TimeSinceStart_min"], plt.ylim()[0], f'Block {int(row["BlockNum"])}',
                     rotation=90, verticalalignment='bottom', horizontalalignment='right', fontsize=8, color='gray')

        plt.title(f"Pin Drop Distance Over Time (by coin type)
Participant: {participant}, CoinSet: {coinSet}")
        plt.xlabel("Minutes Since First Drop")
        plt.ylabel("Pin Distance (dropDist)")
        plt.legend()
        plt.tight_layout()

        out_path = output_dir / f"{participant}_CoinSet{coinSet}_dropDist_coinval_plot.png"
        plt.savefig(out_path)
        plt.close()
        print(f"✅ Saved enhanced plot for {participant}, CoinSet {coinSet} → {out_path}")
