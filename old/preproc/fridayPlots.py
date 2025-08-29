# ----------------------------
# Plots from Friday (reproducible)
# ----------------------------
# Requirements: pandas, matplotlib
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

# === Load data ===
# Update this path if your file lives elsewhere:
CSV_PATH = Path("/mnt/data/ObsReward_A_02_17_2025_15_11_events_with_walks.csv")
df = pd.read_csv(CSV_PATH)

# === Basic cleaning ===
# keep usable coin labels
df["coinLabel"] = df["coinLabel"].astype(str).str.strip()
df = df[df["coinLabel"].notna() & (df["coinLabel"] != "")]

# parse timestamps for overall-time plots
if "AN_ParsedTS" in df.columns:
    df["AN_ParsedTS"] = pd.to_datetime(df["AN_ParsedTS"], errors="coerce")

# === Styling maps ===
marker_map = {"HV": "*", "LV": "o", "NV": "o"}      # NV will be hollow
is_filled_map = {"HV": True, "LV": True, "NV": False}
color_map = {"good": "blue", "bad": "red"}          # drop quality colors

# === Helper to scatter with our conventions ===
def scatter_points(ax, x, y, coin, qual, alpha=0.5, size=100):
    marker = marker_map.get(coin, "o")
    filled = is_filled_map.get(coin, True)
    color = color_map.get(str(qual).lower(), "gray")
    if filled:
        ax.scatter(x, y, marker=marker, s=size, alpha=alpha,
                   facecolors=color, edgecolors=color, linewidth=1)
    else:
        ax.scatter(x, y, marker=marker, s=size, alpha=alpha,
                   facecolors="none", edgecolors=color, linewidth=1)

def add_legends(ax):
    # Legend A: coin shapes (all in neutral edgecolor)
    shape_handles = [
        Line2D([0], [0], marker="*",  linestyle="None", markersize=10,
               markerfacecolor="none", markeredgecolor="black", label="HV (star)"),
        Line2D([0], [0], marker="o",  linestyle="None", markersize=10,
               markerfacecolor="black", markeredgecolor="black", label="LV (filled circle)"),
        Line2D([0], [0], marker="o",  linestyle="None", markersize=10,
               markerfacecolor="none", markeredgecolor="black", label="NV (hollow circle)"),
    ]
    leg1 = ax.legend(handles=shape_handles, title="Coin Type", loc="upper left")
    ax.add_artist(leg1)

    # Legend B: drop quality colors (simple filled squares)
    qual_handles = [
        Line2D([0], [0], marker="s", linestyle="None", markersize=10,
               markerfacecolor=color_map["good"], markeredgecolor=color_map["good"], label="good"),
        Line2D([0], [0], marker="s", linestyle="None", markersize=10,
               markerfacecolor=color_map["bad"], markeredgecolor=color_map["bad"], label="bad"),
    ]
    ax.legend(handles=qual_handles, title="Drop Quality", loc="upper right")

# ----------------------------------------------------
# PLOT 1: Block 3 — X: BlockElapsedTime, Y: RoundElapsedTime
# (filtered to RoundElapsedTime <= 200)
# ----------------------------------------------------
block3 = df[
    (df["BlockNum"] == 3) &
    df["BlockElapsedTime"].notna() &
    df["RoundElapsedTime"].notna() &
    df["dropQual"].notna() &
    (df["RoundElapsedTime"] <= 200)
][["BlockElapsedTime", "RoundElapsedTime", "coinLabel", "dropQual"]].copy()

fig1, ax1 = plt.subplots(figsize=(10, 6))
for _, r in block3.iterrows():
    scatter_points(ax1,
                   x=r["BlockElapsedTime"],
                   y=r["RoundElapsedTime"],
                   coin=r["coinLabel"],
                   qual=r["dropQual"],
                   alpha=0.5,
                   size=100)

ax1.set_title("Pin Drop Latency in Block 3 (≤ 200s Round Elapsed Time)")
ax1.set_xlabel("Block Elapsed Time")
ax1.set_ylabel("Round Elapsed Time")
add_legends(ax1)
fig1.tight_layout()

# ----------------------------------------------------
# PLOT 2: Blocks > 3 — X: AN_ParsedTS (overall time), Y: BlockElapsedTime
# ----------------------------------------------------
gt3 = df[
    (df["BlockNum"] > 3) &
    df["BlockElapsedTime"].notna() &
    df["AN_ParsedTS"].notna() &
    df["dropQual"].notna()
][["AN_ParsedTS", "BlockElapsedTime", "coinLabel", "dropQual"]].copy()

fig2, ax2 = plt.subplots(figsize=(12, 6))
for _, r in gt3.iterrows():
    scatter_points(ax2,
                   x=r["AN_ParsedTS"],
                   y=r["BlockElapsedTime"],
                   coin=r["coinLabel"],
                   qual=r["dropQual"],
                   alpha=0.5,
                   size=100)

ax2.set_title("Pin Drop Latency in Blocks > 3 by Coin Type and Drop Quality")
ax2.set_xlabel("Overall Time (AN_ParsedTS)")
ax2.set_ylabel("Block Elapsed Time")
add_legends(ax2)
fig2.tight_layout()

plt.show()
