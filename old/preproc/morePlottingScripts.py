# ============================================================
# End-to-end plotting script (matplotlib-only, no seaborn)
# - Computes corrected elapsed times if TrueSessionElapsedTime exists
# - Reproduces the two key plots with your styling choices
#     1) Block 3: X=BlockElapsedTime, Y=RoundElapsedTime (≤200s), color=dropQual, shape=coinLabel
#     2) Blocks > 3: X=AN_ParsedTS, Y=BlockElapsedTime, color=dropQual, shape=coinLabel
# ============================================================

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

# -------------------------------
# Configuration
# -------------------------------
CSV_PATH = Path("/mnt/data/ObsReward_A_02_17_2025_15_11_events_with_walks.csv")

# Marker/Color conventions (explicitly requested)
MARKER_BY_COIN = {"HV": "*", "LV": "o", "NV": "o"}       # NV will be hollow
FILLED_BY_COIN = {"HV": True, "LV": True, "NV": False}
COLOR_BY_QUAL  = {"good": "blue", "bad": "red"}          # else -> gray
ALPHA = 0.5
SIZE  = 100

# -------------------------------
# Helpers
# -------------------------------
def read_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Normalize coin label & drop quality text
    if "coinLabel" in df.columns:
        df["coinLabel"] = df["coinLabel"].astype(str).str.strip()
    if "dropQual" in df.columns:
        df["dropQual"] = df["dropQual"].astype(str).str.strip().str.lower()
    # Parse session timestamp for overall-time plots
    if "AN_ParsedTS" in df.columns:
        df["AN_ParsedTS"] = pd.to_datetime(df["AN_ParsedTS"], errors="coerce")
    # Numeric-ize block/round
    for c in ("BlockNum", "RoundNum"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def recompute_elapsed_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    If TrueSessionElapsedTime exists, compute corrected elapsed times.
    Otherwise, fall back to existing BlockElapsedTime/RoundElapsedTime columns.
    """
    out = df.copy()

    if "TrueSessionElapsedTime" in out.columns:
        tse = pd.to_numeric(out["TrueSessionElapsedTime"], errors="coerce")
        out = out.assign(TrueSessionElapsedTime=tse)
        # drop rows missing the necessary keys
        out = out.dropna(subset=["TrueSessionElapsedTime", "BlockNum", "RoundNum"])
        # compute corrected elapsed times
        out["CorrectedBlockElapsedTime"] = (
            out.groupby("BlockNum")["TrueSessionElapsedTime"].transform(lambda x: x - x.min())
        )
        out["CorrectedRoundElapsedTime"] = (
            out.groupby(["BlockNum", "RoundNum"])["TrueSessionElapsedTime"].transform(lambda x: x - x.min())
        )
        # use corrected names for the plotting columns below
        out["Plot_BlockElapsed"] = out["CorrectedBlockElapsedTime"]
        out["Plot_RoundElapsed"] = out["CorrectedRoundElapsedTime"]
    else:
        # Fall back to existing columns if present
        for c in ("BlockElapsedTime", "RoundElapsedTime"):
            if c in out.columns:
                out[c] = pd.to_numeric(out[c], errors="coerce")
        if "BlockElapsedTime" not in out.columns or "RoundElapsedTime" not in out.columns:
            raise ValueError(
                "Neither TrueSessionElapsedTime nor Block/RoundElapsedTime columns are available for plotting."
            )
        out["Plot_BlockElapsed"] = out["BlockElapsedTime"]
        out["Plot_RoundElapsed"] = out["RoundElapsedTime"]

    return out


def scatter_point(ax, x, y, coin, qual, alpha=ALPHA, size=SIZE):
    marker = MARKER_BY_COIN.get(coin, "o")
    filled = FILLED_BY_COIN.get(coin, True)
    color  = COLOR_BY_QUAL.get(qual, "gray")
    if filled:
        ax.scatter(x, y, marker=marker, s=size, alpha=alpha,
                   facecolors=color, edgecolors=color, linewidth=1)
    else:
        ax.scatter(x, y, marker=marker, s=size, alpha=alpha,
                   facecolors="none", edgecolors=color, linewidth=1)


def add_legends(ax):
    # Coin type legend (shapes)
    coin_handles = [
        Line2D([0], [0], marker="*", linestyle="None", markersize=10,
               markerfacecolor="none", markeredgecolor="black", label="HV (star)"),
        Line2D([0], [0], marker="o", linestyle="None", markersize=10,
               markerfacecolor="black", markeredgecolor="black", label="LV (filled circle)"),
        Line2D([0], [0], marker="o", linestyle="None", markersize=10,
               markerfacecolor="none", markeredgecolor="black", label="NV (hollow circle)"),
    ]
    leg1 = ax.legend(handles=coin_handles, title="Coin Type", loc="upper left")
    ax.add_artist(leg1)

    # Drop quality legend (colors)
    qual_handles = [
        Line2D([0], [0], marker="s", linestyle="None", markersize=10,
               markerfacecolor=COLOR_BY_QUAL["good"], markeredgecolor=COLOR_BY_QUAL["good"], label="good"),
        Line2D([0], [0], marker="s", linestyle="None", markersize=10,
               markerfacecolor=COLOR_BY_QUAL["bad"], markeredgecolor=COLOR_BY_QUAL["bad"], label="bad"),
    ]
    ax.legend(handles=qual_handles, title="Drop Quality", loc="upper right")


# -------------------------------
# Plot 1: Block 3 — X=BlockElapsed, Y=RoundElapsed (≤200s)
# -------------------------------
def plot_block3_round_vs_block(df: pd.DataFrame):
    req = ["BlockNum", "Plot_BlockElapsed", "Plot_RoundElapsed", "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Block 3 plot: {missing}")

    block3 = df[
        (df["BlockNum"] == 3)
        & df["Plot_BlockElapsed"].notna()
        & df["Plot_RoundElapsed"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].notna()
        & (df["Plot_RoundElapsed"] <= 200)
    ][["Plot_BlockElapsed", "Plot_RoundElapsed", "coinLabel", "dropQual"]].copy()

    fig, ax = plt.subplots(figsize=(10, 6))
    for _, r in block3.iterrows():
        scatter_point(ax, r["Plot_BlockElapsed"], r["Plot_RoundElapsed"], r["coinLabel"], r["dropQual"])
    ax.set_title("Pin Drop Latency in Block 3 (≤ 200s Round Elapsed Time)")
    ax.set_xlabel("Block Elapsed Time")
    ax.set_ylabel("Round Elapsed Time")
    add_legends(ax)
    fig.tight_layout()
    plt.show()


# -------------------------------
# Plot 2: Blocks > 3 — X=AN_ParsedTS, Y=BlockElapsed
# -------------------------------
def plot_blocks_gt3_overall_vs_block(df: pd.DataFrame):
    req = ["BlockNum", "AN_ParsedTS", "Plot_BlockElapsed", "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Blocks>3 plot: {missing}")

    gt3 = df[
        (df["BlockNum"] > 3)
        & df["AN_ParsedTS"].notna()
        & df["Plot_BlockElapsed"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].notna()
    ][["AN_ParsedTS", "Plot_BlockElapsed", "coinLabel", "dropQual"]].copy()

    fig, ax = plt.subplots(figsize=(12, 6))
    for _, r in gt3.iterrows():
        scatter_point(ax, r["AN_ParsedTS"], r["Plot_BlockElapsed"], r["coinLabel"], r["dropQual"])
    ax.set_title("Pin Drop Latency in Blocks > 3 by Coin Type and Drop Quality")
    ax.set_xlabel("Overall Time (AN_ParsedTS)")
    ax.set_ylabel("Block Elapsed Time")
    add_legends(ax)
    fig.tight_layout()
    plt.show()


# -------------------------------
# Main
# -------------------------------
def main():
    df = read_data(CSV_PATH)
    # keep only rows with non-empty coin labels
    if "coinLabel" in df.columns:
        df = df[df["coinLabel"] != ""]
    df = recompute_elapsed_times(df)

    # Generate both plots
    plot_block3_round_vs_block(df)
    plot_blocks_gt3_overall_vs_block(df)


if __name__ == "__main__":
    main()
