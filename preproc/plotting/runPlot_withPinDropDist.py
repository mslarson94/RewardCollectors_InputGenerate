import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


import pandas as pd

def add_route_type(
    df: pd.DataFrame,
    *,
    group_cols=("BlockNum", "RoundNum"),         # per-round within block
    label_col="coinLabel",
    pin_mask_col="dropDist",                     # rows that are pin drops
    order_col="chestPin_num",                    # primary ordering within a round
    ts_fallback_col="mLTimestamp",               # fallback ordering if order_col missing
    out_col="routeType",
) -> pd.DataFrame:
    """
    For each (BlockNum, RoundNum), find all pin-drop rows, order them, and
    assign the same route string (e.g., 'NV - HV - LV') to each pin-drop row.
    Non pin-drop rows get NA in `out_col`. Does not reorder rows.
    """
    # normalize types
    if label_col in df.columns:
        labels_norm = pd.Series(df[label_col], dtype="string").str.strip()
    else:
        raise KeyError(f"Missing label column: {label_col}")

    pin_mask = df[pin_mask_col].notna() if pin_mask_col in df.columns else pd.Series(False, index=df.index)
    order_vals = pd.to_numeric(df[order_col], errors="coerce") if order_col in df.columns else pd.Series(pd.NA, index=df.index)
    ts_vals = pd.to_datetime(df[ts_fallback_col], errors="coerce") if ts_fallback_col in df.columns else None

    route_col = pd.Series(pd.NA, index=df.index, dtype="string")

    # build per-group route and broadcast to that group's pin rows
    for _, g in df.groupby(list(group_cols), dropna=False, sort=False):
        p = g[pin_mask.loc[g.index]].copy()
        if p.empty:
            continue

        # sort by chestPin_num if available; else by timestamp; else by original order
        if order_vals.loc[p.index].notna().any():
            p = p.sort_values([order_col, ts_fallback_col] if ts_vals is not None else [order_col])
        elif ts_vals is not None and ts_vals.loc[p.index].notna().any():
            p = p.sort_values(ts_fallback_col)
        # else keep original order

        route = " - ".join(labels_norm.loc[p.index].tolist())
        route_col.loc[p.index] = route

    df[out_col] = route_col
    return df

# Setup
# participants = [f"P{i:02d}" for i in range(1, 7)]
# n_trials = 50
route_categories = ['HV - LV - NV', 'HV - NV - LV', 'LV - NV - HV', 'LV - HV - NV', 'NV - HV - LV', 'NV - LV - HV']
category_palette = {
    'HV - LV - NV': '#66c2a5',
    'HV - NV - LV': '#e78ac3',
    'LV - NV - HV': '#fc8d62',
    'LV - HV - NV': '#8da0cb',
    'NV - HV - LV': '#a6d854',
    'NV - LV - HV': '#ffd92f'
}


# --- Example usage on your CSV ---
from pathlib import Path
csv_in = Path("/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/Merged_PtRoleCoinSet_Flat_csv/116A_02_19_2025_AN_D_ML2G_main_116A_AN_D_events.csv")  # replace with your path
#csv_in = "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/MergedEvents_noWalks/Merged_ParticipantDayRoleCoinSet/MergedEvents/R037_AN_B_main_events.csv"
df = pd.read_csv(csv_in)
df = add_route_type(df)  # adds 'routeType' on pin-drop rows
df.to_csv(csv_in.with_name(csv_in.stem + "_with_routeType.csv"), index=False)

df_multi = df

# Filter for a single participant
single_participant = df['participantID'].unique()[0]
# Make sure BlockNum is numeric so grouping/sorting works
df["BlockNum"] = pd.to_numeric(df["BlockNum"], errors="coerce")

# Build one route per block from the earliest pin drop in that block
routes_by_block = (
    df.loc[df["dropDist"].notna() & df["routeType"].notna(),  # pin rows only
           ["BlockNum", "RoundNum", "chestPin_num", "mLTimestamp", "routeType"]]
      .sort_values(["BlockNum", "RoundNum", "chestPin_num", "mLTimestamp"], kind="stable")
      .groupby("BlockNum", sort=True)["routeType"]
      .first()
)



from matplotlib.patches import Patch
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# assumes you already have:
# df = pd.read_csv(...); df = add_route_type(df)
# route_categories and category_palette defined as in your code

def _routes_by_block(df: pd.DataFrame) -> pd.Series:
    """
    One route string per BlockNum from the earliest pin-drop row.
    Returns a Series indexed by BlockNum.
    """
    # Work on a copy; ensure numeric BlockNum
    g = df.copy()
    g["BlockNum"] = pd.to_numeric(g["BlockNum"], errors="coerce")

    # Only rows that are pin drops AND have a computed route
    mask = g["dropDist"].notna() & g["routeType"].notna() & g["BlockNum"].notna()

    # Columns to keep (no duplicates)
    keep_cols = ["BlockNum", "routeType"]
    order_cols = [c for c in ["RoundNum", "chestPin_num", "mLTimestamp"] if c in g.columns]  # <- no BlockNum here
    cols = keep_cols + order_cols

    sub = g.loc[mask, cols].copy()
    # If your CSV itself had duplicate column names, this also removes them:
    sub = sub.loc[:, ~sub.columns.duplicated()]

    # Stable sort so we take the earliest pin sequence within each block
    if order_cols:
        sub = sub.sort_values(["BlockNum", *order_cols], kind="stable")
    else:
        sub = sub.sort_values(["BlockNum"], kind="stable")

    # First route per BlockNum
    return sub.groupby("BlockNum", sort=True)["routeType"].first()


def _route_legend(ax, routes: pd.Series):
    """Legend for background route colors."""
    present = [r for r in route_categories if r in set(routes.dropna())]
    handles = [Patch(facecolor=category_palette.get(r, "#dddddd"), edgecolor="none", alpha=0.3, label=r)
               for r in present]
    if handles:
        ax.legend(handles=handles, title=f"{df['participantID'].iat[0]} Route Type", loc="upper left", frameon=True)

# ---------- 1) your distance-vs-block plot with legend ----------
def plot_pin_drop_dist_with_route_bars(df: pd.DataFrame):
    routes = _routes_by_block(df)

    plt.figure(figsize=(12, 4))

    # background bars per block
    for blk in routes.index:
        route = routes.loc[blk]
        color = category_palette.get(route, "#dddddd")
        plt.axvspan(blk - 0.4, blk + 0.4, color=color, alpha=0.3, zorder=0)

    # jittered pin-drops
    mask = df["dropDist"].notna()
    x = df.loc[mask, "BlockNum"].astype(float).to_numpy()
    x = x + np.random.normal(0, 0.1, size=x.size)
    y = df.loc[mask, "dropDist"].to_numpy()
    plt.scatter(x, y, color="black", s=20, alpha=0.8, zorder=1)

    # legend for route colors
    ax = plt.gca()
    _route_legend(ax, routes)

    plt.axhline(1, color="red", linestyle="--", linewidth=1)
    plt.ylim(0, 8)
    plt.xlabel("BlockNum")
    plt.ylabel("Pin Drop Distance")
    plt.title(f"Pin Drop Distances per Block (Participant: {df['participantID'].iat[0]})")
    plt.tight_layout()
    plt.show()

# ---------- 2) run plot of route types over all blocks ----------
def plot_route_run(df: pd.DataFrame):
    routes = _routes_by_block(df)
    routes = routes.dropna()

    # map route -> y index
    route_to_y = {r: i for i, r in enumerate(route_categories, start=1)}
    ys = routes.map(route_to_y).dropna()
    xs = ys.index.astype(float)

    fig, ax = plt.subplots(figsize=(12, 4))
    # step line + colored markers
    ax.plot(xs, ys, drawstyle="steps-mid", linewidth=2)
    ax.scatter(xs, ys, s=60, c=[category_palette.get(r, "#999999") for r in routes.loc[ys.index]], zorder=3)

    # y ticks as route names
    ax.set_yticks(list(route_to_y.values()))
    ax.set_yticklabels(list(route_to_y.keys()))
    ax.set_xlabel("BlockNum")
    ax.set_ylabel("Route Type")
    ax.set_title(f"Route Type Run Chart by Block (Participant: {df['participantID'].iat[0]})")

    # legend for colors
    _route_legend(ax, routes)

    ax.grid(True, axis="y", linestyle=":", linewidth=0.8)
    fig.tight_layout()
    plt.show()

# ---- call them ----
plot_pin_drop_dist_with_route_bars(df)
plot_route_run(df)
