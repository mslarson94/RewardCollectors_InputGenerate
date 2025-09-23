# ============================================================
# End-to-end plotting script (matplotlib-only, no seaborn)
# - Computes corrected elapsed times if TrueSessionElapsedTime exists
# - Reproduces the two key plots with your styling choices
#     1) Block 3: X=BlockElapsedTime, Y=RoundElapsedTime (≤200s), color=dropQual, shape=coinLabel
#     2) Blocks > 3: X=mLTimestamp, Y=BlockElapsedTime, color=dropQual, shape=coinLabel
# ============================================================

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

# -------------------------------
# Configuration
# -------------------------------
#CSV_PATH = Path("/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/MergedEvents_noWalks/Merged_ParticipantDayRoleCoinSet/MergedEvents/R037_AN_B_main_events.csv")
CSV_PATH = Path("/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/PinDrops_All/PinDrops_ALL.csv")
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
    if "mLTimestamp" in df.columns:
        df["mLTimestamp"] = pd.to_datetime(df["mLTimestamp"], errors="coerce")
    # Numeric-ize block/round
    for c in ("BlockNum", "RoundNum"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

import pandas as pd
import pandas as pd

def add_true_session_elapsed_by_block_events(
    df: pd.DataFrame,
    source_col: str = "truecontent_elapsed_s",
    event_col: str = "lo_eventType",
    start_token: str = "BlockStart",
    end_token: str = "BlockEnd",
    out_col: str = "trueSession_block_elapsed_s",
    include_end_row: bool = False,) -> pd.DataFrame:
    """
    Cumulative timer based only on `source_col`, resetting for each BlockStart→BlockEnd span.
    Assumes rows are already sorted chronologically.
    """
    if event_col not in df.columns:
        raise KeyError(f"Missing event column: {event_col}")

    t = pd.to_numeric(df[source_col], errors="coerce")

    ev = df[event_col].astype(str)
    is_start = ev.eq(start_token)
    is_end   = ev.eq(end_token)

    starts_cum = is_start.cumsum()
    ends_cum   = is_end.cumsum()

    # in-block mask: from BlockStart until (but not including) BlockEnd by default
    in_block = starts_cum.gt(ends_cum) if not include_end_row else starts_cum.ge(ends_cum)

    # Unique block id only while inside a block; NaN outside
    block_id = starts_cum.where(in_block)

    # Per-block deltas from truecontent_elapsed_s (handles per-TrueContent resets)
    prev = t.groupby(block_id).shift(1)
    inside = in_block & t.notna()
    continuing = inside & prev.notna() & (t >= prev)
    starting   = inside & ~continuing

    delta = pd.Series(0.0, index=df.index, dtype="float64")
    delta.loc[continuing] = (t - prev).loc[continuing]
    delta.loc[starting]   = t.loc[starting].fillna(0.0)

    df[out_col] = delta.groupby(block_id).cumsum()  # NaN outside blocks
    return df

def add_true_session_elapsed(df: pd.DataFrame,
                             source_col: str = "truecontent_elapsed_s",
                             out_col: str = "trueSession_elapsed_s") -> pd.DataFrame:
    """
    Creates a cumulative 'session-only' timer that increases only when
    `source_col` is active. Handles per-round resets in `source_col`.
    Does NOT reorder rows.
    """
    t = pd.to_numeric(df[source_col], errors="coerce")  # NaN outside TrueContent
    prev = t.shift(1)

    inside = t.notna()
    continuing = inside & prev.notna() & (t >= prev)   # same TrueContent run
    starting   = inside & ~continuing                   # first row of a run (prev NaN or reset)

    # per-row increment:
    # - continuing: add the increase since previous row
    # - starting: add the starting value (often 0, but include if first sample > 0)
    # - outside (NaN): add 0
    delta = pd.Series(0.0, index=df.index)
    delta.loc[continuing] = (t - prev).loc[continuing]
    delta.loc[starting]   = t.loc[starting].fillna(0.0)

    df[out_col] = delta.cumsum()
    return df


import pandas as pd


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
def plot_block3_round_vs_block_v1(df: pd.DataFrame):
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

def plot_block3_round_vs_block(df: pd.DataFrame):
    req = ["BlockNum", "trueSession_block_elapsed_s", "trueSession_elapsed_s", "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Block 3 plot: {missing}")

    block3 = df[
        (df["BlockNum"] == 3)
        & (df["BlockStatus"] == "complete")
        & df["dropDist"].notna()
        & df["trueSession_elapsed_s"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good","bad"])
        & (df["truecontent_elapsed_s"])
    ][["trueSession_elapsed_s", "dropDist", "coinLabel", "dropQual"]].copy()

    fig, ax = plt.subplots(figsize=(10, 6))
    for _, r in block3.iterrows():
        scatter_point(ax, r["trueSession_elapsed_s"], r["dropDist"], r["coinLabel"], r["dropQual"])
    ax.set_title("Pin Drop Dist in Block 3 ( Round Elapsed Time)")
    ax.set_xlabel("Session Elapsed Time")
    ax.set_ylabel("PinDrop Dist to Closest Coin Not Yet Collected (m)")
    add_legends(ax)
    fig.tight_layout()
    plt.show()

# -------------------------------
# Plot 2: Blocks > 3 — X=mLTimestamp, Y=BlockElapsed
# -------------------------------
def plot_blocks_gt3_overall_vs_block_v1(df: pd.DataFrame):
    req = ["BlockNum", "mLTimestamp", "Plot_BlockElapsed", "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Blocks>3 plot: {missing}")

    gt3 = df[
        (df["BlockNum"] > 3)
        & df["mLTimestamp"].notna()
        & df["Plot_BlockElapsed"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].notna()
    ][["mLTimestamp", "Plot_BlockElapsed", "coinLabel", "dropQual"]].copy()

    fig, ax = plt.subplots(figsize=(12, 6))
    for _, r in gt3.iterrows():
        scatter_point(ax, r["mLTimestamp"], r["Plot_BlockElapsed"], r["coinLabel"], r["dropQual"])
    ax.set_title("Pin Drop Latency in Blocks > 3 by Coin Type and Drop Quality")
    ax.set_xlabel("Overall Time (mLTimestamp)")
    ax.set_ylabel("Block Elapsed Time")
    add_legends(ax)
    fig.tight_layout()
    plt.show()

def plot_blocks_gt3_overall_vs_block(df: pd.DataFrame):
    req = ["BlockNum", "trueSession_elapsed_s", "dropDist", "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Blocks>3 plot: {missing}")

    gt3 = df[
        (df["BlockNum"] > 3)
        & (df["BlockStatus"] == "complete")
        & df["dropDist"].notna()
        & df["trueSession_elapsed_s"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good","bad"])
    ][["dropDist", "trueSession_elapsed_s", "coinLabel", "dropQual"]].copy()

    fig, ax = plt.subplots(figsize=(12, 6))
    for _, r in gt3.iterrows():
        scatter_point(ax, r["trueSession_elapsed_s"],r["dropDist"], r["coinLabel"], r["dropQual"])
    ax.set_title("Pin Drop Distance in Blocks > 3 by Coin Type and Drop Quality")
    ax.set_xlabel("Overall Time (trueSession_elapsed_s)")
    ax.set_ylabel("Pin Drop Distance to Closest Coin Not Yet Collected (m)")
    add_legends(ax)
    fig.tight_layout()
    plt.show()


# -------------------------------
# Plot 2a: Blocks > 3 — X=mLTimestamp, Y=BlockElapsed
# -------------------------------
def plot_blocks_gt3_overall_vs_session(df: pd.DataFrame):
    req = ["BlockNum", "truecontent_elapsed_s", "trueSession_elapsed_s", "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Blocks>3 plot: {missing}")

    gt3 = df[
        (df["BlockNum"] > 3)
        & (df["BlockStatus"] == "complete")
        & df["truecontent_elapsed_s"].notna()
        & df["trueSession_elapsed_s"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good","bad"])
    ][["truecontent_elapsed_s", "trueSession_elapsed_s", "coinLabel", "dropQual"]].copy()

    fig, ax = plt.subplots(figsize=(12, 6))
    for _, r in gt3.iterrows():
        scatter_point(ax, r["trueSession_elapsed_s"], r["truecontent_elapsed_s"], r["coinLabel"], r["dropQual"])
    ax.set_title("Pin Drop Latency in Blocks > 3 by Coin Type and Drop Quality")
    ax.set_xlabel("Overall Time (TrueSessionElapsedTime)")
    ax.set_ylabel("Round Elapsed Time")
    add_legends(ax)
    fig.tight_layout()
    plt.show()

def plot_blocks_gt3_overall_vs_block_facet(df: pd.DataFrame, blocks_per_facet: int = 10):
    req = ["BlockNum", "trueSession_elapsed_s", "dropDist", "coinLabel", "dropQual", "BlockStatus"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Blocks>3 plot: {missing}")

    sub = df[
        (pd.to_numeric(df["BlockNum"], errors="coerce").notna())
        & (df["BlockNum"] > 3)
        & (df["BlockStatus"] == "complete")
        & df["dropDist"].notna()
        & df["trueSession_elapsed_s"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good","bad"])
    ][["BlockNum", "dropDist", "trueSession_elapsed_s", "coinLabel", "dropQual"]].copy()

    # integer block numbers for binning
    sub["BlockNum"] = pd.to_numeric(sub["BlockNum"], errors="coerce").astype(int)

    # facet index: 1–blocks_per_facet => 0, (blocks_per_facet+1)–2*blocks_per_facet => 1, etc.
    sub["facet_idx"] = (sub["BlockNum"] - 1) // blocks_per_facet

    # one figure per facet (Blocks 1–N, N+1–2N, …)
    for idx, g in sub.groupby("facet_idx", sort=True):
        start = idx * blocks_per_facet + 1
        end = (idx + 1) * blocks_per_facet

        fig, ax = plt.subplots(figsize=(12, 6))
        for _, r in g.iterrows():
            scatter_point(ax, r["trueSession_elapsed_s"], r["dropDist"], r["coinLabel"], r["dropQual"])

        ax.set_title(f"Pin Drop Distance — Blocks {start}–{end} by Coin Type and Drop Quality")
        ax.set_xlabel("Overall Time (trueSession_elapsed_s)")
        ax.set_ylabel("Pin Drop Distance to Closest Coin Not Yet Collected (m)")
        add_legends(ax)
        fig.tight_layout()
        plt.show()



# -------------------------------
# Plot 2a: Blocks > 3 — X=mLTimestamp, Y=BlockElapsed
# -------------------------------
def plot_blocks_gt3_overall_vs_BlockNum(df: pd.DataFrame):
    req = ["BlockNum", "truecontent_elapsed_s", "trueSession_elapsed_s", "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for Blocks>3 plot: {missing}")

    gt3 = df[
        (df["BlockNum"] > 3)
        & (df["BlockStatus"] == "complete")
        & df["truecontent_elapsed_s"].notna()
        & df["trueSession_elapsed_s"].notna()
        & df["coinLabel"].notna()
        & df["dropQual"].isin(["good","bad"])
    ][["truecontent_elapsed_s", "BlockNum", "coinLabel", "dropQual"]].copy()

    fig, ax = plt.subplots(figsize=(12, 6))
    for _, r in gt3.iterrows():
        scatter_point(ax, r["BlockNum"], r["truecontent_elapsed_s"], r["coinLabel"], r["dropQual"])
    ax.set_title("Pin Drop Latency in Blocks > 3 by Coin Type and Drop Quality")
    ax.set_xlabel("BlockNum")
    ax.set_ylabel("Round Elapsed Time")
    add_legends(ax)
    fig.tight_layout()
    plt.show()


import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_dropdist_hist_kde_by_coin(
    df: pd.DataFrame,
    *,
    blocks_min: int = 3,
    bins: int | str = "auto",
    stat: str = "density",
    common_norm: bool = False,
    palette: str | dict = "tab10",
):
    req = ["BlockNum", "BlockStatus", "dropDist", "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for drop distance plot: {missing}")

    # Clean & filter
    sdf = df.copy()
    sdf["dropDist"] = pd.to_numeric(sdf["dropDist"], errors="coerce")
    filt = (
        (sdf["BlockNum"] > blocks_min)
        & (sdf["BlockStatus"] == "complete")
        & sdf["coinLabel"].notna()
        & sdf["dropQual"].isin(["good", "bad"])
        & sdf["dropDist"].notna()
    )
    dat = sdf.loc[filt, ["dropDist", "coinLabel"]].reset_index(drop=True)

    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    hue_order = sorted(dat["coinLabel"].unique().tolist())

    # Plot
    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(12, 6))

    # Histogram by coin type
    sns.histplot(
        data=dat,
        x="dropDist",
        hue="coinLabel",
        hue_order=hue_order,
        bins=bins,
        stat=stat,            # "count", "frequency", "probability", or "density"
        common_norm=common_norm,
        element="step",       # outlines to reduce occlusion
        alpha=0.35,
        multiple="layer",
        ax=ax,
        palette=palette,
        legend=True,
    )

    # KDE overlays (same hues/order/palette, no extra legend entries)
    sns.kdeplot(
        data=dat,
        x="dropDist",
        hue="coinLabel",
        hue_order=hue_order,
        common_norm=common_norm,
        ax=ax,
        palette=palette,
        lw=2,
        legend=False,
    )

    ax.set_title(f"Pin Drop Distance Distribution (Blocks > {blocks_min}) by Coin Type")
    ax.set_xlabel("Pin Drop Distance")
    ax.set_ylabel("Density" if stat == "density" else stat.capitalize())
    ax.set_xlim(left=0)
    sns.despine(ax=ax)
    fig.tight_layout()
    plt.show()



import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_droplatency_hist_with_smoothed_lines_by_coin(
    df: pd.DataFrame,
    *,
    blocks_min: int = 3,
    bins: int | str | list = "auto",
    stat: str = "density",          # "count" | "density" | "probability" | "frequency"
    common_norm: bool = False,      # keep False so each coin’s area (or count) stands alone
    palette: str | dict = "tab10",
    # smoothing controls (applied to per-coin histogram series)
    smooth: str = "gaussian",       # "gaussian" | "moving_average"
    sigma_bins: float = 1.5,        # for gaussian smoothing (in *bins*)
    ma_window: int = 5,             # for moving average (odd integer, in *bins*)
    line_width: float = 2.0,
    line_alpha: float = 0.95,
):
    req = ["BlockNum", "BlockStatus", "truecontent_elapsed_s", "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for drop latency plot: {missing}")

    # Filter
    sdf = df.copy()
    sdf["truecontent_elapsed_s"] = pd.to_numeric(sdf["truecontent_elapsed_s"], errors="coerce")
    filt = (
        (sdf["BlockNum"] > blocks_min)
        & (sdf["BlockStatus"] == "complete")
        & sdf["coinLabel"].notna()
        & sdf["dropQual"].isin(["good", "bad"])
        & sdf["truecontent_elapsed_s"].notna()
    )
    dat = sdf.loc[filt, ["truecontent_elapsed_s", "coinLabel"]].reset_index(drop=True)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    hue_order = sorted(dat["coinLabel"].unique().tolist())
    if isinstance(palette, dict):
        color_map = {k: palette.get(k, "#333333") for k in hue_order}
    else:
        color_map = dict(zip(hue_order, sns.color_palette(palette, n_colors=len(hue_order))))

    # Decide on common bin edges up front so hist & lines align
    all_x = dat["truecontent_elapsed_s"].to_numpy()
    bin_edges = np.histogram_bin_edges(all_x, bins=bins)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
    bin_width = np.diff(bin_edges)
    uniform_bins = np.allclose(bin_width, bin_width[0])

    # Smoothing helpers (guarantee output length == input length)
    def _apply_kernel_same_len(y: np.ndarray, kernel: np.ndarray) -> np.ndarray:
        if y.size == 0:
            return y
        out = np.convolve(y, kernel, mode="same")
        if out.size != y.size:
            # center-crop/pad to match y length (handles kernel longer than y)
            if out.size > y.size:
                start = (out.size - y.size) // 2
                out = out[start:start + y.size]
            else:
                pad = y.size - out.size
                out = np.pad(out, (pad // 2, pad - pad // 2), mode="edge")
        return out

    def _gaussian_smooth(y: np.ndarray, sigma: float) -> np.ndarray:
        # build kernel in "bins" units
        radius = int(max(1, np.ceil(sigma * 3)))
        kx = np.arange(-radius, radius + 1, dtype=float)
        kernel = np.exp(-(kx**2) / (2.0 * sigma**2))
        kernel /= kernel.sum()
        return _apply_kernel_same_len(y, kernel)

    def _ma_smooth(y: np.ndarray, window: int) -> np.ndarray:
        window = max(1, int(window))
        if window % 2 == 0:
            window += 1
        kernel = np.ones(window, dtype=float) / window
        return _apply_kernel_same_len(y, kernel)

    smooth_fn = (lambda y: _gaussian_smooth(y, sigma_bins)) if smooth == "gaussian" else (lambda y: _ma_smooth(y, ma_window))

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(12, 6))

    # Raw histogram (per coin, overlaid)
    sns.histplot(
        data=dat,
        x="truecontent_elapsed_s",
        hue="coinLabel",
        hue_order=hue_order,
        bins=bin_edges,
        stat=stat,
        common_norm=common_norm,
        element="step",
        multiple="layer",
        alpha=0.35,
        palette=color_map,
        ax=ax,
        legend=True,
    )

    # Smoothed lines (per coin) overlaid on the same y-scale as the histogram
    density_mode = (stat == "density")
    probability_mode = (stat == "probability")
    frequency_mode = (stat == "frequency")  # seaborn frequency ~= count / n

    for coin, sub in dat.groupby("coinLabel", sort=False):
        x = sub["truecontent_elapsed_s"].to_numpy()

        if probability_mode or frequency_mode:
            counts, _ = np.histogram(x, bins=bin_edges)
            y = counts / max(1, len(x))
        else:
            counts, _ = np.histogram(x, bins=bin_edges, density=density_mode)
            y = counts  # count or density depending on mode

        y_smooth = smooth_fn(y)

        # If density & bins are non-uniform, preserve area approximately
        if density_mode and not uniform_bins:
            area_orig = (y * bin_width).sum()
            area_smooth = (y_smooth * bin_width).sum()
            if area_smooth > 0:
                y_smooth *= (area_orig / area_smooth)

        # Plot with x, y lengths guaranteed to match
        ax.plot(
            bin_centers,
            y_smooth,
            linewidth=line_width,
            alpha=line_alpha,
            color=color_map[coin],
            label=None,
            zorder=4,
        )

    ax.set_title(f"Pin Drop Latency: Histogram + Smoothed Lines (Blocks > {blocks_min})")
    ax.set_xlabel("Round Elapsed Time (s)")
    ylabel = {"density": "Density", "count": "Count", "probability": "Probability", "frequency": "Frequency"}.get(stat, stat.capitalize())
    ax.set_ylabel(ylabel)
    ax.set_xlim(left=0)
    sns.despine(ax=ax)
    fig.tight_layout()
    plt.show()


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_droplatency_hist_kde_by_coin(
    df: pd.DataFrame,
    *,
    blocks_min: int = 3,
    bins: int | str = "auto",
    stat: str = "density",
    common_norm: bool = False,
    palette: str | dict = "tab10",
    # dot options
    dot_mode: str = "panel",        # "panel" (separate axis) or "baseline" (thin band on main axis)
    dot_alpha: float = 0.6,
    dot_size: float = 12,
    dot_jitter: float = 0.15,       # vertical jitter for "panel" (in axis units) or fraction of y-range for "baseline"
    max_points_per_group: int | None = 4000,
):
    """
    dot_mode:
      - "panel": draw a small strip of dots on a second axis below the density plot (most readable).
      - "baseline": draw dots in a very thin band near y=0 on the main axis.
    """
    req = ["BlockNum", "BlockStatus", "truecontent_elapsed_s", "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for drop latency plot: {missing}")

    sdf = df.copy()
    sdf["truecontent_elapsed_s"] = pd.to_numeric(sdf["truecontent_elapsed_s"], errors="coerce")
    filt = (
        (sdf["BlockNum"] > blocks_min)
        & (sdf["BlockStatus"] == "complete")
        & sdf["coinLabel"].notna()
        & sdf["dropQual"].isin(["good", "bad"])
        & sdf["truecontent_elapsed_s"].notna()
    )
    dat = sdf.loc[filt, ["truecontent_elapsed_s", "coinLabel"]].reset_index(drop=True)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    hue_order = sorted(dat["coinLabel"].unique().tolist())
    if isinstance(palette, dict):
        color_map = {k: palette.get(k, "#333333") for k in hue_order}
    else:
        color_map = dict(zip(hue_order, sns.color_palette(palette, n_colors=len(hue_order))))

    sns.set_theme(style="whitegrid")

    if dot_mode == "panel":
        # Two stacked axes, shared x
        fig, (ax, ax_dots) = plt.subplots(
            2, 1, figsize=(12, 7),
            gridspec_kw={"height_ratios": [6, 1], "hspace": 0.05},
            sharex=True
        )
    else:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax_dots = None

    # Histogram
    sns.histplot(
        data=dat,
        x="truecontent_elapsed_s",
        hue="coinLabel",
        hue_order=hue_order,
        bins=bins,
        stat=stat,              # "count", "frequency", "probability", or "density"
        common_norm=common_norm,
        element="step",
        alpha=0.35,
        multiple="layer",
        ax=ax,
        palette=color_map,
        legend=True,
    )

    # KDE overlays
    sns.kdeplot(
        data=dat,
        x="truecontent_elapsed_s",
        hue="coinLabel",
        hue_order=hue_order,
        common_norm=common_norm,
        ax=ax,
        palette=color_map,
        lw=2,
        legend=False,
    )

    # === Real data points ===
    rng = np.random.default_rng(0)
    if dot_mode == "panel":
        ax_dots.set_ylim(-0.5, 0.5)
        ax_dots.axis("off")
        # one horizontal strip; jitter vertically for overplot reduction
        for coin, sub in dat.groupby("coinLabel", sort=False):
            x = sub["truecontent_elapsed_s"].to_numpy()
            if max_points_per_group is not None and len(x) > max_points_per_group:
                x = rng.choice(x, size=max_points_per_group, replace=False)
            y = rng.uniform(-dot_jitter, dot_jitter, size=len(x))
            ax_dots.scatter(
                x, y,
                s=dot_size,
                alpha=dot_alpha,
                color=color_map[coin],
                edgecolors="white",
                linewidths=0.4,
            )

    elif dot_mode == "baseline":
        # dots in a thin band near y=0 on the main axis
        y0, y1 = ax.get_ylim()
        band = (y1 - y0) * (dot_jitter if dot_jitter > 0 else 0.04)
        for coin, sub in dat.groupby("coinLabel", sort=False):
            x = sub["truecontent_elapsed_s"].to_numpy()
            if max_points_per_group is not None and len(x) > max_points_per_group:
                x = rng.choice(x, size=max_points_per_group, replace=False)
            y = rng.uniform(y0, y0 + band, size=len(x))
            ax.scatter(
                x, y,
                s=dot_size,
                alpha=dot_alpha,
                color=color_map[coin],
                edgecolors="white",
                linewidths=0.4,
                zorder=4,
            )
        ax.set_ylim(y0, y1)

    ax.set_title(f"Pin Drop Latency Distribution (Blocks > {blocks_min}) by Coin Type")
    ax.set_xlabel("Round Elapsed Time (s)")
    ax.set_ylabel("Density" if stat == "density" else stat.capitalize())
    ax.set_xlim(left=0)
    sns.despine(ax=ax)
    if ax_dots is not None:
        sns.despine(ax=ax_dots, left=True, bottom=True)
    fig.tight_layout()
    plt.show()


def plot_hist_kde_by_coin(
    df: pd.DataFrame,
    *,
    variableOfInterest: str = "truecontent_elapsed_s",
    blocks_min: int = 3,
    bins: int | str = "auto",
    stat: str = "density",
    common_norm: bool = False,
    palette: str | dict = "tab10",
    # dot options
    dot_mode: str = "panel",        # "panel" (separate axis) or "baseline" (thin band on main axis)
    dot_alpha: float = 0.6,
    dot_size: float = 12,
    dot_jitter: float = 0.15,       # vertical jitter for "panel" (in axis units) or fraction of y-range for "baseline"
    max_points_per_group: int | None = 4000,
):
    """
    dot_mode:
      - "panel": draw a small strip of dots on a second axis below the density plot (most readable).
      - "baseline": draw dots in a very thin band near y=0 on the main axis.
    """
    req = ["BlockNum", "BlockStatus", variableOfInterest, "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for {variableOfInterest} plot: {missing}")

    sdf = df.copy()
    sdf[variableOfInterest] = pd.to_numeric(sdf[variableOfInterest], errors="coerce")
    filt = (
        (sdf["BlockNum"] > blocks_min)
        & (sdf["BlockStatus"] == "complete")
        & sdf["coinLabel"].notna()
        & sdf["dropQual"].isin(["good", "bad"])
        & sdf[variableOfInterest].notna()
    )
    dat = sdf.loc[filt, [variableOfInterest, "coinLabel"]].reset_index(drop=True)
    if dat.empty:
        raise ValueError("No data left after filtering; cannot plot.")

    hue_order = sorted(dat["coinLabel"].unique().tolist())
    if isinstance(palette, dict):
        color_map = {k: palette.get(k, "#333333") for k in hue_order}
    else:
        color_map = dict(zip(hue_order, sns.color_palette(palette, n_colors=len(hue_order))))

    sns.set_theme(style="whitegrid")

    if dot_mode == "panel":
        # Two stacked axes, shared x
        fig, (ax, ax_dots) = plt.subplots(
            2, 1, figsize=(12, 7),
            gridspec_kw={"height_ratios": [6, 1], "hspace": 0.05},
            sharex=True
        )
    else:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax_dots = None

    # Histogram
    sns.histplot(
        data=dat,
        x=variableOfInterest,
        hue="coinLabel",
        hue_order=hue_order,
        bins=bins,
        stat=stat,              # "count", "frequency", "probability", or "density"
        common_norm=common_norm,
        element="step",
        alpha=0.35,
        multiple="layer",
        ax=ax,
        palette=color_map,
        legend=True,
    )

    # KDE overlays
    sns.kdeplot(
        data=dat,
        x=variableOfInterest,
        hue="coinLabel",
        hue_order=hue_order,
        common_norm=common_norm,
        ax=ax,
        palette=color_map,
        lw=2,
        legend=False,
    )

    # === Real data points ===
    rng = np.random.default_rng(0)
    if dot_mode == "panel":
        ax_dots.set_ylim(-0.5, 0.5)
        ax_dots.axis("off")
        # one horizontal strip; jitter vertically for overplot reduction
        for coin, sub in dat.groupby("coinLabel", sort=False):
            x = sub[variableOfInterest].to_numpy()
            if max_points_per_group is not None and len(x) > max_points_per_group:
                x = rng.choice(x, size=max_points_per_group, replace=False)
            y = rng.uniform(-dot_jitter, dot_jitter, size=len(x))
            ax_dots.scatter(
                x, y,
                s=dot_size,
                alpha=dot_alpha,
                color=color_map[coin],
                edgecolors="white",
                linewidths=0.4,
            )

    elif dot_mode == "baseline":
        # dots in a thin band near y=0 on the main axis
        y0, y1 = ax.get_ylim()
        band = (y1 - y0) * (dot_jitter if dot_jitter > 0 else 0.04)
        for coin, sub in dat.groupby("coinLabel", sort=False):
            x = sub[variableOfInterest].to_numpy()
            if max_points_per_group is not None and len(x) > max_points_per_group:
                x = rng.choice(x, size=max_points_per_group, replace=False)
            y = rng.uniform(y0, y0 + band, size=len(x))
            ax.scatter(
                x, y,
                s=dot_size,
                alpha=dot_alpha,
                color=color_map[coin],
                edgecolors="white",
                linewidths=0.4,
                zorder=4,
            )
        ax.set_ylim(y0, y1)

    ax.set_title(f"{variableOfInterest} Distribution (Blocks > {blocks_min}) by Coin Type")
    ax.set_xlabel(f"{variableOfInterest}")
    ax.set_ylabel("Density" if stat == "density" else stat.capitalize())
    ax.set_xlim(left=0)
    sns.despine(ax=ax)
    if ax_dots is not None:
        sns.despine(ax=ax_dots, left=True, bottom=True)
    fig.tight_layout()
    plt.show()

# Omnibus + pairwise tests for "are the latency distributions different by coin type?"

import numpy as np
import pandas as pd
from itertools import combinations
from collections import defaultdict

from scipy.stats import (
    anderson_ksamp,       # k-sample Anderson–Darling (omnibus, distributional)
    kruskal,              # Kruskal–Wallis (omnibus, location shift)
    ks_2samp,             # Kolmogorov–Smirnov (pairwise, distributional)
    mannwhitneyu,         # for effect size via A12 / Cliff's delta
)

try:
    from scipy.stats import epps_singleton_2samp  # pairwise, distributional
    _HAS_ES = True
except Exception:
    _HAS_ES = False


def _fdr_bh(pvals):
    """Benjamini–Hochberg FDR correction (returns array of q-values in original order)."""
    pvals = np.asarray(pvals, float)
    n = pvals.size
    order = np.argsort(pvals)
    ranked = pvals[order]
    q = np.empty_like(ranked)
    prev = 1.0
    for i in range(n - 1, -1, -1):
        rank = i + 1
        val = ranked[i] * n / rank
        prev = min(prev, val)
        q[i] = prev
    out = np.empty_like(q)
    out[order] = q
    return out


def _cliffs_delta(x, y):
    """
    Cliff's delta via Mann–Whitney U:
      delta = 2*A12 - 1, where A12 = U_greater / (n*m).
    Positive -> x tends to be larger than y.
    """
    x = np.asarray(x); y = np.asarray(y)
    n, m = len(x), len(y)
    if n == 0 or m == 0:
        return np.nan
    U_greater = mannwhitneyu(x, y, alternative="greater", method="asymptotic").statistic
    A12 = U_greater / (n * m)
    return 2 * A12 - 1


def test_coin_latency_distributions(
    df: pd.DataFrame,
    *,
    blocks_min: int = 3,
    min_n_per_group: int = 10,
    alpha: float = 0.05,
    verbose: bool = True,
):
    """
    Filters like your plots, then runs:
      - Omnibus (distributional): Anderson–Darling k-sample
      - Omnibus (location): Kruskal–Wallis
      - Pairwise (distributional): KS 2-sample (+ Epps–Singleton if available)
      - Effect size: Cliff's delta for each pair
    Returns a dict of results; prints a compact summary if verbose=True.
    """
    req = ["BlockNum", "BlockStatus", "truecontent_elapsed_s", "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    sdf = df.copy()
    sdf["truecontent_elapsed_s"] = pd.to_numeric(sdf["truecontent_elapsed_s"], errors="coerce")
    filt = (
        (sdf["BlockNum"] > blocks_min)
        & (sdf["BlockStatus"] == "complete")
        & sdf["coinLabel"].notna()
        & sdf["dropQual"].isin(["good", "bad"])
        & sdf["truecontent_elapsed_s"].notna()
    )
    dat = sdf.loc[filt, ["truecontent_elapsed_s", "coinLabel"]].reset_index(drop=True)
    if dat.empty:
        raise ValueError("No data after filtering.")

    groups = {k: v["truecontent_elapsed_s"].to_numpy()
              for k, v in dat.groupby("coinLabel")}
    # enforce min size
    groups = {k: x for k, x in groups.items() if len(x) >= min_n_per_group}
    if len(groups) < 2:
        raise ValueError("Need at least two coin types with sufficient data.")

    labels = sorted(groups.keys())
    samples = [groups[k] for k in labels]

    # Omnibus tests
    ad_res = anderson_ksamp(samples)  # statistic, critical_values, significance_level
    kw_res = kruskal(*samples)        # H, pvalue

    # Pairwise tests + effect sizes
    pair_rows = []
    for a, b in combinations(labels, 2):
        xa, xb = groups[a], groups[b]
        ks = ks_2samp(xa, xb, alternative="two-sided", method="auto")
        es_stat, es_p = (np.nan, np.nan)
        if _HAS_ES:
            try:
                es = epps_singleton_2samp(xa, xb)
                es_stat, es_p = es.statistic, es.pvalue
            except Exception:
                pass
        delta = _cliffs_delta(xa, xb)
        pair_rows.append({
            "A": a, "B": b,
            "n_A": len(xa), "n_B": len(xb),
            "KS_D": ks.statistic, "KS_p": ks.pvalue,
            "ES_stat": es_stat, "ES_p": es_p,
            "Cliffs_delta": delta,
        })

    pair_df = pd.DataFrame(pair_rows)
    # FDR for KS (and ES if present)
    pair_df["KS_q"] = _fdr_bh(pair_df["KS_p"].values)
    if _HAS_ES and pair_df["ES_p"].notna().any():
        es_mask = pair_df["ES_p"].notna()
        qs = np.full(len(pair_df), np.nan)
        qs[es_mask] = _fdr_bh(pair_df.loc[es_mask, "ES_p"].values)
        pair_df["ES_q"] = qs
    else:
        pair_df["ES_q"] = np.nan

    results = {
        "labels": labels,
        "sizes": {k: len(v) for k, v in groups.items()},
        "anderson_ksamp": {
            "statistic": float(ad_res.statistic),
            "significance_level": float(ad_res.significance_level),  # ≈ p-value (%)
        },
        "kruskal": {
            "H": float(kw_res.statistic),
            "pvalue": float(kw_res.pvalue),
        },
        "pairwise": pair_df.sort_values(["KS_q", "KS_p"], ignore_index=True),
        "alpha": alpha,
    }

    if verbose:
        print("== Omnibus tests ==")
        print(f"Anderson–Darling k-sample: A² = {ad_res.statistic:.3f}, approx p ≈ {ad_res.significance_level/100:.4f}")
        print(f"Kruskal–Wallis: H = {kw_res.statistic:.3f}, p = {kw_res.pvalue:.4g}")
        print("\n== Pairwise (Benjamini–Hochberg FDR on KS) ==")
        show_cols = ["A","B","n_A","n_B","KS_D","KS_p","KS_q","Cliffs_delta"]
        if _HAS_ES:
            show_cols += ["ES_stat","ES_p","ES_q"]
        print(pair_df[show_cols].to_string(index=False, float_format=lambda x: f"{x:.4g}"))
        print("\nCliff's δ thresholds (|δ|): small≈0.147, medium≈0.33, large≈0.474")

        # quick yes/no “consistent pattern” heuristic
        ad_p = ad_res.significance_level / 100.0
        kw_p = kw_res.pvalue
        sig_pairs = (pair_df["KS_q"] <= alpha).sum()
        total_pairs = len(pair_df)
        print(f"\nHeuristic summary:")
        if ad_p <= alpha or kw_p <= alpha:
            print(f"- Omnibus difference detected (AD p≈{ad_p:.4g} or KW p={kw_p:.4g}).")
        else:
            print(f"- No omnibus difference detected (AD p≈{ad_p:.4g}, KW p={kw_p:.4g}).")
        print(f"- Pairwise KS: {sig_pairs}/{total_pairs} significant at FDR q≤{alpha}.")
        strong = (pair_df["KS_q"] <= alpha) & (pair_df["Cliffs_delta"].abs() >= 0.33)
        if strong.any():
            print(f"- {strong.sum()} pair(s) show ≥medium effect (|δ|≥0.33).")

    return results


def test_coin_distributions(
    df: pd.DataFrame,
    *,
    variableOfInterest: str = "truecontent_elapsed_s",
    blocks_min: int = 3,
    min_n_per_group: int = 10,
    alpha: float = 0.05,
    verbose: bool = True,
):
    """
    Filters like your plots, then runs:
      - Omnibus (distributional): Anderson–Darling k-sample
      - Omnibus (location): Kruskal–Wallis
      - Pairwise (distributional): KS 2-sample (+ Epps–Singleton if available)
      - Effect size: Cliff's delta for each pair
    Returns a dict of results; prints a compact summary if verbose=True.
    """
    req = ["BlockNum", "BlockStatus", variableOfInterest, "coinLabel", "dropQual"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    sdf = df.copy()
    sdf[variableOfInterest] = pd.to_numeric(sdf[variableOfInterest], errors="coerce")
    filt = (
        (sdf["BlockNum"] > blocks_min)
        & (sdf["BlockStatus"] == "complete")
        & sdf["coinLabel"].notna()
        & sdf["dropQual"].isin(["good", "bad"])
        & sdf[variableOfInterest].notna()
    )
    dat = sdf.loc[filt, [variableOfInterest, "coinLabel"]].reset_index(drop=True)
    if dat.empty:
        raise ValueError("No data after filtering.")

    groups = {k: v[variableOfInterest].to_numpy()
              for k, v in dat.groupby("coinLabel")}
    # enforce min size
    groups = {k: x for k, x in groups.items() if len(x) >= min_n_per_group}
    if len(groups) < 2:
        raise ValueError("Need at least two coin types with sufficient data.")

    labels = sorted(groups.keys())
    samples = [groups[k] for k in labels]

    # Omnibus tests
    ad_res = anderson_ksamp(samples)  # statistic, critical_values, significance_level
    kw_res = kruskal(*samples)        # H, pvalue

    # Pairwise tests + effect sizes
    pair_rows = []
    for a, b in combinations(labels, 2):
        xa, xb = groups[a], groups[b]
        ks = ks_2samp(xa, xb, alternative="two-sided", method="auto")
        es_stat, es_p = (np.nan, np.nan)
        if _HAS_ES:
            try:
                es = epps_singleton_2samp(xa, xb)
                es_stat, es_p = es.statistic, es.pvalue
            except Exception:
                pass
        delta = _cliffs_delta(xa, xb)
        pair_rows.append({
            "A": a, "B": b,
            "n_A": len(xa), "n_B": len(xb),
            "KS_D": ks.statistic, "KS_p": ks.pvalue,
            "ES_stat": es_stat, "ES_p": es_p,
            "Cliffs_delta": delta,
        })

    pair_df = pd.DataFrame(pair_rows)
    # FDR for KS (and ES if present)
    pair_df["KS_q"] = _fdr_bh(pair_df["KS_p"].values)
    if _HAS_ES and pair_df["ES_p"].notna().any():
        es_mask = pair_df["ES_p"].notna()
        qs = np.full(len(pair_df), np.nan)
        qs[es_mask] = _fdr_bh(pair_df.loc[es_mask, "ES_p"].values)
        pair_df["ES_q"] = qs
    else:
        pair_df["ES_q"] = np.nan

    results = {
        "labels": labels,
        "sizes": {k: len(v) for k, v in groups.items()},
        "anderson_ksamp": {
            "statistic": float(ad_res.statistic),
            "significance_level": float(ad_res.significance_level),  # ≈ p-value (%)
        },
        "kruskal": {
            "H": float(kw_res.statistic),
            "pvalue": float(kw_res.pvalue),
        },
        "pairwise": pair_df.sort_values(["KS_q", "KS_p"], ignore_index=True),
        "alpha": alpha,
    }

    if verbose:
        print(f"{variableOfInterest} & Coin Type")
        print("== Omnibus tests ==")
        print(f"Anderson–Darling k-sample: A² = {ad_res.statistic:.3f}, approx p ≈ {ad_res.significance_level/100:.4f}")
        print(f"Kruskal–Wallis: H = {kw_res.statistic:.3f}, p = {kw_res.pvalue:.4g}")
        print("\n== Pairwise (Benjamini–Hochberg FDR on KS) ==")
        show_cols = ["A","B","n_A","n_B","KS_D","KS_p","KS_q","Cliffs_delta"]
        if _HAS_ES:
            show_cols += ["ES_stat","ES_p","ES_q"]
        print(pair_df[show_cols].to_string(index=False, float_format=lambda x: f"{x:.4g}"))
        print("\nCliff's δ thresholds (|δ|): small≈0.147, medium≈0.33, large≈0.474")

        # quick yes/no “consistent pattern” heuristic
        ad_p = ad_res.significance_level / 100.0
        kw_p = kw_res.pvalue
        sig_pairs = (pair_df["KS_q"] <= alpha).sum()
        total_pairs = len(pair_df)
        print(f"\nHeuristic summary:")
        if ad_p <= alpha or kw_p <= alpha:
            print(f"- Omnibus difference detected (AD p≈{ad_p:.4g} or KW p={kw_p:.4g}).")
        else:
            print(f"- No omnibus difference detected (AD p≈{ad_p:.4g}, KW p={kw_p:.4g}).")
        print(f"- Pairwise KS: {sig_pairs}/{total_pairs} significant at FDR q≤{alpha}.")
        strong = (pair_df["KS_q"] <= alpha) & (pair_df["Cliffs_delta"].abs() >= 0.33)
        if strong.any():
            print(f"- {strong.sum()} pair(s) show ≥medium effect (|δ|≥0.33).")

    return results
# -------------------------------
# Main
# -------------------------------
def main():
    df = read_data(CSV_PATH)
    # keep only rows with non-empty coin labels
    if "coinLabel" in df.columns:
        df = df[df["coinLabel"] != ""]
    
    df_elapsed = add_true_session_elapsed(df)  # adds 'trueSession_elapsed_s'
    df_elapsed = add_true_session_elapsed_by_block_events(df_elapsed)
    # Generate both plots
    plot_block3_round_vs_block(df_elapsed)
    plot_blocks_gt3_overall_vs_block(df_elapsed)
    plot_blocks_gt3_overall_vs_session(df_elapsed)
    plot_blocks_gt3_overall_vs_block_facet(df_elapsed, 20)
    plot_blocks_gt3_overall_vs_BlockNum(df_elapsed)
    # plot_dropdist_hist_kde_by_coin(df_elapsed)
    # plot_droplatency_hist_with_smoothed_lines_by_coin(df_elapsed)
    # plot_droplatency_hist_kde_by_coin(df_elapsed)
    plot_hist_kde_by_coin(df_elapsed, variableOfInterest="dropDist")
    plot_hist_kde_by_coin(df_elapsed, variableOfInterest="truecontent_elapsed_s")


    results = test_coin_latency_distributions(df_elapsed, blocks_min=3, min_n_per_group=10, alpha=0.05)
    #print('Stats for PinDrop Latency Distributions by Coin Type')
    print(results["pairwise"])  # pandas DataFrame with per-pair stats (KS, ES if available, and Cliff's delta)

    results2 = test_coin_distributions(df_elapsed, blocks_min=3, min_n_per_group=10, alpha=0.05, variableOfInterest="dropDist")
    #print('Stats for PinDrop Distance Distributions by Coin Type')
    print(results2["pairwise"])  # pandas DataFrame with per-pair stats (KS, ES if available, and Cliff's delta)

if __name__ == "__main__":
    main()
