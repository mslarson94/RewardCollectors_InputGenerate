import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# ========= USER PATHS =========
INPUT_DIR  = Path("/Users/mairahmac/Desktop/RC_TestingNotes/ResurrectedData/pin_drops")
OUTPUT_DIR = Path("/Users/mairahmac/Desktop/RC_TestingNotes/ResurrectedData/pin_drops_plots_meanSD_brokenXY")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ========= SETTINGS =========
BIN_MINUTES        = 2        # bin size for means/SD
GAP_MIN_MINUTES    = 20       # split into a new x-panel if gap >= this
MAX_X_PANELS       = 3        # cap number of x-panels; tail is merged if many
USE_Y_BREAK        = True     # y-axis break for outliers
Y_BREAK_QUANTILE   = 0.95     # quantile to split lower vs outlier panel
BREAK_GAP_MIN      = 0.6      # require at least this gap (max - p95) to break
THRESHOLD_LINE     = 1.1      # dashed horizontal reference
Y_PAD_FRAC_LOW     = 0.05     # y padding lower panel
Y_PAD_FRAC_HIGH    = 0.05     # y padding upper panel

COIN_LABEL_CANDIDATES = ["coinLabel", "valueTag", "CoinRegistry"]
MARKERS = {"HV": "*", "LV": "o", "NV": "o"}
COLORS  = {"HV": "green", "LV": "yellow", "NV": "gray"}

def pick_coin_label_column(df: pd.DataFrame) -> str | None:
    for c in COIN_LABEL_CANDIDATES:
        if c in df.columns:
            return c
    return None

def bin_means_std(frame: pd.DataFrame, time_col: str, value_col: str, bin_minutes: int) -> pd.DataFrame:
    """Return columns: t_center_min, mean, std (one row per bin)."""
    if frame.empty:
        return pd.DataFrame(columns=["t_center_min", "mean", "std"])
    bins = np.floor(frame[time_col] / bin_minutes).astype(int)
    g = frame.groupby(bins)[value_col]
    out = g.agg(["mean", "std"]).reset_index(drop=True)
    out["t_center_min"] = (out.index + 0.5) * bin_minutes
    return out[["t_center_min", "mean", "std"]]

def plot_stats(ax, stats: pd.DataFrame, color: str, lw: float, label: str | None = None, alpha: float = 0.15):
    """Plot precomputed stats dataframe with columns t_center_min, mean, std."""
    if stats is None or stats.empty:
        return
    ax.plot(stats["t_center_min"], stats["mean"], color=color, lw=lw, label=label)
    ax.fill_between(stats["t_center_min"], stats["mean"] - stats["std"], stats["mean"] + stats["std"],
                    color=color, alpha=alpha)

def split_by_gaps(df: pd.DataFrame, time_col: str, gap_minutes: float, max_panels: int):
    """Return list of (start_idx, end_idx) for contiguous x-segments based on time gaps."""
    if df.empty:
        return []
    t = df[time_col].to_numpy()
    cut = np.where(np.diff(t) >= gap_minutes)[0]
    starts = [0] + (cut + 1).tolist()
    ends   = cut.tolist() + [len(df) - 1]
    segments = list(zip(starts, ends))
    if len(segments) > max_panels:
        head = segments[:max_panels-1]
        last = (segments[max_panels-1][0], segments[-1][1])
        return head + [last]
    return segments

for file in INPUT_DIR.glob("*_pin_drops.csv"):
    try:
        df = pd.read_csv(file, parse_dates=["ParsedTimestamp"])
    except Exception as e:
        print(f"Skipping {file.name}: read error → {e}")
        continue

    req_cols = {"ParsedTimestamp", "dropDist", "coinSet"}
    if not req_cols.issubset(df.columns):
        print(f"Skipping {file.name}: missing {req_cols - set(df.columns)}")
        continue

    coin_col = pick_coin_label_column(df)
    if coin_col is None:
        print(f"Skipping {file.name}: no coin label column found in {COIN_LABEL_CANDIDATES}")
        continue

    participant = df["participantID"].iloc[0] if "participantID" in df.columns else file.stem.split("_")[0]

    for coin_set in df["coinSet"].dropna().unique():
        sub = df[df["coinSet"] == coin_set].copy()
        if sub.empty:
            continue

        # Sort and compute minutes since first drop
        sub = sub.sort_values("ParsedTimestamp")
        t0 = sub["ParsedTimestamp"].iloc[0]
        sub["TimeSinceStart_min"] = (sub["ParsedTimestamp"] - t0).dt.total_seconds() / 60.0

        # Adaptive y-break detection (global for this participant × coinSet)
        y = sub["dropDist"].astype(float).to_numpy()
        y_min, y_max = np.nanmin(y), np.nanmax(y)
        p95 = np.nanpercentile(y, Y_BREAK_QUANTILE * 100)
        do_y_break = USE_Y_BREAK and ((y_max - p95) >= BREAK_GAP_MIN)
        y_break = p95

        # Split into x-panels
        x_segments = split_by_gaps(sub, "TimeSinceStart_min", GAP_MIN_MINUTES, MAX_X_PANELS)
        if not x_segments:
            continue

        # Create axes grid: rows = 2 if y-break, else 1; cols = number of segments
        n_rows = 2 if do_y_break else 1
        n_cols = len(x_segments)
        fig, axes = plt.subplots(n_rows, n_cols, sharey=(n_rows == 1),
                                 figsize=(12, 4.8 if n_rows == 1 else 6.4))
        axes = np.atleast_2d(axes)

        # Establish y-limits
        if do_y_break:
            low_mask = y <= y_break
            hi_mask  = y >  y_break
            if np.any(low_mask):
                low_min = min(0, np.nanmin(y[low_mask]))
                low_max = max(y_break, np.nanmax(y[low_mask]))
            else:
                low_min, low_max = 0, y_break
            pad_low = (low_max - low_min) if low_max > low_min else 1.0
            low_ylim = (low_min - Y_PAD_FRAC_LOW*pad_low, low_max + Y_PAD_FRAC_LOW*pad_low)

            if np.any(hi_mask):
                up_min = np.nanmin(y[hi_mask]); up_max = np.nanmax(y[hi_mask])
            else:
                up_min, up_max = y_break, y_break + 1.0
            pad_up = (up_max - up_min) if up_max > up_min else 1.0
            up_ylim = (up_min - Y_PAD_FRAC_HIGH*pad_up, up_max + Y_PAD_FRAC_HIGH*pad_up)
        else:
            pad = (y_max - y_min) if y_max > y_min else 1.0
            low_ylim = (max(0, y_min - Y_PAD_FRAC_LOW*pad), y_max + Y_PAD_FRAC_LOW*pad)

        # Plot each x-segment
        for ci, (s_idx, e_idx) in enumerate(x_segments):
            seg = sub.iloc[s_idx:e_idx+1].copy()
            xmin, xmax = seg["TimeSinceStart_min"].min(), seg["TimeSinceStart_min"].max()
            xspan = xmax - xmin if xmax > xmin else 1.0

            # Axes: bottom row always exists; top row exists if y-break
            ax_low  = axes[0, ci] if not do_y_break else axes[1, ci]
            ax_high = None if not do_y_break else axes[0, ci]

            # ---- Scatter by coin type ----
            for c in ["HV", "LV", "NV"]:
                pts = seg[seg[coin_col] == c]
                if pts.empty: continue
                if do_y_break:
                    low_pts = pts[pts["dropDist"] <= y_break]
                    hi_pts  = pts[pts["dropDist"] >  y_break]
                    if not low_pts.empty:
                        ax_low.scatter(low_pts["TimeSinceStart_min"], low_pts["dropDist"],
                                       marker=MARKERS[c], c=COLORS[c], s=25, alpha=0.5,
                                       label=f"{c} points" if (ci == 0) else None)
                    if not hi_pts.empty:
                        ax_high.scatter(hi_pts["TimeSinceStart_min"], hi_pts["dropDist"],
                                        marker=MARKERS[c], c=COLORS[c], s=25, alpha=0.5)
                else:
                    ax_low.scatter(pts["TimeSinceStart_min"], pts["dropDist"],
                                   marker=MARKERS[c], c=COLORS[c], s=25, alpha=0.5,
                                   label=f"{c} points" if (ci == 0) else None)

            # ---- Means ± SD (ALL + per-coin) for this segment ----
            stats_all = bin_means_std(seg, "TimeSinceStart_min", "dropDist", BIN_MINUTES)
            if do_y_break:
                low_all = stats_all.copy(); hi_all = stats_all.copy()
                low_all.loc[low_all["mean"] > y_break, ["mean","std"]] = np.nan
                hi_all.loc[hi_all["mean"] <= y_break, ["mean","std"]] = np.nan
                plot_stats(ax_low,  low_all.dropna(), "black", 2.5, label=("ALL mean" if ci==0 else None), alpha=0.15)
                plot_stats(ax_high, hi_all.dropna(), "black", 2.5, alpha=0.15)
            else:
                plot_stats(ax_low, stats_all, "black", 2.5, label=("ALL mean" if ci==0 else None), alpha=0.15)

            for c in ["HV", "LV", "NV"]:
                segc = seg[seg[coin_col] == c]
                if segc.empty: continue
                st = bin_means_std(segc, "TimeSinceStart_min", "dropDist", BIN_MINUTES)
                if do_y_break:
                    low_st = st.copy(); hi_st = st.copy()
                    low_st.loc[low_st["mean"] > y_break, ["mean","std"]] = np.nan
                    hi_st.loc[hi_st["mean"] <= y_break, ["mean","std"]] = np.nan
                    plot_stats(ax_low,  low_st.dropna(), COLORS[c], 1.6, label=(f"{c} mean" if ci==0 else None), alpha=0.12)
                    plot_stats(ax_high, hi_st.dropna(), COLORS[c], 1.6, alpha=0.12)
                else:
                    plot_stats(ax_low, st, COLORS[c], 1.6, label=(f"{c} mean" if ci==0 else None), alpha=0.12)

            # ---- Threshold lines & limits ----
            if do_y_break:
                if THRESHOLD_LINE <= y_break: ax_low.axhline(THRESHOLD_LINE, color="red", linestyle="--", lw=1)
                else:                          ax_high.axhline(THRESHOLD_LINE, color="red", linestyle="--", lw=1)
                ax_high.set_ylim(*up_ylim); ax_low.set_ylim(*low_ylim)
                ax_high.spines['bottom'].set_visible(False)
                ax_low.spines['top'].set_visible(False)
                ax_high.tick_params(labelbottom=False)
            else:
                ax_low.axhline(THRESHOLD_LINE, color="red", linestyle="--", lw=1)
                ax_low.set_ylim(*low_ylim)

            # x-lims tight per segment
            ax_low.set_xlim(xmin - 0.02*xspan, xmax + 0.02*xspan)
            if do_y_break:
                ax_high.set_xlim(xmin - 0.02*xspan, xmax + 0.02*xspan)

            # diagonal x-break marks between columns
            if ci < n_cols - 1:
                d = .015
                if do_y_break:
                    for ax in (ax_high, ax_low):
                        kwargs = dict(transform=ax.transAxes, color='k', clip_on=False)
                        ax.plot((1 - d, 1 + d), (-d, +d), **kwargs)
                        ax.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)
                else:
                    kwargs = dict(transform=axes[0, ci].transAxes, color='k', clip_on=False)
                    axes[0, ci].plot((1 - d, 1 + d), (-d, +d), **kwargs)
                    axes[0, ci].plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)

            if (not do_y_break and ci == 0) or (do_y_break and ci == 0):
                ax_low.set_ylabel("Pin Distance (dropDist)")

        # shared x label
        (axes[1, 0] if do_y_break else axes[0, 0]).set_xlabel("Minutes Since First Drop")

        # Title
        axes[0, 0].set_title(
            f"Pin Distance Over Time (means ± SD, {BIN_MINUTES}m bins)\n"
            f"Participant: {participant} · CoinSet: {coin_set}"
        )

        # Legend outside on the right
        first_ax = axes[1, 0] if do_y_break else axes[0, 0]
        handles, labels = first_ax.get_legend_handles_labels()
        if handles:
            fig = first_ax.get_figure()
            fig.legend(handles, labels, loc="center left",
                       bbox_to_anchor=(1.02, 0.5), borderaxespad=0, frameon=False)
            fig.tight_layout(rect=[0, 0, 0.85, 0.93])
        else:
            fig = first_ax.get_figure()
            fig.tight_layout(rect=[0, 0, 1, 0.93])

        out = OUTPUT_DIR / f"{participant}_CoinSet{coin_set}_meanSD_brokenXY.png"
        fig.savefig(out, dpi=200)
        plt.close(fig)
        print(f"✅ Saved {out}")
