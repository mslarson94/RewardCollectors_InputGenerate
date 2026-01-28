from __future__ import annotations
from pathlib import Path
from typing import Iterable, Sequence, Any
import re
import contextlib

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.lines import Line2D
from matplotlib.offsetbox import AnchoredText

# ---------- visual config ----------
MARKER_BY_COIN = {"HV": "*", "LV": "o", "NV": "o"}       # NV hollow
FILLED_BY_COIN = {"HV": True, "LV": True, "NV": False}
COLOR_BY_QUAL  = {"good": "blue", "bad": "red"}          # else -> gray
ALPHA = 0.5
SIZE  = 100

# ---------- convenience ----------
def log(msg: str) -> None:
    print(msg, flush=True)

def _slugify(x: Any, maxlen: int = 64) -> str:
    s = re.sub(r"\W+", "_", str(x)).strip("_")
    return (s[:maxlen] or "NA")

def _ensure_dirs(*paths: Path) -> None:
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)

# ---------- IO / data prep ----------
def read_data(path: Path | str) -> pd.DataFrame:
    path = Path(path)
    df = pd.read_csv(path)
    if "coinLabel" in df.columns:
        df["coinLabel"] = df["coinLabel"].astype(str).str.strip()
    if "dropQual" in df.columns:
        df["dropQual"] = df["dropQual"].astype(str).str.strip().str.lower()
    if "mLTimestamp" in df.columns:
        df["mLTimestamp"] = pd.to_datetime(df["mLTimestamp"], errors="coerce")
    for c in ("BlockNum", "RoundNum"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def add_true_session_elapsed(df: pd.DataFrame,
                             source_col: str = "truecontent_elapsed_s",
                             out_col: str = "trueSession_elapsed_s") -> pd.DataFrame:
    t = pd.to_numeric(df.get(source_col), errors="coerce")
    prev = t.shift(1)
    inside = t.notna()
    continuing = inside & prev.notna() & (t >= prev)
    starting   = inside & ~continuing
    delta = pd.Series(0.0, index=df.index)
    delta.loc[continuing] = (t - prev).loc[continuing]
    delta.loc[starting]   = t.loc[starting].fillna(0.0)
    df[out_col] = delta.cumsum()
    return df

def add_true_session_elapsed_by_block_events(
    df: pd.DataFrame,
    source_col: str = "truecontent_elapsed_s",
    event_col: str = "lo_eventType",
    start_token: str = "BlockStart",
    end_token: str = "BlockEnd",
    out_col: str = "trueSession_block_elapsed_s",
    include_end_row: bool = False,
) -> pd.DataFrame:
    if event_col not in df.columns:
        return df
    t = pd.to_numeric(df[source_col], errors="coerce")
    ev = df[event_col].astype(str)
    is_start, is_end = ev.eq(start_token), ev.eq(end_token)
    starts_cum, ends_cum = is_start.cumsum(), is_end.cumsum()
    in_block = starts_cum.gt(ends_cum) if not include_end_row else starts_cum.ge(ends_cum)
    block_id = starts_cum.where(in_block)
    prev = t.groupby(block_id).shift(1)
    inside = in_block & t.notna()
    continuing = inside & prev.notna() & (t >= prev)
    starting   = inside & ~continuing
    delta = pd.Series(0.0, index=df.index, dtype="float64")
    delta.loc[continuing] = (t - prev).loc[continuing]
    delta.loc[starting]   = t.loc[starting].fillna(0.0)
    df[out_col] = delta.groupby(block_id).cumsum()
    return df

def exclude_outliers(
    df: pd.DataFrame,
    column: str,
    *,
    method: str = "median",
    sigma: float = 2.0,
    ddof: int = 1,
    groupby: str | list[str] | None = None,
    keep_na: bool = False,
) -> pd.DataFrame:
    if column not in df.columns:
        return df
    x = pd.to_numeric(df[column], errors="coerce")
    if groupby is None:
        center = pd.Series((x.mean() if method == "mean" else x.median()), index=df.index)
        scale  = pd.Series(x.std(ddof=ddof), index=df.index)
    else:
        center = df.groupby(groupby)[column].transform(lambda s: pd.to_numeric(s, errors="coerce").agg(method))
        scale  = df.groupby(groupby)[column].transform(lambda s: pd.to_numeric(s, errors="coerce").std(ddof=ddof))
    scale = scale.replace(0, np.nan)
    z = (x - center).abs() / scale
    mask = (z <= sigma) | z.isna()
    if not keep_na:
        mask &= x.notna()
    return df.loc[mask].copy()

def _prep_df_for_file(df: pd.DataFrame,
                      *,
                      use_outlier_filter: bool = False,
                      filter_columns: Iterable[str] = ("truecontent_elapsed_s", "dropDist"),
                      outlier_groupby: str | list[str] | None = "coinLabel",
                      outlier_method: str = "median",
                      outlier_sigma: float = 2.0,
                      outlier_ddof: int = 1,
                      outlier_keep_na: bool = False) -> pd.DataFrame:
    df = df.copy()
    if "coinLabel" in df.columns:
        df["coinLabel"] = df["coinLabel"].astype(str)
    if use_outlier_filter:
        for col in filter_columns:
            if col in df.columns:
                df = exclude_outliers(
                    df, col,
                    method=outlier_method,
                    sigma=outlier_sigma,
                    ddof=outlier_ddof,
                    groupby=outlier_groupby,
                    keep_na=outlier_keep_na,
                )
    df = add_true_session_elapsed(df)
    df = add_true_session_elapsed_by_block_events(df)
    return df

def _opt_mask(df: pd.DataFrame, col: str, *, values: Iterable[str] | None = None) -> pd.Series:
    """If `col` exists, return a mask for (values or non-NaN); else all True."""
    if col not in df.columns:
        return pd.Series(True, index=df.index)
    s = df[col]
    if values is None:
        return s.notna()
    s_str = s.astype(str).str.lower()
    vals = [str(v).lower() for v in values]
    return s_str.isin(vals)

def _safe_count(df: pd.DataFrame, mask: pd.Series) -> int:
    try:
        return int(mask.sum())
    except Exception:
        return 0

# ---------- subtitles ----------
def _subtitle_from(frame: pd.DataFrame) -> str:
    parts = []
    for label, col in (("Pair", "pairID"),
                       ("Participant", "participantID"),
                       ("Coin Set", "coinSet")):
        if col in frame.columns:
            vals = pd.unique(frame[col].dropna())
            if len(vals) == 1:
                parts.append(f"{label} {vals[0]}")
            elif len(vals) > 1:
                parts.append(f"{label} {len(vals)} values")
    return " | ".join(parts)

# ---------- plotting helpers ----------
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
    qual_handles = [
        Line2D([0], [0], marker="s", linestyle="None", markersize=10,
               markerfacecolor=COLOR_BY_QUAL["good"], markeredgecolor=COLOR_BY_QUAL["good"], label="good"),
        Line2D([0], [0], marker="s", linestyle="None", markersize=10,
               markerfacecolor=COLOR_BY_QUAL["bad"], markeredgecolor=COLOR_BY_QUAL["bad"], label="bad"),
    ]
    ax.legend(handles=qual_handles, title="Drop Quality", loc="upper right")

def make_axes_with_dots(
    *,
    dot_mode: str = "panel",
    figsize: tuple[float, float] = (12, 7),
    height_ratios: tuple[int, int] = (6, 1),
    hspace: float = 0.40,
):
    """Create (fig, ax_main, ax_dots) sharing x; if dot_mode!='panel', ax_dots=None."""
    if dot_mode == "panel":
        fig, (ax, ax_dots) = plt.subplots(
            2, 1,
            figsize=figsize,
            sharex=True,
            gridspec_kw={"height_ratios": list(height_ratios), "hspace": hspace},
        )
        return fig, ax, ax_dots
    else:
        fig, ax = plt.subplots(figsize=(figsize[0], max(2.0, figsize[1] - 1.0)))
        return fig, ax, None

def draw_dots_strip(
    ax_dots: plt.Axes,
    dat: pd.DataFrame,
    *,
    x_col: str,
    group_col: str = "coinLabel",
    color_map: dict[str, str],
    dot_size: float = 12,
    dot_alpha: float = 0.6,
    dot_jitter: float = 0.15,
    max_points_per_group: int | None = 4000,
    rng: np.random.Generator | None = None,
):
    """Render a thin jittered strip of points by group on ax_dots."""
    if ax_dots is None:
        return
    if rng is None:
        rng = np.random.default_rng(0)

    ax_dots.set_ylim(-0.5, 0.5)
    ax_dots.set_yticks([]); ax_dots.set_ylabel(""); ax_dots.grid(False)
    for s in ("top", "left", "right"):
        ax_dots.spines[s].set_visible(False)

    for grp, sub in dat.groupby(group_col, sort=False):
        x = pd.to_numeric(sub[x_col], errors="coerce").dropna().to_numpy()
        if max_points_per_group is not None and x.size > max_points_per_group:
            x = rng.choice(x, size=max_points_per_group, replace=False)
        y = rng.uniform(-dot_jitter, dot_jitter, size=x.size)
        ax_dots.scatter(
            x, y,
            s=dot_size, alpha=dot_alpha,
            color=color_map.get(grp, "0.3"),
            edgecolors="white", linewidths=0.4,
        )

def style_x_for_main_and_dots(
    ax_main: plt.Axes,
    ax_dots: plt.Axes | None,
    *,
    main_xlabel: str,
    dots_label_top: str = "Observed points (jittered)",
    main_labelpad: float = 6.0,
    dots_top_pad: float = 8.0,
    despine: bool = True,
    dots_frame: bool = False,
    frame_lw: float = 1.2,
    frame_color: str = "0.2",
):
    if ax_dots is None:
        ax_main.set_xlabel(main_xlabel, labelpad=main_labelpad)
        if despine:
            sns.despine(ax=ax_main)
        return

    ax_main.tick_params(axis="x", which="both", labelbottom=True)
    ax_main.set_xlabel(main_xlabel, labelpad=main_labelpad)

    ax_dots.tick_params(axis="x", which="both", labelbottom=False)
    ax_dots.xaxis.set_label_position("top")
    ax_dots.set_xlabel(dots_label_top, labelpad=dots_top_pad)

    if despine:
        sns.despine(ax=ax_main)

    if dots_frame:
        for side in ("top", "right", "bottom", "left"):
            sp = ax_dots.spines[side]
            sp.set_visible(True)
            sp.set_linewidth(frame_lw)
            sp.set_edgecolor(frame_color)
    else:
        sns.despine(ax=ax_dots, left=True)  # keep bottom spine only


# --- in histoHelpers.py ---

from typing import Sequence, Callable, Any
import contextlib
import matplotlib.pyplot as plt
from pathlib import Path

PlotFn = Callable[..., Any]

def _run_and_collect_figs(fn: PlotFn, *args, **kwargs) -> list[plt.Figure]:
    """Run a plotting function that calls plt.show() internally and capture the new figures it created."""
    before = set(plt.get_fignums())
    _orig_show = plt.show
    try:
        plt.show = lambda *a, **k: None  # suppress interactive window
        fn(*args, **kwargs)
    except Exception as e:
        print(f"[plot] {fn.__name__} skipped: {e}", flush=True)
        return []
    finally:
        plt.show = _orig_show
    after = set(plt.get_fignums())
    new_nums = sorted(after - before)
    return [plt.figure(n) for n in new_nums]

def _save_figs(
    figs: Sequence[plt.Figure],
    *,
    common_dir: Path,
    per_file_dir: Path,
    stem: str,
    tag: str,
    formats: Sequence[str] = ("png", "pdf"),
    dpi: int = 220,
    save_to_common: bool = True,   # optional flag if you want it
) -> None:
    common_dir.mkdir(parents=True, exist_ok=True)
    per_file_dir.mkdir(parents=True, exist_ok=True)
    multi = len(figs) > 1
    for i, fig in enumerate(figs, 1):
        suffix = f"_p{i:02d}" if multi else ""
        base_common = f"{stem}__{tag}{suffix}"
        base_specific = f"{tag}{suffix}"
        with contextlib.suppress(Exception):
            fig.tight_layout()
        for ext in formats:
            fig.savefig(per_file_dir / f"{base_specific}.{ext}", dpi=dpi, bbox_inches="tight")
            if save_to_common:
                fig.savefig(common_dir / f"{base_common}.{ext}", dpi=dpi, bbox_inches="tight")
    plt.close("all")


# --- Shared wrapper helpers (safe to re-add if not present) ---
from pathlib import Path
import contextlib
import matplotlib.pyplot as plt
from typing import Iterable, Sequence, Callable, Any
import re

PlotFn = Callable[..., Any]

def _ensure_dirs(*paths: Path) -> None:
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)

def _run_and_collect_figs(fn: PlotFn, *args, **kwargs) -> list[plt.Figure]:
    before = set(plt.get_fignums())
    _orig_show = plt.show
    try:
        plt.show = lambda *a, **k: None
        fn(*args, **kwargs)
    except Exception as e:
        print(f"[plot] {fn.__name__} skipped: {e}", flush=True)
        return []
    finally:
        plt.show = _orig_show
    after = set(plt.get_fignums())
    new_nums = sorted(after - before)
    return [plt.figure(n) for n in new_nums]

def _save_figs(
    figs: Sequence[plt.Figure],
    *,
    common_dir: Path,
    per_file_dir: Path,
    stem: str,
    tag: str,
    formats: Iterable[str] = ("png", "pdf"),
    dpi: int = 220,
    write_common: bool = True,   # you can toggle whether to also write to common_dir
) -> None:
    _ensure_dirs(common_dir, per_file_dir)
    multi = len(figs) > 1
    for i, fig in enumerate(figs, 1):
        suffix = f"_p{i:02d}" if multi else ""
        base_common   = f"{stem}__{tag}{suffix}"
        base_specific = f"{tag}{suffix}"
        with contextlib.suppress(Exception):
            fig.tight_layout()
        for ext in formats:
            # always save into the per-file dir
            fig.savefig(Path(per_file_dir) / f"{base_specific}.{ext}", dpi=dpi, bbox_inches="tight")
            # optionally also mirror into the shared _ALL dir
            if write_common:
                fig.savefig(Path(common_dir) / f"{base_common}.{ext}", dpi=dpi, bbox_inches="tight")
    plt.close("all")

def _slugify(x: Any, maxlen: int = 64) -> str:
    s = re.sub(r"\W+", "_", str(x)).strip("_")
    return (s[:maxlen] or "NA")


def annotate_hist_bins(
    ax: plt.Axes,
    data: pd.DataFrame,
    *,
    x: str,
    bins: int | str | Sequence = "auto",
    hue: str | None = None,
    stat: str = "count",
    fmt: str = "{label}: n={n}, μ={mean:.2g}, σ={std:.2g}, bin≈{bw:.2g}",
    loc: str = "upper left",
    pad: float = 0.2,
    framealpha: float = 0.2,
    fontsize: int = 9,
    ) -> None:
    """
    Annotate a histogram with bin edges/width and basic stats per hue level.
    Why: reproducible annotation independent of seaborn internals.
    bin: (Seaborn/NumPy arg): how to place the bin edges — can be an int, a rule ("auto"), or an explicit edge array.
    """
    if x not in data.columns:
        return
    # common bin edges across all data
    x_all = pd.to_numeric(data[x], errors="coerce").dropna().to_numpy()
    if x_all.size == 0:
        return
    edges = np.histogram_bin_edges(x_all, bins=bins)
    bw = np.diff(edges)
    # stable single width if uniform; else show median width
    bw_val = (bw[0] if np.allclose(bw, bw[0]) else np.median(bw))

    rows: list[str] = []
    groups = [(None, data)] if not hue or hue not in data.columns else data.groupby(hue, sort=True)
    for glabel, gdf in groups:
        xg = pd.to_numeric(gdf[x], errors="coerce").dropna().to_numpy()
        if xg.size == 0:
            continue
        counts, _ = np.histogram(xg, bins=edges)
        if stat == "density":
            # normalize by total area under histogram (sum count*width)
            area = (counts * bw).sum()
            # fall back to count if degenerate
            sn = f"{area:.2g}" if area > 0 else f"{xg.size}"
        else:
            sn = f"{xg.size}"
        label = str(glabel) if glabel is not None else "All"
        row = fmt.format(
            label=label,
            n=xg.size,
            mean=np.nanmean(xg),
            median=np.nanmedian(xg),
            std=np.nanstd(xg, ddof=1) if xg.size > 1 else 0.0,
            bw=bw_val,
        )
        rows.append(row)

    if not rows:
        return

    txt = "\n".join(rows)
    box = AnchoredText(txt, loc=loc, prop=dict(size=fontsize), pad=pad, frameon=True)
    box.patch.set_alpha(framealpha)
    ax.add_artist(box)


def _freedman_diaconis_width(x: np.ndarray) -> float:
    """FD rule; falls back to Scott when IQR=0. Why: robust to outliers."""
    x = np.asarray(x)
    x = x[np.isfinite(x)]
    if x.size < 2:
        return 0.0
    q75, q25 = np.percentile(x, [75, 25])
    iqr = q75 - q25
    if iqr <= 0:
        # Scott's rule fallback
        sd = np.nanstd(x, ddof=1) if x.size > 1 else 0.0
        return 3.49 * sd / (x.size ** (1/3)) if sd > 0 else 0.0
    return 2 * iqr / (x.size ** (1/3))

def _collect_numeric(df: pd.DataFrame, col: str) -> np.ndarray:
    """Numeric vector with NaNs/inf removed."""
    v = pd.to_numeric(df[col], errors="coerce")
    a = v.to_numpy()
    return a[np.isfinite(a)]

def compute_fixed_bin_edges(
    dfs: list[pd.DataFrame] | pd.DataFrame,
    *,
    x: str,
    bin_width: float | None = None,
    min_bins: int = 8,
    pad_frac: float = 0.02,
) -> np.ndarray:
    """
    Build one global set of bin edges for variable `x` across all provided dataframes.
    If `bin_width` None: use Freedman–Diaconis on the concatenated data.
    bin_width: a single scalar width. We compute one global edge array using this width and feed it into bins = so all plots align (for apples to apples comparison)
    """
    if isinstance(dfs, pd.DataFrame):
        dfs = [dfs]
    chunks: list[np.ndarray] = []
    for df in dfs:
        if x in df.columns:
            arr = _collect_numeric(df, x)
            if arr.size:
                chunks.append(arr)
    if not chunks:
        raise ValueError(f"No numeric data for '{x}' to compute bin edges.")
    x_all = np.concatenate(chunks)
    xmin, xmax = float(np.min(x_all)), float(np.max(x_all))
    if not np.isfinite(xmin) or not np.isfinite(xmax) or xmin == xmax:
        xmin, xmax = xmin - 0.5, xmax + 0.5

    span = xmax - xmin
    xmin -= pad_frac * span
    xmax += pad_frac * span

    width = float(bin_width) if (bin_width is not None and bin_width > 0) else _freedman_diaconis_width(x_all)
    if not np.isfinite(width) or width <= 0:
        width = span / max(min_bins, 1)

    nbins = max(int(np.ceil((xmax - xmin) / width)), min_bins)
    width = (xmax - xmin) / nbins
    edges = np.arange(xmin, xmax + width * 1.0000001, width)
    return edges

def coerce_xlim(
    xlim: tuple[float, float] | None,
    edges: np.ndarray | None,
) -> tuple[float, float] | None:
    """
    Prefer explicit xlim; else derive from edges; else None.
    Why: consistent axes across plots for apples-to-apples viewing.
    """
    if xlim is not None and len(xlim) == 2 and all(np.isfinite(xlim)):
        lo, hi = float(xlim[0]), float(xlim[1])
        if lo == hi:
            hi = lo + 1.0
        return (min(lo, hi), max(lo, hi))
    if edges is not None and len(edges) > 1:
        return (float(edges[0]), float(edges[-1]))
    return None