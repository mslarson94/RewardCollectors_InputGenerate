import pandas as pd
import matplotlib.pyplot as plt
import random
import numpy as np
from itertools import permutations
from matplotlib.colors import ListedColormap

# Simulate participants with uneven trial counts
participants_uneven = {
    'P01': 103,
    'P02': 88,
    'P03': 76,
    'P04': 95,
    'P05': 65
}

# All possible orders (6 permutations)
all_orders = list(permutations(['HV', 'LV', 'NV']))

# Correct mapping: each permutation appears exactly once
order_map = {
    ('HV', 'LV', 'NV'): 'Type 1',
    ('HV', 'NV', 'LV'): 'Type 2',
    ('LV', 'HV', 'NV'): 'Type 3',
    ('LV', 'NV', 'HV'): 'Type 4',
    ('NV', 'HV', 'LV'): 'Type 5',
    ('NV', 'LV', 'HV'): 'Type 6',
}

category_palette = {
    'Type 1': '#66c2a5',
    'Type 2': '#e78ac3',
    'Type 3': '#fc8d62',
    'Type 4': '#8da0cb',
    'Type 5': '#a6d854',
    'Type 6': '#ffd92f',
    'Other': '#999999',     # safe fallback
    'No Trial': '#d3d3d3',  # padding
}

# Simulate data
uneven_dummy_data = []
for pid, n_trials in participants_uneven.items():
    for trial in range(1, n_trials + 1):
        order = random.choice(all_orders)
        label = order_map.get(order, 'Other')  # should never hit 'Other' now, but safe
        uneven_dummy_data.append({
            'Participant': pid,
            'Trial': trial,
            'Category': label
        })

df_uneven = pd.DataFrame(uneven_dummy_data)

# # Pivot and fill missing values
# df_runplot_un = df_uneven.pivot(index='Participant', columns='Trial', values='Category')
# df_runplot_un_filled = df_runplot_un.fillna('No Trial')

# # Convert to categorical codes (numeric) in a stable category order
# all_categories = list(category_palette.keys())
# cat = pd.Categorical(df_runplot_un_filled.to_numpy().ravel(), categories=all_categories, ordered=True)
# codes = cat.codes.reshape(df_runplot_un_filled.shape)  # int array for imshow

# # Colormap must align with category order above
# cmap = ListedColormap([category_palette[c] for c in all_categories])

# # Plot
# plt.figure(figsize=(16, 4))
# plt.imshow(codes, aspect='auto', cmap=cmap, vmin=0, vmax=len(all_categories) - 1)

# plt.yticks(ticks=range(len(df_runplot_un_filled.index)), labels=df_runplot_un_filled.index)

# max_trials = max(participants_uneven.values())
# # x positions are 0-based pixel columns; labels are 1-based trial numbers
# tick_positions = list(range(0, max_trials, 10))
# tick_labels = [str(i + 1) for i in tick_positions]
# plt.xticks(ticks=tick_positions, labels=tick_labels)

# plt.xlabel('Trial')
# plt.ylabel('Participant')
# plt.title('Categorical Run Plot with Uneven Trial Counts')

# # Legend (in the same order as the colormap)
# handles = [
#     plt.Line2D([0], [0], marker='s', color=category_palette[cat_name],
#                label=cat_name, linestyle='', markersize=10)
#     for cat_name in all_categories
# ]
# plt.legend(handles=handles, title='Route Type', bbox_to_anchor=(1.01, 1), loc='upper left')

# plt.tight_layout()
# plt.show()



import matplotlib.patheffects as pe


def add_dummy_trial_metadata(
    df: pd.DataFrame,
    startpos_col: str = "StartPos",
    coinset_col: str = "CoinSetID",
    startpos_range=(1, 8),
    coinset_range=(1, 4),
    seed: int | None = 42,
) -> pd.DataFrame:
    out = df.copy()
    rng = np.random.default_rng(seed)

    if startpos_col not in out.columns:
        out[startpos_col] = rng.integers(startpos_range[0], startpos_range[1] + 1, size=len(out))
    else:
        m = out[startpos_col].isna()
        if m.any():
            out.loc[m, startpos_col] = rng.integers(startpos_range[0], startpos_range[1] + 1, size=int(m.sum()))

    if coinset_col not in out.columns:
        out[coinset_col] = rng.integers(coinset_range[0], coinset_range[1] + 1, size=len(out))
    else:
        m = out[coinset_col].isna()
        if m.any():
            out.loc[m, coinset_col] = rng.integers(coinset_range[0], coinset_range[1] + 1, size=int(m.sum()))

    return out


def plot_participant_overlay_strip(
    df: pd.DataFrame,
    category_palette: dict,
    participant_col: str = "Participant",
    trial_col: str = "Trial",
    category_col: str = "Category",
    startpos_col: str = "StartPos",
    coinset_col: str = "CoinSetID",
    tick_step: int = 10,
    figsize=(16, 2.6),
    marker_size=110,
    text_size=11,
    overlay_dx=0.18,  # number left, marker right
):
    """
    Per participant: 1 strip plot (category colors) with StartPos (text) + CoinSetID (marker)
    OVERLAID in each trial cell.
      CoinSetID mapping:
        1 = filled circle, 2 = square, 3 = star, 4 = empty circle
    """

    df = df.copy()

    # Ensure palette has padding/fallback categories
    if "No Trial" not in category_palette:
        category_palette = {**category_palette, "No Trial": "#d3d3d3"}
    if "Other" not in category_palette:
        category_palette = {**category_palette, "Other": "#999999"}

    categories_order = list(category_palette.keys())
    cmap = ListedColormap([category_palette[c] for c in categories_order])

    def _set_x_ticks(ax, ncols):
        tick_positions = list(range(0, ncols, tick_step))
        tick_labels = [str(i + 1) for i in tick_positions]  # trials are 1-based labels
        ax.set_xticks(tick_positions, tick_labels)

    figs = {}

    for pid, g in df.groupby(participant_col, sort=True):
        g = g.sort_values(trial_col)

        max_trial = int(g[trial_col].max())
        full_trials = pd.Index(range(1, max_trial + 1), name=trial_col)
        g2 = g.set_index(trial_col).reindex(full_trials)

        # category strip (pad missing)
        cat_series = g2[category_col].fillna("No Trial").astype(str)
        cat = pd.Categorical(cat_series.to_numpy(), categories=categories_order, ordered=True)
        cat_codes = cat.codes.astype(int)  # (n_trials,)

        n_trials = len(full_trials)
        x = np.arange(n_trials)
        y0 = np.zeros(n_trials)

        startpos = g2[startpos_col] if startpos_col in g2.columns else pd.Series([np.nan] * n_trials, index=full_trials)
        coinset = g2[coinset_col] if coinset_col in g2.columns else pd.Series([np.nan] * n_trials, index=full_trials)

        fig, ax = plt.subplots(1, 1, figsize=figsize)

        # Base strip
        ax.imshow(cat_codes.reshape(1, -1), aspect="auto", cmap=cmap, vmin=0, vmax=len(categories_order) - 1)
        ax.set_yticks([])
        ax.set_ylabel("Participant", rotation=0, labelpad=35, va="center")
        ax.set_title(f"Participant {pid}")

        # Legend for route types
        handles = [
            plt.Line2D([0], [0], marker="s", color=category_palette[name],
                       label=name, linestyle="", markersize=9)
            for name in categories_order
        ]
        ax.legend(handles=handles, title="Route Type", bbox_to_anchor=(1.01, 1), loc="upper left")

        # Overlay text + marker per trial (skip padded "No Trial" by default)
        # NOTE: you can remove the "No Trial" skip if you want overlays there too.
        pe_outline = [pe.Stroke(linewidth=2.5, foreground="white"), pe.Normal()]
        cs = coinset.to_numpy()
        sp = startpos.to_numpy()

        for i in range(n_trials):
            if cat_series.iloc[i] == "No Trial":
                continue
            if pd.notna(sp[i]):
                ax.text(
                    i - overlay_dx, 0.0, str(int(sp[i])),
                    ha="center", va="center", fontsize=text_size, color="black",
                    path_effects=pe_outline,
                )

        # Plot each coinset id with its marker style (center-right in the cell)
        for coin_id in (1, 2, 3, 4):
            idx = np.where(cs == coin_id)[0]
            if idx.size == 0:
                continue

            xs = idx + overlay_dx
            ys = np.zeros_like(xs, dtype=float)

            if coin_id == 1:      # filled circle
                ax.scatter(xs, ys, marker="o", s=marker_size, edgecolors="black", facecolors="black",
                           linewidths=1.2, zorder=3)
            elif coin_id == 2:    # square
                ax.scatter(xs, ys, marker="s", s=marker_size, edgecolors="black", facecolors="black",
                           linewidths=1.2, zorder=3)
            elif coin_id == 3:    # star
                ax.scatter(xs, ys, marker="*", s=marker_size * 1.35, edgecolors="black", facecolors="black",
                           linewidths=1.0, zorder=3)
            elif coin_id == 4:    # empty circle
                ax.scatter(xs, ys, marker="o", s=marker_size, edgecolors="black", facecolors="none",
                           linewidths=1.4, zorder=3)

        # X ticks/label
        _set_x_ticks(ax, n_trials)
        ax.set_xlabel("Trial")

        # Clean spines
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)

        fig.tight_layout()
        figs[pid] = fig

    return figs


def plot_participant_overlay_startpos_only(
    df: pd.DataFrame,
    category_palette: dict,
    participant_col: str = "Participant",
    trial_col: str = "Trial",
    category_col: str = "Category",
    startpos_col: str = "StartPos",
    tick_step: int = 10,
    figsize=(16, 2.6),
    text_size=11,
    text_color="black",
    outline_color="white",
    outline_width=2.5,
):
    """
    Per participant: 1 strip plot (category colors) with StartPos (text) OVERLAID in each trial cell.
    (No CoinSetID markers.)
    """

    df = df.copy()

    # Ensure palette has padding/fallback categories
    if "No Trial" not in category_palette:
        category_palette = {**category_palette, "No Trial": "#d3d3d3"}
    if "Other" not in category_palette:
        category_palette = {**category_palette, "Other": "#999999"}

    categories_order = list(category_palette.keys())
    cmap = ListedColormap([category_palette[c] for c in categories_order])

    def _set_x_ticks(ax, ncols):
        tick_positions = list(range(0, ncols, tick_step))
        tick_labels = [str(i + 1) for i in tick_positions]  # trials are 1-based labels
        ax.set_xticks(tick_positions, tick_labels)

    figs = {}

    pe_outline = [pe.Stroke(linewidth=outline_width, foreground=outline_color), pe.Normal()]

    for pid, g in df.groupby(participant_col, sort=True):
        g = g.sort_values(trial_col)

        max_trial = int(g[trial_col].max())
        full_trials = pd.Index(range(1, max_trial + 1), name=trial_col)
        g2 = g.set_index(trial_col).reindex(full_trials)

        # category strip (pad missing)
        cat_series = g2[category_col].fillna("No Trial").astype(str)
        cat = pd.Categorical(cat_series.to_numpy(), categories=categories_order, ordered=True)
        cat_codes = cat.codes.astype(int)

        n_trials = len(full_trials)
        startpos = g2[startpos_col] if startpos_col in g2.columns else pd.Series([np.nan] * n_trials, index=full_trials)

        fig, ax = plt.subplots(1, 1, figsize=figsize)

        # Base strip
        ax.imshow(cat_codes.reshape(1, -1), aspect="auto", cmap=cmap, vmin=0, vmax=len(categories_order) - 1)
        ax.set_yticks([])
        ax.set_ylabel("Participant", rotation=0, labelpad=35, va="center")
        ax.set_title(f"Participant {pid}")

        # Legend for route types
        handles = [
            plt.Line2D([0], [0], marker="s", color=category_palette[name],
                       label=name, linestyle="", markersize=9)
            for name in categories_order
        ]
        ax.legend(handles=handles, title="Route Type", bbox_to_anchor=(1.01, 1), loc="upper left")

        # Overlay StartPos text (skip padded "No Trial")
        sp = startpos.to_numpy()
        for i in range(n_trials):
            if cat_series.iloc[i] == "No Trial":
                continue
            if pd.notna(sp[i]):
                ax.text(
                    i, 0.0, str(int(sp[i])),
                    ha="center", va="center",
                    fontsize=text_size, color=text_color,
                    path_effects=pe_outline,
                    zorder=3,
                )

        # X ticks/label
        _set_x_ticks(ax, n_trials)
        ax.set_xlabel("Trial")

        # Clean spines
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)

        fig.tight_layout()
        figs[pid] = fig

    return figs



def plot_multi_participant_strip_with_startpos(
    df: pd.DataFrame,
    category_palette: dict,
    participant_col: str = "Participant",
    trial_col: str = "Trial",
    category_col: str = "Category",
    startpos_col: str = "StartPos",
    tick_step: int = 10,
    figsize=(16, 4),
    text_size: int = 8,
    text_color: str = "black",
    outline_color: str = "white",
    outline_width: float = 2.0,
    skip_no_trial: bool = True,):
    '''
    Multi-participant run plot (imshow) with StartPos text overlaid per trial cell.

    df must have: Participant, Trial, Category, StartPos
    Missing trials are padded as 'No Trial'. StartPos for padded trials is left blank.
    '''

    df = df.copy()

    # Ensure palette has padding/fallback categories
    if "No Trial" not in category_palette:
        category_palette = {**category_palette, "No Trial": "#d3d3d3"}
    if "Other" not in category_palette:
        category_palette = {**category_palette, "Other": "#999999"}

    categories_order = list(category_palette.keys())
    cmap = ListedColormap([category_palette[c] for c in categories_order])

    # Build complete participant x trial grid up to global max trial
    participants = sorted(df[participant_col].unique().tolist())
    max_trial = int(df[trial_col].max())
    trials = pd.Index(range(1, max_trial + 1), name=trial_col)

    # Pivot Category and StartPos
    cat_grid = (
        df.pivot(index=participant_col, columns=trial_col, values=category_col)
          .reindex(index=participants, columns=trials)
          .fillna("No Trial")
          .astype(str)
    )

    sp_grid = (
        df.pivot(index=participant_col, columns=trial_col, values=startpos_col)
          .reindex(index=participants, columns=trials)
    )

    # Convert categories to numeric codes for imshow
    cat_flat = pd.Categorical(cat_grid.to_numpy().ravel(), categories=categories_order, ordered=True)
    codes = cat_flat.codes.reshape(cat_grid.shape).astype(int)

    # Plot
    fig, ax = plt.subplots(1, 1, figsize=figsize)
    ax.imshow(codes, aspect="auto", cmap=cmap, vmin=0, vmax=len(categories_order) - 1)

    # Axes labels/ticks
    ax.set_yticks(range(len(participants)), participants)
    tick_positions = list(range(0, max_trial, tick_step))
    tick_labels = [str(i + 1) for i in tick_positions]
    ax.set_xticks(tick_positions, tick_labels)
    ax.set_xlabel("Trial")
    ax.set_ylabel("Participant")
    ax.set_title("Categorical Run Plot with StartPos Labels (Uneven Trial Counts)")

    # Legend
    handles = [
        plt.Line2D([0], [0], marker="s", color=category_palette[name],
                   label=name, linestyle="", markersize=9)
        for name in categories_order
    ]
    ax.legend(handles=handles, title="Route Type", bbox_to_anchor=(1.01, 1), loc="upper left")

    # Overlay StartPos text at cell centers
    pe_outline = [pe.Stroke(linewidth=outline_width, foreground=outline_color), pe.Normal()]

    sp_vals = sp_grid.to_numpy()
    for r in range(sp_vals.shape[0]):
        for c in range(sp_vals.shape[1]):
            if skip_no_trial and cat_grid.iat[r, c] == "No Trial":
                continue
            v = sp_vals[r, c]
            if pd.isna(v):
                continue
            ax.text(
                c, r, str(int(v)),
                ha="center", va="center",
                fontsize=text_size, color=text_color,
                path_effects=pe_outline,
                zorder=3,
            )

    # Clean spines
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.tight_layout()
    return fig



def plot_categorical_runplot_uneven(
    df: pd.DataFrame,
    category_palette: dict,
    participant_col: str = "Participant",
    trial_col: str = "Trial",
    category_col: str = "Category",
    no_trial_label: str = "No Trial",
    tick_step: int = 10,
    figsize=(16, 4),
    title: str = "Categorical Run Plot with Uneven Trial Counts",
):
    """
    Plots a multi-participant categorical run plot from an existing dataframe.

    Expected columns (customizable):
      - participant_col (e.g. 'Participant')
      - trial_col (e.g. 'Trial') : should be 1-based ints (or coercible)
      - category_col (e.g. 'Category')

    Handles uneven trial counts by pivoting and filling missing cells with no_trial_label.
    Uses category_palette keys to define the category order and colors.
    """

    # Defensive copy + basic sanitization
    df = df.copy()
    df[trial_col] = pd.to_numeric(df[trial_col], errors="coerce").astype("Int64")
    df = df.dropna(subset=[participant_col, trial_col, category_col])

    # Ensure No Trial is in the palette
    if no_trial_label not in category_palette:
        category_palette = {**category_palette, no_trial_label: "#d3d3d3"}

    # Pivot and fill missing trials
    df_runplot = df.pivot(index=participant_col, columns=trial_col, values=category_col)
    df_runplot_filled = df_runplot.fillna(no_trial_label).astype(str)

    # Ensure columns are sorted numerically (important if trials are ints)
    df_runplot_filled = df_runplot_filled.reindex(columns=sorted(df_runplot_filled.columns))

    # Category order and cmap must align
    all_categories = list(category_palette.keys())
    cat = pd.Categorical(
        df_runplot_filled.to_numpy().ravel(),
        categories=all_categories,
        ordered=True,
    )
    codes = cat.codes.reshape(df_runplot_filled.shape).astype(int)
    cmap = ListedColormap([category_palette[c] for c in all_categories])

    # Plot
    fig, ax = plt.subplots(1, 1, figsize=figsize)
    ax.imshow(codes, aspect="auto", cmap=cmap, vmin=0, vmax=len(all_categories) - 1)

    # Y axis
    ax.set_yticks(range(len(df_runplot_filled.index)), df_runplot_filled.index)

    # X axis ticks: 0-based positions, 1-based labels
    max_trials = int(df_runplot_filled.shape[1])
    tick_positions = list(range(0, max_trials, tick_step))
    tick_labels = [str(i + 1) for i in tick_positions]
    ax.set_xticks(tick_positions, tick_labels)

    ax.set_xlabel("Trial")
    ax.set_ylabel("Participant")
    ax.set_title(title)

    # Legend in palette order
    handles = [
        plt.Line2D([0], [0], marker="s", color=category_palette[name],
                   label=name, linestyle="", markersize=10)
        for name in all_categories
    ]
    ax.legend(handles=handles, title="Route Type", bbox_to_anchor=(1.01, 1), loc="upper left")

    fig.tight_layout()
    return fig, ax


# --- Example usage ---
# df_with_startpos = add_dummy_trial_startpos(df_uneven, seed=42)
# fig = plot_multi_participant_strip_with_startpos(df_with_startpos, category_palette, text_size=7)
# plt.show()

# --- Example usage ---
df_with_aux = add_dummy_trial_metadata(df_uneven, seed=42)
#figs = plot_participant_overlay_strip(df_with_aux, category_palette)
#plt.show()

#figs1 = plot_participant_overlay_startpos_only(df_with_aux, category_palette)
#plt.show()

# fig = plot_multi_participant_strip_with_startpos(df_with_aux, category_palette, text_size=7)
# plt.show()

fig, ax = plot_categorical_runplot_uneven(df_with_aux, category_palette)
plt.show()