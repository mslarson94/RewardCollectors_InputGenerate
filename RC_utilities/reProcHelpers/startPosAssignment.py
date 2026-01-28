import numpy as np
import pandas as pd

import numpy as np
import pandas as pd
from pathlib import Path

def build_default_start_positions():
    # pos_ = [AN, PO]
    pos1 = [(0.0, 5.0),     (5.0, 0.0)]
    pos2 = [(3.5, 3.5),     (3.5, -3.5)]
    pos3 = [(5.0, 0.0),     (0.0, -5.0)]
    pos4 = [(3.5, -3.5),    (-3.5, -3.5)]
    pos5 = [(0.0, -5.0),    (-5.0, 0.0)]
    pos6 = [(-3.5, -3.5),   (-3.5, 3.5)]
    pos7 = [(-5.0, 0.0),    (0.0, 5.0)]
    pos8 = [(-3.5, 3.5),    (3.5, 3.5)]
    tutorial_pos = [(0.0, -5.0), (2.0, -5.0)]

    position_list = [pos1, pos2, pos3, pos4, pos5, pos6, pos7, pos8, tutorial_pos]
    labels = ['pos1', 'pos2', 'pos3', 'pos4', 'pos5', 'pos6', 'pos7', 'pos8', 'tutorial_pos']

    an_positions = np.array([p[0] for p in position_list], dtype=float)  # (P,2) -> (x,z)
    po_positions = np.array([p[1] for p in position_list], dtype=float)  # (P,2) -> (x,z)
    return an_positions, po_positions, labels


def nearest_startpos_by_role(
    x: np.ndarray,
    z: np.ndarray,
    role: np.ndarray,
    *,
    an_positions: np.ndarray,
    po_positions: np.ndarray,
    labels: list[str],
    strict_roles: bool = True,
):
    """
    role must be 'AN' or 'PO'.
    Returns (label_array, dist_array).
    """
    x = np.asarray(x, dtype=float)
    z = np.asarray(z, dtype=float)
    role = np.asarray(role, dtype=object)

    n = len(x)
    out_labels = np.array([pd.NA] * n, dtype=object)
    out_dist = np.full(n, np.nan, dtype=float)

    pts = np.stack([x, z], axis=1)
    valid_xy = np.isfinite(pts).all(axis=1)

    role_s = pd.Series(role).astype("string")
    is_an = (role_s == "AN").to_numpy()
    is_po = (role_s == "PO").to_numpy()

    if strict_roles:
        bad_role = valid_xy & ~(is_an | is_po)
        if bad_role.any():
            bad_vals = pd.Series(role)[bad_role].unique().tolist()
            raise ValueError(f"Unexpected role values in currentRole (expected only 'AN'/'PO'): {bad_vals}")

    def _compute(mask: np.ndarray, ref: np.ndarray):
        if not mask.any():
            return
        pts_v = pts[mask]  # (Nv,2)
        diff = pts_v[:, None, :] - ref[None, :, :]          # (Nv,P,2)
        dist = np.sqrt((diff ** 2).sum(axis=2))             # (Nv,P)
        best_idx = dist.argmin(axis=1)                      # (Nv,)
        best_dist = dist[np.arange(dist.shape[0]), best_idx]
        out_labels[mask] = np.array([labels[i] for i in best_idx], dtype=object)
        out_dist[mask] = best_dist

    _compute(valid_xy & is_an, an_positions)
    _compute(valid_xy & is_po, po_positions)

    return out_labels, out_dist


def compute_startpos_for_events_flexible(
    df: pd.DataFrame,
    events_of_interest: dict[str, str],
    *,
    event_type_col: str = "lo_eventType",
    role_col: str = "currentRole",
    x_start_col: str = "HeadPosAnchored_x_at_start",
    z_start_col: str = "HeadPosAnchored_z_at_start",
    x_end_col: str = "HeadPosAnchored_x_at_end",
    z_end_col: str = "HeadPosAnchored_z_at_end",
    startpos_label_col: str = "startPos",
    startpos_dist_col: str = "startPos_dist",
    strict: bool = True,
    strict_roles: bool = True,
    add_used_xy_cols: bool = True,
    # optional override for start positions
    an_positions: np.ndarray | None = None,
    po_positions: np.ndarray | None = None,
    startpos_labels: list[str] | None = None,
) -> pd.DataFrame:
    """
    events_of_interest maps eventType -> "start" or "end"
      e.g. {"RoundStart":"start", "InterRoundCylinderWalk_segment":"end", "TrueContentStart":"start"}

    Filters df to those event types, chooses x/z per row based on "start"/"end",
    then computes nearest startPos using AN/PO coordinates depending on currentRole.
    """
    if not isinstance(events_of_interest, dict) or not events_of_interest:
        raise ValueError("events_of_interest must be a non-empty dict of {eventType: 'start'|'end'}")

    # validate direction values
    bad = {k: v for k, v in events_of_interest.items() if str(v).lower() not in ("start", "end")}
    if bad:
        raise ValueError(f"events_of_interest values must be 'start' or 'end'. Bad entries: {bad}")

    out = df.copy()

    required_cols = [event_type_col, role_col, x_start_col, z_start_col, x_end_col, z_end_col]
    missing = [c for c in required_cols if c not in out.columns]
    if missing and strict:
        raise ValueError("Missing required columns:\n  - " + "\n  - ".join(missing))
    if missing and not strict:
        for c in missing:
            out[c] = pd.NA

    # start positions
    if an_positions is None or po_positions is None or startpos_labels is None:
        an_positions_d, po_positions_d, labels_d = build_default_start_positions()
        an_positions = an_positions if an_positions is not None else an_positions_d
        po_positions = po_positions if po_positions is not None else po_positions_d
        startpos_labels = startpos_labels if startpos_labels is not None else labels_d

    # filter to desired events
    wanted = set(events_of_interest.keys())
    out = out[out[event_type_col].isin(wanted)].copy()

    # coerce numeric for coordinate columns
    for c in (x_start_col, z_start_col, x_end_col, z_end_col):
        out[c] = pd.to_numeric(out[c], errors="coerce")

    # choose per-row coordinates based on mapping
    direction = out[event_type_col].map(events_of_interest).astype("string").str.lower()  # "start"/"end"

    x_used = np.full(len(out), np.nan, dtype=float)
    z_used = np.full(len(out), np.nan, dtype=float)

    start_mask = (direction == "start").to_numpy()
    end_mask = (direction == "end").to_numpy()

    x_used[start_mask] = out.loc[start_mask, x_start_col].to_numpy(dtype=float, copy=False)
    z_used[start_mask] = out.loc[start_mask, z_start_col].to_numpy(dtype=float, copy=False)
    x_used[end_mask] = out.loc[end_mask, x_end_col].to_numpy(dtype=float, copy=False)
    z_used[end_mask] = out.loc[end_mask, z_end_col].to_numpy(dtype=float, copy=False)

    roles = out[role_col].to_numpy()

    labels_arr, dist_arr = nearest_startpos_by_role(
        x_used, z_used, roles,
        an_positions=an_positions,
        po_positions=po_positions,
        labels=startpos_labels,
        strict_roles=strict_roles,
    )

    out[startpos_label_col] = labels_arr
    out[startpos_dist_col] = dist_arr

    if add_used_xy_cols:
        out["startPos_x_used"] = x_used
        out["startPos_z_used"] = z_used
        out["startPos_xy_source"] = direction  # "start" or "end" per row

    return out.reset_index(drop=True)


# ---------------------------
# Example usage:
# ---------------------------
if __name__ == "__main__":
    eventsOfInterest = {
        "InterRoundCylinderWalk_segment": "end",
        "TrueContentStart": "start",
        "PreBlock_CylinderWalk_segment": "end",
        "InterRound_PostCylinderWalk_segment": "start",

    }


    from pathlib import Path
    csv_in = Path("/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/Events_Final_NoWalks/augmented/ObsReward_B_02_17_2025_15_11_events_final.csv")  # replace with your path
    df = pd.read_csv(csv_in)

    startPos_df = compute_startpos_for_events_flexible(df, eventsOfInterest)

    csv_out = Path("/Users/mairahmac/Desktop/test/out_PO.csv")
    startPos_df.to_csv(csv_out)
