#!/usr/bin/env python3
"""
Compute value-weighted & distance-only ("unweighted") visit orders to LV/NV/HV coins
for each AN start position (pos1..pos8), per participant row in
compiledCoinLocations.csv.

Weighted mode = distance + coin incentive penalty, with penalties:
    HV: 0, LV: 10, NV: 30 (start S: 0)
Unweighted mode = distance only (no coin incentive).

We generate:
1) Greedy next-best order under each mode (choose the next coin with minimal edge cost).
2) Fixed order S→LV→NV→HV under each mode with per-leg weights and distances.

Inputs
------
- compiledCoinLocations.csv : CSV with columns at least
  [participantID, currentRole, CoinSet, fileName,
   LV_x, LV_y, LV_z, NV_x, NV_y, NV_z, HV_x, HV_y, HV_z].
  Only x,z are used (y is ignored).

Outputs
-------
- WeightedDijkstra_AN.csv      (ValueWeightedDijkstra rows; greedy next-best)
- UnweightedDijkstra_AN.csv    (UnweightedDijkstra rows; greedy next-best)
- Weighted_LV_NV_HV_AN.csv     (ValueWeightedDijkstra with fixed order LV→NV→HV)
- Unweighted_LV_NV_HV_AN.csv   (UnweightedDijkstra with fixed order LV→NV→HV)
"""
from __future__ import annotations
import math
from typing import Dict, Tuple, List
import pandas as pd

# --------------------------- Configuration ---------------------------------
INPUT_CSV = "/Users/mairahmac/Desktop/RC_TestingNotes/compiledCoinLocations.csv"  # adjust path if needed
WEIGHTED_OUT = "/Users/mairahmac/Desktop/RC_TestingNotes/WeightedDijkstra_AN.csv"
UNWEIGHTED_OUT = "/Users/mairahmac/Desktop/RC_TestingNotes/UnweightedDijkstra_AN.csv"

# AN participant start positions (x, z)
AN_POSITIONS: Dict[str, Tuple[float, float]] = {
    "pos1": (0.0, 5.0),
    "pos2": (3.5, 3.5),
    "pos3": (5.0, 0.0),
    "pos4": (3.5, -3.5),
    "pos5": (0.0, -5.0),
    "pos6": (-3.5, -3.5),
    "pos7": (-5.0, 0.0),
    "pos8": (-3.5, 3.5),
}

COIN_LABELS = ("HV", "LV", "NV")

# Coin incentive penalties: lower is more incentivized to visit
COIN_VALUE_WEIGHT = {"HV": 0.0, "LV": 10.0, "NV": 30.0, "S": 0.0}

# ----------------------------- Core logic ----------------------------------

def _hypot(a: float, b: float) -> float:
    return math.hypot(a, b)

def edge_cost(u: str, v: str, coords: Dict[str, Tuple[float, float]], *, weighted: bool) -> float:
    """Edge cost from u -> v.
    - If weighted: distance + destination coin penalty (HV 0, LV 10, NV 30; S 0)
    - If unweighted: distance only
    """
    (x1, z1) = coords[u]
    (x2, z2) = coords[v]
    d = _hypot(x1 - x2, z1 - z2)
    if not weighted:
        return d
    return d + COIN_VALUE_WEIGHT.get(v, 0.0)

def greedy_visit_order(start_label: str, coords: Dict[str, Tuple[float, float]], *, weighted: bool) -> Tuple[List[str], float]:
    remaining = set(COIN_LABELS)
    order: List[str] = []
    cur = start_label
    total = 0.0
    while remaining:
        nxt = min(remaining, key=lambda c: edge_cost(cur, c, coords, weighted=weighted))
        total += edge_cost(cur, nxt, coords, weighted=weighted)
        order.append(nxt)
        remaining.remove(nxt)
        cur = nxt
    return order, total

def fixed_order_cost(coords: Dict[str, Tuple[float, float]], *, weighted: bool) -> Tuple[List[str], Dict[str, float]]:
    """Compute edge weights and total for fixed order S→LV→NV→HV.
    Returns (order, metrics) where metrics has per-edge and total costs and distances.
    """
    order = ["LV", "NV", "HV"]
    # distances (pure Euclidean)
    d_S_LV = edge_cost("S", "LV", coords, weighted=False)
    d_LV_NV = edge_cost("LV", "NV", coords, weighted=False)
    d_NV_HV = edge_cost("NV", "HV", coords, weighted=False)
    # weights under selected mode
    e_S_LV = edge_cost("S", "LV", coords, weighted=weighted)
    e_LV_NV = edge_cost("LV", "NV", coords, weighted=weighted)
    e_NV_HV = edge_cost("NV", "HV", coords, weighted=weighted)
    metrics = {
        "dist_S_LV": d_S_LV,
        "dist_LV_NV": d_LV_NV,
        "dist_NV_HV": d_NV_HV,
        "edge_S_LV": e_S_LV,
        "edge_LV_NV": e_LV_NV,
        "edge_NV_HV": e_NV_HV,
        "cost_total": e_S_LV + e_LV_NV + e_NV_HV,
    }
    return order, metrics

def compute_for_role_an(df: pd.DataFrame, *, weighted: bool) -> pd.DataFrame:
    rows: List[Dict] = []
    df_an = df[df["currentRole"].astype(str).str.upper() == "AN"].copy()

    for _, row in df_an.iterrows():
        coin_coords = {
            "LV": (float(row["LV_x"]), float(row["LV_z"])),
            "NV": (float(row["NV_x"]), float(row["NV_z"])),
            "HV": (float(row["HV_x"]), float(row["HV_z"]))
        }
        for start_label, start_xz in AN_POSITIONS.items():
            coords = {"S": start_xz, **coin_coords}
            order, total = greedy_visit_order("S", coords, weighted=weighted)
            cS = {lbl: edge_cost("S", lbl, coords, weighted=weighted) for lbl in COIN_LABELS}
            rows.append({
                "TheoPathType": ("ValueWeightedDijkstra" if weighted else "UnweightedDijkstra"),
                "participantID": str(row.get("participantID", "")).strip(),
                "CoinSet": str(row.get("CoinSet", "")),
                "fileName": str(row.get("fileName", "")),
                "startLabel": start_label,
                "start_x": float(start_xz[0]),
                "start_z": float(start_xz[1]),
                "LV_x": coin_coords["LV"][0], "LV_z": coin_coords["LV"][1],
                "NV_x": coin_coords["NV"][0], "NV_z": coin_coords["NV"][1],
                "HV_x": coin_coords["HV"][0], "HV_z": coin_coords["HV"][1],
                "order_1": order[0], "order_2": order[1], "order_3": order[2],
                "cost_total": float(total),
                "cost_S_HV": float(cS["HV"]),
                "cost_S_LV": float(cS["LV"]),
                "cost_S_NV": float(cS["NV"]),
            })
    return pd.DataFrame(rows)

def compute_fixed_order_for_role_an(df: pd.DataFrame, *, weighted: bool) -> pd.DataFrame:
    rows: List[Dict] = []
    df_an = df[df["currentRole"].astype(str).str.upper() == "AN"].copy()

    for _, row in df_an.iterrows():
        coin_coords = {
            "LV": (float(row["LV_x"]), float(row["LV_z"])),
            "NV": (float(row["NV_x"]), float(row["NV_z"])),
            "HV": (float(row["HV_x"]), float(row["HV_z"]))
        }
        for start_label, start_xz in AN_POSITIONS.items():
            coords = {"S": start_xz, **coin_coords}
            order, metrics = fixed_order_cost(coords, weighted=weighted)
            rows.append({
                "TheoPathType": ("ValueWeightedDijkstra" if weighted else "UnweightedDijkstra"),
                "participantID": str(row.get("participantID", "")).strip(),
                "CoinSet": str(row.get("CoinSet", "")),
                "fileName": str(row.get("fileName", "")),
                "startLabel": start_label,
                "start_x": float(start_xz[0]),
                "start_z": float(start_xz[1]),
                "LV_x": coin_coords["LV"][0], "LV_z": coin_coords["LV"][1],
                "NV_x": coin_coords["NV"][0], "NV_z": coin_coords["NV"][1],
                "HV_x": coin_coords["HV"][0], "HV_z": coin_coords["HV"][1],
                "order_1": order[0], "order_2": order[1], "order_3": order[2],
                **metrics,
                "penalty_LV": (COIN_VALUE_WEIGHT["LV"] if weighted else 0.0),
                "penalty_NV": (COIN_VALUE_WEIGHT["NV"] if weighted else 0.0),
                "penalty_HV": (COIN_VALUE_WEIGHT["HV"] if weighted else 0.0),
            })
    return pd.DataFrame(rows)

def main() -> None:
    df = pd.read_csv(INPUT_CSV, dtype={"participantID": "string"})

    # Greedy next-best per mode
    weighted_df = compute_for_role_an(df, weighted=True)
    unweighted_df = compute_for_role_an(df, weighted=False)

    weighted_df.to_csv(WEIGHTED_OUT, index=False)
    unweighted_df.to_csv(UNWEIGHTED_OUT, index=False)

    # Fixed order LV→NV→HV per mode
    fixed_w = compute_fixed_order_for_role_an(df, weighted=True)
    fixed_u = compute_fixed_order_for_role_an(df, weighted=False)

    fixed_w.to_csv("/Users/mairahmac/Desktop/RC_TestingNotes/Weighted_LV_NV_HV_AN.csv", index=False)
    fixed_u.to_csv("/Users/mairahmac/Desktop/RC_TestingNotes/Unweighted_LV_NV_HV_AN.csv", index=False)

    print(f"Wrote {WEIGHTED_OUT} with {len(weighted_df)} rows.")
    print(f"Wrote {UNWEIGHTED_OUT} with {len(unweighted_df)} rows.")
    print(f"Wrote Weighted_LV_NV_HV_AN.csv with {len(fixed_w)} rows.")
    print(f"Wrote Unweighted_LV_NV_HV_AN.csv with {len(fixed_u)} rows.")

if __name__ == "__main__":
    main()
