from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import permutations
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd
import re


# ---------------------------
# Core geometry + scoring
# ---------------------------
@dataclass(frozen=True)
class RouteResult:
    start_pos: str
    order: tuple[str, str, str]
    distance: float
    points: float
    utility: float  # higher is better by default


def euclid(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.hypot(a[0] - b[0], a[1] - b[1])


def norm_order(s: str) -> str:
    # turns "HV -> LV -> NV" (and variants) into "HV->LV->NV"
    s = str(s).strip().upper().replace("→", "->")
    parts = [p.strip() for p in re.split(r"\s*->\s*", s) if p.strip()]
    return "->".join(parts)

def load_ideal_reference_lookup(master_csv: str | Path) -> dict[tuple[str, str, str], float]:
    """
    Key: (coinSet, startPos, path_order_round) -> ideal_distance
    Expects columns:
      coinSet, startPos, path_order_round, ideal_distance
    """
    df = pd.read_csv(master_csv)

    for col in ["coinSet", "startPos", "path_order_round", "ideal_distance"]:
        if col not in df.columns:
            raise ValueError(f"Missing column '{col}' in {master_csv}. Found: {df.columns.tolist()}")

    df["coinSet"] = df["coinSet"].astype(str).str.strip()
    df["startPos"] = df["startPos"].astype(str).str.strip()
    df["path_order_round"] = df["path_order_round"].map(norm_order)

    return {
        (r["coinSet"], r["startPos"], r["path_order_round"]): float(r["ideal_distance"])
        for _, r in df.iterrows()
    }


def evaluate_order_v1(
    start_xy: tuple[float, float],
    coin_xy: Mapping[str, tuple[float, float]],
    coin_points: Mapping[str, float],
    path_order_round: tuple[str, str, str],
    *,
    first_two_multiplier: float = 2.0,
    distance_weight: float = 1.0,
    utility_mode: str = "points_minus_distance",  # or "distance_minus_points"
) -> tuple[float, float, float]:
    d0 = euclid(start_xy, coin_xy[order[0]])
    d1 = euclid(coin_xy[order[0]], coin_xy[order[1]])
    d2 = euclid(coin_xy[order[1]], coin_xy[order[2]])
    dist = d0 + d1 + d2

    pts = (
        first_two_multiplier * float(coin_points[order[0]])
        + first_two_multiplier * float(coin_points[order[1]])
        + float(coin_points[order[2]])
    )

    if utility_mode == "points_minus_distance":
        util = pts - distance_weight * dist
    elif utility_mode == "distance_minus_points":
        util = distance_weight * dist - pts
    else:
        raise ValueError("utility_mode must be 'points_minus_distance' or 'distance_minus_points'")

    return dist, pts, util

def evaluate_order(
    start_xy, coin_xy, coin_points, order,
    *,
    first_two_multiplier=2.0,
    distance_weight=1.0,
    utility_mode="points_minus_distance",
    ideal_distance: float | None = None,
):
    if ideal_distance is None:
        d0 = euclid(start_xy, coin_xy[order[0]])
        d1 = euclid(coin_xy[order[0]], coin_xy[order[1]])
        d2 = euclid(coin_xy[order[1]], coin_xy[order[2]])
        dist = d0 + d1 + d2
    else:
        dist = float(ideal_distance)

    pts = (
        first_two_multiplier * float(coin_points[order[0]])
        + first_two_multiplier * float(coin_points[order[1]])
        + float(coin_points[order[2]])
    )

    if utility_mode == "points_minus_distance":
        util = pts - distance_weight * dist
    elif utility_mode == "distance_minus_points":
        util = distance_weight * dist - pts
    else:
        raise ValueError("utility_mode must be 'points_minus_distance' or 'distance_minus_points'")

    return dist, pts, util


def best_coin_order_for_start(
    start_xy: tuple[float, float],
    coin_xy: Mapping[str, tuple[float, float]],
    coin_points: Mapping[str, float],
    coins: Iterable[str],
    *,
    first_two_multiplier: float = 2.0,
    distance_weight: float = 1.0,
    utility_mode: str = "points_minus_distance",
) -> RouteResult:
    coins = tuple(coins)
    if len(coins) != 3:
        raise ValueError("This helper expects exactly 3 coins (e.g., HV/LV/NV).")

    best: RouteResult | None = None
    for perm in permutations(coins, 3):
        dist, pts, util = evaluate_order(
            start_xy, coin_xy, coin_points, perm,
            first_two_multiplier=first_two_multiplier,
            distance_weight=distance_weight,
            utility_mode=utility_mode,
        )
        cand = RouteResult(start_pos="", path_order_round=perm, distance=dist, points=pts, utility=util)
        if best is None:
            best = cand
        else:
            if utility_mode == "points_minus_distance":
                if cand.utility > best.utility:
                    best = cand
            else:
                if cand.utility < best.utility:
                    best = cand

    assert best is not None
    return best


def best_orders_all_starts(
    start_positions: Mapping[str, tuple[float, float]],
    coin_xy: Mapping[str, tuple[float, float]],
    coin_points: Mapping[str, float],
    *,
    coins: Iterable[str] = ("HV", "LV", "NV"),
    first_two_multiplier: float = 2.0,
    distance_weight: float = 1.0,
    utility_mode: str = "points_minus_distance",
) -> list[RouteResult]:
    results: list[RouteResult] = []
    for sp_name, sp_xy in start_positions.items():
        best = best_coin_order_for_start(
            sp_xy, coin_xy, coin_points, coins,
            first_two_multiplier=first_two_multiplier,
            distance_weight=distance_weight,
            utility_mode=utility_mode,
        )
        results.append(RouteResult(sp_name, best.path_order_round, best.distance, best.points, best.utility))

    results.sort(key=lambda r: r.utility, reverse=(utility_mode == "points_minus_distance"))
    return results


# ---------------------------
# NEW: load coin_xy from a chosen layout in your triangle CSV
# ---------------------------
def load_coin_xy_from_triangle_csv(
    csv_path: str | Path,
    *,
    version: str,
    coin_labels: tuple[str, str, str] = ("HV", "LV", "NV"),
    vertex_cols: tuple[str, str, str, str, str, str] = ("X1", "Y1", "X2", "Y2", "X3", "Y3"),
    version_col: str = "Version",
) -> dict[str, tuple[float, float]]:
    """
    Reads a row from triangle_positions-formatted__*.csv and maps vertices to coin labels.

    Default mapping:
      HV -> (X1,Y1)
      LV -> (X2,Y2)
      NV -> (X3,Y3)

    If you want a different mapping, pass coin_labels in the desired vertex order.
    """
    csv_path = Path(csv_path)
    df = pd.read_csv(csv_path)

    if version_col not in df.columns:
        raise ValueError(f"CSV missing '{version_col}'. Found: {df.columns.tolist()}")

    row = df.loc[df[version_col].astype(str).str.strip() == str(version).strip()]
    if row.empty:
        available = sorted(df[version_col].astype(str).str.strip().unique().tolist())
        raise ValueError(f"Version '{version}' not found. Available versions: {available}")

    row = row.iloc[0]
    x1c, y1c, x2c, y2c, x3c, y3c = vertex_cols
    required = [x1c, y1c, x2c, y2c, x3c, y3c]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing required vertex columns: {missing}")

    verts = [
        (float(row[x1c]), float(row[y1c])),
        (float(row[x2c]), float(row[y2c])),
        (float(row[x3c]), float(row[y3c])),
    ]
    if len(coin_labels) != 3:
        raise ValueError("coin_labels must have exactly 3 labels.")

    return {coin_labels[i]: verts[i] for i in range(3)}

def all_coin_orders_for_start_v1(
    start_xy: tuple[float, float],
    coin_xy: Mapping[str, tuple[float, float]],
    coin_points: Mapping[str, float],
    coins: Iterable[str],
    *,
    first_two_multiplier: float = 2.0,
    distance_weight: float = 1.0,
    utility_mode: str = "points_minus_distance",
    start_pos_name: str = "",
) -> list[RouteResult]:
    coins = tuple(coins)
    if len(coins) != 3:
        raise ValueError("This helper expects exactly 3 coins (e.g., HV/LV/NV).")

    out: list[RouteResult] = []
    for perm in permutations(coins, 3):
        dist, pts, util = evaluate_order(
            start_xy, coin_xy, coin_points, perm,
            first_two_multiplier=first_two_multiplier,
            distance_weight=distance_weight,
            utility_mode=utility_mode,
        )
        out.append(RouteResult(start_pos_name, perm, dist, pts, util))

    # sort best-first (or lowest-first if minimizing)
    out.sort(key=lambda r: r.utility, reverse=(utility_mode == "points_minus_distance"))
    return out

def all_coin_orders_for_start(
    start_xy, coin_xy, coin_points, coins,
    *,
    first_two_multiplier=2.0,
    distance_weight=1.0,
    utility_mode="points_minus_distance",
    startPos="",
    coinSet: str = "",
    ideal_lookup=None,
):
    coins = tuple(coins)
    out = []

    for perm in permutations(coins, 3):
        order_str = norm_order("->".join(perm))

        ideal_dist = None
        if ideal_lookup is not None:
            ideal_dist = ideal_lookup[(coinSet, startPos, order_str)]

        dist, pts, util = evaluate_order(
            start_xy, coin_xy, coin_points, perm,
            first_two_multiplier=first_two_multiplier,
            distance_weight=distance_weight,
            utility_mode=utility_mode,
            ideal_distance=ideal_dist,
        )
        out.append(RouteResult(startPos, perm, dist, pts, util))

    out.sort(key=lambda r: r.utility, reverse=(utility_mode == "points_minus_distance"))
    return out


def all_orders_all_starts_df_v1(
    start_positions: Mapping[str, tuple[float, float]],
    coin_xy: Mapping[str, tuple[float, float]],
    coin_points: Mapping[str, float],
    *,
    coins: Iterable[str] = ("HV", "LV", "NV"),
    first_two_multiplier: float = 2.0,
    distance_weight: float = 1.0,
    utility_mode: str = "points_minus_distance",
) -> pd.DataFrame:
    rows = []
    for sp_name, sp_xy in start_positions.items():
        results = all_coin_orders_for_start(
            sp_xy, coin_xy, coin_points, coins,
            first_two_multiplier=first_two_multiplier,
            distance_weight=distance_weight,
            utility_mode=utility_mode,
            start_pos_name=sp_name,
        )
        for r in results:
            rows.append({
                "start_pos": r.start_pos,
                "order": "->".join(r.order),
                "coin1": r.order[0],
                "coin2": r.order[1],
                "coin3": r.order[2],
                "distance": r.distance,
                "points": r.points,
                "utility": r.utility,
            })
    df = pd.DataFrame(rows)

    # nice ordering: per start, best utility first
    df = df.sort_values(
        ["start_pos", "utility"],
        ascending=[True, utility_mode != "points_minus_distance"],
        kind="mergesort",
    ).reset_index(drop=True)

    return df

def all_orders_all_starts_df(
    start_positions: Mapping[str, tuple[float, float]],
    coin_xy: Mapping[str, tuple[float, float]],
    coin_points: Mapping[str, float],
    *,
    coins: Iterable[str] = ("HV", "LV", "NV"),
    first_two_multiplier: float = 2.0,
    distance_weight: float = 1.0,
    utility_mode: str = "points_minus_distance",
    coinSet: str,  # NEW (required)
    ideal_lookup: Mapping[tuple[str, str, str], float] | None = None,  # NEW
) -> pd.DataFrame:
    rows = []
    for sp_name, sp_xy in start_positions.items():
        results = all_coin_orders_for_start(
            sp_xy, coin_xy, coin_points, coins,
            first_two_multiplier=first_two_multiplier,
            distance_weight=distance_weight,
            utility_mode=utility_mode,
            startPos=sp_name,
            coinSet=coinSet,             # NEW
            ideal_lookup=ideal_lookup,   # NEW
        )
        for r in results:
            rows.append({
                "startPos": r.start_pos,
                "path_order_round": "->".join(r.order),
                "coin1": r.order[0],
                "coin2": r.order[1],
                "coin3": r.order[2],
                "distance": r.distance,   # now ideal distance if lookup provided
                "points": r.points,
                "utility": r.utility,
            })

    df = pd.DataFrame(rows).sort_values(
        ["startPos", "utility"],
        ascending=[True, utility_mode != "points_minus_distance"],
        kind="mergesort",
    ).reset_index(drop=True)
    return df


# ---------------------------
# Example usage
# ---------------------------
# if __name__ == "__main__":
#     # Start positions (example: AN side)
#     start_positions = {
#         "pos1": (0.0, 5.0),
#         "pos2": (3.5, 3.5),
#         "pos3": (5.0, 0.0),
#         "pos4": (3.5, -3.5),
#         "pos5": (0.0, -5.0),
#         "pos6": (-3.5, -3.5),
#         "pos7": (-5.0, 0.0),
#         "pos8": (-3.5, 3.5),
#     }

#     # Pick a triangle layout by Version from your CSV
#     triangle_csv = "/Users/mairahmac/Desktop/TriangleSets/triangle_positions-formatted__A_D_.csv"
#     chosen_version = "C"

#     # Default mapping: HV=(X1,Y1), LV=(X2,Y2), NV=(X3,Y3)
#     coin_xy = load_coin_xy_from_triangle_csv(triangle_csv, version=chosen_version)

#     # If you want a different vertex-to-coin mapping, do e.g.:
#     # coin_xy = load_coin_xy_from_triangle_csv(triangle_csv, version=chosen_version, coin_labels=("LV","HV","NV"))

#     coin_points = {"HV": 10.0, "LV": 5.0, "NV": 0.0}

#     results = best_orders_all_starts(
#         start_positions,
#         coin_xy,
#         coin_points,
#         coins=("HV", "LV", "NV"),
#         first_two_multiplier=2.0,
#         distance_weight=1.0,
#         utility_mode="points_minus_distance",
#     )

#     print(f"Using layout Version={chosen_version} from {triangle_csv}")
#     print("coin_xy:", coin_xy)
#     for r in results:
#         print(
#             f"{r.start_pos}: best order {r.order} | dist={r.distance:.2f} | pts={r.points:.1f} | util={r.utility:.2f}"
#         )
if __name__ == "__main__":
    start_positions = {
        "pos1": (0.0, 5.0),
        "pos2": (3.5, 3.5),
        "pos3": (5.0, 0.0),
        "pos4": (3.5, -3.5),
        "pos5": (0.0, -5.0),
        "pos6": (-3.5, -3.5),
        "pos7": (-5.0, 0.0),
        "pos8": (-3.5, 3.5),
    }

    triangle_csv = "/Users/mairahmac/Desktop/TriangleSets/triangle_positions-formatted__A_D_.csv"
    chosen_version = "D"
    coin_xy = load_coin_xy_from_triangle_csv(triangle_csv, version=chosen_version)
    ideal_lookup = load_ideal_reference_lookup("/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtilEff/ideal_reference_master_all_coinsets.csv")
    coin_points = {"HV": 10.0, "LV": 5.0, "NV": 0.0}

    df_all = all_orders_all_starts_df(
        start_positions,
        coin_xy,
        coin_points,
        coins=("HV", "LV", "NV"),
        first_two_multiplier=2.0,
        distance_weight=3.0,
        utility_mode="points_minus_distance",
        ideal_lookup=ideal_lookup,
        coinSet= chosen_version,
    )

    print(f"Using layout Version={chosen_version} from {triangle_csv}")
    print("coin_xy:", coin_xy)

    # show top 6 per start (i.e., all orders)
    for sp, sub in df_all.groupby("startPos", sort=True):
        print("\n" + sp)
        print(sub[["path_order_round", "distance", "points", "utility"]].to_string(index=False))

    # optional: save
    out_csv = f"/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda3/all_orders__layout_{chosen_version}_L3.csv"
    df_all["coinSet"] = chosen_version
    df_all.to_csv(out_csv, index=False)
    print(f"\nWrote: {out_csv}")
