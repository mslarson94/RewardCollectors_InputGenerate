#!/usr/bin/env python3
import os
import re
import glob
import itertools
import numpy as np
import pandas as pd
from scipy.optimize import minimize

# ------------------ Task Geometry ------------------

POSITIONS = {
    "pos1": (0.0, 5.0),
    "pos2": (3.5, 3.5),
    "pos3": (5.0, 0.0),
    "pos4": (3.5, -3.5),
    "pos5": (0.0, -5.0),
    "pos6": (-3.5, -3.5),
    "pos7": (-5.0, 0.0),
    "pos8": (-3.5, 3.5),
}

START_RADIUS = 1.0
COIN_RADIUS  = 1.1
VALID_LABELS = ("LV", "NV", "HV")

# ------------------ Parsing / Normalization ------------------

def normalize_path_order_round(s: str) -> str:
    """
    Normalizes any of:
      'LV->NV->HV', 'lv -> nv -> hv', 'LV→NV→HV', 'LV-NV-HV', 'LV,NV,HV'
    into canonical 'LV->NV->HV'
    """
    if pd.isna(s):
        return s
    s = str(s).strip().upper()
    # unify arrows / separators
    #s = s.replace("->", "->")
    s = re.sub(r"[,\|/]+", "->", s)
    #s = re.sub(r"-{1,2}", "->", s)  # '-' or '--' to '->'
    parts = [p.strip() for p in re.split(r"\s*->\s*", s) if p.strip()]
    if len(parts) != 3 or any(p not in VALID_LABELS for p in parts):
        raise ValueError(f"Bad path_order_round='{s}' -> {parts} (expected 3 of {VALID_LABELS})")
    return "->".join(parts)

# ------------------ Ideal Distance Optimizer ------------------

def coin_center(coin_df: pd.DataFrame, coinset: str, label: str) -> np.ndarray:
    row = coin_df.loc[coinset]
    return np.array([row[f"{label}_x"], row[f"{label}_z"]], dtype=float)

def ideal_distance_through_discs(centers, radii, tol=1e-10, maxiter=800):
    centers = np.asarray(centers, dtype=float)
    radii   = np.asarray(radii, dtype=float)
    n = centers.shape[0]

    def unpack(x):
        return x.reshape(n, 2)

    def objective(x):
        pts = unpack(x)
        seg = pts[1:] - pts[:-1]
        return float(np.sum(np.linalg.norm(seg, axis=1)))

    cons = []
    for i in range(n):
        ci = centers[i].copy()
        ri = float(radii[i])
        cons.append({
            "type": "ineq",
            "fun": lambda x, i=i, ci=ci, ri=ri: ri*ri - np.sum((unpack(x)[i] - ci)**2)
        })

    # a few inits
    inits = []
    # toward next
    ptsA = []
    for i in range(n):
        c = centers[i]
        if i < n - 1:
            v = centers[i + 1] - c
            d = np.linalg.norm(v)
            ptsA.append(c if d < 1e-12 else (c + radii[i] * 0.999 * (v / d)))
        else:
            ptsA.append(c.copy())
    inits.append(np.concatenate(ptsA))
    # centers
    inits.append(centers.reshape(-1).copy())

    best = None
    for x0 in inits:
        res = minimize(
            objective, x0,
            method="SLSQP",
            constraints=cons,
            options={"ftol": tol, "maxiter": maxiter, "disp": False},
        )
        if res.success and (best is None or res.fun < best.fun):
            best = res

    if best is None:
        raise RuntimeError("Optimization failed for all initializations.")

    pts = unpack(best.x)
    return float(best.fun), pts

def compute_ideal_reference_all_coinsets(coinsets_csv_path: str) -> pd.DataFrame:
    coin_df = pd.read_csv(coinsets_csv_path)
    required_cols = {"coinSet", "LV_x", "LV_z", "NV_x", "NV_z", "HV_x", "HV_z"}
    missing = required_cols - set(coin_df.columns)
    if missing:
        raise ValueError(f"CoinSets.csv missing columns: {sorted(missing)}")

    coin_df = coin_df.set_index("coinSet")

    start_keys = list(POSITIONS.keys())
    orders = list(itertools.permutations(VALID_LABELS, 3))

    rows = []
    for coinset in coin_df.index.tolist():
        for start_key in start_keys:
            start_center = np.array(POSITIONS[start_key], dtype=float)

            for order in orders:
                path_str = " -> ".join(order)
                centers = [start_center]
                radii   = [START_RADIUS]
                for lab in order:
                    centers.append(coin_center(coin_df, coinset, lab))
                    radii.append(COIN_RADIUS)

                length, pts = ideal_distance_through_discs(centers, radii)

                rows.append({
                    "coinSet": coinset,
                    "startPos": start_key,
                    "path_order_round": path_str,
                    "ideal_distance": length,
                    "p0_x": pts[0,0], "p0_z": pts[0,1],
                    "p1_x": pts[1,0], "p1_z": pts[1,1],
                    "p2_x": pts[2,0], "p2_z": pts[2,1],
                    "p3_x": pts[3,0], "p3_z": pts[3,1],
                })

    return (pd.DataFrame(rows)
            .sort_values(["coinSet", "startPos", "path_order_round"])
            .reset_index(drop=True))

def load_ideal_reference_from_folder(folder: str) -> pd.DataFrame:
    paths = sorted(glob.glob(os.path.join(folder, "ideal_routes_*.csv")))
    if not paths:
        raise FileNotFoundError(f"No ideal_routes_*.csv found in {folder}")
    dfs = [pd.read_csv(p) for p in paths]
    out = pd.concat(dfs, ignore_index=True)
    # normalize to be safe
    out["path_order_round"] = out["path_order_round"].map(normalize_path_order_round)
    return out

# ------------------ Norm-utility stacking + merge ------------------

def load_and_stack_norm_utility(norm_dir: str, pattern: str = "*_normUtil.csv") -> pd.DataFrame:
    paths = sorted(glob.glob(os.path.join(norm_dir, pattern)))
    if not paths:
        raise FileNotFoundError(f"No files matched {os.path.join(norm_dir, pattern)}")

    dfs = []
    for p in paths:
        df = pd.read_csv(p)
        df["__source_file"] = os.path.basename(p)
        dfs.append(df)

    master = pd.concat(dfs, ignore_index=True)

    # normalize keys (adjust column names here if yours differ)
    for col in ["coinSet", "startPos", "path_order_round"]:
        if col not in master.columns:
            raise ValueError(f"norm_utility files missing required column '{col}'")

    master["path_order_round"] = master["path_order_round"].map(normalize_path_order_round)

    return master

def merge_master_with_ideal(master: pd.DataFrame, ideal: pd.DataFrame) -> pd.DataFrame:
    # Ensure normalized key columns exist
    ideal = ideal.copy()
    ideal["path_order_round"] = ideal["path_order_round"].map(normalize_path_order_round)

    key_cols = ["coinSet", "startPos", "path_order_round"]

    merged = master.merge(
        ideal,
        on=key_cols,
        how="left",
        validate="m:1",  # many participant rows -> one ideal row
    )

    missing = merged["ideal_distance"].isna().sum()
    if missing:
        # show a small diagnostic sample
        sample = (merged.loc[merged["ideal_distance"].isna(), key_cols]
                 .drop_duplicates()
                 .head(20))
        raise ValueError(
            f"{missing} rows did not match an ideal reference row.\n"
            f"Sample unmatched keys:\n{sample}"
        )

    return merged

# ------------------ Main ------------------

def main():
    # ---- configure these ----
    norm_dir = "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm"  # folder containing norm_utility*.csv
    coinsets_csv_path = "/Users/mairahmac/Desktop/RC_TestingNotes/CoinSets.csv"

    # Option A: compute ideal reference fresh from CoinSets.csv
    compute_ideal = True

    # Option B: load precomputed per-coinset ideal tables from a folder
    ideal_folder = "ideal_reference_by_coinset"

    out_master_csv = "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtilEff/master_norm_utility_with_ideal.csv"
    out_ideal_master_csv = "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtilEff/ideal_reference_master_all_coinsets.csv"
    # -------------------------

    master = load_and_stack_norm_utility(norm_dir)

    if compute_ideal:
        ideal = compute_ideal_reference_all_coinsets(coinsets_csv_path)
    else:
        ideal = load_ideal_reference_from_folder(ideal_folder)

    # Save the ideal master too (often useful)
    ideal.to_csv(out_ideal_master_csv, index=False)

    merged = merge_master_with_ideal(master, ideal)
    merged.to_csv(out_master_csv, index=False)

    print(f"Wrote ideal reference master: {out_ideal_master_csv} ({len(ideal)} rows)")
    print(f"Wrote merged master:         {out_master_csv} ({len(merged)} rows)")

if __name__ == "__main__":
    main()

