#!/usr/bin/env python3
"""
Generate plots for CoinSets.csv:

1) Complete graph among 8 neutral positions, with HV/LV/NV overlaid.
2) For each reward (HV, LV, NV): lines from that reward to each of the 8 positions,
   while also plotting the other two rewards.

Reads /mnt/data/CoinSets.csv by default (edit DEFAULT_CSV if needed).

CSV columns expected:
  CoinSet, LV_x, LV_z, NV_x, NV_z, HV_x, HV_z
(We treat z as the vertical axis in the plots, like your earlier y.)
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import matplotlib.pyplot as plt
import pandas as pd


# --- Fixed neutral positions (x, z) ---
NEUTRAL_POSITIONS: List[Tuple[float, float]] = [
    (0.0, 5.0), (3.5, 3.5), (5.0, 0.0), (3.5, -3.5),
    (0.0, -5.0), (-3.5, -3.5), (-5.0, 0.0), (-3.5, 3.5),
]

DEFAULT_CSV = "CoinSets.csv"


@dataclass(frozen=True)
class RewardSet:
    hv: Tuple[float, float]
    lv: Tuple[float, float]
    nv: Tuple[float, float]

    def as_dict(self) -> Dict[str, Tuple[float, float]]:
        return {"HV": self.hv, "LV": self.lv, "NV": self.nv}


def load_coinsets(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    required = {"CoinSet", "LV_x", "LV_z", "NV_x", "NV_z", "HV_x", "HV_z"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {csv_path}: {sorted(missing)}")

    return df


def get_reward_set(df: pd.DataFrame, coinset: str) -> RewardSet:
    row = df.loc[df["CoinSet"].astype(str) == str(coinset)]
    if row.empty:
        available = ", ".join(df["CoinSet"].astype(str).tolist())
        raise ValueError(f"CoinSet '{coinset}' not found. Available: {available}")

    r = row.iloc[0]
    return RewardSet(
        hv=(float(r["HV_x"]), float(r["HV_z"])),
        lv=(float(r["LV_x"]), float(r["LV_z"])),
        nv=(float(r["NV_x"]), float(r["NV_z"])),
    )


def plot_reward_rays(
    coinset: str,
    rewards: RewardSet,
    positions: List[Tuple[float, float]],
    out_dir: Path,
    show: bool,
) -> None:
    pos_xs, pos_zs = zip(*positions)
    reward_dict = rewards.as_dict()

    for active_name, (rx, rz) in reward_dict.items():
        fig, ax = plt.subplots(figsize=(7, 7))

        # Lines from active reward to each neutral position
        for (px, pz) in positions:
            ax.plot([rx, px], [rz, pz])

        # Neutral positions
        ax.scatter(pos_xs, pos_zs, s=90)
        for i, (px, pz) in enumerate(positions, start=1):
            ax.text(px, pz, f"P{i}", ha="center", va="center")

        # All rewards; highlight active one
        for name, (x, z) in reward_dict.items():
            if name == active_name:
                ax.scatter([x], [z], s=220, marker="x", linewidths=2.8)
                ax.text(x, z, f" {name}", ha="left", va="center", fontweight="bold")
            else:
                ax.scatter([x], [z], s=130, marker="o", edgecolors="black", linewidths=1.0)
                ax.text(x, z, f" {name}", ha="left", va="center")

        ax.set_aspect("equal", adjustable="box")
        ax.set_xlabel("x")
        ax.set_ylabel("z")
        ax.set_title(f"{coinset}: Lines from {active_name} to each position (other rewards shown)")
        ax.grid(True)

        out_path = out_dir / f"{coinset}_rays_{active_name}.png"
        fig.savefig(out_path, dpi=200, bbox_inches="tight")

        if show:
            plt.show()
        plt.close(fig)


def plot_complete_graph(
    coinset: str,
    rewards: RewardSet,
    positions: List[Tuple[float, float]],
    out_dir: Path,
    show: bool,
) -> None:
    fig, ax = plt.subplots(figsize=(7, 7))

    # All pairwise connections among neutral positions
    for (x1, z1), (x2, z2) in combinations(positions, 2):
        ax.plot([x1, x2], [z1, z2], linewidth=1)

    # Neutral positions
    xs, zs = zip(*positions)
    ax.scatter(xs, zs, s=90)
    for i, (x, z) in enumerate(positions, start=1):
        ax.text(x, z, f"P{i}", ha="center", va="center")

    # Rewards overlaid
    for name, (x, z) in rewards.as_dict().items():
        ax.scatter([x], [z], s=160, marker="x", linewidths=2.5)
        ax.text(x, z, f" {name}", ha="left", va="center", fontweight="bold")

    ax.set_aspect("equal", adjustable="box")
    ax.set_xlabel("x")
    ax.set_ylabel("z")
    ax.set_title(f"{coinset}: All neutral connections (rewards overlaid)")
    ax.grid(True)

    out_path = out_dir / f"{coinset}_complete_graph.png"
    fig.savefig(out_path, dpi=200, bbox_inches="tight")

    if show:
        plt.show()
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", type=Path, default=Path(DEFAULT_CSV), help="Path to CoinSets.csv")
    p.add_argument(
        "--coinsets",
        nargs="*",
        default=None,
        help="CoinSet names to plot (e.g., A B C). If omitted, plots all CoinSets in the CSV.",
    )
    p.add_argument("--out", type=Path, default=Path("plots_out"), help="Output directory for PNGs")
    p.add_argument("--no-show", action="store_true", help="Do not display plots interactively")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    df = load_coinsets(args.csv)

    coinsets: Iterable[str]
    if args.coinsets:
        coinsets = [str(c) for c in args.coinsets]
    else:
        coinsets = df["CoinSet"].astype(str).tolist()

    out_dir: Path = args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    show = not args.no_show

    for cs in coinsets:
        rewards = get_reward_set(df, cs)
        plot_reward_rays(cs, rewards, NEUTRAL_POSITIONS, out_dir, show)
        plot_complete_graph(cs, rewards, NEUTRAL_POSITIONS, out_dir, show)

    print(f"Saved plots to: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
