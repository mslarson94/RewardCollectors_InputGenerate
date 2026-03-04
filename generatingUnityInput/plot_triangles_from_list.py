#!/usr/bin/env python3
"""
plot_triangles_from_list.py

Read triangle versions from a list file (one per line) and plot ALL of them
on the same arena figure.

Example list file (triangles_to_plot.txt):
    A
    Bx
    Ax
    D

Run:
    python plot_triangles_from_list.py \
        --triangles-csv /path/to/triangle_positions-formatted__A_D_.csv \
        --list-file /path/to/triangles_to_plot.txt \
        --output /path/to/out/triangles_selected.png \
        --xlim -9.8 9.8 \
        --ylim -15 10
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List, Tuple, Dict

import pandas as pd
import matplotlib.pyplot as plt


# Fixed octagons (same as your existing script)
ARENA_OCTAGON = [
    (4.0, 0.0), (2.8, 2.8), (0.0, 4.0), (-2.8, 2.8),
    (-4.0, 0.0), (-2.8, -2.8), (0.0, -4.0), (2.8, -2.8),
]

START_OCTAGON = [
    (5.0, 0.0), (3.5, 3.5), (0.0, 5.0), (-3.5, 3.5),
    (-5.0, 0.0), (-3.5, -3.5), (0.0, -5.0), (3.5, -3.5),
]

PO_STARTS = [
    (-4.25, 1.75), (-1.75, 4.25), (4.25, 1.75), (1.75, 4.25),
    (4.25, -1.75), (1.75, -4.25), (-1.75, -4.25), (-4.25, -1.75)
]

# X1/Y1, X2/Y2, X3/Y3 rewards
VERTEX_VALUES = [10, 5, 0]


def _plot_octagon(ax, points: List[Tuple[float, float]], style: str, label: str) -> None:
    closed = points + [points[0]]
    x, y = zip(*closed)
    ax.plot(x, y, style, label=label)


def _setup_arena(ax, xlim=(-5.5, 5.5), ylim=(-5.5, 5.5)) -> None:
    ax.grid(color="grey", linestyle="--", linewidth=0.5, alpha=0.5)
    ax.axhline(0, color="black", linewidth=0.5)
    ax.axvline(0, color="black", linewidth=0.5)

    _plot_octagon(ax, ARENA_OCTAGON, "b:", "Testing Arena")
    _plot_octagon(ax, START_OCTAGON, "ko-", "Start Positions")

    # Start position markers (matching your original behavior)
    an_start_x, an_start_y = zip(*START_OCTAGON)
    ax.plot(an_start_x, an_start_y, "o", color="black", markersize=10)

    ax.set_xlim(xlim)
    ax.set_ylim(ylim)
    ax.set_aspect("equal", "box")
    ax.set_xlabel("X")
    ax.set_ylabel("Y")


def assign_colors(versions: Iterable[str]) -> Dict[str, Tuple[float, float, float, float]]:
    """
    Assign a distinct-ish color per version using a Matplotlib colormap.
    (No hardcoded colors, so it scales to many triangles.)
    """
    versions = list(dict.fromkeys(versions))  # de-dup while preserving order
    n = max(len(versions), 1)
    cmap = plt.get_cmap("tab20" if n <= 20 else "hsv")
    colors: Dict[str, Tuple[float, float, float, float]] = {}
    for i, v in enumerate(versions):
        # Evenly sample the colormap
        colors[v] = cmap(i / max(n - 1, 1))
    return colors


def plot_triangles_from_list(
    triangles_csv: str | Path,
    output: str | Path,
    xlim: Tuple[float, float] = (-5.5, 5.5),
    ylim: Tuple[float, float] = (-5.5, 5.5),
    transparent: bool = True,
    show: bool = False,
) -> Path:
    """
    Plot all triangles whose "Version" appears in list_file.
    Saves to output (file path). If output is a directory, a default filename is used.
    """
    triangles_csv = Path(triangles_csv)
    if not triangles_csv.exists():
        raise FileNotFoundError(f"Triangles CSV not found: {triangles_csv}")

    df = pd.read_csv(triangles_csv)

    # Filter down to requested versions

    # Report missing versions (don’t hard-fail—just warn)
    present = set(df["Version"].astype(str).tolist())

    colors = assign_colors(df["Version"].astype(str).tolist())
    print('colors', colors, type(colors))
    #colors["A"] = (1.0, 0.0, 0.0, 1.0)

    fig, ax = plt.subplots(figsize=(8, 8))
    if transparent:
        fig.patch.set_alpha(0)
        ax.set_facecolor("none")

    _setup_arena(ax, xlim=xlim, ylim=ylim)

    # Keep legend clean: only add one legend entry per triangle
    used_labels = set()


    for _, row in df.iterrows():
        v = str(row["Version"])
        color = colors.get(v, (0.2, 0.2, 0.2, 1.0))

        verts = [
            (float(row["X1"]), float(row["Y1"])),
            (float(row["X2"]), float(row["Y2"])),
            (float(row["X3"]), float(row["Y3"])),
        ]

        tri_x = [p[0] for p in verts] + [verts[0][0]]
        tri_y = [p[1] for p in verts] + [verts[0][1]]

        label = f"Coin Set {v}"
        plot_label = label if label not in used_labels else None
        used_labels.add(label)

        ax.plot(tri_x, tri_y, "-", color=color, linewidth=2, label=plot_label)
        ax.fill(tri_x, tri_y, color=color, alpha=0.25)

        # Annotate vertices with 10, 5, 0 (for X1/Y1, X2/Y2, X3/Y3)
        for (x, y), val in zip(verts, VERTEX_VALUES):
            ax.scatter([x], [y], color=color, s=50, zorder=3, marker="x")
            ax.text(x + 0.1, y + 0.1, str(val), fontsize=10, fontweight="bold", color=color)
        # --- Centroid marker (average of vertices) ---
        cx = sum(p[0] for p in verts) / 3.0
        cy = sum(p[1] for p in verts) / 3.0

        # Draw centroid as a distinct marker with outline
        ax.scatter(
            [cx], [cy],
            s=90,
            marker="*",
            facecolors="white",
            edgecolors=color,
            linewidths=2,
            zorder=4,
        )

        # # Optional: label the centroid with the version
        # ax.text(
        #     cx + 0.12, cy + 0.12,
        #     f"C{v}",
        #     fontsize=9,
        #     fontweight="bold",
        #     color=color,
        #     zorder=5,
        # )


    ax.set_title(f"Selected Coin Sets ({len(df)} plotted)")
    ax.legend(loc="upper right")

    plt.tight_layout()

    output = Path(output)
    if output.is_dir():
        output = output / "triangles_selected.png"
    else:
        output.parent.mkdir(parents=True, exist_ok=True)

    plt.savefig(output, dpi=300, transparent=transparent)
    if show:
        plt.show()
    plt.close(fig)
    return output


def main() -> int:
    p = argparse.ArgumentParser(description="Plot any number of triangles listed in a file.")
    p.add_argument("--triangles-csv", required=True, help="CSV containing triangle vertices with a 'Version' column.")
    p.add_argument("--output", required=True, help="Output PNG path, or a directory.")
    p.add_argument("--xlim", nargs=2, type=float, default=[-5.5, 5.5], metavar=("XMIN", "XMAX"))
    p.add_argument("--ylim", nargs=2, type=float, default=[-5.5, 5.5], metavar=("YMIN", "YMAX"))
    p.add_argument("--no-transparent", action="store_true", help="Disable transparent background.")
    p.add_argument("--show", action="store_true", help="Show plot window (in addition to saving).")

    args = p.parse_args()

    out = plot_triangles_from_list(
        triangles_csv=args.triangles_csv,
        output=args.output,
        xlim=(args.xlim[0], args.xlim[1]),
        ylim=(args.ylim[0], args.ylim[1]),
        transparent=not args.no_transparent,
        show=args.show,
    )
    print(f"Saved: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
