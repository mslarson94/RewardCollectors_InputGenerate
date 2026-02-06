import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Fixed octagons (same as in triangle_version_13.png)
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

def _setup_arena(ax, xlim=(-5.5, 5.5), ylim=(-5.5, 5.5)):
    ax.grid(color="grey", linestyle="--", linewidth=0.5, alpha=0.5)
    ax.axhline(0, color="black", linewidth=0.5)
    ax.axvline(0, color="black", linewidth=0.5)

    def plot_octagon(points, style, label):
        closed = points + [points[0]]
        x, y = zip(*closed)
        ax.plot(x, y, style, label=label)

    plot_octagon(ARENA_OCTAGON, "b:", "Testing Arena")
    plot_octagon(START_OCTAGON, "ko-", "Start Positions")
    an_start_x, an_start_y = zip(*START_OCTAGON)
    po_start_x, po_start_y = zip(*PO_STARTS)
    ax.plot(an_start_x, an_start_y, 'o', color="black", markersize=10)
    #ax.plot(po_start_x, po_start_y, 'o', color="black", markersize=15)

    ax.set_xlim(xlim)
    ax.set_ylim(ylim)
    ax.set_aspect("equal", "box")
    ax.set_xlabel("X")
    ax.set_ylabel("Y")


def plot_triangles_A_B_with_arenav1(
    csv_path: str = "triangle_positions-formatted__A_D_.csv",
    output_path: str = "triangles_A_B_with_arena.png",
    pairs: list = ["A", "B"]
):
    df = pd.read_csv(csv_path)

    # Only keep A & B
    df = df[df["Version"].isin(pairs)]

    colors = {pairs[0]: "tab:purple", pairs[1]: "darkcyan"}
    labels = {pairs[0]: f"Coin Set {pairs[0]}", pairs[1]: f"Coin Set {pairs[1]}"}

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.grid(color="grey", linestyle="--", linewidth=0.5, alpha=0.5)

    # Axes through origin
    ax.axhline(0, color="black", linewidth=0.5)
    ax.axvline(0, color="black", linewidth=0.5)

    # Helper to plot an octagon
    def plot_octagon(points, style, label):
        closed = points + [points[0]]
        x, y = zip(*closed)
        ax.plot(x, y, style, label=label)

    # Testing arena & start positions (like triangle_version_13.png)
    plot_octagon(ARENA_OCTAGON, "b:", "Testing Arena")
    plot_octagon(START_OCTAGON, "ko-", "Start Positions")

    # Plot triangles A & B with vertex value labels
    for _, row in df.iterrows():
        v = row["Version"]
        color = colors[v]

        verts = [
            (row["X1"], row["Y1"]),
            (row["X2"], row["Y2"]),
            (row["X3"], row["Y3"]),
        ]

        # Close triangle
        tri_x = [p[0] for p in verts] + [verts[0][0]]
        tri_y = [p[1] for p in verts] + [verts[0][1]]

        ax.plot(tri_x, tri_y, "-", color=color, linewidth=2, label=labels[v])
        ax.fill(tri_x, tri_y, color=color, alpha=0.25)

        # Annotate vertices with 10, 5, 0 (for X1/Y1, X2/Y2, X3/Y3)
        for (x, y), val in zip(verts, VERTEX_VALUES):
            ax.scatter([x], [y], color=color, s=50, zorder=3, marker="x")
            ax.text(
                x + 0.1,
                y + 0.1,
                str(val),
                fontsize=10,
                fontweight="bold",
                color=color,
            )

    ax.set_xlim(-9.8, 9.8)
    ax.set_ylim(-15, 10)
    ax.set_aspect("equal", "box")
    ax.set_xlabel("X")
    ax.set_ylabel("Y")
    ax.set_title(f"Coin Sets {pairs[0]} & {pairs[1]}")
    ax.legend(loc="upper right")

    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.show()

def plot_single_triangle_with_arena(
    version: str,
    csv_path: str = "triangle_positions-formatted__A_D_.csv",
    output_path: str | None = None,
    xlim: tuple = (-5.5, 5.5),
    ylim: tuple = (-5.5, 5.5),
):
    """
    Plot a single triangle (e.g., 'A', 'B', 'C', or 'D') with arena and vertex values.
    """
    df = pd.read_csv(csv_path)
    row = df[df["Version"] == version].iloc[0]

    # Choose a color per version (fallback if not listed)
    colors = {"A": "darkcyan", "B": "tab:green", "C": "tab:blue", "D": "tab:red"}
    color = colors.get(version, "tab:purple")

    fig, ax = plt.subplots(figsize=(8, 8))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _setup_arena(ax, xlim, ylim)

    verts = [
        (row["X1"], row["Y1"]),
        (row["X2"], row["Y2"]),
        (row["X3"], row["Y3"]),
    ]

    tri_x = [p[0] for p in verts] + [verts[0][0]]
    tri_y = [p[1] for p in verts] + [verts[0][1]]

    ax.plot(tri_x, tri_y, "-", color=color, linewidth=2, label=f"Coin Set {version}")
    ax.fill(tri_x, tri_y, color=color, alpha=0.25)

    for (x, y), val in zip(verts, VERTEX_VALUES):
        ax.scatter([x], [y], color=color, s=50, zorder=3, marker="x")
        ax.text(x + 0.1, y + 0.1, str(val),
                fontsize=10, fontweight="bold", color=color)

    ax.set_title(f"Coin Set {version}")
    ax.legend(loc="upper right")
    plt.tight_layout()

    if output_path is None:
        output_path = f"triangle_{version}_with_arena.png"
    else:
        output_path = Path(output_path)
        if output_path.is_dir():
            output_path = output_path / f"triangle_{version}_with_arena.png"

    plt.savefig(output_path, dpi=300, transparent=True)
    plt.close(fig)

def plot_triangles_A_B_with_arena(
    csv_path: str = "triangle_positions-formatted__A_D_.csv",
    output_path: str = "triangles_A_B_with_arena.png",
    pairs: list = ["A", "B"],
    xlim: tuple = (-5.5, 5.5),
    ylim: tuple = (-5.5, 5.5),
):
    df = pd.read_csv(csv_path)
    #df = df[df["Version"].isin(["A", "B"])]
        # Only keep A & B
    df = df[df["Version"].isin(pairs)]

    colors = {pairs[0]: "tab:purple", pairs[1]: "darkcyan"}
    labels = {pairs[0]: f"Coin Set {pairs[0]}", pairs[1]: f"Coin Set {pairs[1]}"}

    #colors = {"A": "tab:orange", "B": "tab:green"}
    #labels = {"A": "Triangle A", "B": "Triangle B"}

    fig, ax = plt.subplots(figsize=(8, 8))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _setup_arena(ax, xlim, ylim)

    for _, row in df.iterrows():
        v = row["Version"]
        color = colors.get(v, "tab:blue")

        verts = [
            (row["X1"], row["Y1"]),
            (row["X2"], row["Y2"]),
            (row["X3"], row["Y3"]),
        ]

        tri_x = [p[0] for p in verts] + [verts[0][0]]
        tri_y = [p[1] for p in verts] + [verts[0][1]]

        ax.plot(tri_x, tri_y, "-", color=color, linewidth=2, label=labels.get(v, f"Coin Set {v}"))
        ax.fill(tri_x, tri_y, color=color, alpha=0.25)

        for (x, y), val in zip(verts, VERTEX_VALUES):
            ax.scatter([x], [y], color=color, s=50, zorder=3, marker="x")
            ax.text(x + 0.1, y + 0.1, str(val),
                    fontsize=10, fontweight="bold", color=color)

    ax.set_title(f"Coin Sets {pairs[0]} & {pairs[1]}")
    ax.legend(loc="upper right")
    plt.tight_layout()
    #fig.patch.set_alpha(0)
    #ax.set_facecolor("none")
    if output_path is None:
        output_path = Path(f"triangle_{pairs[0]}_{pairs[1]}_with_arena.png")
    else:
        output_path = Path(output_path)
        if output_path.is_dir():
            output_path = output_path / f"triangle_{pairs[0]}_{pairs[1]}_with_arena.png"
    plt.savefig(output_path, dpi=300, transparent=True)
    plt.close(fig)

def plot_single_triangle_no_background(
    version: str,
    csv_path: str = "triangle_positions-formatted__A_D_.csv",
    output_path: str | None = None,
    xlim=(-5.5, 5.5),
    ylim=(-5.5, 5.5),
):
    df = pd.read_csv(csv_path)
    row = df[df["Version"] == version].iloc[0]

    colors = {"A": "darkcyan", "B": "tab:green", "C": "tab:blue", "D": "tab:red"}
    color = colors.get(version, "tab:purple")

    fig, ax = plt.subplots(figsize=(8, 8))

    # No grid, no arena, just limits
    ax.set_xlim(xlim)
    ax.set_ylim(ylim)
    ax.set_aspect("equal", "box")

    # Turn off axes if you truly want nothing visible except triangle
    ax.axis("off")

    verts = [
        (row["X1"], row["Y1"]),
        (row["X2"], row["Y2"]),
        (row["X3"], row["Y3"]),
    ]

    tri_x = [p[0] for p in verts] + [verts[0][0]]
    tri_y = [p[1] for p in verts] + [verts[0][1]]

    ax.plot(tri_x, tri_y, "-", color=color, linewidth=2)
    ax.fill(tri_x, tri_y, color=color, alpha=0.5)

    for (x, y), val in zip(verts, VERTEX_VALUES):
        ax.scatter([x], [y], color=color, s=50, zorder=3, marker="x")
        ax.text(x + 0.1, y + 0.1, str(val),
                fontsize=10, fontweight="bold", color=color)
    ax.set_title(f"Coin Set {version}")
    # Transparent background (no figure/axes background in PNG)
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")

    if output_path is None:
        output_path = Path(f"triangle_{version}_no_background.png")
    else:
        output_path = Path(output_path)
        if output_path.is_dir():
            output_path = output_path / f"triangle_{version}_no_background.png"

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, transparent=True)
    plt.close(fig)
if __name__ == "__main__":
    infile = '/Users/mairahmac/Desktop/TriangleSets/triangle_positions-formatted__A_D_.csv'
    outfileCombo = '/Users/mairahmac/Desktop/TriangleSets/coinSetAxandBx.png'

    outPath = '/Users/mairahmac/Desktop/TriangleSets'
    pairlist = ['Ax', 'Bx']
    xlim_real = (-9.8, 9.8)
    ylim_real = (-15, 10)
    plot_triangles_A_B_with_arena(infile, outPath, pairlist)
    #plot_single_triangle_with_arena('A', infile, outPath, xlim_real, ylim_real)
    plot_single_triangle_with_arena('Bx', infile, outPath, xlim_real, ylim_real)
    #plot_single_triangle_no_background('A', infile, outPath, xlim_real, ylim_real)
