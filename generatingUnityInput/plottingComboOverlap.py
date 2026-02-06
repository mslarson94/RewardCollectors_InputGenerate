import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from shapely.geometry import Polygon
from shapely.ops import unary_union

# Load your data
indir = "/Users/mairahmac/Desktop/TriangleSets/"
comboChoiceFile = indir + 'triangle_positions-formatted__A_D_.csv'
combo_choices = pd.read_csv(comboChoiceFile)
grouped = combo_choices.groupby('triangleCombos')
outFile =  indir + 'comboOverlap.png'

# Color settings for combos
combo_colors = {1: 'blue', 2: 'red'}

# Marker styles for triangles within each combo
triangle_markers = ['o', '*', 's', 'D', '^', 'v', '<', '>', 'X', 'P']

# Define function for arena
def define_arena(radius=5):
    return Polygon([(radius * np.cos(theta), radius * np.sin(theta)) 
                    for theta in np.linspace(0, 2 * np.pi, 9)[:-1]])

# Create arena polygons
arena_polygon = define_arena(radius=5)
inner_arena_polygon = define_arena(radius=4)

# Create the plot
fig, ax = plt.subplots()
ax.set_aspect('equal')
background_color = fig.get_facecolor()
ax.grid(True)

# Plot each combo
for combo_id, group in grouped:
    combo_polygons = []

    for i, (_, row) in enumerate(group.iterrows()):
        pts = [(row['X1'], row['Y1']), (row['X2'], row['Y2']), (row['X3'], row['Y3'])]
        poly = Polygon(pts)
        combo_polygons.append(poly)

        # # Plot triangle vertices with distinct marker
        # xs, ys = zip(*pts)
        # marker_style = triangle_markers[i % len(triangle_markers)]
        # ax.scatter(xs, ys,
        #            s=15,
        #            facecolors='none',
        #            edgecolors=combo_colors[combo_id],
        #            marker=marker_style,
        #            linewidths=1,
        #            zorder=5,
        #            label=f'Combo {combo_id} - T{i+1}')

    # Union and plot combo with hole support
    merged_combo = unary_union(combo_polygons)
    if merged_combo.geom_type == 'Polygon':
        merged_combo = [merged_combo]

    for poly in merged_combo:
        # Exterior
        x, y = poly.exterior.xy
        #ax.fill(x, y, alpha=0.1, color=combo_colors[combo_id], edgecolor='none')
        #ax.fill(x, y, color='none', edgecolor=combo_colors[combo_id], linewidth=1)
        # Interiors (holes with transparency)
        for interior in poly.interiors:
            hole_x, hole_y = interior.xy
            #ax.fill(hole_x, hole_y, facecolor=background_color, edgecolor=combo_colors[combo_id], linewidth=1, zorder=3)
            ax.fill(hole_x, hole_y, facecolor='white', alpha = 0.45, edgecolor='none', zorder=3)
            #ax.fill(hole_x, hole_y, facecolor='none', edgecolor=combo_colors[combo_id], linewidth=1, zorder=3)
        ax.fill(x, y, alpha=0.1, color=combo_colors[combo_id], edgecolor='none')
        ax.fill(x, y, color='none', edgecolor=combo_colors[combo_id], linewidth=1)

    for poly in merged_combo:
        for interior in poly.interiors:
            ax.fill(hole_x, hole_y, facecolor='none', edgecolor=combo_colors[combo_id], linewidth=1, zorder=3)

# Arena outlines
outer_x, outer_y = arena_polygon.exterior.xy
inner_x, inner_y = inner_arena_polygon.exterior.xy
ax.plot(outer_x, outer_y, 'k--', linewidth=2, label='Outer Arena')
ax.plot(inner_x, inner_y, 'k:', linewidth=2, label='Inner Arena')

# Axis labels and legend
ax.set_xlabel("Meters")
ax.set_ylabel("Meters")
ax.set_title("Triangle Combos with Transparent Holes and Vertex Markers")
#ax.grid(True)
#ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=3)

# Save
plt.tight_layout()
plt.savefig(outFile, bbox_inches='tight')
