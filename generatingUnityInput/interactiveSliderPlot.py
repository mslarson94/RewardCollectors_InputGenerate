import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from shapely.geometry import Polygon
from shapely.ops import unary_union
from matplotlib.widgets import Slider

# Load data
# Load your data
indir = "/Users/mairahmac/Desktop/TriangleSets/newIteration2/firstCombo/"
comboChoiceFile = indir + 'comboChoices2.csv'
combo_df = pd.read_csv(comboChoiceFile)
# grouped = combo_choices.groupby('triangleCombos')
# outFile =  indir + 'comboOverlap.png'

#combo_df = pd.read_csv("comboChoices.csv")
combo1_df = combo_df[combo_df['triangleCombos'] == 1]
combo2_df = combo_df[combo_df['triangleCombos'] == 2]

# Rotation helper
def rotate_points(xy, angle_deg, origin=(0,0)):
    rad = np.radians(angle_deg)
    ox, oy = origin
    return [
        (ox + np.cos(rad)*(x-ox) - np.sin(rad)*(y-oy),
         oy + np.sin(rad)*(x-ox) + np.cos(rad)*(y-oy))
        for x,y in xy
    ]

# Arena setup
def define_arena(r):
    return Polygon([(r*np.cos(t), r*np.sin(t)) for t in np.linspace(0,2*np.pi,9)[:-1]])
arena = define_arena(5)
inner = define_arena(4)

# Precompute Combo 1
combo1_polys = [Polygon([(r['X1'],r['Y1']), (r['X2'],r['Y2']), (r['X3'],r['Y3'])])
                for _,r in combo1_df.iterrows()]
combo1_merged = unary_union(combo1_polys)

# Extract Combo 2 raw points
combo2_pts = [[(r['X1'],r['Y1']), (r['X2'],r['Y2']), (r['X3'],r['Y3'])]
              for _,r in combo2_df.iterrows()]

# Plot + slider
fig, ax = plt.subplots()
plt.subplots_adjust(bottom=0.25)
ax.set_aspect('equal')
ax.set_title("Rotate Combo 2 vs Fixed Combo 1")
ax.set_xlabel("Meters"); ax.set_ylabel("Meters")
ax.grid(True)

# Draw arenas
ax.plot(*arena.exterior.xy, 'k--', linewidth=2, label='Outer Arena')
ax.plot(*inner.exterior.xy, 'k:', linewidth=2, label='Inner Arena')

# Combo 1
for poly in (combo1_merged,) if combo1_merged.geom_type=='Polygon' else combo1_merged:
    ax.fill(*poly.exterior.xy, alpha=0.1, color='blue', edgecolor='k')

# Combo 2 container and draw function
combo2_patches = []
def update(angle):
    for p in combo2_patches:
        p.remove()
    combo2_patches.clear()
    for pts in combo2_pts:
        rotated = rotate_points(pts, angle)
        poly = Polygon(rotated)
        p = ax.fill(*poly.exterior.xy, alpha=0.1, color='red', edgecolor='k')[0]
        combo2_patches.append(p)
    fig.canvas.draw_idle()

angle_slider = Slider(plt.axes([0.2,0.1,0.6,0.03]), 'Rotation°', -45, 45, valinit=0)
angle_slider.on_changed(update)
update(0)

ax.legend(loc='upper right')
plt.show()
