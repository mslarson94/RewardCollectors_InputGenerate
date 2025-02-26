import numpy as np
import matplotlib.pyplot as plt
import random
import os
import pandas as pd
from shapely.geometry import Point, Polygon

def random_point_in_octagon(octagon):
    '''Generate a random point inside the given octagon'''
    min_x = min(p[0] for p in octagon)
    max_x = max(p[0] for p in octagon)
    min_y = min(p[1] for p in octagon)
    max_y = max(p[1] for p in octagon)

    polygon = Polygon(octagon)
    while True:
        x = random.uniform(min_x, max_x)
        y = random.uniform(min_y, max_y)
        if polygon.contains(Point(x, y)):
            return (x, y)

def triangle_area(p1, p2, p3):
    '''Calculate the area of a triangle given three points using the determinant formula'''
    x1, y1 = p1
    x2, y2 = p2
    x3, y3 = p3
    return 0.5 * abs(x1*(y2 - y3) + x2*(y3 - y1) + x3*(y1 - y2))

def generate_valid_triangle(octagon, min_area=10, max_area=12):
    '''Generate a valid triangle with an area between min_area and max_area'''
    while True:
        p1 = random_point_in_octagon(octagon)
        p2 = random_point_in_octagon(octagon)
        p3 = random_point_in_octagon(octagon)
        area = triangle_area(p1, p2, p3)
        if min_area <= area <= max_area:
            return p1, p2, p3, area

def plot_triangle(startOctagon, arenaOctagon, outDir, numIterated):
    '''Generate a triangle with the desired area range, save the plot, and return rounded coordinates'''
    
    # Generate a valid triangle
    p1, p2, p3, area = generate_valid_triangle(arenaOctagon)

    # Round all coordinates to 2 decimal places
    p1 = (round(p1[0], 2), round(p1[1], 2))
    p2 = (round(p2[0], 2), round(p2[1], 2))
    p3 = (round(p3[0], 2), round(p3[1], 2))
    area = round(area, 2)

    # Plot the octagons
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.grid(color = 'grey', linestyle = '--', linewidth = 0.5, alpha = 0.5)
    # Close the octagon shape for plotting
    arenaOctagon.append(arenaOctagon[0])
    x_Aoct, y_Aoct = zip(*arenaOctagon)
    ax.plot(x_Aoct, y_Aoct, 'b:', label="Testing Arena")

    startOctagon.append(startOctagon[0])
    x_Soct, y_Soct = zip(*startOctagon)
    ax.plot(x_Soct, y_Soct, 'ko-', label="Start Positions")
    #ax.scatter(x_Soct, y_Soct, color='black', marker='.', label="Start Positions")
    
    # Plot the triangle
    x_tri = [p1[0], p2[0], p3[0], p1[0]]
    y_tri = [p1[1], p2[1], p3[1], p1[1]]
    ax.plot(x_tri, y_tri, 'r-', label=f"Triangle (Area: {area:.2f})")
    ax.scatter([p1[0], p2[0], p3[0]], [p1[1], p2[1], p3[1]], color='red', marker='*')

    # Formatting plot
    ax.axhline(0, color='black', linewidth=0.5)
    ax.axvline(0, color='black', linewidth=0.5)
    ax.set_xlim(-5.5, 5.5)
    ax.set_ylim(-5.5, 5.5)
    ax.set_aspect('equal')
    ax.legend()
    
    # Save plot with version number
    titleStr = f"Triangle with Area {area:.2f} | Version {numIterated}"
    plt.title(titleStr)
    outFile = os.path.join(outDir, f'triangle_version_{numIterated}.png')
    plt.savefig(outFile)
    plt.close()

    # Return rounded values
    return {
        'Version': numIterated,
        'X1': p1[0], 'Y1': p1[1],
        'X2': p2[0], 'Y2': p2[1],
        'X3': p3[0], 'Y3': p3[1],
        'Area': area
    }


def iterateTriangles(startOctagon, arenaOctagon, outDir, numIterations):
    '''Iterate over plot_triangle numIterations times and save all results'''
    
    # Ensure output directory exists
    os.makedirs(outDir, exist_ok=True)

    # Data storage for all generated triangles
    all_data = []

    for i in range(1, numIterations + 1):
        triangle_data = plot_triangle(startOctagon.copy(), arenaOctagon.copy(), outDir, i)
        all_data.append(triangle_data)

    # Convert to DataFrame and save
    df = pd.DataFrame(all_data)
    csv_file = os.path.join(outDir, 'triangle_positions.csv')
    df.to_csv(csv_file, index=False)

    print(f"All {numIterations} triangles generated and saved. Data stored in '{csv_file}'")

def plot_selected_triangles(startOctagon, arenaOctagon, csv_file, version_list, outFile='selected_triangles.png'):
    '''Plot selected triangle versions from the CSV on the same graph'''

    # Read triangle data from CSV
    df = pd.read_csv(csv_file)

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.grid(color = 'grey', linestyle = '--', linewidth = 0.5, alpha = 0.5)
    # Close the octagon shape for plotting
    arenaOctagon.append(arenaOctagon[0])
    x_Aoct, y_Aoct = zip(*arenaOctagon)
    ax.plot(x_Aoct, y_Aoct, 'b:', label="Testing Arena")

    startOctagon.append(startOctagon[0])
    x_Soct, y_Soct = zip(*startOctagon)
    ax.plot(x_Soct, y_Soct, 'k.-', label="Start Positions")

    # Plot selected triangles
    for version in version_list:
        triangle = df[df['Version'] == version]
        if not triangle.empty:
            x_coords = [triangle['X1'].values[0], triangle['X2'].values[0], triangle['X3'].values[0], triangle['X1'].values[0]]
            y_coords = [triangle['Y1'].values[0], triangle['Y2'].values[0], triangle['Y3'].values[0], triangle['Y1'].values[0]]
            ax.plot(x_coords, y_coords, label=f'Triangle Version {version}', alpha = 0.75, marker="*")

    # Formatting the plot
    ax.axhline(0, color='black', linewidth=0.5)
    ax.axvline(0, color='black', linewidth=0.5)
    ax.set_xlim(-5.5, 5.5)
    ax.set_ylim(-5.5, 5.5)
    ax.set_aspect('equal')
    #ax.legend()
    box = ax.get_position()
    ax.set_position([box.x0, box.y0 + box.height * 0.1,
                 box.width, box.height * 0.9])

    # Put a legend below current axis
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
          fancybox=True, shadow=True, ncol=5)

    plt.title("Selected Triangle Versions")
    plt.savefig(outFile)
    #plt.show()

# Given octagon points
myArenaOctagon = [(4,0), (2.8,2.8), (0,4), (-2.8,2.8), (-4,0), (-2.8,-2.8), (0,-4), (2.8,-2.8)]
myStartOctagon = [(5,0), (3.5, 3.5), (0, 5), (-3.5,3.5), (-5,0), (-3.5, -3.5), (0, -5), (3.5, -3.5)]
myOutDir = '/Users/mairahmac/Desktop/TriangleSets'

# Run multiple iterations and save outputs
iterateTriangles(myStartOctagon, myArenaOctagon, myOutDir, 10)

csv_file = os.path.join(myOutDir, 'triangle_positions_Selected.csv')
#plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[2, 3, 8], outFile='selected_triangles_1a.png')

# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[3], outFile='selected_triangles_3.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[9], outFile='selected_triangles_9.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[2], outFile='selected_triangles_2.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[1], outFile='selected_triangles_1.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[7], outFile='selected_triangles_7.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[8], outFile='selected_triangles_8.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[4], outFile='selected_triangles_4.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[10], outFile='selected_triangles_10.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[11], outFile='selected_triangles_11.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[12], outFile='selected_triangles_12.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[13], outFile='selected_triangles_13.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[5], outFile='selected_triangles_5.png')

plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[1, 7], outFile='selected_triangles_AB.png')
plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[9, 15], outFile='selected_triangles_BC.png')
plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[1, 10, 15, 3, 9], outFile='selected_triangles_CD.png')
plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[1, 7, 9, 15 ], outFile='selected_triangles_DE.png')
plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[1, 7, 3, 15], outFile='selected_triangles_EF.png')

plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[15], outFile='selected_triangles_FG.png')
plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[7, 10], outFile='selected_triangles_710.png')
plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[7, 15], outFile='selected_triangles_715.png')
plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[7, 15, 8, 12], outFile='selected_triangles_josh.png')
plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[7, 10, 8, 12], outFile='selected_triangles_johnson.png')
plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[1, 4, 12, 8], outFile='selected_triangles_napo.png')

# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[2, 3, 9, 4, 13], outFile='selected_triangles_2_13.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[7, 8], outFile='selected_triangles_7_8.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[4, 10], outFile='selected_triangles_4_10.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[11, 12], outFile='selected_triangles_11_12.png')
# plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[13, 5], outFile='selected_triangles_13_5.png')
#plot_selected_triangles(myStartOctagon, myArenaOctagon, csv_file, version_list=[11, 12, 13, 5], outFile='selected_triangles_3.png')
