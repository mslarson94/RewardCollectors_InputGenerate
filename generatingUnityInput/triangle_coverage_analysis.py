
import pandas as pd
import itertools
import numpy as np
import matplotlib.pyplot as plt
from shapely.geometry import Polygon
from shapely.ops import unary_union
from sklearn.metrics.pairwise import cosine_similarity

def load_triangles(csv_path):
    df = pd.read_csv(csv_path)
    triangles = []
    for _, row in df.iterrows():
        triangle = Polygon([(row['X1'], row['Y1']), (row['X2'], row['Y2']), (row['X3'], row['Y3'])])
        triangles.append({'version': row['Version'], 'polygon': triangle, 'area': row['Area']})
    return triangles, df

def define_arena(radius=5):
    points = [(radius * np.cos(theta), radius * np.sin(theta)) 
              for theta in np.linspace(0, 2 * np.pi, 9)[:-1]]
    return Polygon(points)

def top_triangle_combinations(triangles, arena_polygon, top_n=10):
    results = []
    for combo in itertools.combinations(triangles, 3):
        combined = unary_union([t['polygon'] for t in combo])
        intersect = combined.intersection(arena_polygon)
        results.append({
            'versions': tuple(t['version'] for t in combo),
            'covered_area': intersect.area
        })
    return sorted(results, key=lambda x: -x['covered_area'])[:top_n]

def visualize_combinations(triangle_data, combinations, arena_polygon, filename_prefix='combination'):
    for i, entry in enumerate(combinations):
        fig, ax = plt.subplots()
        x, y = arena_polygon.exterior.xy
        ax.plot(x, y, 'k--', label='Arena')
        for v in entry['versions']:
            row = triangle_data[triangle_data['Version'] == v].iloc[0]
            tri = Polygon([(row['X1'], row['Y1']), (row['X2'], row['Y2']), (row['X3'], row['Y3'])])
            tx, ty = tri.exterior.xy
            ax.plot(tx, ty)
            ax.fill(tx, ty, alpha=0.3)
        ax.set_title(f"Combination {entry['versions']} | Covered Area: {entry['covered_area']:.2f}")
        ax.axis('equal')
        ax.legend()
        plt.savefig(f"/Users/mairahmac/Desktop/TriangleSets/newIteration2/{filename_prefix}_{i+1}.png")
        plt.close()

def triangle_similarity_matrix(df):
    features = df[['X1', 'Y1', 'X2', 'Y2', 'X3', 'Y3']].values
    sim_matrix = cosine_similarity(features)
    return pd.DataFrame(sim_matrix, index=df['Version'], columns=df['Version'])

def main(csv_path):
    triangles, df = load_triangles(csv_path)
    arena = define_arena()
    top_combos = top_triangle_combinations(triangles, arena)
    visualize_combinations(df, top_combos, arena)
    sim_matrix = triangle_similarity_matrix(df)
    pd.DataFrame(top_combos).to_csv("/Users/mairahmac/Desktop/TriangleSets/newIteration2/top_triangle_combinations.csv", index=False)
    sim_matrix.to_csv("/Users/mairahmac/Desktop/TriangleSets/newIteration2/triangle_similarity_matrix.csv")
    print("Output written: top_triangle_combinations.csv, triangle_similarity_matrix.csv, combination_*.png")

# Example use:
main("/Users/mairahmac/Desktop/TriangleSets/newIteration2/mega_triangle_positions.csv")
