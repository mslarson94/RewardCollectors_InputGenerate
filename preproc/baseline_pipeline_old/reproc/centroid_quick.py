
import pandas as pd 

in_path = '/Users/mairahmac/Desktop/TriangleSets/triangles_centroidCalc.csv'
out_path = '/Users/mairahmac/Desktop/TriangleSets/triangles_centroidCalculated.csv'

df = pd.read_csv(in_path)
centroids = df.groupby('Version', as_index=False)[['x','y']].mean()
centroids = centroids.round(3)
print(centroids)
centroids.to_csv(out_path, index=False)