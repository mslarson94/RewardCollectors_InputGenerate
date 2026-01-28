import numpy as np
import pandas as pd
import math

def triangle_side_lengths(p1, p2, p3):
    def d(a, b):
        return math.hypot(a[0]-b[0], a[1]-b[1])
    a = d(p2, p3)
    b = d(p1, p3)
    c = d(p1, p2)
    return sorted([a, b, c])  # s1 <= s2 <= s3

def triangle_shape_descriptor(p1, p2, p3):
    s1, s2, s3 = triangle_side_lengths(p1, p2, p3)
    if s3 == 0:
        return np.array([np.nan, np.nan], dtype=float)
    return np.array([s1 / s3, s2 / s3], dtype=float)

def shape_similarity_matrix_from_df(df, xcols=("X1","X2","X3"), ycols=("Y1","Y2","Y3")):
    # Build descriptors
    desc = []
    for _, r in df.iterrows():
        p1 = (float(r[xcols[0]]), float(r[ycols[0]]))
        p2 = (float(r[xcols[1]]), float(r[ycols[1]]))
        p3 = (float(r[xcols[2]]), float(r[ycols[2]]))
        desc.append(triangle_shape_descriptor(p1, p2, p3))
    D = np.vstack(desc)

    # Pairwise distances in descriptor space
    # dist_ij = ||D[i] - D[j]||
    diffs = D[:, None, :] - D[None, :, :]
    dist = np.linalg.norm(diffs, axis=2)

    # Convert distance -> similarity in [0,1]
    # similarity = exp(-dist / tau)
    tau = np.nanmedian(dist[~np.isnan(dist)])
    if not np.isfinite(tau) or tau == 0:
        tau = 1.0
    sim = np.exp(-dist / tau)

    return pd.DataFrame(sim, index=df.index, columns=df.index), pd.DataFrame(dist, index=df.index, columns=df.index)

tri = pd.read_csv("/Users/mairahmac/Desktop/TriangleSets/triangle_positions-formatted__A_D_.csv")
tri = tri.set_index("Version")
sim, dist = shape_similarity_matrix_from_df(tri)
sim.to_csv("/Users/mairahmac/Desktop/TriangleSets/triangle_shape_similarity.csv")
dist.to_csv("/Users/mairahmac/Desktop/TriangleSets/triangle_shape_distance.csv")
