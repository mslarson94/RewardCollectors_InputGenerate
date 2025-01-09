from scipy.spatial.transform import Rotation as R
import numpy as np
import matplotlib.pyplot as plt

def rotate_vector(vector, rotation_matrix):
    return np.dot(rotation_matrix, vector)

def calculate_endpoint(start, a, b, c, d):
    rotation_matrix = R.from_quat([a, b, c, d]).as_matrix()
    unit_vector = np.array([0, 0, 1])
    endpoint = rotate_vector(unit_vector, rotation_matrix)
    return start + endpoint

start_point = np.array([[x, y, z]])
end_point = calculate_endpoint(start_point, a, b, c, d)

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
ax.quiver(start_point[0], start_point[1], start_point[2],
                end_point[0], end_point[1], end_point[2],
                length = 1, normalize = True)
ax.set_xlabel('x')
ax.set_ylabel('y')
ax.set_zlabel('z')
plt.show()