import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib.animation import FuncAnimation
from stl import mesh

def quaternion_to_euler(q):
    """
    Convert quaternion (w, x, y, z) to Euler angles (roll, pitch, yaw)
    """
    w, x, y, z = q
    t0 = +2.0 * (w * x + y * z)
    t1 = +1.0 - 2.0 * (x * x + y * y)
    roll = np.arctan2(t0, t1)

    t2 = +2.0 * (w * y - z * x)
    t2 = +1.0 if t2 > +1.0 else t2
    t2 = -1.0 if t2 < -1.0 else t2
    pitch = np.arcsin(t2)

    t3 = +2.0 * (w * z + x * y)
    t4 = +1.0 - 2.0 * (y * y + z * z)
    yaw = np.arctan2(t3, t4)

    return roll, pitch, yaw

# Load the STL file of the room
room_mesh = mesh.Mesh.from_file('RewardCollectorsModel.stl')

# Sample trajectory data
time = np.linspace(0, 10, 100)  # Time vector
x = np.sin(time)
y = np.cos(time)
z = time / 2
quaternions = np.array([
    [np.cos(t/2), np.sin(t/2), 0, 0] for t in time
])

# Prepare the figure
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
ax.set_xlabel('X')
ax.set_ylabel('Y')
ax.set_zlabel('Z')

# Plot the room
ax.add_collection3d(mplot3d.art3d.Poly3DCollection(room_mesh.vectors, alpha=0.1, edgecolor='k'))

# Plot the trajectory
line, = ax.plot(x, y, z, label='Trajectory')

# Quiver to show orientation
quiver = ax.quiver([], [], [], [], [], [], length=0.3, normalize=True, color='r')

def update(frame):
    ax.clear()
    ax.set_xlabel('X')
    ax.set_ylabel('Y')
    ax.set_zlabel('Z')

    # Plot the room
    ax.add_collection3d(mplot3d.art3d.Poly3DCollection(room_mesh.vectors, alpha=0.1, edgecolor='k'))

    ax.plot(x[:frame], y[:frame], z[:frame], label='Trajectory')
    ax.scatter(x[frame], y[frame], z[frame], color='b', s=50)

    roll, pitch, yaw = quaternion_to_euler(quaternions[frame])
    dx = np.cos(yaw) * np.cos(pitch)
    dy = np.sin(yaw) * np.cos(pitch)
    dz = np.sin(pitch)
    ax.quiver(x[frame], y[frame], z[frame], dx, dy, dz, length=0.3, color='r', normalize=True)

    return line, quiver

ani = FuncAnimation(fig, update, frames=len(time), blit=False, interval=100)

#To save the animation, uncomment the following lines
Writer = animation.writers['ffmpeg']
writer = Writer(fps=10, metadata=dict(artist='Me'), bitrate=1800)
ani.save('trajectory.mp4', writer=writer)

# plt.legend()
# plt.show()