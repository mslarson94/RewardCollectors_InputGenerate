import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D, art3d
from matplotlib.animation import FuncAnimation
import matplotlib.animation as animation
import matplotlib.patches as mpatches
import pandas as pd

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
df = pd.read_csv('MyraMovements_yFixed.csv')
# Sample trajectory data
time = df['AppTime']  # Time vector
x = df['Position_Z']
y = df['Position_X']
z = df['Position_Y']
# Selecting multiple columns (for example, 'Age' and 'Salary')
selected_columns = df[['Rotation_W', 'Rotation_X', 'Rotation_Y','Rotation_Z']]

# Converting to a NumPy array
quaternions = selected_columns.to_numpy()
# quaternions = [df['Rotation_W'], df['Rotation_X'], df['Rotation_Y'], df['Rotation_Z']]

# Prepare the figure
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
ax.set_xlabel('X')
ax.set_ylabel('Y')
ax.set_zlabel('Z')
ax.set_ylim([-4, 4])
ax.set_zlim([1, 2])
ax.set_xlim([8, 15])

# Plot the trajectory
line, = ax.plot(x, y, z, label='Trajectory')

# Quiver to show orientation
quiver = ax.quiver([], [], [], [], [], [], length=0.3, normalize=True, color='r')

def update(frame):
    ax.clear()
    ax.set_xlabel('X')
    ax.set_ylabel('Y')
    ax.set_zlabel('Z')
    ax.set_ylim([-4, 4])
    ax.set_zlim([1, 2])
    ax.set_xlim([8, 15])


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