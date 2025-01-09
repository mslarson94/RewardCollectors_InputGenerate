import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import ast
import os

# Load the dataset
directory_path = '/Users/mairahmac/Desktop/RC_TestingNotes/12032024/Processed'
file_name = 'ObsReward_A_12_03_2024_12_49_processed.csv'
file_path = os.path.join(directory_path, file_name)
data = pd.read_csv(file_path)

# Parsing function for extracting x and z positions
def parse_position_xz(position):
    try:
        values = list(map(float, position.split()))
        return (values[0], values[2]) if len(values) == 3 else None
    except Exception:
        return None

# Filter rows with numeric RoundNum and parse HeadPosAnchored
data['RoundNum'] = pd.to_numeric(data['RoundNum'], errors='coerce')
data['HeadPosAnchored'] = data['HeadPosAnchored'].apply(parse_position_xz)
data['x'] = data['HeadPosAnchored'].apply(lambda pos: pos[0] if isinstance(pos, tuple) else None)
data['z'] = data['HeadPosAnchored'].apply(lambda pos: pos[1] if isinstance(pos, tuple) else None)
valid_data = data.dropna(subset=['RoundNum', 'x', 'z'])

# Select data for RoundNum = 1.0
round_data_example = valid_data[valid_data['RoundNum'] == 3.0]

# Animation function
def animate_round_dynamic_xz(round_data, round_num):
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.set_title(f'Participant Path - Round {round_num}')
    ax.set_xlim(valid_data['x'].min() - 1, valid_data['x'].max() + 1)
    ax.set_ylim(valid_data['z'].min() - 1, valid_data['z'].max() + 1)
    ax.set_xlabel("X Position")
    ax.set_ylabel("Z Position")

    trail, = ax.plot([], [], color='blue', alpha=0.3, linewidth=2)
    point, = ax.plot([], [], 'ko', markersize=8)

    def init():
        trail.set_data([], [])
        point.set_data([], [])
        return trail, point

    def update(frame):
        # Ensure frame index is valid
        if frame >= len(round_data):
            return trail, point

        try:
            # Debugging: Print the frame index and corresponding data
            #print(f"Frame: {frame}, x: {round_data['x'].iloc[frame]}, z: {round_data['z'].iloc[frame]}")

            # Update the trail and point data
            x_data = round_data['x'].iloc[:frame + 1]
            z_data = round_data['z'].iloc[:frame + 1]
            trail.set_data(x_data, z_data)
            # Wrap the single values for x and z in a tuple or list
            point.set_data([round_data['x'].iloc[frame]], [round_data['z'].iloc[frame]])
        except Exception as e:
            print(f"Error updating frame {frame}: {e}")  # Debugging: Catch and log errors

        return trail, point



    ani = FuncAnimation(fig, update, frames=len(round_data), init_func=init, blit=True)
    plt.close(fig)
    return ani

# Generate and save the animation
round_animation_xz = animate_round_dynamic_xz(round_data_example, round_num=1.0)
output_video_path_xz_final = 'round_animation_dynamic_xz_final_3.mp4'
out_path = os.path.join(directory_path, output_video_path_xz_final)
round_animation_xz.save(out_path, writer='ffmpeg', fps=30)
print(f"Animation saved to {output_video_path_xz_final}")
