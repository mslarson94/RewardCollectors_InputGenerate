# Reload libraries and reprocess data
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import re
from datetime import datetime

# Reload the dataset
file_path = '/mnt/data/ObsReward_A_12_03_2024_12_49_processed.csv'
data = pd.read_csv(file_path)

# Convert `Timestamp` to datetime for comparison
def parse_timestamp(timestamp):
    try:
        return datetime.strptime(timestamp, "%H:%M:%S:%f")
    except Exception:
        return None

data['ParsedTimestamp'] = data['Timestamp'].apply(parse_timestamp)

# Parse `HeadPosAnchored` and extract x and z
def parse_position_xz(position):
    try:
        values = list(map(float, position.split()))
        return (values[0], values[2]) if len(values) == 3 else None
    except Exception:
        return None

data['HeadPosAnchored'] = data['HeadPosAnchored'].apply(parse_position_xz)
data['x'] = data['HeadPosAnchored'].apply(lambda pos: pos[0] if isinstance(pos, tuple) else None)
data['z'] = data['HeadPosAnchored'].apply(lambda pos: pos[1] if isinstance(pos, tuple) else None)
valid_data = data.dropna(subset=['RoundNum', 'x', 'z'])

# Select data for RoundNum = 1.0
round_data_example = valid_data[valid_data['RoundNum'] == 1.0]

# Extract pin and coin locations from the `Message` column
def extract_pin_or_coin(message, pattern, group_idx):
    match = re.search(pattern, message)
    if match:
        try:
            values = list(map(float, match.group(group_idx).split()))
            return (values[0], values[2])  # Extract x and z
        except Exception:
            return None
    return None

# Patterns to extract data
pin_pattern = r"localpos: ([\-0-9. ]+)"  # Match after "localpos:"
coin_pattern = r"Closest location was: ([\-0-9. ]+)"  # Match after "Closest location was:"

# Extract pins and coins
data['PinLocation'] = data['Message'].astype(str).apply(lambda msg: extract_pin_or_coin(msg, pin_pattern, 1))
data['CoinLocation'] = data['Message'].astype(str).apply(lambda msg: extract_pin_or_coin(msg, coin_pattern, 1))

# Filter rows with valid pin and coin locations
pins = data.dropna(subset=['PinLocation']).reset_index(drop=True)
coins = data.dropna(subset=['CoinLocation']).reset_index(drop=True)

# Create animation with pins and coins
def animate_round_with_pins_and_coins(round_data, round_num, pins, coins):
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.set_title(f'Participant Path with Pins and Coins - Round {round_num}')
    ax.set_xlim(valid_data['x'].min() - 1, valid_data['x'].max() + 1)
    ax.set_ylim(valid_data['z'].min() - 1, valid_data['z'].max() + 1)
    ax.set_xlabel("X Position")
    ax.set_ylabel("Z Position")

    trail, = ax.plot([], [], color='blue', alpha=0.3, linewidth=2)
    point, = ax.plot([], [], 'ko', markersize=8)
    pins_plot, = ax.plot([], [], 'ro', markersize=6, label='Pins')  # Red dots for pins
    coins_plot, = ax.plot([], [], 'go', markersize=6, label='Coins')  # Green dots for coins

    pin_timestamps = pins['ParsedTimestamp'].tolist()
    coin_timestamps = coins['ParsedTimestamp'].tolist()
    pin_coords = pins['PinLocation'].tolist()
    coin_coords = coins['CoinLocation'].tolist()

    def init():
        trail.set_data([], [])
        point.set_data([], [])
        pins_plot.set_data([], [])
        coins_plot.set_data([], [])
        return trail, point, pins_plot, coins_plot

    def update(frame):
        x_data = round_data['x'].iloc[:frame + 1]
        z_data = round_data['z'].iloc[:frame + 1]
        trail.set_data(x_data, z_data)
        point.set_data([round_data['x'].iloc[frame]], [round_data['z'].iloc[frame]])

        # Add pins and coins dynamically based on timestamps
        current_time = round_data['ParsedTimestamp'].iloc[frame]
        active_pins = [
            coord for ts, coord in zip(pin_timestamps, pin_coords) if ts <= current_time
        ]
        active_coins = [
            coord for ts, coord in zip(coin_timestamps, coin_coords) if ts <= current_time
        ]
        if active_pins:
            pins_plot.set_data(*zip(*active_pins))
        if active_coins:
            coins_plot.set_data(*zip(*active_coins))

        return trail, point, pins_plot, coins_plot

    ani = FuncAnimation(fig, update, frames=len(round_data), init_func=init, blit=True)
    plt.close(fig)
    return ani

# Generate and save the animation
round_animation_pins_coins = animate_round_with_pins_and_coins(
    round_data_example, round_num=1.0, pins=pins, coins=coins
)

output_video_with_pins_coins = '/mnt/data/round_animation_with_pins_coins.mp4'
round_animation_pins_coins.save(output_video_with_pins_coins, writer='ffmpeg', fps=30)

output_video_with_pins_coins
