import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Setup
participants = [f"P{i:02d}" for i in range(1, 7)]
n_trials = 50
route_categories = ['Type 1', 'Type 3', 'Type 4', 'Other']
category_palette = {
    'Type 1': '#66c2a5',
    'Type 3': '#fc8d62',
    'Type 4': '#8da0cb',
    'Other': '#e78ac3'
}

# Generate constrained dummy data
data_multi_pins = []
for pid in participants:
    for trial in range(1, n_trials + 1):
        route = np.random.choice(route_categories)
        base_dist = 1.5 if route == 'Type 1' else 4
        for drop in range(3):
            dist = np.random.normal(loc=base_dist, scale=1.0)
            dist = np.clip(dist, 0, 8)  # constrain within 0 to 8
            data_multi_pins.append({
                'Participant': pid,
                'Trial': trial,
                'RouteType': route,
                'PinDropDistance': dist,
                'PinIndex': drop
            })

df_multi = pd.DataFrame(data_multi_pins)

# Filter for a single participant
single_participant = df_multi['Participant'].unique()[0]
df_single = df_multi[df_multi['Participant'] == single_participant]

# Plot
plt.figure(figsize=(12, 4))

# Background route bars
for trial in sorted(df_single['Trial'].unique()):
    route = df_single[df_single['Trial'] == trial]['RouteType'].iloc[0]
    plt.axvspan(trial - 0.4, trial + 0.4, color=category_palette[route], alpha=0.3, zorder=0)

# Jittered black dots
jittered_x = df_single['Trial'] + np.random.normal(0, 0.1, size=len(df_single))
plt.scatter(jittered_x, df_single['PinDropDistance'], color='black', s=20, alpha=0.8, zorder=1)

# Add threshold line
plt.axhline(1, color='red', linestyle='--', linewidth=1)

# Labels and limits
plt.ylim(0, 8)
plt.xlabel("Trial")
plt.ylabel("Pin Drop Distance")
plt.title(f"Constrained Pin Drop Distances per Trial (Participant: {single_participant})")
plt.tight_layout()
plt.show()
