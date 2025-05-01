import pandas as pd
import matplotlib.pyplot as plt

def plot_timeline_single_cascade(timeline_df, cascade_id):
    df = timeline_df[timeline_df["cascade_id"] == cascade_id].sort_values("AppTime")

    fig, ax = plt.subplots(figsize=(10, 2))
    for idx, row in df.iterrows():
        ax.scatter(row["AppTime"], 0, s=100, label=row["event_type"])
        ax.text(row["AppTime"], 0.1, row["event_type"], rotation=45, ha="right", fontsize=8)

    ax.set_yticks([])
    ax.set_xlabel("AppTime (s)")
    ax.set_title(f"Timeline of Cascade #{cascade_id}")
    handles, labels = ax.get_legend_handles_labels()
    unique = dict(zip(labels, handles))
    ax.legend(unique.values(), unique.keys(), bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()
