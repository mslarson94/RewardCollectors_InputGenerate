import pandas as pd
import matplotlib.pyplot as plt


def plot_cascade_timeline(timeline_df, cascade_id):
    """
    Plots a timeline of events for a given cascade_id from a timeline DataFrame.
    """
    df = timeline_df[timeline_df['cascade_id'] == cascade_id].copy()
    df = df.sort_values(by="AppTime")

    fig, ax = plt.subplots(figsize=(12, 2))
    
    for idx, row in df.iterrows():
        ax.scatter(row["AppTime"], 0, label=row["event_type"], s=100)
        ax.text(row["AppTime"], 0.1, row["event_type"], rotation=45, ha="right", fontsize=8)

    ax.set_yticks([])
    ax.set_xlabel("AppTime (s)")
    ax.set_title(f"Event Cascade Timeline: Cascade #{cascade_id}")
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')
    plt.tight_layout()
    plt.show()


# Example usage:
# timeline_df = pd.read_csv("/path/to/event_summary.csv")
# plot_cascade_timeline(timeline_df, cascade_id=12)
