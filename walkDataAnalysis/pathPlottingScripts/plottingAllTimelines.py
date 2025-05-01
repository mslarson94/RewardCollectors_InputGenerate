import pandas as pd
import matplotlib.pyplot as plt

######## Plot Specific Timelines  #########
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
    plt.show(block=True)


# Example usage:
# timeline_df = pd.read_csv("/path/to/event_summary.csv")
# plot_cascade_timeline(timeline_df, cascade_id=12)

######## Gantt All Cascades #########

def plot_gantt_all_cascades(timeline_df):
    gantt_data = timeline_df.groupby("cascade_id").agg(start=("AppTime", "min"), end=("AppTime", "max")).reset_index()
    gantt_data["duration"] = gantt_data["end"] - gantt_data["start"]

    fig, ax = plt.subplots(figsize=(12, len(gantt_data) * 0.4))
    for i, row in gantt_data.iterrows():
        ax.barh(i, row["duration"], left=row["start"], height=0.4)
        ax.text(row["start"], i, f"Cascade {int(row['cascade_id'])}", va="center", fontsize=8)

    ax.set_xlabel("AppTime (s)")
    ax.set_yticks(range(len(gantt_data)))
    ax.set_yticklabels([f"{int(cid)}" for cid in gantt_data["cascade_id"]])
    ax.set_title("Gantt Chart of Cascades")
    plt.tight_layout()
    plt.show(block=True)


########### Plot Duration Histograms ##########

def plot_duration_histogram(timeline_df):
    durations = timeline_df.groupby("cascade_id").agg(duration=("AppTime", lambda x: x.max() - x.min())).reset_index()

    plt.figure(figsize=(10, 5))
    plt.bar(durations["cascade_id"].astype(str), durations["duration"])
    plt.xticks(rotation=90)
    plt.xlabel("Cascade ID")
    plt.ylabel("Duration (seconds)")
    plt.title("Duration of Each Cascade")
    plt.tight_layout()
    plt.show(block=True)


timeline_df = pd.read_csv("EventCascades/event_summary.csv")

plot_timeline_single_cascade(timeline_df, cascade_id=12)
plot_gantt_all_cascades(timeline_df)
plot_duration_histogram(timeline_df)
launch_interactive_dashboard(timeline_df)
