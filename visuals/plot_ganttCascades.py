import matplotlib.pyplot as plt

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
    plt.show()
