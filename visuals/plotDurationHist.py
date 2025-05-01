def plot_duration_histogram(timeline_df):
    durations = timeline_df.groupby("cascade_id").agg(duration=("AppTime", lambda x: x.max() - x.min())).reset_index()

    plt.figure(figsize=(10, 5))
    plt.bar(durations["cascade_id"].astype(str), durations["duration"])
    plt.xticks(rotation=90)
    plt.xlabel("Cascade ID")
    plt.ylabel("Duration (seconds)")
    plt.title("Duration of Each Cascade")
    plt.tight_layout()
    plt.show()


plot_timeline_single_cascade(timeline_df, cascade_id=12)
plot_gantt_all_cascades(timeline_df)
plot_duration_histogram(timeline_df)
launch_interactive_dashboard(timeline_df)
