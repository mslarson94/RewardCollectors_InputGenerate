
import pandas as pd

# Load your high-frequency data (200Hz)
hf_df = pd.read_csv("high_freq_stream.csv")  # must contain 'AppTime' column

# Load the semantic timeline
timeline_df = pd.read_csv("structured_timeline_metadata.csv")

# Align each timeline event to the nearest high-freq sample
hf_df = hf_df.set_index("AppTime")
timeline_df["aligned_row_index"] = timeline_df["AppTime"].apply(lambda t: hf_df.index.get_indexer([t], method='nearest')[0])

# Merge aligned rows back into the timeline
aligned_times = hf_df.iloc[timeline_df["aligned_row_index"].values].reset_index()
timeline_df["aligned_AppTime"] = aligned_times["AppTime"]
timeline_df["aligned_sample_data"] = aligned_times.drop(columns="AppTime").to_dict(orient='records')

# Save aligned timeline
timeline_df.to_csv("aligned_timeline.csv", index=False)
