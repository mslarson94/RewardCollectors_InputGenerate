import pandas as pd
from datetime import datetime

# Load the new chest and cylinder walk files
chest_morning_path = '/mnt/data/R019_AN_Chest_Morning.csv'
cylinder_afternoon_path = '/mnt/data/R037_AN_Cylinders_Afternoon.csv'
cylinder_morning_path = '/mnt/data/R037_PO_Cylinders_Morning.csv'

chest_morning_df = pd.read_csv(chest_morning_path)
cylinder_afternoon_df = pd.read_csv(cylinder_afternoon_path)
cylinder_morning_df = pd.read_csv(cylinder_morning_path)

# Convert timestamps to datetime
def parse_timestamp(ts_str):
    return datetime.strptime(ts_str, "%H:%M:%S:%f")

# Parse Chest Morning Walks
chest_morning_df['start_Timestamp'] = chest_morning_df['start_Timestamp'].apply(parse_timestamp)
chest_morning_df['end_Timestamp'] = chest_morning_df['end_Timestamp'].apply(parse_timestamp)

# Parse Cylinder Afternoon Walks
cylinder_afternoon_df['start_Timestamp'] = cylinder_afternoon_df['start_Timestamp'].apply(parse_timestamp)
cylinder_afternoon_df['end_Timestamp'] = cylinder_afternoon_df['end_Timestamp'].apply(parse_timestamp)

# Parse Cylinder Morning Walks
cylinder_morning_df['start_Timestamp'] = cylinder_morning_df['start_Timestamp'].apply(parse_timestamp)
cylinder_morning_df['end_Timestamp'] = cylinder_morning_df['end_Timestamp'].apply(parse_timestamp)

# Calculate duration in seconds for Chest walks
chest_morning_df['Duration'] = (chest_morning_df['end_Timestamp'] - chest_morning_df['start_Timestamp']).dt.total_seconds()

# Calculate duration in seconds for Cylinder walks (Afternoon)
cylinder_afternoon_df['Duration'] = (cylinder_afternoon_df['end_Timestamp'] - cylinder_afternoon_df['start_Timestamp']).dt.total_seconds()

# Calculate duration in seconds for Cylinder walks (Morning)
cylinder_morning_df['Duration'] = (cylinder_morning_df['end_Timestamp'] - cylinder_morning_df['start_Timestamp']).dt.total_seconds()

# For each chest walk, find the closest-duration cylinder walk
results = []
for idx, chest_row in chest_morning_df.iterrows():
    chest_duration = chest_row['Duration']

    # Check Cylinder Afternoon Walks
    afternoon_diffs = (cylinder_afternoon_df['Duration'] - chest_duration).abs()
    min_afternoon_idx = afternoon_diffs.idxmin()
    min_afternoon_diff = afternoon_diffs[min_afternoon_idx]

    # Check Cylinder Morning Walks
    morning_diffs = (cylinder_morning_df['Duration'] - chest_duration).abs()
    min_morning_idx = morning_diffs.idxmin()
    min_morning_diff = morning_diffs[min_morning_idx]

    # Determine the best match
    if min_afternoon_diff < min_morning_diff:
        best_match = f"R037_AN_Cylinder_Afternoon_{min_afternoon_idx}"
        best_match_duration = cylinder_afternoon_df.loc[min_afternoon_idx, 'Duration']
        duration_diff = min_afternoon_diff
    else:
        best_match = f"R037_PO_Cylinder_Morning_{min_morning_idx}"
        best_match_duration = cylinder_morning_df.loc[min_morning_idx, 'Duration']
        duration_diff = min_morning_diff

    results.append({
        'ChestStart': chest_row['start_Timestamp'].strftime("%H:%M:%S:%f"),
        'ChestEnd': chest_row['end_Timestamp'].strftime("%H:%M:%S:%f"),
        'ChestDurationSeconds': chest_duration,
        'BestMatchID': best_match,
        'BestMatchDurationSeconds': best_match_duration,
        'DurationDifferenceSeconds': duration_diff
    })

# Save the full rows of the matches
matched_rows = []

for result in results:
    best_match_id = result['BestMatchID']
    if "Afternoon" in best_match_id:
        best_index = int(best_match_id.split("_")[-1])
        matched_row = cylinder_afternoon_df.iloc[best_index].copy()
    else:
        best_index = int(best_match_id.split("_")[-1])
        matched_row = cylinder_morning_df.iloc[best_index].copy()

    matched_row['Matched_ChestStart'] = result['ChestStart']
    matched_row['Matched_ChestEnd'] = result['ChestEnd']
    matched_row['ChestDurationSeconds'] = result['ChestDurationSeconds']
    matched_row['BestMatchDurationSeconds'] = result['BestMatchDurationSeconds']
    matched_row['DurationDifferenceSeconds'] = result['DurationDifferenceSeconds']
    matched_row['BestMatchID'] = best_match_id
    matched_rows.append(matched_row)

# Combine into DataFrame
matched_cylinder_df = pd.DataFrame(matched_rows)

# Display the DataFrame
import ace_tools as tools
tools.display_dataframe_to_user(name="Matched_Cylinder_Rows_Morning", dataframe=matched_cylinder_df)
