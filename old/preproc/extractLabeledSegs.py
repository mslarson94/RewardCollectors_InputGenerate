def extract_labeled_segments_from_specs(events_df, tracking_df, specs):
    """
    Extracts labeled segments based on flexible (start_tag, [end_tags], label) specs.

    Parameters:
    - events_df: DataFrame with 'Timestamp' and 'Message'
    - tracking_df: DataFrame with 'Timestamp' and tracking data
    - specs: List of tuples (start_tag, list_of_end_tags, label)

    Returns:
    - List of segments: each is a dict with 'start', 'end', 'segment_data', 'label'
    """
    all_segments = []

    for start_tag, end_tags, label in specs:
        start_rows = events_df[events_df['Message'].str.contains(start_tag, case=False, na=False)]

        for _, start_row in start_rows.iterrows():
            start_time = start_row['Timestamp']

            # Look for the first matching end tag after the start time
            end_rows = pd.concat([
                events_df[
                    (events_df['Message'].str.contains(end_tag, case=False, na=False)) &
                    (events_df['Timestamp'] > start_time)
                ]
                for end_tag in end_tags
            ]).sort_values(by='Timestamp')

            if not end_rows.empty:
                end_time = end_rows.iloc[0]['Timestamp']
                segment_data = tracking_df[
                    (tracking_df['Timestamp'] >= start_time) &
                    (tracking_df['Timestamp'] <= end_time)
                ]

                all_segments.append({
                    'start': start_time,
                    'end': end_time,
                    'segment_data': segment_data,
                    'label': label
                })

    return all_segments


## Example Usage
segment_specs = [
    ("Repositioned and ready to start block or round", ["Dropped pin"], "RoundStartToDrop"),
    ("Mark should happen if checked on terminal", ["Repositioned and ready to start block or round"], "ToCylinderStart"),
    ("finished current task", ["Mark should happen if checked on terminal"], "PostBlockIdle"),
    ("Mark should happen if checked on terminal", ["finished current task"], "FullBlockSpan"),
    ("Started watching", ["finished current task", "Mark should happen"], "WatcherPhase")
]

segments = extract_labeled_segments_from_specs(events_df, tracking_df, segment_specs)