import pandas as pd
import os

def augment_events_with_aligned_data(events_file, processed_file, output_file, is_reference=False):
    df_events = pd.read_csv(events_file)
    df_processed = pd.read_csv(processed_file)

    # Ensure datetime column is actually parsed
    if 'ParsedTimestamp' in df_processed.columns:
        df_processed['ParsedTimestamp'] = pd.to_datetime(df_processed['ParsedTimestamp'])
    if 'AN_ParsedTS' in df_processed.columns:
        df_processed['AN_ParsedTS'] = pd.to_datetime(df_processed['AN_ParsedTS'])

    # Build list of columns to merge in
    columns_to_add = ['original_row_start', 'ParsedTimestamp', 'HeadPosAnchored_x', 'HeadPosAnchored_y', 'HeadPosAnchored_z']
    if 'AN_ParsedTS' in df_processed.columns:
        columns_to_add.append('AN_ParsedTS')

    df_augmented = pd.merge(
        df_events,
        df_processed[columns_to_add],
        on='original_row_start',
        how='left'
    )

    # If A (reference), copy ParsedTimestamp into AN_ParsedTS
    if is_reference:
        df_augmented['AN_ParsedTS'] = df_augmented['ParsedTimestamp']

    df_augmented.to_csv(output_file, index=False)
    print(f"✅ Augmented events file saved to {output_file}")


if __name__ == "__main__":

    base_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day"
    events_dir = "ExtractedEvents_csv_Flat"
    proc_dir = "ProcessedData_Flat"

    events_file_A = os.path.join(events_dir, 'ObsReward_A_02_17_2025_15_11_processed_events.csv')
    events_file_B = os.path.join(events_dir, 'ObsReward_B_02_17_2025_15_11_processed_events_unaligned.csv')
    processed_file_A = os.path.join(proc_dir, 'ObsReward_A_02_17_2025_15_11_processed.csv')
    processed_file_B_aligned = os.path.join(proc_dir, 'ObsReward_B_02_17_2025_15_11_processed.csv')

    output_file_A = os.path.join(events_dir, 'ObsReward_A_02_17_2025_15_11_events_augmented.csv')
    output_file_B = os.path.join(events_dir, 'ObsReward_B_02_17_2025_15_11_events_augmented.csv')

    augment_events_with_aligned_data(events_file_A, processed_file_A, output_file_A, is_reference=True)
    augment_events_with_aligned_data(events_file_B, processed_file_B_aligned, output_file_B)
