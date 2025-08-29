import os
import pandas as pd
import sys

## Not working yet! 08-14-2025
# Add the baseline_pipeline directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'baseline_pipeline'))

from preprocHelpers import safe_parse_timestamp  
#from preproc_helpers import safe_parse_timestamp

def fill_logged_event_positions(events_augmented_file, processed_file, output_file):
    df_events = pd.read_csv(events_augmented_file)
    df_proc = pd.read_csv(processed_file)

    if 'ParsedTimestamp' not in df_proc.columns and 'Timestamp' in df_proc.columns:
        df_proc['ParsedTimestamp'] = df_proc['Timestamp'].apply(safe_parse_timestamp)

    if 'original_index' not in df_proc.columns:
        raise ValueError(f"'original_index' column missing in {processed_file}")

    # Rename and cast to int
    df_proc = df_proc.rename(columns={'original_index': 'original_row_start'})
    df_proc['original_row_start'] = df_proc['original_row_start'].astype(int)
    df_proc.set_index('original_row_start', inplace=True)

    # Also make sure event lookup keys are int
    df_events['original_row_start'] = df_events['original_row_start'].astype(int)

    

    # df_proc = df_proc.rename(columns={'original_index': 'original_row_start'})
    # df_proc.set_index('original_row_start', inplace=True)


    position_cols = [
        'HeadPosAnchored_x', 'HeadPosAnchored_y', 'HeadPosAnchored_z',
        'HeadForthAnchored_yaw', 'HeadForthAnchored_pitch', 'HeadForthAnchored_roll'
    ]

    for idx, row in df_events.iterrows():
        if row.get('EventType') == 'logged' and row[position_cols].isnull().any():
            search_index = row['original_row_start'] - 1
            while search_index >= 0:
                if search_index in df_proc.index:
                    candidate = df_proc.loc[search_index]
                    if pd.notnull(candidate[position_cols]).all():
                        for col in position_cols:
                            print(f"Backfilling {row['EventType']} at index {idx} using processed row {search_index}")
                            df_events.at[idx, col] = candidate[col]
                        break
                search_index -= 1

    df_events.to_csv(output_file, index=False)
    print(f"✅ Backfilled: {os.path.basename(output_file)}")

def batch_fill_all(events_augmented_dir, processed_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    for filename in os.listdir(events_augmented_dir):
        if filename.endswith('_events_augmented.csv'):
            base = filename.replace('_events_augmented.csv', '')
            participant_id = base.split('_')[1]

            events_path = os.path.join(events_augmented_dir, filename)
            proc_file = base + '.csv'
            print('proc_file', proc_file)

            processed_path = os.path.join(processed_dir, proc_file)
            print('processed_path', processed_path)
            if not os.path.exists(processed_path):
                print(f"⚠️ Missing processed file for {filename}")
                continue

            output_path = os.path.join(output_dir, filename.replace('_augmented.csv', '_augmented_filled.csv'))
            fill_logged_event_positions(events_path, processed_path, output_path)

if __name__ == "__main__":
    base_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day"
    events_augmented_dir = os.path.join(base_dir, "Events_AugmentedPart1")
    processed_dir = os.path.join(base_dir, "ProcessedData_Flat")
    output_dir = os.path.join(base_dir, "Events_AugmentedPart2")
    print(base_dir)
    batch_fill_all(events_augmented_dir, processed_dir, output_dir)
