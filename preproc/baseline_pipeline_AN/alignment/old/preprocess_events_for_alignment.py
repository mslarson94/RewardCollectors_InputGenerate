import os
import pandas as pd
import sys

# Add the baseline_pipeline directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'baseline_pipeline'))

from preprocHelpers import safe_parse_timestamp 
#from preproc_helpers import safe_parse_timestamp

def preprocess_events(events_file, processed_file, output_file, is_reference=False):
    df_events = pd.read_csv(events_file)
    df_proc = pd.read_csv(processed_file)

    if 'ParsedTimestamp' not in df_proc.columns and 'Timestamp' in df_proc.columns:
        df_proc['ParsedTimestamp'] = df_proc['Timestamp'].apply(safe_parse_timestamp)

        # Rename 'original_index' → 'original_row_start' for join compatibility
    df_proc = df_proc.rename(columns={'original_index': 'original_row_start'})

    cols_to_add = [
        'original_row_start', 'BlockInstance', 'ParsedTimestamp', 'totalRounds',
        'SessionElapsedTime', 'BlockElapsedTime', 'RoundElapsedTime',
        'HeadPosAnchored_x', 'HeadPosAnchored_y', 'HeadPosAnchored_z',
        'HeadForthAnchored_yaw', 'HeadForthAnchored_pitch', 'HeadForthAnchored_roll'
    ]
    df_proc_subset = df_proc[cols_to_add]


    df_merged = pd.merge(df_events, df_proc_subset, on='original_row_start', how='left')

    # Reference timeline only: duplicate ParsedTimestamp to AN_ParsedTS
    if is_reference:
        df_merged['AN_ParsedTS'] = df_merged['ParsedTimestamp']

    df_merged.to_csv(output_file, index=False)
    print(f"✅ Preprocessed and saved: {output_file}")

def batch_preprocess_events(events_dir, processed_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    print(events_dir)
    #event_files = [f for f in os.listdir(events_dir) if f.endswith('_processed_events.csv')]
    event_files = [
        f for f in os.listdir(events_dir)
        if f.endswith('_processed_events.csv') or f.endswith('_processed_events_unaligned.csv')
    ]
    print('event_files', event_files)

    for ev_file in event_files:
        participant_id = ev_file.split('_')[1]  # A or B
        #base_name = ev_file.replace('_processed_events.csv', '')
        if ev_file.endswith('_processed_events.csv'):
            base_name = ev_file.replace('_processed_events.csv', '')
        elif ev_file.endswith('_processed_events_unaligned.csv'):
            base_name = ev_file.replace('_processed_events_unaligned.csv', '')
        else:
            continue  # Skip any unexpected filenames

        events_path = os.path.join(events_dir, ev_file)
        print(events_path)
        if participant_id == 'A':
            proc_name = base_name + '_processed.csv'
        else:
            proc_name = base_name + '_processed_unaligned.csv'

        processed_path = os.path.join(processed_dir, proc_name)
        print(processed_path)

        if not os.path.exists(processed_path):
            print(f"⚠️ Missing processed file for: {ev_file}")
            continue

        #output_name = base_name + '_processed_events_augmented.csv'
        if participant_id == 'A':
            output_name = base_name + '_processed_events_augmented.csv'
        else:
            output_name = base_name + '_processed_events_augmented_unaligned.csv'
        output_path = os.path.join(output_dir, output_name)

        preprocess_events(events_path, processed_path, output_path, is_reference=(participant_id == 'A'))

if __name__ == "__main__":
    base_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/ResurrectedData"
    #base_dir = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day'
    events_dir = os.path.join(base_dir, "ExtractedEvents_csv_Flat")
    proc_dir = os.path.join(base_dir, "ProcessedData_Flat")
    output_dir = os.path.join(base_dir, "Events_AugmentedPart1")

    batch_preprocess_events(events_dir, proc_dir, output_dir)
