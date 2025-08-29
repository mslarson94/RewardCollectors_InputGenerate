#!/usr/bin/env python3
"""
Batch-align PO _processed.csv with AN's TrueBlockStart timestamps using _events files.
"""
import os
import pandas as pd
import numpy as np
import sys

# Add the baseline_pipeline directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'baseline_pipeline'))
from preprocHelpers import safe_parse_timestamp


def align_po_to_an_events(an_events_file, po_events_file, po_processed_file, output_file):
    df_an_events = pd.read_csv(an_events_file)
    if 'ParsedTimestamp' not in df_an_events.columns:
        df_an_events['ParsedTimestamp'] = df_an_events['Timestamp'].apply(safe_parse_timestamp)

    df_an_starts = df_an_events[df_an_events['lo_eventType'] == 'TrueBlockStart']
    df_an_starts = df_an_starts.sort_values(['BlockNum', 'BlockInstance', 'original_row_start'])
    df_an_starts = df_an_starts.groupby(['BlockNum', 'BlockInstance'], as_index=False).first()

    df_po = pd.read_csv(po_processed_file)
    if 'ParsedTimestamp' in df_po.columns:
        df_po['ParsedTimestamp'] = pd.to_datetime(df_po['ParsedTimestamp'])

    df_po['AN_ParsedTS'] = pd.NaT

    for _, an_row in df_an_starts.iterrows():
        bn = an_row['BlockNum']
        bi = an_row['BlockInstance']
        an_start_ts = an_row['ParsedTimestamp']
        mask = (df_po['BlockNum'] == bn) & (df_po['BlockInstance'] == bi)
        if mask.sum() > 0:
            df_po.loc[mask, 'AN_ParsedTS'] = an_start_ts
        else:
            print(f"⚠️ BlockNum={bn}, BlockInstance={bi} found in AN but not in PO.")

    df_po.to_csv(output_file, index=False)
    print(f"✅ Aligned saved to {output_file}")


def batch_align_all(events_dir, proc_dir):
    for filename in os.listdir(events_dir):
        if not filename.startswith("ObsReward_B_") or not filename.endswith("_processed_events_augmented_unaligned.csv"):
            continue

        # Match A participant by time string
        time_id = "_".join(filename.split('_')[2:7])  # MM_DD_YYYY_HH_MM
        file_base_B = f"ObsReward_B_{time_id}"
        file_base_A = f"ObsReward_A_{time_id}"

        an_events_file = os.path.join(events_dir, f"{file_base_A}_processed_events_augmented.csv")
        po_events_file = os.path.join(events_dir, f"{file_base_B}_processed_events_augmented_unaligned.csv")
        po_proc_file = os.path.join(proc_dir, f"{file_base_B}_processed_unaligned.csv")
        output_file = os.path.join(proc_dir, f"{file_base_B}_processed.csv")

        if not (os.path.exists(an_events_file) and os.path.exists(po_events_file) and os.path.exists(po_proc_file)):
            print(f"❌ Missing files for time ID: {time_id}")
            continue

        align_po_to_an_events(an_events_file, po_events_file, po_proc_file, output_file)


if __name__ == "__main__":
    base_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day"
    events_dir = os.path.join(base_dir, "Events_AugmentedPart1")
    proc_dir = os.path.join(base_dir, "ProcessedData_Flat")
    print('starting')
    batch_align_all(events_dir, proc_dir)


# if __name__ == "__main__":
#     # Assumes all files are in the same directory
#     base_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day"
#     events_dir = "Events_AugmentedPart1"
#     proc_dir = "ProcessedData_Flat"
    
#     an_events_file = os.path.join(base_dir, events_dir, 'ObsReward_A_02_17_2025_15_11_processed_events_augmented.csv')
#     po_events_file = os.path.join(base_dir, events_dir, 'ObsReward_B_02_17_2025_15_11_processed_events_augmented_unaligned.csv')
#     po_processed_file = os.path.join(base_dir, proc_dir, 'ObsReward_B_02_17_2025_15_11_processed_unaligned.csv')
#     output_file = os.path.join(base_dir, proc_dir, 'ObsReward_B_02_17_2025_15_11_processed.csv')

#     align_po_to_an_events(an_events_file, po_events_file, po_processed_file, output_file)
