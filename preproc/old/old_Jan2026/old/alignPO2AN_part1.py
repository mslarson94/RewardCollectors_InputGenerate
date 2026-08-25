#!/usr/bin/env python3
"""
Batch-align PO _processed.csv with AN's TrueBlockStart timestamps using _events files.
Also updates AN_ParsedTS column in the B participant's events_augmented_unaligned.csv file.
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
    print(f"✅ Aligned processed file saved to {output_file}")
    return df_po  # Return for use in updating the events file


def backfill_event_AN_parsedTS(events_file, aligned_proc_df, output_file):
    df_events = pd.read_csv(events_file)
    df_proc = aligned_proc_df.copy()

    # Ensure correct column for mapping
    df_proc = df_proc.rename(columns={'original_index': 'original_row_start'})
    mapping = df_proc.set_index('original_row_start')['AN_ParsedTS'].dropna()

    df_events['AN_ParsedTS'] = df_events['original_row_start'].map(mapping)
    df_events.to_csv(output_file, index=False)
    print(f"📝 Updated AN_ParsedTS in: {output_file}")


def batch_align_all(events_dir, proc_dir):
    for filename in os.listdir(events_dir):
        if not filename.startswith("ObsReward_B_") or not filename.endswith("_processed_events_augmented_unaligned.csv"):
            continue

        time_id = "_".join(filename.split('_')[2:7])  # MM_DD_YYYY_HH_MM
        file_base_B = f"ObsReward_B_{time_id}"
        file_base_A = f"ObsReward_A_{time_id}"

        an_events_file = os.path.join(events_dir, f"{file_base_A}_processed_events_augmented.csv")
        po_events_file = os.path.join(events_dir, f"{file_base_B}_processed_events_augmented_unaligned.csv")
        po_proc_file = os.path.join(proc_dir, f"{file_base_B}_processed_unaligned.csv")
        aligned_proc_outfile = os.path.join(proc_dir, f"{file_base_B}_processed.csv")
        updated_events_outfile = os.path.join(events_dir, f"{file_base_B}_processed_events_augmented.csv")

        if not (os.path.exists(an_events_file) and os.path.exists(po_events_file) and os.path.exists(po_proc_file)):
            print(f"❌ Missing files for time ID: {time_id}")
            continue

        # Align PO processed file and get the updated DataFrame
        aligned_df = align_po_to_an_events(an_events_file, po_events_file, po_proc_file, aligned_proc_outfile)

        # Use the aligned info to backfill AN_ParsedTS into PO's events file
        backfill_event_AN_parsedTS(po_events_file, aligned_df, updated_events_outfile)


if __name__ == "__main__":
    base_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/ResurrectedData"
    #base_dir = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day'
    events_dir = os.path.join(base_dir, "Events_AugmentedPart1")
    proc_dir = os.path.join(base_dir, "ProcessedData_Flat")

    print("🚀 Starting batch alignment and backfill...")
    batch_align_all(events_dir, proc_dir)
