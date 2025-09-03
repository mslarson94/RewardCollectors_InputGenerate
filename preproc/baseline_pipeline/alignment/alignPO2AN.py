#!/usr/bin/env python3
"""
alignPO2AN.py
Batch-align PO _processed.csv with AN's BlockStart & BlockEnd timestamps using _events files. 

"""
import os
import pandas as pd
import numpy as np
import sys
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'eventSeg'))
sys.path.append(base_dir)
# Add the baseline_pipeline directory to the path
#sys.path.append(os.path.join(os.path.dirname(__file__), 'baseline_pipeline'))
#from eventCascade_VariablePathHelpers import generate_nestedDir

def align_po_to_an_events_v1(an_events_file, po_events_file, po_processed_file, output_file):
    df_an_events = pd.read_csv(an_events_file)
    # if 'ParsedTimestamp' not in df_an_events.columns:
    #     df_an_events['ParsedTimestamp'] = df_an_events['Timestamp'].apply(safe_parse_timestamp)
    print("we have started the function align_po_to_an_events")
    df_an_starts = df_an_events[df_an_events['lo_eventType'] == 'BlockStart']
    df_an_starts = df_an_starts.sort_values(['BlockNum', 'BlockInstance', 'origRow_start'])
    df_an_starts = df_an_starts.groupby(['BlockNum', 'BlockInstance'], as_index=False).first()

    df_po = pd.read_csv(po_processed_file)
    # if 'ParsedTimestamp' in df_po.columns:
    #     df_po['ParsedTimestamp'] = pd.to_datetime(df_po['ParsedTimestamp'])

    df_po['mLTimestamp'] = pd.NaT

    for _, an_row in df_an_starts.iterrows():
        bn = an_row['BlockNum']
        bi = an_row['BlockInstance']
        an_start_ts = an_row['mLTimestamp']
        
        if isinstance(an_start_ts, str):
            an_start_ts = pd.to_datetime(an_start_ts, errors="coerce")

        mask = (df_po['BlockNum'] == bn) & (df_po['BlockInstance'] == bi)
        if mask.sum() > 0:
            df_po.loc[mask, 'mLTimestamp'] = an_start_ts
        else:
            print(f"⚠️ BlockNum={bn}, BlockInstance={bi} found in AN but not in PO.")


    df_po.to_csv(output_file, index=False)
    print(f"✅ Aligned processed file saved to {output_file}")
    return df_po  # Return for use in updating the events file


def align_po_to_an_events(an_events_file, po_events_file, po_processed_file, output_file, nestedOutFile):
    import pandas as pd

    print("📂 Loading AN events file:", an_events_file)
    df_an_events = pd.read_csv(an_events_file)

    # Confirm required columns exist
    #print("🧪 Columns in AN events file:", df_an_events.columns.tolist())

        # Try parsing mLTimestamp early on
    if 'mLTimestamp' not in df_an_events.columns or df_an_events['mLTimestamp'].isna().all():
        print("⚠️ mLTimestamp missing or empty. Attempting to parse from backup columns...")
        if 'ParsedTimestamp' in df_an_events.columns:
            df_an_events['mLTimestamp'] = pd.to_datetime(df_an_events['ParsedTimestamp'], errors='coerce')
        elif 'Timestamp' in df_an_events.columns:
            df_an_events['mLTimestamp'] = pd.to_datetime(df_an_events['Timestamp'], errors='coerce')
        else:
            raise ValueError("No usable timestamp column found in AN events.")

    # Filter and sort AN block starts
    #print(df_an_events['lo_eventType'].value_counts())
    #print(df_an_events[df_an_events['lo_eventType'] == 'BlockStart'][['BlockNum', 'BlockInstance', 'mLTimestamp']].head(10))
    #print(df_an_events[df_an_events['lo_eventType'] == 'BlockStart'].isna().sum())

    df_an_starts = df_an_events[df_an_events['lo_eventType'] == 'BlockStart']
    df_an_starts = df_an_starts.sort_values(['BlockNum', 'BlockInstance', 'origRow_start'])
    df_an_starts = df_an_starts.groupby(['BlockNum', 'BlockInstance'], as_index=False).first()


    #print("✅ Sample aligned AN starts:")
    #print(df_an_starts[['BlockNum', 'BlockInstance', 'mLTimestamp']].head())

    df_po = pd.read_csv(po_processed_file)
    print("📂 Loaded PO processed file:", po_processed_file)
    #print("🧪 PO columns:", df_po.columns.tolist())

    # Create empty mLTimestamp column
    df_po['mLTimestamp'] = pd.NaT

    # Align by BlockNum and BlockInstance
    for _, an_row in df_an_starts.iterrows():
        bn = an_row['BlockNum']
        bi = an_row['BlockInstance']
        an_start_ts = an_row['mLTimestamp']
        if isinstance(an_start_ts, str):
            an_start_ts = pd.to_datetime(an_start_ts, errors='coerce')
        mask = (df_po['BlockNum'] == bn) & (df_po['BlockInstance'] == bi)
        if mask.sum() > 0:
            df_po.loc[mask, 'mLTimestamp'] = an_start_ts
        else:
            print(f"⚠️ BlockNum={bn}, BlockInstance={bi} found in AN but not in PO.")

    df_po.to_csv(output_file, index=False)
    df_po.to_csv(nestedOutFile, index=False)
    print(f"✅ Aligned processed file saved to {output_file}")
    print(f"✅ Aligned processed file saved to {nestedOutFile}")
    return df_po

def backfill_event_AN_parsedTS(events_file, aligned_proc_df, output_file, nestedOutFile):
    print("we have started the function backfill_event_AN_parsedTS")
    df_events = pd.read_csv(events_file)
    df_proc = aligned_proc_df.copy()
    #print("events_file", events_file)
    #print("aligned_proc_df", aligned_proc_df)
    # Ensure correct column for mapping
    #df_proc = df_proc.rename(columns={'original_index': 'original_row_start'})
    mapping = df_proc.set_index('origRow')['mLTimestamp'].dropna()

    df_events['mLTimestamp'] = df_events['origRow_start'].map(mapping)
    df_events.to_csv(output_file, index=False)
    #df_events.to_csv(nestedOutFile, index=False)
    print(f"📝 Updated mLTimestamp in: {output_file}")


def generate_nestedDir_v1(proc_dir, metadataFile, target_file):

    meta_df = pd.read_excel(metadataFile, sheet_name="MagicLeapFiles")
    meta_df = meta_df.dropna(subset=["cleanedFile"])
    
    meta_df["cleanedFile"] = meta_df["cleanedFile"].astype(str).str.strip().str.lower()
    matched_meta = meta_df[meta_df["cleanedFile"] == target_file.lower()] 
    nested_json = {
        "target_file": target_file,
        "proc_dir": proc_dir, 
        "pairID": meta_df.get("pairID_py", "unknown"),
        "testingDate": str(meta_df.get("testingDate", "unknown")),
        "sessionType": meta_df.get("sessionType", "Morning"),
        "MagicLeaps": "MagicLeaps",
        "device": meta_df.get("device", "unknown")
    }
    pairID= meta_df.get("pairID_py", "unknown")
    testingDate = str(meta_df.get("testingDate", "unknown"))
    sessionType = meta_df.get("sessionType", "Morning")
    device = meta_df.get("device", "unknown")
    
    almostNested = f"ProcessedData/{pairID}/{testingDate}/{sessionType}/MagicLeaps/{device}"
    nestedDirName = os.path.join(proc_dir, almostNested)

    return nestedDirName

def generate_nestedDir(proc_dir, metadataFile, target_file):
    meta_df = pd.read_excel(metadataFile, sheet_name="MagicLeapFiles")
    meta_df = meta_df.dropna(subset=["cleanedFile"])
    meta_df["cleanedFile"] = meta_df["cleanedFile"].astype(str).str.strip().str.lower()

    matched_meta = meta_df[meta_df["cleanedFile"] == target_file.lower()]
    if matched_meta.empty:
        raise ValueError(f"❌ No metadata match found for: {target_file}")

    row = matched_meta.iloc[0]
    pairID = row.get("pairID_py", "unknown")
    testingDate = str(row.get("testingDate", "unknown"))
    sessionType = row.get("sessionType", "Morning")
    device = row.get("device", "unknown")

    # Safe fallback just in case
    if pd.isna(pairID): pairID = "unknown"
    if pd.isna(testingDate): testingDate = "unknown"
    if pd.isna(sessionType): sessionType = "Morning"
    if pd.isna(device): device = "unknown"

    # Construct path
    almostNested = f"ProcessedData/{pairID}/{testingDate}/{sessionType}/MagicLeaps/{device}"
    nestedDirName = os.path.join(base_dir, almostNested)
    
    # ✅ Ensure directory exists
    os.makedirs(nestedDirName, exist_ok=True)

    # Return full path to CSV file (not just folder)
    nested_csv_file = os.path.join(nestedDirName, target_file)

    return nested_csv_file


def batch_align_all(events_dir, proc_dir, finalproc_dir, metadataFile, base_dir):
    print("we have started the function batch_align_all")
    for filename in os.listdir(events_dir):
        if not filename.startswith("ObsReward_B_") or not filename.endswith("_processed_orig_events.csv"):
            continue


        time_id = "_".join(filename.split('_')[2:7])  # MM_DD_YYYY_HH_MM
        file_base_B = f"ObsReward_B_{time_id}"
        file_base_A = f"ObsReward_A_{time_id}"

        an_events_file = os.path.join(events_dir, f"{file_base_A}_processed_events.csv")
        po_events_file = os.path.join(events_dir, f"{file_base_B}_processed_orig_events.csv")
        po_proc_file = os.path.join(proc_dir, f"{file_base_B}_processed_orig.csv")
        aligned_proc_outfile = os.path.join(finalproc_dir, f"{file_base_B}_processed.csv")
        updated_events_outfile = os.path.join(events_dir, f"{file_base_B}_processed_events.csv")

        nestedDirName = generate_nestedDir(finalproc_dir, metadataFile, f"{file_base_B}_processed.csv")
        
        if not (os.path.exists(an_events_file) and os.path.exists(po_events_file) and os.path.exists(po_proc_file)):
            print("os.path.exists(an_events_file)", os.path.exists(an_events_file))
            print("os.path.exists(po_events_file)", os.path.exists(po_events_file))
            print("os.path.exists(po_proc_file)", os.path.exists(po_proc_file))
            print(f"❌ Missing files for time ID: {time_id}")
            continue

        # Align PO processed file and get the updated DataFrame
        aligned_df = align_po_to_an_events(an_events_file, po_events_file, po_proc_file, aligned_proc_outfile, nestedDirName)

        # Use the aligned info to backfill AN_ParsedTS into PO's events file
        backfill_event_AN_parsedTS(po_events_file, aligned_df, updated_events_outfile, nestedDirName)


if __name__ == "__main__":
    #base_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/ResurrectedData"
    #base_dir = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallBatchData/Ideals/ideal_day'


    allowed_statuses = ["complete", "truncated"]
    trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
    #procDir = 'SmallSelectedData/RNS/alignedPO'
    base_dir = os.path.join(trueRootDir, 'FreshStart')
    metadataFile = os.path.join(trueRootDir, "collatedData.xlsx")

    events_dir = os.path.join(base_dir, "glia/Events_Flat_csv")
    print(events_dir)
    proc_dir = os.path.join(base_dir, "ProcessedData_Flat_PO_orig")
    finalproc_dir = os.path.join(base_dir, "ProcessedData_Flat")

    print("🚀 Starting batch alignment and backfill...")
    batch_align_all(events_dir, proc_dir, finalproc_dir, metadataFile, base_dir)
