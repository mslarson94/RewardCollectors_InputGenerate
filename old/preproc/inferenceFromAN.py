import pandas as pd
from datetime import datetime
from pathlib import Path
import os

def robust_parse_timestamp_to_time(ts):
    try:
        hh, mm, ss, ms = ts.split(":")
        return time(int(hh), int(mm), int(ss), int(ms) * 1000)
    except:
        return pd.NaT

def infer_7777_from_an(an_path, po_df):
    an_df = pd.read_csv(an_path, dtype={"Timestamp": str}, low_memory=False)
    an_df["Timestamp_dt"] = an_df["Timestamp"].apply(robust_parse_timestamp_to_time)
    po_df["Timestamp_dt"] = po_df["Timestamp"].apply(robust_parse_timestamp_to_time)

    ranges_7777 = []
    in_range = False
    start_time = None

    for _, row in an_df.iterrows():
        if row["RoundNum"] == 7777 and not in_range:
            start_time = row["Timestamp_dt"]
            in_range = True
        elif row["RoundNum"] != 7777 and in_range:
            end_time = row["Timestamp_dt"]
            ranges_7777.append((start_time, end_time))
            in_range = False

    if in_range and start_time is not None:
        ranges_7777.append((start_time, an_df["Timestamp_dt"].iloc[-1]))

    po_df["RoundNum"] = po_df.apply(
        lambda row: 7777 if any(start <= row["Timestamp_dt"] <= end for start, end in ranges_7777)
        else row["RoundNum"],
        axis=1
    )
    return po_df

def extract_suffix(filename):
    parts = filename.split("_")
    return "_".join(parts[2:6]) if len(parts) >= 6 else ""

# def aligned_output_path(metadata_path, baseDir, output_path, pairs=None):
#     metadata1 = pd.read_excel(metadata_path, sheet_name="MagicLeapFiles")
#     columns_needed = ['pairID', 'testingDate', 'currentRole', 'sessionType', 'device', 'cleanedFile', 'unalignedFile', 'time_MLReported']
#     metadata = metadata1[columns_needed].copy()
#     valid_metadata = metadata[
#         metadata["currentRole"].isin(["AN", "PO"]) &
#         metadata["cleanedFile"].notna() &
#         metadata["testingDate"].notna() &
#         metadata["sessionType"].notna() &
#         metadata["device"].notna() &
#         metadata["pairID"].apply(lambda x: str(x).isdigit())
#     ].copy()

#     valid_metadata["pairID"] = valid_metadata["pairID"].apply(lambda x: f"pair_{int(x):03}")
    
#     # ✅ Filter by selected pairs
#     if pairs:
#         formatted_pairs = [f"pair_{int(p):03}" for p in pairs]
#         valid_metadata = valid_metadata[valid_metadata["pairID"].isin(formatted_pairs)]

#     valid_metadata["suffix"] = valid_metadata["cleanedFile"].apply(extract_suffix)

#     an_files = valid_metadata[valid_metadata["currentRole"] == "AN"]
#     po_files = valid_metadata[valid_metadata["currentRole"] == "PO"]

#     merged = pd.merge(
#         an_files,
#         po_files,
#         on=["pairID", "testingDate", "sessionType", "suffix"],
#         suffixes=("_an", "_po")
#     )

#     base_dir = Path(baseDir)
#     merged["an_path"] = merged.apply(
#         lambda row: str(
#             base_dir / row["pairID"] / row["testingDate"] / row["sessionType"] / "MagicLeaps" / row["device_an"] / row["cleanedFile_an"]
#         ), axis=1
#     )
#     merged["po_unaligned_path"] = merged.apply(
#         lambda row: str(
#             base_dir / row["pairID"] / row["testingDate"] / row["sessionType"] / "MagicLeaps" / row["device_po"] / row["cleanedFile_po"]
#         ), axis=1
#     )
#     merged["aligned_output_path"] = merged.apply(
#         lambda row: str(
#             base_dir / row["pairID"] / row["testingDate"] / row["sessionType"] / "MagicLeaps" / row["device_po"] / row["cleanedFile_po"].replace("_processed_unaligned.csv", "_processed.csv")
#         ), axis=1
#     )
#     merged = merged.drop_duplicates(subset=["time_MLReported_an"], keep='first')

#     os.makedirs(output_path, exist_ok=True)
#     timestr = datetime.now().strftime("%Y_%m_%d-%H_%M")
#     outFile = os.path.join(output_path, f'matched_ANPO_{timestr}.csv')
#     merged.to_csv(outFile, index=False)

#     return merged

def aligned_output_path(metadata_path, baseDir, output_path, pairs=None):
    metadata1 = pd.read_excel(metadata_path, sheet_name="MagicLeapFiles")
    columns_needed = ['pairID', 'testingDate', 'currentRole', 'sessionType', 'device', 'cleanedFile', 'unalignedFile', 'time_MLReported']
    metadata = metadata1[columns_needed].copy()

    valid_metadata = metadata[
        metadata["currentRole"].isin(["AN", "PO"]) &
        metadata["cleanedFile"].notna() &
        metadata["testingDate"].notna() &
        metadata["sessionType"].notna() &
        metadata["device"].notna() &
        metadata["pairID"].apply(lambda x: str(x).isdigit())
    ].copy()

    valid_metadata["pairID"] = valid_metadata["pairID"].apply(lambda x: f"pair_{int(x):03}")

    if pairs:
        formatted_pairs = [f"pair_{int(p):03}" for p in pairs]
        valid_metadata = valid_metadata[valid_metadata["pairID"].isin(formatted_pairs)]

    valid_metadata["suffix"] = valid_metadata["cleanedFile"].apply(extract_suffix)

    an_files = valid_metadata[valid_metadata["currentRole"] == "AN"]
    po_files = valid_metadata[valid_metadata["currentRole"] == "PO"]

    merged = pd.merge(
        an_files,
        po_files,
        on=["pairID", "testingDate", "sessionType", "suffix"],
        suffixes=("_an", "_po")
    )

    base_dir = Path(baseDir)
    merged["an_path"] = merged.apply(
        lambda row: str(
            base_dir / row["pairID"] / row["testingDate"] / row["sessionType"] / "MagicLeaps" / row["device_an"] / row["cleanedFile_an"]
        ), axis=1
    )
    merged["po_unaligned_path"] = merged.apply(
        lambda row: str(
            base_dir / row["pairID"] / row["testingDate"] / row["sessionType"] / "MagicLeaps" / row["device_po"] / row["cleanedFile_po"]
        ), axis=1
    )
    merged["aligned_output_path"] = merged.apply(
        lambda row: str(
            base_dir / row["pairID"] / row["testingDate"] / row["sessionType"] / "MagicLeaps" / row["device_po"] / row["cleanedFile_po"].replace("_processed_unaligned.csv", "_processed.csv")
        ), axis=1
    )

    # Deduplicate on po_unaligned_path to prevent multiple ANs for one PO
    merged = merged.drop_duplicates(subset=["po_unaligned_path"], keep='first')

    os.makedirs(output_path, exist_ok=True)
    timestr = datetime.now().strftime("%Y_%m_%d-%H_%M")
    outFile = os.path.join(output_path, f'matched_ANPO_{timestr}.csv')
    merged.to_csv(outFile, index=False)

    return merged


def batch_enrich_from_csv(matches_df):
    """
    Batch enrich PO files using AN files based on pre-matched file paths.
    """
    #matches_df = pd.read_csv(match_file_path)
    enriched_paths = []

    print(f"Processing pairs: {matches_df['pairID'].unique().tolist()}")

    for idx, row in matches_df.iterrows():
        an_path = row["an_path"]
        po_unaligned_path = row["po_unaligned_path"]
        aligned_output_path = row["aligned_output_path"]

        try:
            if not os.path.exists(po_unaligned_path):
                print(f"⚠️ Skipped (missing PO): {po_unaligned_path}")
                continue
            if not os.path.exists(an_path):
                print(f"⚠️ Skipped (missing AN): {an_path}")
                continue

            po_df = pd.read_csv(po_unaligned_path, dtype={"Timestamp": str}, low_memory=False)
            enriched_df = infer_7777_from_an(an_path, po_df)

            enriched_df.to_csv(aligned_output_path, index=False)
            enriched_paths.append(aligned_output_path)

            print(f"✅ Processed and saved: {aligned_output_path}")
        except Exception as e:
            print(f"❌ Error processing {po_unaligned_path}: {e}")

    print(f"\nTotal enriched: {len(enriched_paths)}")
    return enriched_paths


metadata_path = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
baseDir = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/twopairs/ProcessedData"
output_path = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/twopairs/Summary"
pairs = [8, 200]


matches_df = aligned_output_path(metadata_path, baseDir, output_path, pairs)
batch_enrich_from_csv(matches_df)