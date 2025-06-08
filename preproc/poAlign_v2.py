import os
import pandas as pd
from datetime import datetime

trueRootDir = "/Users/mairahmac/Desktop/RC_TestingNotes/"
baseDirName = "SmallSelectedData/RNS"
baseDir = os.path.join(trueRootDir, baseDirName)
procDir = os.path.join(baseDir, "ProcessedData_Flat")
outDir = os.path.join(baseDir, "alignedPO")
os.makedirs(outDir, exist_ok=True)

poBlocksDir = os.path.join(baseDir, "trueBlocks_PO")
anBlocksDir = os.path.join(baseDir, "trueBlocks_AN")

def log_loader(fileName, dir):
    filePath = os.path.join(dir, fileName)
    return pd.read_csv(filePath)

def safe_parse_timestamp(ts):
    try:
        if isinstance(ts, str) and ts.count(':') == 3:
            hh, mm, ss, ms = ts.split(':')
            ms = (ms + '000')[:6]
            return datetime.strptime(f"{hh}:{mm}:{ss}:{ms}", "%H:%M:%S:%f")
        return datetime.strptime(ts, "%H:%M:%S:%f")
    except Exception:
        return None

def align_full_po_log(an_blocks_df, po_blocks_df, po_log_df, timestamp_col='Timestamp'):
    aligned_po_df = po_log_df.copy()
    aligned_po_df['AdjustedTimestamp'] = None

    for _, an_row in an_blocks_df.iterrows():
        block_num = an_row["BlockNum"]
        block_instance = an_row["BlockInstance"]
        an_time = safe_parse_timestamp(an_row["Timestamp"])

        po_match = po_blocks_df[
            (po_blocks_df["BlockNum"] == block_num) &
            (po_blocks_df["BlockInstance"] == block_instance)
        ]

        if po_match.empty:
            print(f"⚠️ BlockNum {block_num} instance {block_instance} missing in PO log.")
            continue

        po_time = safe_parse_timestamp(po_match.iloc[0]["Timestamp"])
        offset = an_time - po_time

        mask = (po_log_df["BlockNum"] == block_num)
        aligned_po_df.loc[mask, "AdjustedTimestamp"] = po_log_df.loc[mask, timestamp_col].apply(
            lambda ts: (safe_parse_timestamp(ts) + offset).strftime('%H:%M:%S:%f')
        )

    return aligned_po_df

# Find all 'A' logs and align them with their corresponding 'B' logs
all_an_files = [f for f in os.listdir(procDir) if f.startswith('ObsReward_A') and f.endswith('_processed.csv')]

for an_file in all_an_files:
    po_file = an_file.replace('A', 'B', 1)
    if not os.path.exists(os.path.join(procDir, po_file)):
        print(f"⚠️ Skipping {an_file}: no corresponding {po_file}")
        continue

    print(f"Aligning {an_file} and {po_file}...")
    an_log_df = log_loader(an_file, procDir)
    po_log_df = log_loader(po_file, procDir)

    # Load corresponding trueBlocks files
    an_blocks_file = f"trueBlocks_{an_file.replace('_processed.csv', '')}.csv"
    po_blocks_file = f"trueBlocks_{po_file.replace('_processed.csv', '')}.csv"
    an_blocks_path = os.path.join(anBlocksDir, an_blocks_file)
    po_blocks_path = os.path.join(poBlocksDir, po_blocks_file)

    if not os.path.exists(an_blocks_path) or not os.path.exists(po_blocks_path):
        print(f"⚠️ Missing block files for {an_file}. Skipping...")
        continue

    an_blocks_df = pd.read_csv(an_blocks_path)
    po_blocks_df = pd.read_csv(po_blocks_path)
    an_blocks_df["BlockInstance"] = an_blocks_df.groupby("BlockNum").cumcount()
    po_blocks_df["BlockInstance"] = po_blocks_df.groupby("BlockNum").cumcount()

    aligned_po_log = align_full_po_log(an_blocks_df, po_blocks_df, po_log_df)
    aligned_output_file = f"Align_{po_file}"
    aligned_po_log.to_csv(os.path.join(outDir, aligned_output_file), index=False)
