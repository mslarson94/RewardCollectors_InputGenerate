
import pandas as pd
import os
import re
import traceback

# --- CONFIGURATION ---
collated_path = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
root_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/ProcessedData"
out_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/Summary"
os.makedirs(out_dir, exist_ok=True)
summaryFile = os.path.join(out_dir, "testingSummary_AN.csv")
missingFile = os.path.join(out_dir, "missingFiles_AN.csv")
orphanRows = os.path.join(out_dir, "orphanRows_AN.csv")
orphanRowSummaryFile = os.path.join(out_dir, "orphan_row_summary_AN.csv")

# --- Load metadata ---
magic_leap_df = pd.read_excel(collated_path, sheet_name='MagicLeapFiles')
valid_files_df = magic_leap_df.dropna(subset=['cleanedFile'])
valid_files_df = valid_files_df[valid_files_df['primaryRole'] == 'AN']

# --- Helper Functions ---
def block_completed(df, block_type, coinset_id=None):
    subset = df[df['BlockType'] == block_type]
    if coinset_id is not None:
        subset = subset[subset['CoinSetID'] == coinset_id]
    return not subset.empty

def get_final_tp1_round(df):
    tp1 = df[(df['BlockType'] == 'pindropping') & (df['CoinSetID'] == 1)]
    rounds = tp1.groupby('BlockNum')['RoundNum'].max()
    return rounds.max() if not rounds.empty else None

def get_tp2_blocks(df):
    # Ensure numeric types
    df['RoundNum'] = pd.to_numeric(df['RoundNum'], errors='coerce')
    df['BlockNum'] = pd.to_numeric(df['BlockNum'], errors='coerce')
    df['CoinSetID'] = pd.to_numeric(df['CoinSetID'], errors='coerce')

    # Filter only 'pindropping' rows with CoinSetID in [1, 2, 3]
    tp2_df = df[(df['BlockType'] == 'pindropping') & (df['CoinSetID'].isin([1, 2, 3]))]

    # Group by BlockNum and filter for blocks with only one round > 0
    round_counts = tp2_df[tp2_df['RoundNum'] > 0].groupby('BlockNum')['RoundNum'].nunique()
    valid_block_nums = round_counts[round_counts == 1].index

    # Select only those valid blocks
    valid_tp2 = tp2_df[tp2_df['BlockNum'].isin(valid_block_nums)]

    return {
        'TP2_total_blocks': valid_tp2['BlockNum'].nunique(),
        'TP2_Original_blocks': valid_tp2[valid_tp2['CoinSetID'] == 1]['BlockNum'].nunique(),
        'TP2_PPE_blocks': valid_tp2[valid_tp2['CoinSetID'] == 2]['BlockNum'].nunique(),
        'TP2_NPE_blocks': valid_tp2[valid_tp2['CoinSetID'] == 3]['BlockNum'].nunique(),
        'TP2_Validated_BlockNums': valid_tp2['BlockNum'].unique()
    }


# Rewritten functions to mirror PO script logic for AN data

def is_perfect_an_message(msg):
    match = re.search(r"Finished a perfect round with:([0-9.]+)", msg)
    if match:
        return float(match.group(1)) > 0.0
    return False

def count_perfect_an_rounds(df, coinset_id, valid_blocks):
    marker = "Finished a perfect round with:"
    if 'Messages_filled' not in df.columns or 'CoinSetID' not in df.columns or 'BlockNum' not in df.columns:
        return 0

    messages = df[
        df['Messages_filled'].str.startswith(marker, na=False) &
        (df['CoinSetID'] == coinset_id) &
        (df['BlockNum'].isin(valid_blocks))
    ]
    if 'BlockNum' not in messages.columns or messages.empty:
        return 0

    perfects = messages[messages['Messages_filled'].apply(is_perfect_an_message)]
    return perfects['BlockNum'].nunique() if 'BlockNum' in perfects.columns else 0

def is_tut_ie_complete(df):
    return not df[(df['BlockType'] == 'collection') & (df['CoinSetID'] == 4)].empty

def is_tut_tp_complete(df):
    return not df[(df['BlockType'] == 'pindropping') & (df['CoinSetID'] == 4)].empty

def get_tut_tp_final_round(df):
    tp = df[(df['BlockType'] == 'pindropping') & (df['CoinSetID'] == 4)]
    return tp['RoundNum'].max() if not tp.empty else None

# --- Processing ---
summaries = []
missing_files = []
orphan_rows = []

for _, row in valid_files_df.iterrows():
    cleaned_file = row['cleanedFile']
    pair_id = str(row['pairID']).zfill(2)
    testing_date = row['testingDate']
    device = row['device']

    file_path = os.path.join(
        root_dir,
        f"pair_{pair_id}",
        testing_date,
        "MagicLeaps",
        device,
        cleaned_file
    )

    if not os.path.exists(file_path):
        print(f"Missing file: {file_path}")
        missing_files.append({
            'participantID': row['participantID'],
            'pairID': row['pairID'],
            'cleanedFile': cleaned_file,
            'expectedPath': file_path
        })
        continue

    try:
        df = pd.read_csv(file_path, skiprows=range(1, 7), low_memory=False)

        required_cols = ['BlockNum', 'RoundNum', 'CoinSetID', 'Messages_filled']
        if not all(col in df.columns for col in required_cols):
            print(f"Missing required columns in {file_path}")
            missing_files.append({
                'participantID': row['participantID'],
                'pairID': row['pairID'],
                'cleanedFile': cleaned_file,
                'expectedPath': file_path,
                'missing_columns': [col for col in required_cols if col not in df.columns]
            })
            continue

        df['CoinSetID'] = pd.to_numeric(df['CoinSetID'], errors='coerce')
        df['RoundNum'] = pd.to_numeric(df['RoundNum'], errors='coerce')
        df['BlockNum'] = pd.to_numeric(df['BlockNum'], errors='coerce')

        last_rows = df.tail(10)
        orphan_mask = last_rows[['BlockNum', 'RoundNum', 'CoinSetID', 'Messages_filled']].isna().all(axis=1)
        orphan_tail_indices = last_rows[orphan_mask].index.tolist()
        if orphan_tail_indices:
            orphaned = df.loc[orphan_tail_indices].copy()
            orphaned['source_file'] = cleaned_file
            orphaned['original_row_index'] = orphaned.index
            orphaned['total_rows_in_file'] = len(df)
            orphaned['is_last_row'] = orphaned['original_row_index'] == (len(df) - 1)
            orphan_rows.append(orphaned)
            df = df.drop(index=orphan_tail_indices)

        tp2_info = get_tp2_blocks(df)
        tp2_blocks = tp2_info.pop('TP2_Validated_BlockNums')

        summaries.append({
            'pairID': row['pairID'],
            'participantID': row['participantID'],
            'cleanedFile': cleaned_file,
            'coinSet': row['coinSet'],
            'testingDate': row['testingDate'],
            'startTime_ML': row['time_MLReported'],
            'paradigm': row['paradigm'],
            'main_RR': row['main_RR'],
            'Tut_IE_complete': is_tut_ie_complete(df),
            'Tut_TP_complete': is_tut_tp_complete(df),
            'Tut_TP_final_round': get_tut_tp_final_round(df),
            'IE_completed': block_completed(df, 'collection', coinset_id=1),
            'TP1_completed': block_completed(df, 'pindropping', coinset_id=1),
            'TP1_final_round': get_final_tp1_round(df),
            **tp2_info,
            'TP2_Original_perfect': count_perfect_an_rounds(df, 1, tp2_blocks),
            'TP2_PPE_perfect': count_perfect_an_rounds(df, 2, tp2_blocks),
            'TP2_NPE_perfect': count_perfect_an_rounds(df, 3, tp2_blocks)

        })

    except Exception as e:
        print(f"\n[ERROR] {file_path}")
        traceback.print_exc()
        missing_files.append({
            'participantID': row['participantID'],
            'pairID': row['pairID'],
            'cleanedFile': cleaned_file,
            'expectedPath': file_path,
            'error': str(e)
        })
        continue

# --- Output ---
summary_df = pd.DataFrame(summaries)
summary_df.to_csv(summaryFile, index=False)
print("AN summary saved to testingSummary_AN.csv")

if missing_files:
    pd.DataFrame(missing_files).to_csv(missingFile, index=False)
    print(f"Logged {len(missing_files)} missing files to missingFiles_AN.csv")
else:
    print("No missing AN files.")

if orphan_rows:
    orphan_df = pd.concat(orphan_rows)
    orphan_df.to_csv(orphanRows, index=False)
    orphan_summary = (
        orphan_df.groupby('source_file')
        .agg(orphan_row_count=('original_row_index', 'count'),
             had_final_row_orphaned=('is_last_row', 'any'))
        .reset_index()
    )
    orphan_summary.to_csv(orphanRowSummaryFile, index=False)
    print("Wrote orphan_row_summary_AN.csv")
    print(f"Logged {len(orphan_df)} orphaned rows to orphanRows_AN.csv")
else:
    print("No orphaned rows detected.")
