import pandas as pd
import os
import re
import traceback

# --- CONFIGURATION ---
collated_path = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
root_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/ProcessedData"
out_dir = "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/Summary"
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
    tp2 = df[
        (df['BlockType'] == 'pindropping') &
        (df['RoundNum'] == 1) &
        (df['Messages_filled'].str.contains("finished current task", na=False)) &
        (df['CoinSetID'].isin([1, 2, 3]))
    ]
    return {
        'TP2_total_blocks': tp2['BlockNum'].nunique(),
        'TP2_Original_blocks': tp2[tp2['CoinSetID'] == 1]['BlockNum'].nunique(),
        'TP2_PPE_blocks': tp2[tp2['CoinSetID'] == 2]['BlockNum'].nunique(),
        'TP2_NPE_blocks': tp2[tp2['CoinSetID'] == 3]['BlockNum'].nunique(),
        'TP2_Validated_BlockNums': tp2['BlockNum'].unique()
    }

def count_perfect_rounds(df, coinset_id, valid_blocks):
    perfect_msgs = df[
        df['Messages_filled'].str.startswith("Finished a perfect round with:", na=False) &
        (df['CoinSetID'] == coinset_id) &
        (df['BlockNum'].isin(valid_blocks))
    ]
    return perfect_msgs['BlockNum'].nunique() if 'BlockNum' in perfect_msgs.columns else 0

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
        df = pd.read_csv(file_path, low_memory=False)
        required_cols = ['BlockNum', 'RoundNum', 'CoinSetID', 'Messages_filled']
        if not set(required_cols).issubset(df.columns):
            print(f"Missing required columns in {file_path}")
            missing_files.append({
                'participantID': row['participantID'],
                'pairID': row['pairID'],
                'cleanedFile': cleaned_file,
                'expectedPath': file_path,
                'missing_columns': [col for col in required_cols if col not in df.columns]
            })
            continue

        df_usable = df.iloc[6:].copy()

        # Orphan Detection BEFORE dropping or converting types
        last_rows = df_usable.tail(10)
        orphan_mask = last_rows[required_cols].isna().all(axis=1)
        orphan_tail_indices = last_rows[orphan_mask].index.tolist()
        if orphan_tail_indices:
            orphaned = df_usable.loc[orphan_tail_indices].copy()
            orphaned['source_file'] = cleaned_file
            orphaned['original_row_index'] = orphaned.index
            orphaned['total_rows_in_file'] = len(df)
            orphaned['is_last_row'] = orphaned['original_row_index'] == (len(df) - 1)
            orphan_rows.append(orphaned)

        for col in ['BlockNum', 'RoundNum', 'CoinSetID']:
            df_usable[col] = pd.to_numeric(df_usable[col], errors='coerce')

        df_usable = df_usable.drop(index=orphan_tail_indices)
        df_usable = df_usable.dropna(subset=['BlockNum', 'RoundNum', 'CoinSetID'])

        tp2_info = get_tp2_blocks(df_usable)
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
            'Tut_IE_complete': is_tut_ie_complete(df_usable),
            'Tut_TP_complete': is_tut_tp_complete(df_usable),
            'Tut_TP_final_round': get_tut_tp_final_round(df_usable),
            'IE_completed': block_completed(df_usable, 'collection', coinset_id=1),
            'TP1_completed': block_completed(df_usable, 'pindropping', coinset_id=1),
            'TP1_final_round': get_final_tp1_round(df_usable),
            **tp2_info,
            'TP2_Original_perfect': count_perfect_rounds(df_usable, 1, tp2_blocks),
            'TP2_PPE_perfect': count_perfect_rounds(df_usable, 2, tp2_blocks),
            'TP2_NPE_perfect': count_perfect_rounds(df_usable, 3, tp2_blocks),
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
