import pandas as pd
import glob

# Define a function to analyze a single CSV file
def analyze_csv(file_path):
    df = pd.read_csv(file_path)

    # Ensure required columns exist
    required_cols = ["BlockNum", "CoinSetID", "coinSet", "RoundNum"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' missing in {file_path}")

    # Total Blocks where BlockNum > 2
    total_blocks_over_2 = df[df['BlockNum'] > 2].shape[0]

    # Breakdown by CoinSetID type & CoinSet type for BlockNum > 2
    breakdown = (
        df[df['BlockNum'] > 2]
        .groupby(['CoinSetID', 'coinSet'])
        .size()
        .reset_index(name='Count')
    )

    # Find blocks where total RoundNums > 1
    roundnum_counts = df.groupby('BlockNum')['RoundNum'].nunique().reset_index(name='UniqueRoundNums')
    blocks_with_multiple_roundnums = roundnum_counts[roundnum_counts['UniqueRoundNums'] > 1]['BlockNum']

    # Count True Round Nums (not in [0, 7777, 8888, 9999]) within those blocks
    true_roundnums = df[
        (df['BlockNum'].isin(blocks_with_multiple_roundnums)) & 
        (~df['RoundNum'].isin([0, 7777, 8888, 9999]))
    ].shape[0]

    return {
        "file": file_path,
        "Total Blocks with BlockNum > 2": total_blocks_over_2,
        "Breakdown": breakdown,
        "Total True Round Nums in blocks with RoundNum > 1": true_roundnums
    }

# Get list of CSV files in directory (assuming files are uploaded to the same directory as the uploaded file)
uploaded_file_path = "/mnt/data/115_02_12_2025_Morning_B_ML2G.csv"
v1_outDir = "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/MergedEvents_V1_flat"
csv_files = glob.glob(f"{v1_outDir}/*.csv")

# Analyze each CSV
results = [analyze_csv(file) for file in csv_files]

# Collect and display results
summary_results = []
for result in results:
    summary_results.append({
        "File": result["file"],
        "Total Blocks > 2": result["Total Blocks with BlockNum > 2"],
        "Total True Round Nums": result["Total True Round Nums in blocks with RoundNum > 1"]
    })
    breakdown = result["Breakdown"]
    breakdown["File"] = result["file"]
    summary_results.append(breakdown)
