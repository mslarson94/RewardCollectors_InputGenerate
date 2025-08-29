
import argparse
import os
import pandas as pd
from preproc_AN import clean_and_process_files

def main():
    parser = argparse.ArgumentParser(description="Run ObsReward event cascade processor.")
    parser.add_argument('--root_directory', required=True, help='Root directory containing raw data')
    parser.add_argument('--output_root_directory', required=True, help='Output root directory to save processed data')
    parser.add_argument('--subset', nargs='*', default=None, help='Optional list of subdirectories to process')
    parser.add_argument('--metadata', required=True, help='Path to Excel metadata file')
    parser.add_argument('--save_large_files', nargs='*', default=True, help='Optional choice to save large files')
    parser.add_argument('--max_memory_mb', nargs='*', default=500, help='Optional choice to determine max memory usage in mb')

    args = parser.parse_args()

    metadata_df = pd.read_excel(args.metadata, sheet_name="MagicLeapFiles")
    metadata_df = metadata_df.dropna(subset=["cleanedFile"])
    metadata_df = metadata_df[metadata_df["currentRole"] == "AN"]
    metadata_df = metadata_df.rename(columns={"cleanedFile": "source_file"})

    process_all_obsreward_files_with_manifest(
        args.rawData,
        args.precProcData,
        metadata_df,
        subset=args.subset,
        args.save_large_files,
        args.max_memory_mb
    )

if __name__ == "__main__":
    main()
