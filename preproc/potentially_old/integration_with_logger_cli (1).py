
import argparse
import os
import pandas as pd
from integration_with_logger import process_all_obsreward_files_with_manifest

def main():
    parser = argparse.ArgumentParser(description="Run ObsReward event cascade processor.")
    parser.add_argument('--root', required=True, help='Root directory containing observation data')
    parser.add_argument('--subset', nargs='*', default=None, help='Optional list of subdirectories to process')
    parser.add_argument('--metadata', required=True, help='Path to Excel metadata file')
    parser.add_argument('--allowed_statuses', nargs='*', default=["complete", "truncated"], help='Allowed status values')

    args = parser.parse_args()

    metadata_df = pd.read_excel(args.metadata, sheet_name="MagicLeapFiles")
    metadata_df = metadata_df.dropna(subset=["cleanedFile"])
    metadata_df = metadata_df[metadata_df["currentRole"] == "AN"]
    metadata_df = metadata_df.rename(columns={"cleanedFile": "source_file"})

    process_all_obsreward_files_with_manifest(
        args.root,
        metadata_df,
        set(args.allowed_statuses),
        subdirs=args.subset
    )

if __name__ == "__main__":
    main()
