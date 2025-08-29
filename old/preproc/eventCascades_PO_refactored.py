# eventCascades_PO_refactored.py

import argparse
import os
import pandas as pd
from metadata_and_manifest_utils import attach_metadata_to_events, record_to_manifest, save_manifest
from eventParser_PO import (
    process_pin_drop, process_pin_drop_vote, process_feedback_collect,
    process_ie_events, process_marks, process_swap_votes, process_block_periods,
    extract_walking_periods
)

def process_file_list(file_list, metadata, dataDir, allowed_statuses=["complete", "truncated"]):
    # Mirror AN structure, to be filled in
    pass

def process_all_obsreward_files(dataDir, metadata, subDirs=None, allowed_statuses=["complete", "truncated"]):
    # Mirror AN structure, to be filled in
    pass

def main():
    parser = argparse.ArgumentParser(description="PO Cascade builder")
    parser.add_argument("--dataDir", required=True)
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--allowed_statuses", nargs="+", default=["complete", "truncated"])
    parser.add_argument("--subDirs", nargs="*")
    parser.add_argument("--file_list", nargs="*")

    args = parser.parse_args()

    if args.file_list:
        if not args.file_list:
            raise ValueError("No files specified for --file_list mode.")
        process_file_list(
            file_list=args.file_list,
            metadata=args.metadata,
            dataDir=args.dataDir,
            allowed_statuses=args.allowed_statuses
        )
    else:
        subDirs = args.subDirs if args.subDirs else None
        process_all_obsreward_files(
            dataDir=args.dataDir,
            metadata=args.metadata,
            subDirs=subDirs,
            allowed_statuses=args.allowed_statuses
        )

if __name__ == "__main__":
    main()
