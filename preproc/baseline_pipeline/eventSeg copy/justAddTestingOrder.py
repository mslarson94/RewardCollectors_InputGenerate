# justAddTestingOrder.py

'''
justAddTestingOrder.py
Author: Myra Sarai Larson   08/18/2025

   This high level script is designed to be the brains of the operation - orchestrating all the little 
   helper & parser scripts - (much like how the PreFrontal Cortex is commonly associated with Executive Functioning 
   i.e. planning/decision making/top-down control)

   This script should be limited to weaving everything together related to our event segmentation pipeline - completely 
   agnostic of participant roles. At the beginning of this script, we are feeding in _processed files in heavily nested directories and
   by the end of the script we should have our _events & _events_orig files saved as well as _meta.json files also saved in those 
   nested parent directories or in flattened directories.

   01/26/2026: Making some bigger changes to make this entire pipeline shift to using AppTime, eMLT_orig, or eMLT_RPi as sorting for time. 
   Also shifting from aligning PO to AN to aligning ML to RPi. Also just trying to simplify helpers to be mostly common helpers 
   where possible

'''
import os
import re
import sys
import pandas as pd
from io import StringIO
from pathlib import Path
import traceback
import json
import argparse

## warning_logger
from RC_utilities.segHelpers.warning_logger import WarningLogger

## extraMetaExtract
from RC_utilities.segHelpers.extraMetaExtract import generate_meta_json

from RC_utilities.segHelpers.eventCascade_VariablePathHelpers import (
    eventParserFolderCreatePart1, 
    nestedEventParserDirs)

## metadata_and_manifest_utils functions
from RC_utilities.segHelpers.metadata_and_manifest_utils import (
    attach_metadata_to_events,
    record_to_manifest, 
    save_manifest,
    pullMetaData,
    load_filtered_df, 
    get_metadata_row_for_file, 
    attach_testingOrder)

## buildGliaEvents_AN 
from RC_utilities.segHelpers.glia_eventsParserHelper_AN import buildGliaEvents_AN

## muscles_eventParser_AN
from RC_utilities.segHelpers.muscles_eventParser_AN import buildEvents_AN_v4

## glia_eventsParserHelper_PO
from RC_utilities.segHelpers.glia_eventsParserHelper_PO import buildGliaEvents_PO_v2

## muscles_eventParser_PO
from RC_utilities.segHelpers.muscles_eventParser_PO import buildEvents_PO

## makeItCannonical
from RC_utilities.segHelpers.makeItCannonical import canonicalize_event_order

#############################################################################


# --- Processing All Data with Entire Directory or Specified Directories ---
def process_all_obsreward_files(dataDir, metadata, role, outDir):
    outDir.mkdir(parents=True, exist_ok=True)
    print('Processing started...')
    if role == 'AN':
        pattern = re.compile(r"ObsReward_A_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_startPosPropagated\.csv$")
    elif role == 'PO':
        pattern = re.compile(r"ObsReward_B_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}.*_startPosPropagated\.csv$") # lazy fix 1/25/2026
    else: 
        raise Exception("The parameter 'role' can only take the values 'AN' or 'PO' - please check your inputs!")

    input_dataDir = Path(dataDir)
    full_metadata_df, metadata_df, all_known_files, valid_files = pullMetaData(metadata)

    for fname in os.listdir(input_dataDir):
        if not pattern.match(fname):
            continue
        fileName = input_dataDir / fname
        print(fileName)
        df = pd.read_csv(fileName)
        print(df)
        source_file = fname.replace("_startPosPropagated.csv", "_processed.csv")
        source_file = source_file.lower()
        print('source_file', source_file)


        flatOutCsvName = fname.replace("_startPosPropagated.csv", "_eventsFinal.csv")

        matched_meta = metadata_df[metadata_df["cleanedFile"] == source_file]
        print(metadata_df["cleanedFile"] == source_file)
        print(metadata_df["cleanedFile"])
        print(matched_meta)
        meta_row = matched_meta.iloc[0].to_dict()
        df_dict = df.to_dict(orient="records")
        enriched_events = attach_testingOrder(df_dict, meta_row)
        flatOutCsv = outDir / flatOutCsvName
        enriched_events = pd.DataFrame(enriched_events)
        enriched_events.to_csv(flatOutCsv, index=False)

        print(f"✓ Processed: {fname}")


# --- For Running This as a Script By Itself (and debugging) ---


def cli() -> None:
    parser = argparse.ArgumentParser(
        prog="justAddTestingOrder",
        description="Run the unified event-segmentation pipeline over processed ObsReward data."
    )
    parser.add_argument(
        "--role", required=True, choices=["AN", "PO"],
        help="Participant role to process."
    )
    parser.add_argument(
        "--trueRootDir", required=True, type=Path,
        help="Base project directory (e.g., '/Users/you/RC_TestingNotes')."
    )
    parser.add_argument(
        "--procDir", required=True, type=Path,
        help="Dataset subdirectory under --trueRootDir that contains 'EventsFinal')."
    )
    parser.add_argument(
        "--outDir", required=True, type=Path,
        help="Out directory (e.g., '/Users/you/RC_TestingNotes/EventsFinalFinal')."
    )

    args = parser.parse_args()

    root = args.trueRootDir.expanduser()
    proc = args.procDir
    data_dir_path = proc if proc.is_absolute() else (root / proc)
    meta_path = root / "collatedData.xlsx"

    process_all_obsreward_files(
        dataDir=str(data_dir_path),
        metadata=str(meta_path),
        role=args.role,
        outDir=args.outDir,
    )

if __name__ == "__main__":
    cli()
