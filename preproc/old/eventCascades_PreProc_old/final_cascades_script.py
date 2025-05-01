import pandas as pd
import re
import os
import json
from datetime import datetime, timedelta

# --- Load Metadata ---
collated_path = "/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"
MAGIC_LEAP_METADATA = pd.read_excel(collated_path, sheet_name='MagicLeapFiles')
MAGIC_LEAP_METADATA = MAGIC_LEAP_METADATA.dropna(subset=['cleanedFile'])
MAGIC_LEAP_METADATA = MAGIC_LEAP_METADATA[MAGIC_LEAP_METADATA['currentRole'] == 'AN']
MAGIC_LEAP_METADATA = MAGIC_LEAP_METADATA.rename(columns={"cleanedFile": "source_file"})

# --- Utilities ---

SKIP_OFFSET = 6  # we have to skip the first 5 rows after the header row
ALLOWED_STATUSES = {"complete"}

# ... (truncated for brevity - full cleaned script continues here)

# --- Execute ---
process_all_obsreward_files(
    root_dir="/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/ProcessedData",
    output_dir="/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/Summary",
    ALLOWED_STATUSES=ALLOWED_STATUSES
)
