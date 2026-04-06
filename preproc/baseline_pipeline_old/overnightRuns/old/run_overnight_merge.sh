#!/usr/bin/env bash
set -Eeuo pipefail

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

# Activate virtual environment (fail hard if missing)
if ! command -v conda >/dev/null 2>&1; then
  echo "❌ conda not found on PATH" | tee -a "$LOG_FILE"
  exit 1
fi
# load conda into this non-interactive shell
eval "$(conda shell.bash hook)"
if ! conda activate RewardCollectors; then
  echo "❌ Failed to activate conda env 'RewardCollectors'" | tee -a "$LOG_FILE"
  exit 1
fi

# Segment barebones
CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
# PROC_DIR="FreshStart"
PROC_DIR="FreshStart_mini"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Final_NoWalks"


# # ####################################
# # # Magic Leap Alignment to RPi Marks 
# # ####################################
echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting batch_split_pipeline.py at $(date)" | tee -a "$LOG_FILE"

python ${CODE_DIR}/alignment/batch_split_pipeline2.py \
  --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
  --device-ip-map "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/configs/DeviceIPAddresses.txt" \
  --code-dir "${CODE_DIR}/alignment" \
  --base-dir "${TRUE_BASE_DIR}" \
  --proc-dir "${PROC_DIR}" \
  --events-dir-name "${EVENTS_DIR}" \
  --csv-timestamp-column mLTimestamp \
  --event-type-column lo_eventType \
  --timezone-offset auto \
  --sheet MagicLeapFiles \
  --out-dir ML_RPi_Aligned \
  --strip-ml-suffixes "_events_final,_processed,_events_final.csv,_processed.csv" \
  --only-rows-with-rpi \
  --blankRowTemplate "${CODE_DIR}/alignment/NewRowInfo.csv" \
  --dedupesec 0.05 \
  --maxmatchgaps 180 \
  --debug \
  >> "$LOG_FILE" 2>&1
echo "✅ batch_split_pipeline.py completed at $(date)" | tee -a "$LOG_FILE"


####################
## Analysis Merges
####################

# echo "💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting mergeEventsV3.py for PAIR-TEST_DATE-SESSION at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/mergeEvents.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --event-dir "ML_RPi_Aligned/BioPacRNS/Events" \
#   --breaker "ML2 " \
#   --eventEnding "_BioPacRNS_events.csv" \
#   --outEnding "_events.csv" \
#   --output-dir "Merged_PairTestDateSession" \
#   --group-key-fields pairID \
#   --group-key-fields testingDate \
#   --group-key-fields sessionType \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mergeEventsV3.py for PAIR-TEST_DATE-SESSION completed at $(date)" | tee -a "$LOG_FILE"


