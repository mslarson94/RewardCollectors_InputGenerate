#!/usr/bin/env bash
set -Eeuo pipefail

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/Occupancy_processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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
#CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline/reproc"
CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
#PROC_DIR="FreshStart_redo"
PROC_DIR="FreshStart_multi"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Pos"

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting 00_batch_reproc_steps_01_04.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/00_batch_reproc_steps_01_04.py" \
#   --events-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/${EVENTS_DIR}" \
#   --processed-root "${TRUE_BASE_DIR}/${PROC_DIR}/ProcessedData_Flat" \
#   --intervals-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventIntervals" \
#   --prelim-reproc-dir "${TRUE_BASE_DIR}/${PROC_DIR}/PreLimReProcessedData_Flat" \
#   --reproc-dir "${TRUE_BASE_DIR}/${PROC_DIR}/ReProcessedData_Flat" \
#   --events-pre-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsPreReproc" \
#   --events-reproc-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsReproc" \
#   --pattern "*_events_pos.csv" \
#   --max-round 100 \
#   --round-mode truecontent \
#   --skip-existing \
#   >> "$LOG_FILE" 2>&1
# echo "✅ 00_batch_reproc_steps_01_04.py completed at $(date)" | tee -a "$LOG_FILE"

# # 4) Events + reprocessed + prelim intervals -> events_pre_reproc + final intervals + final events
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting 05_batch_add_startpos.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/05_batch_add_startpos.py" \
#   --root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsReproc" \
#   --pattern "*_event_reproc.csv" \
#   --interval-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventIntervals" \
#   --also-update-intervals \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsAlmostFinal" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ 05_batch_add_startpos.py completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting occupancy_PreBlockCylinder.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/occupancy_PreBlockCylinder.py" \
  --startprop "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsFinal/ObsReward_A_02_17_2025_15_11_startPosPropagated.csv" \
  --reprocessed "${TRUE_BASE_DIR}/${PROC_DIR}/ReProcessedData_Flat/ObsReward_A_02_17_2025_15_11_reprocessed.csv" \
  --event PreBlock_CylinderWalk_segment \
  --outDir "${TRUE_BASE_DIR}/${PROC_DIR}/OccupancyHeatMaps" \
  >> "$LOG_FILE" 2>&1
echo "✅ occupancy_PreBlockCylinder.py completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting occupancy_PreBlockCylinder_v2.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/occupancy_PreBlockCylinder_v2.py" \
  --startprop "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsFinal/ObsReward_A_02_17_2025_15_11_startPosPropagated.csv" \
  --reprocessed "${TRUE_BASE_DIR}/${PROC_DIR}/ReProcessedData_Flat/ObsReward_A_02_17_2025_15_11_reprocessed.csv" \
  --event PreBlock_CylinderWalk_segment \
  --mode time \
  --tcol eMLT_orig \
  --timestamp-unit auto \
  --max-gap-sec 0.5 \
  --bin-size 0.10 \
  --xmin -10 --xmax 10 --ymin -10 --ymax 10 \
  --outDir "${TRUE_BASE_DIR}/${PROC_DIR}/OccupancyHeatMaps" \
  >> "$LOG_FILE" 2>&1
echo "✅ occupancy_PreBlockCylinder_v2.py completed at $(date)" | tee -a "$LOG_FILE"
