#!/usr/bin/env bash
set -Eeuo pipefail

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/Redo_ReProcessing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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
CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline/reproc"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
PROC_DIR="FreshStart_redoMissing"
#PROC_DIR="FreshStart_multi"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Pos"



echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting 00_batch_reproc_steps_01_04.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/00_batch_reproc_steps_01_04.py" \
  --events-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/${EVENTS_DIR}" \
  --processed-root "${TRUE_BASE_DIR}/${PROC_DIR}/ProcessedData_Flat" \
  --intervals-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventIntervals" \
  --prelim-reproc-dir "${TRUE_BASE_DIR}/${PROC_DIR}/PreLimReProcessedData_Flat" \
  --reproc-dir "${TRUE_BASE_DIR}/${PROC_DIR}/ReProcessedData_Flat" \
  --events-pre-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsPreReproc" \
  --events-reproc-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsReproc" \
  --pattern "*_events_pos.csv" \
  --max-round 100 \
  --round-mode truecontent \
  --skip-existing \
  >> "$LOG_FILE" 2>&1
echo "✅ 00_batch_reproc_steps_01_04.py completed at $(date)" | tee -a "$LOG_FILE"

#4) Events + reprocessed + prelim intervals -> events_pre_reproc + final intervals + final events
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting 05_batch_add_startpos.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/05_batch_add_startpos.py" \
  --root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsReproc" \
  --pattern "*_event_reproc.csv" \
  --interval-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventIntervals" \
  --also-update-intervals \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsAlmostFinal" \
  >> "$LOG_FILE" 2>&1
echo "✅ 05_batch_add_startpos.py completed at $(date)" | tee -a "$LOG_FILE"


echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
echo "🚀 Starting 06a_make_effective_roundnum.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/06a_make_effective_roundnum.py" \
  --root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsAlmostFinal" \
  --pattern "*_withStartPos.csv" \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsAlmostFinal_06a" \
  --write-audit \
  >> "$LOG_FILE" 2>&1
echo "✅ 06a_make_effective_roundnum.py completed at $(date)" | tee -a "$LOG_FILE"


echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🚀 Starting 06b_propagate_pindrop_startpos.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/06b_propagate_pindrop_startpos.py" \
  --root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsAlmostFinal_06a" \
  --pattern "*_withEffectiveRound.csv"  \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsFinal" \
  --interval-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventIntervals" \
  --also-update-intervals
  >> "$LOG_FILE" 2>&1
echo "✅ 06b_propagate_pindrop_startpos.py completed at $(date)" | tee -a "$LOG_FILE"


