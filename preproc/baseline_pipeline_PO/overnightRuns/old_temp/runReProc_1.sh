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
CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
PROC_DIR="FreshStart_redoAgain"
#PROC_DIR="FreshStart_redoAgainSingle"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Pos"


# python report_roundnums_lt100.py \
#   --input-dir "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgain/EventSegmentation/Events_Flat_csv" \
#   --pattern "*_processed_events.csv" \
#   --include-counts \
#   --out "/Users/mairahmac/Desktop/roundnums_lt100_report.csv"

python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/generatingUnityInput/plot_triangles_from_list.py" \
  --triangles-csv /Users/mairahmac/Desktop/TriangleSets/triangle_positions-formatted__A_D_.csv \
  --output /Users/mairahmac/Desktop/TriangleSets/MultiTrianglePlots/CentroidPlot.png \
  --xlim -5.5 5.5 \
  --ylim -5.5 5.5 \
  >> "$LOG_FILE" 2>&1

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting fillEventsPosWalks.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/fillEventsPosWalks.py" \
#   --events-walks "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsMergedWalks/mini/ObsReward_A_02_17_2025_15_11_eventsWalks.csv" \
#   --reprocessed "${TRUE_BASE_DIR}/${PROC_DIR}/ReProc_withDist/ObsReward_A_02_17_2025_15_11_reprocessed_with_dist.csv" \
#   --out "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventWalksFilled/ObsReward_A_02_17_2025_15_11_filled.csv" \
#   --debug TRUE \
#   >> "$LOG_FILE" 2>&1
# echo "✅ fillEventsPosWalks.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting buildIntervalsFromEvents.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/buildIntervalsFromEvents.py" \
#   --interval "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventIntervals/ObsReward_A_02_17_2025_15_11_finalInterval_vert_startPosPropagated.csv" \
#   --events   "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventWalksFilled/ObsReward_A_02_17_2025_15_11_filled.csv" \
#   --out      "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventIntervals_Almost/ObsReward_A_02_17_2025_15_11_interval_fromEvents.csv" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ buildIntervalsFromEvents.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting propagateIntervalCols.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/propagateIntervalCols.py" \
#   --filled "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventWalksFilled/ObsReward_A_02_17_2025_15_11_filled.csv" \
#   --interval "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventIntervals_Almost/ObsReward_A_02_17_2025_15_11_interval_fromEvents.csv" \
#   --out "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MostlyFilledEvents/ObsReward_A_02_17_2025_15_11_filled_intervalProps.csv" \
#  >> "$LOG_FILE" 2>&1
# echo "✅ propagateIntervalCols.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting assign_norm_util_and_efficiency.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/assign_norm_util_and_efficiency.py" \
#   --main "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsMergedWalks/mini" \
#   --ref-dir "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm" \
#   --ref-pattern "*_normUtil.csv" \
#   --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil" \
#   --out-mode dir \
#   --overwrite \
#   >> "$LOG_FILE" 2>&1
# echo "✅ assign_norm_util_and_efficiency.py completed at $(date)" | tee -a "$LOG_FILE"



