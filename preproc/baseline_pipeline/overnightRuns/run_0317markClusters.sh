#!/usr/bin/env bash
set -Eeuo pipefail

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/RPi_processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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
PROC_DIR="FreshStart_redoAgainAgainAgain_PO_redo_new"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Pos"



# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/mark_chunk_review_tool.py" propose \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart/ObsReward_A_03_17_2025_10_30_earliestRoundStart.csv" \
#   --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_30" \
#   --time-col eMLT_orig \
#   --event-col lo_eventType \
#   --block-col BlockNum \
#   --mark-gap-seconds 30 \
#   --context-window-seconds 300 \
#   --boundary-pair-max-seconds 300 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mark_chunk_review_tool.py propose completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting Streamlit review app at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/mark_review_tk.py"\
#   --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_30/mark_clusters.csv" \
#   --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_30/mark_cluster_context.csv" \
#   --source-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart/ObsReward_A_03_17_2025_10_30_earliestRoundStart.csv" \
#   --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_30/review_edits.csv" \
#   --time-col eMLT_orig \
#   --event-col lo_eventType \
#   --block-col BlockNum \
#   --context-seconds 300 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ Streamlit review app closed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting mark_chunk_review_tool.py apply-edits at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/mark_chunk_review_tool.py" apply-edits \
#   --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_30/mark_clusters.csv" \
#   --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_30/review_edits.csv" \
#   --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_30/mark_cluster_context.csv" \
#   --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_30/Finalized" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mark_chunk_review_tool.py apply-edits completed at $(date)" | tee -a "$LOG_FILE"


echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting Streamlit review app at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_review_tk.py"\
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_44/mark_clusters.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_44/mark_cluster_context.csv" \
  --source-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart/ObsReward_A_03_17_2025_10_44_earliestRoundStart.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_44/review_edits.csv" \
  --time-col eMLT_orig \
  --event-col lo_eventType \
  --block-col BlockNum \
  --context-seconds 300 \
  >> "$LOG_FILE" 2>&1
echo "✅ Streamlit review app closed at $(date)" | tee -a "$LOG_FILE"

echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting mark_chunk_review_tool.py apply-edits at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_chunk_review_tool.py" apply-edits \
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_44/mark_clusters.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_44/review_edits.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_44/mark_cluster_context.csv" \
  --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_10_44/Finalized" \
  >> "$LOG_FILE" 2>&1
echo "✅ mark_chunk_review_tool.py apply-edits completed at $(date)" | tee -a "$LOG_FILE"



echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting Streamlit review app at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_review_tk.py"\
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_11_15/mark_clusters.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_11_15/mark_cluster_context.csv" \
  --source-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart/ObsReward_A_03_17_2025_11_15_earliestRoundStart.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_11_15/review_edits.csv" \
  --time-col eMLT_orig \
  --event-col lo_eventType \
  --block-col BlockNum \
  --context-seconds 300 \
  >> "$LOG_FILE" 2>&1
echo "✅ Streamlit review app closed at $(date)" | tee -a "$LOG_FILE"


echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting mark_chunk_review_tool.py apply-edits at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_chunk_review_tool.py" apply-edits \
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_11_15/mark_clusters.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_11_15/review_edits.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_11_15/mark_cluster_context.csv" \
  --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_11_15/Finalized" \
  >> "$LOG_FILE" 2>&1
echo "✅ mark_chunk_review_tool.py apply-edits completed at $(date)" | tee -a "$LOG_FILE"





echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting Streamlit review app at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_review_tk.py"\
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_13_48/mark_clusters.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_13_48/mark_cluster_context.csv" \
  --source-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart/ObsReward_A_03_17_2025_13_48_earliestRoundStart.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_13_48/review_edits.csv" \
  --time-col eMLT_orig \
  --event-col lo_eventType \
  --block-col BlockNum \
  --context-seconds 300 \
  >> "$LOG_FILE" 2>&1
echo "✅ Streamlit review app closed at $(date)" | tee -a "$LOG_FILE"

echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting mark_chunk_review_tool.py apply-edits at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_chunk_review_tool.py" apply-edits \
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_13_48/mark_clusters.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_13_48/review_edits.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_13_48/mark_cluster_context.csv" \
  --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_13_48/Finalized" \
  >> "$LOG_FILE" 2>&1
echo "✅ mark_chunk_review_tool.py apply-edits completed at $(date)" | tee -a "$LOG_FILE"



echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting Streamlit review app at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_review_tk.py"\
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_00/mark_clusters.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_00/mark_cluster_context.csv" \
  --source-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart/ObsReward_A_03_17_2025_14_00_earliestRoundStart.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_00/review_edits.csv" \
  --time-col eMLT_orig \
  --event-col lo_eventType \
  --block-col BlockNum \
  --context-seconds 300 \
  >> "$LOG_FILE" 2>&1
echo "✅ Streamlit review app closed at $(date)" | tee -a "$LOG_FILE"

echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting mark_chunk_review_tool.py apply-edits at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_chunk_review_tool.py" apply-edits \
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_00/mark_clusters.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_00/review_edits.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_00/mark_cluster_context.csv" \
  --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_00/Finalized" \
  >> "$LOG_FILE" 2>&1
echo "✅ mark_chunk_review_tool.py apply-edits completed at $(date)" | tee -a "$LOG_FILE"



echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting Streamlit review app at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_review_tk.py"\
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_09/mark_clusters.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_09/mark_cluster_context.csv" \
  --source-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart/ObsReward_A_03_17_2025_14_09_earliestRoundStart.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_09/review_edits.csv" \
  --time-col eMLT_orig \
  --event-col lo_eventType \
  --block-col BlockNum \
  --context-seconds 300 \
  >> "$LOG_FILE" 2>&1
echo "✅ Streamlit review app closed at $(date)" | tee -a "$LOG_FILE"

echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting mark_chunk_review_tool.py apply-edits at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_chunk_review_tool.py" apply-edits \
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_09/mark_clusters.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_09/review_edits.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_09/mark_cluster_context.csv" \
  --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_09/Finalized" \
  >> "$LOG_FILE" 2>&1
echo "✅ mark_chunk_review_tool.py apply-edits completed at $(date)" | tee -a "$LOG_FILE"



echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting Streamlit review app at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_review_tk.py"\
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_16/mark_clusters.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_16/mark_cluster_context.csv" \
  --source-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart/ObsReward_A_03_17_2025_14_16_earliestRoundStart.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_16/review_edits.csv" \
  --time-col eMLT_orig \
  --event-col lo_eventType \
  --block-col BlockNum \
  --context-seconds 300 \
  >> "$LOG_FILE" 2>&1
echo "✅ Streamlit review app closed at $(date)" | tee -a "$LOG_FILE"

echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting mark_chunk_review_tool.py apply-edits at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_chunk_review_tool.py" apply-edits \
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_16/mark_clusters.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_16/review_edits.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_16/mark_cluster_context.csv" \
  --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_14_16/Finalized" \
  >> "$LOG_FILE" 2>&1
echo "✅ mark_chunk_review_tool.py apply-edits completed at $(date)" | tee -a "$LOG_FILE"


echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting Streamlit review app at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_review_tk.py"\
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_15_50/mark_clusters.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_15_50/mark_cluster_context.csv" \
  --source-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart/ObsReward_A_03_17_2025_15_50_earliestRoundStart.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_15_50/review_edits.csv" \
  --time-col eMLT_orig \
  --event-col lo_eventType \
  --block-col BlockNum \
  --context-seconds 300 \
  >> "$LOG_FILE" 2>&1
echo "✅ Streamlit review app closed at $(date)" | tee -a "$LOG_FILE"

echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting mark_chunk_review_tool.py apply-edits at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_chunk_review_tool.py" apply-edits \
  --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_15_50/mark_clusters.csv" \
  --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_15_50/review_edits.csv" \
  --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_15_50/mark_cluster_context.csv" \
  --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_A_03_17_2025_15_50/Finalized" \
  >> "$LOG_FILE" 2>&1
echo "✅ mark_chunk_review_tool.py apply-edits completed at $(date)" | tee -a "$LOG_FILE"

