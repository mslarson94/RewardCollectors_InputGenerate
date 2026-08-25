#!/usr/bin/env bash
set -Eeuo pipefail

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/summaryPlots_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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
CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/summaryMetricSuite"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
PROC_DIR="FreshStart_redoAgainAgainAgain_PO_redo"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Final_NoWalks"
LAMBDA="1"
ROUND_DUR_FILTER=2.0
MEGA_DIR="${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles"
MEGA_FILE="${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/1st50DataKnotted_AN.csv"
MEGA_SUMMARY="${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles_final/participantSummaryData_1st50_AN_noCD.csv"
# MEGA_SUMMARY="${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles_final/participantSummaryData_1st50_AN.csv"
# MEGA_SUMMARY="${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles_final/participantSummaryData_AN_noCD.csv"
# MEGA_SUMMARY="${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles_final/participantSummaryData_AN.csv"
MEGA_SUMMARY_ORIG="${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles_final/participantSummaryData_AN.csv"
OUTDIR="${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/mad_${ROUND_DUR_FILTER}/1st50Rounds/all/noCD"


echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting summaryMetricWrapper for Swap Vote Score at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/summaryMetricWrapper.py" \
  --input ${MEGA_SUMMARY} \
  --voi swapVoteScore \
  --voi-str "Swap vote score" \
  --facet-by main_RR \
  --out-dir "${OUTDIR}/SummaryPlots" \
  >> "$LOG_FILE" 2>&1
echo "✅ summaryMetricWrapper for Swap Vote Score completed at $(date)" | tee -a "$LOG_FILE"


echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting summaryMetricWrapper for rounds to criterion at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/summaryMetricWrapper.py" \
  --input ${MEGA_SUMMARY} \
  --voi rounds2criterion \
  --voi-str "Rounds to criterion" \
  --where main_RR=main \
  --out-dir "${OUTDIR}/SummaryPlots" \
  --bin-width 1 \
  >> "$LOG_FILE" 2>&1
echo "✅ summaryMetricWrapper for rounds to criterion completed at $(date)" | tee -a "$LOG_FILE"


echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting summaryMetricWrapper for Total Score at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/summaryMetricWrapper.py" \
  --input ${MEGA_SUMMARY} \
  --voi totScore \
  --voi-str "Total score" \
  --facet-by main_RR \
  --out-dir "${OUTDIR}/SummaryPlots" \
  >> "$LOG_FILE" 2>&1
echo "✅ summaryMetricWrapper for Total Score completed at $(date)" | tee -a "$LOG_FILE"

echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting summaryMetricWrapper for Total Rounds at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/summaryMetricWrapper.py" \
  --input ${MEGA_SUMMARY} \
  --voi actualTestRoundsIncluded \
  --voi-str "Total Rounds Completed" \
  --facet-by main_RR \
  --out-dir "${OUTDIR}/SummaryPlots" \
  >> "$LOG_FILE" 2>&1
echo "✅ summaryMetricWrapper for Total Round completed at $(date)" | tee -a "$LOG_FILE"

echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting summaryMetricWrapper for Swap Rate at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/summaryMetricWrapper.py" \
  --input ${MEGA_SUMMARY} \
  --voi swapRate_tot \
  --voi-str "Swap Rate" \
  --facet-by main_RR \
  --out-dir "${OUTDIR}/SummaryPlots" \
  >> "$LOG_FILE" 2>&1
echo "✅ summaryMetricWrapper for Swap Rate completed at $(date)" | tee -a "$LOG_FILE"


echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting summaryMetricWrapper for Total Points at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/summaryMetricWrapper.py" \
  --input ${MEGA_SUMMARY} \
  --voi totPoints \
  --voi-str "Total Points" \
  --facet-by main_RR \
  --out-dir "${OUTDIR}/SummaryPlots" \
  >> "$LOG_FILE" 2>&1
echo "✅ summaryMetricWrapper for Total Points completed at $(date)" | tee -a "$LOG_FILE"



echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting summaryMetricWrapper for PVSS_AvgScore at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/summaryMetricWrapper.py" \
  --input ${MEGA_SUMMARY} \
  --voi PVSS_AvgScore \
  --voi-str "PVSS Average Score" \
  --where main_RR=main \
  --out-dir "${OUTDIR}/SummaryPlots" \
  >> "$LOG_FILE" 2>&1
echo "✅ summaryMetricWrapper for PVSS_AvgScore completed at $(date)" | tee -a "$LOG_FILE"

echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting summaryMetricWrapper for Age at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/summaryMetricWrapper.py" \
  --input ${MEGA_SUMMARY} \
  --voi Age \
  --voi-str "Age" \
  --where main_RR=main \
  --out-dir "${OUTDIR}/SummaryPlots" \
  >> "$LOG_FILE" 2>&1
echo "✅ summaryMetricWrapper for Age completed at $(date)" | tee -a "$LOG_FILE"


# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting summaryMetricWrapper for coinSet at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/summaryMetricWrapper.py" \
#   --input ${MEGA_SUMMARY} \
#   --voi coinSet \
#   --voi-str "Coin Layouts" \
#   --where main_RR=main \
#   --out-dir "${OUTDIR}/SummaryPlots" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ summaryMetricWrapper for coinSet completed at $(date)" | tee -a "$LOG_FILE"