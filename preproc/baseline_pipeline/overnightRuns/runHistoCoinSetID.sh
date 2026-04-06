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
CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
PROC_DIR="FreshStart"
# PROC_DIR="FreshStart_mini"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Final_NoWalks"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting coinset_by_coinlabel_plots.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/coinset_by_coinlabel_plots.py" \
  --input "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_Main/CoinSetByCoinLabel" \
  --formats png,pdf \
  --voi dropDist \
  --use-outlier-filter \
  >> "$LOG_FILE" 2>&1
echo "✅ coinset_by_coinlabel_plots.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting coinset_by_coinlabel_plots.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/coinset_by_coinlabel_plots.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_Main/CoinSetByCoinLabel" \
#   --formats png,pdf \
#   --voi truecontent_elapsed_s \
#   --use-outlier-filter \
#   >> "$LOG_FILE" 2>&1
# echo "✅ coinset_by_coinlabel_plots.py completed at $(date)" | tee -a "$LOG_FILE"