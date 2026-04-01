#!/usr/bin/env bash
set -Eeuo pipefail

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/modelingData_processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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
CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
PROC_DIR="FreshStart_redoAgainAgainAgain"
#PROC_DIR="FreshStart_redoAgainSingle"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Pos"
LAMBDA="1"


# ### Making my decision expansion file
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting prepDataForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/extraction/prepDataForDecisionModel.py" \
#   --interval_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalData_L${LAMBDA}.csv" \
#   --utility_csv "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_All/pathUtility_lambda${LAMBDA}.csv" \
#   --out_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/decisionExpanded_L${LAMBDA}.csv" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ prepDataForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"

# ### Adding Learning Knots
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting addKnotsForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/extraction/addKnotsForDecisionModel.py" \
#   --in_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/decisionExpanded_L${LAMBDA}.csv" \
#   --out_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/decisionExpandedKnotted_L${LAMBDA}.csv" \
#   --knots 15 20 25 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ addKnotsForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"

# ### Adding Learning Knots
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting addKnotsForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/extraction/addKnotsForDecisionModel.py" \
#   --in_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalData_L${LAMBDA}.csv" \
#   --out_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_L${LAMBDA}.csv" \
#   --out_pruned "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_Prune_L${LAMBDA}.csv" \
#   --knots 15 20 25 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ addKnotsForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"


# ### Making my decision expansion file
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting prepDataForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/extraction/prepDataForDecisionModel.py" \
#   --interval_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_Prune_L${LAMBDA}.csv" \
#   --utility_csv "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_All/pathUtility_lambda${LAMBDA}.csv" \
#   --out_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/decisionExpanded_L${LAMBDA}.csv" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ prepDataForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"

## running mixed model stuff
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting mixedModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/plotting/mixedModel.py" \
  >> "$LOG_FILE" 2>&1
echo "✅ mixedModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"

# ## running prelim condit logit All Subjects stuff
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting prelimConditLogit.py in All Subjects Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/plotting/prelimConditLogit.py" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ prelimConditLogit_noA.py in All Subjects Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"

# ## running prelim condit logit No Coin Set A Subjects stuff
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting prelimConditLogit_noA.py No Coin Set A Subjects Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/plotting/prelimConditLogit_noA.py" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ prelimConditLogit_noA.py No Coin Set A Subjects Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"

# ## running prelim condit logit stuff
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting prelimConditLogit_onlyA.py Only Coin Set A Subjects Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/plotting/prelimConditLogit_onlyA.py" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ prelimConditLogit_onlyA.py Only Coin Set A Subjects Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"



# ## running prelim condit logit stuff
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting prelimConditLogit_onlyAx.py Only Coin Set Ax Subjects Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/plotting/prelimConditLogit_onlyAx.py" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ prelimConditLogit_onlyAx.py Only Coin Set Ax Subjects Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"


# ## running prelim condit logit stuff
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting prelimConditLogit_onlyB.py Only Coin Set B Subjects Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/plotting/prelimConditLogit_onlyB.py" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ prelimConditLogit_onlyB.py Only Coin Set B Subjects Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"


# ## running prelim condit logit stuff
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting prelimConditLogit_onlyBx.py Only Coin Set Bx Subjects Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/plotting/prelimConditLogit_onlyBx.py" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ prelimConditLogit_onlyBx.py Only Coin Set Bx Subjects Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"


# ## running prelim condit logit stuff
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting prelimConditLogit_learningAll.py All Subjects t = 20 Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/plotting/prelimConditLogit_learningAll.py" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ prelimConditLogit_learningAll.py All Subjects t = 20 Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"
