#!/usr/bin/env bash
set -Eeuo pipefail

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/modFreshStart_processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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

# # #############################################################################################################################################################
# # Generating Ideal Distances, Path Utility, & Path Efficiency Stuff 
# #############################################################################################################################################################

# python "${CODE_DIR}/overnightRuns/calcIdealDistances.py"

# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/walkDataAnalysis/theoPaths_Classifiers/greedy_v2.py"

# # #############################################################################################################################################################
# # Normalizing Path Utility Stuff Lamba 1 - 5
# #############################################################################################################################################################


python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extraction/normalize_path_utility.py" \
  --root "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda20" \
  --pattern "all_orders__layout_*_L20.csv" \
  --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L20" \
  --require-order-col \
  --write-summary \
  --overwrite \
  >> "$LOG_FILE" 2>&1

python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extraction/normalize_path_utility.py" \
  --root "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda25" \
  --pattern "all_orders__layout_*_L25.csv" \
  --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L25" \
  --require-order-col \
  --write-summary \
  --overwrite \
  >> "$LOG_FILE" 2>&1

python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extraction/normalize_path_utility.py" \
  --root "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda33" \
  --pattern "all_orders__layout_*_L33.csv" \
  --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L33" \
  --require-order-col \
  --write-summary \
  --overwrite \
  >> "$LOG_FILE" 2>&1

python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extraction/normalize_path_utility.py" \
  --root "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda50" \
  --pattern "all_orders__layout_*_L50.csv" \
  --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L50" \
  --require-order-col \
  --write-summary \
  --overwrite \
  >> "$LOG_FILE" 2>&1


#########################################################################################################
# Event/Interval Augmentation Pipeline | Assigning Path Utility & Path Efficiency to Walks | Lamda 1 - 5
#########################################################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting assign_norm_util_and_efficiency.py Lambda 0.20 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/assign_norm_util_and_efficiency.py" \
  --main-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FilledEventsIntervals" \
  --main-pattern "*_filledIntervals.csv" \
  --ref-dir "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L20" \
  --ref-pattern "all_orders__layout_*_L20_normUtil.csv" \
  --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L20" \
  --out-mode dir \
  --suffix "_normUtil_L20" \
  --overwrite \
  >> "$LOG_FILE" 2>&1
echo "✅ assign_norm_util_and_efficiency.py completed at $(date)" | tee -a "$LOG_FILE"


echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting assign_norm_util_and_efficiency.py Lambda 0.25 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/assign_norm_util_and_efficiency.py" \
  --main-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FilledEventsIntervals" \
  --main-pattern "*_filledIntervals.csv" \
  --ref-dir "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L25" \
  --ref-pattern "all_orders__layout_*_L25_normUtil.csv" \
  --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L25" \
  --out-mode dir \
  --suffix "_normUtil_L25" \
  --overwrite \
  >> "$LOG_FILE" 2>&1 
echo "✅ assign_norm_util_and_efficiency.py completed at $(date)" | tee -a "$LOG_FILE"


echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting assign_norm_util_and_efficiency.py Lambda 0.33 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/assign_norm_util_and_efficiency.py" \
  --main-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FilledEventsIntervals" \
  --main-pattern "*_filledIntervals.csv" \
  --ref-dir "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L33" \
  --ref-pattern "all_orders__layout_*_L33_normUtil.csv" \
  --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L33" \
  --out-mode dir \
  --suffix "_normUtil_L33" \
  --overwrite \
  >> "$LOG_FILE" 2>&1
echo "✅ assign_norm_util_and_efficiency.py completed at $(date)" | tee -a "$LOG_FILE"


echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting assign_norm_util_and_efficiency.py Lambda 0.50 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/assign_norm_util_and_efficiency.py" \
  --main-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FilledEventsIntervals" \
  --main-pattern "*_filledIntervals.csv" \
  --ref-dir "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L50" \
  --ref-pattern "all_orders__layout_*_L50_normUtil.csv" \
  --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L50" \
  --out-mode dir \
  --overwrite \
  --suffix "_normUtil_L50" \
  --overwrite \
  >> "$LOG_FILE" 2>&1
echo "✅ assign_norm_util_and_efficiency.py completed at $(date)" | tee -a "$LOG_FILE"


#########################################################################################################
# Event/Interval Augmentation Pipeline | Assigning Path Utility & Path Efficiency to Walks | Lambda 1 - 5
#########################################################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting group_and_append.py Lambda 0.20 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/group_and_append.py" \
    --meta "${TRUE_BASE_DIR}/collatedData.xlsx" \
    --sheet 0 \
    --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L20" \
    --suffix "_filledIntervals_normUtil_L20.csv" \
    --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FullIntervals_L20" \
    --id-cols participantID pairID testingDate sessionType main_RR currentRole \
    --order-col testingOrder \
    --cleanedfile-col cleanedFile \
    --manifest --group-json --check-columns --check-order --check-empty \
    >> "$LOG_FILE" 2>&1
echo "✅ group_and_append.py Lambda 0.20 completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting group_and_append.py Lambda 0.25 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/group_and_append.py" \
    --meta "${TRUE_BASE_DIR}/collatedData.xlsx" \
    --sheet 0 \
    --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L25" \
    --suffix "_filledIntervals_normUtil_L25.csv" \
    --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FullIntervals_L25" \
    --id-cols participantID pairID testingDate sessionType main_RR currentRole \
    --order-col testingOrder \
    --cleanedfile-col cleanedFile \
    --manifest --group-json --check-columns --check-order --check-empty \
    >> "$LOG_FILE" 2>&1
echo "✅ group_and_append.py Lambda 0.25 completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting group_and_append.py Lambda 0.33 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/group_and_append.py" \
    --meta "${TRUE_BASE_DIR}/collatedData.xlsx" \
    --sheet 0 \
    --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L33" \
    --suffix "_filledIntervals_normUtil_L33.csv" \
    --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FullIntervals_L33" \
    --id-cols participantID pairID testingDate sessionType main_RR currentRole \
    --order-col testingOrder \
    --cleanedfile-col cleanedFile \
    --manifest --group-json --check-columns --check-order --check-empty \
    >> "$LOG_FILE" 2>&1
echo "✅ group_and_append.py Lambda 0.33 completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting group_and_append.py Lambda 0.50 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/group_and_append.py" \
    --meta "${TRUE_BASE_DIR}/collatedData.xlsx" \
    --sheet 0 \
    --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L50" \
    --suffix "_filledIntervals_normUtil_L50.csv" \
    --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FullIntervals_L50" \
    --id-cols participantID pairID testingDate sessionType main_RR currentRole \
    --order-col testingOrder \
    --cleanedfile-col cleanedFile \
    --manifest --group-json --check-columns --check-order --check-empty \
    >> "$LOG_FILE" 2>&1
echo "✅ group_and_append.py Lambda 0.50 completed at $(date)" | tee -a "$LOG_FILE"

