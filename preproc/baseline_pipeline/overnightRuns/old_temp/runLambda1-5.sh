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


# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extraction/normalize_path_utility.py" \
#   --root "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda5" \
#   --pattern "all_orders__layout_*_L5.csv" \
#   --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L5" \
#   --require-order-col \
#   --write-summary \
#   --overwrite \
#   >> "$LOG_FILE" 2>&1

python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extraction/normalize_path_utility.py" \
  --root "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda4" \
  --pattern "all_orders__layout_*_L4.csv" \
  --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L4" \
  --require-order-col \
  --write-summary \
  --overwrite \
  >> "$LOG_FILE" 2>&1

python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extraction/normalize_path_utility.py" \
  --root "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda3" \
  --pattern "all_orders__layout_*_L3.csv" \
  --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L3" \
  --require-order-col \
  --write-summary \
  --overwrite \
  >> "$LOG_FILE" 2>&1

# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extraction/normalize_path_utility.py" \
#   --root "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda2" \
#   --pattern "all_orders__layout_*_L2.csv" \
#   --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L2" \
#   --require-order-col \
#   --write-summary \
#   --overwrite \
#   >> "$LOG_FILE" 2>&1

# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extraction/normalize_path_utility.py" \
#   --root "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda1" \
#   --pattern "all_orders__layout_*_L1.csv" \
#   --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L1" \
#   --require-order-col \
#   --write-summary \
#   --overwrite \
#   >> "$LOG_FILE" 2>&1

# #########################################################################################################
# # Event/Interval Augmentation Pipeline | Assigning Path Utility & Path Efficiency to Walks | Lamda 1 - 5
# #########################################################################################################
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting assign_norm_util_and_efficiency.py Lambda 5 at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/assign_norm_util_and_efficiency.py" \
#   --main-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FilledEventsIntervals" \
#   --main-pattern "*_filledIntervals.csv" \
#   --ref-dir "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L5" \
#   --ref-pattern "all_orders__layout_*_L5_normUtil.csv" \
#   --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L5" \
#   --out-mode dir \
#   --suffix "_normUtil_L5" \
#   --overwrite \
#   >> "$LOG_FILE" 2>&1
# echo "✅ assign_norm_util_and_efficiency.py completed at $(date)" | tee -a "$LOG_FILE"


echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting assign_norm_util_and_efficiency.py Lambda 4 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/assign_norm_util_and_efficiency.py" \
  --main-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FilledEventsIntervals" \
  --main-pattern "*_filledIntervals.csv" \
  --ref-dir "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L4" \
  --ref-pattern "all_orders__layout_*_L4_normUtil.csv" \
  --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L5" \
  --out-mode dir \
  --suffix "_normUtil_L5" \
  --overwrite \
  >> "$LOG_FILE" 2>&1 
echo "✅ assign_norm_util_and_efficiency.py completed at $(date)" | tee -a "$LOG_FILE"


echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting assign_norm_util_and_efficiency.py Lambda 3 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/assign_norm_util_and_efficiency.py" \
  --main-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FilledEventsIntervals" \
  --main-pattern "*_filledIntervals.csv" \
  --ref-dir "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L3" \
  --ref-pattern "all_orders__layout_*_L3_normUtil.csv" \
  --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L5" \
  --out-mode dir \
  --suffix "_normUtil_L3" \
  --overwrite \
  >> "$LOG_FILE" 2>&1
echo "✅ assign_norm_util_and_efficiency.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting assign_norm_util_and_efficiency.py Lambda 2 at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/assign_norm_util_and_efficiency.py" \
#   --main-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FilledEventsIntervals" \
#   --main-pattern "*_filledIntervals.csv" \
#   --ref-dir "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L2" \
#   --ref-pattern "all_orders__layout_*_L2_normUtil.csv" \
#   --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L2" \
#   --out-mode dir \
#   --overwrite \
#   --suffix "_normUtil_L2" \
#   --overwrite \
#   >> "$LOG_FILE" 2>&1
# echo "✅ assign_norm_util_and_efficiency.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting assign_norm_util_and_efficiency.py Lambda 1 at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/assign_norm_util_and_efficiency.py" \
#   --main-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FilledEventsIntervals" \
#   --main-pattern "*_filledIntervals.csv" \
#   --ref-dir "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L1" \
#   --ref-pattern "all_orders__layout_*_L1_normUtil.csv" \
#   --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L1" \
#   --out-mode dir \
#   --suffix "_normUtil_L1" \
#   --overwrite \
#   >> "$LOG_FILE" 2>&1
# echo "✅ assign_norm_util_and_efficiency.py completed at $(date)" | tee -a "$LOG_FILE"


#########################################################################################################
# Event/Interval Augmentation Pipeline | Assigning Path Utility & Path Efficiency to Walks | Lambda 1 - 5
#########################################################################################################
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting group_and_append.py Lambda 5 at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/group_and_append.py" \
#     --meta "${TRUE_BASE_DIR}/collatedData.xlsx" \
#     --sheet 0 \
#     --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L5" \
#     --suffix "_filledIntervals_normUtil_L5.csv" \
#     --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FullIntervals_L5" \
#     --id-cols participantID pairID testingDate sessionType main_RR currentRole \
#     --order-col testingOrder \
#     --cleanedfile-col cleanedFile \
#     --manifest --group-json --check-columns --check-order --check-empty \
#     >> "$LOG_FILE" 2>&1
# echo "✅ group_and_append.py Lambda 5 completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting group_and_append.py Lambda 4 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/group_and_append.py" \
    --meta "${TRUE_BASE_DIR}/collatedData.xlsx" \
    --sheet 0 \
    --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L4" \
    --suffix "_filledIntervals_normUtil_L4.csv" \
    --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FullIntervals_L4" \
    --id-cols participantID pairID testingDate sessionType main_RR currentRole \
    --order-col testingOrder \
    --cleanedfile-col cleanedFile \
    --manifest --group-json --check-columns --check-order --check-empty \
    >> "$LOG_FILE" 2>&1
echo "✅ group_and_append.py Lambda 4 completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting group_and_append.py Lambda 3 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/group_and_append.py" \
    --meta "${TRUE_BASE_DIR}/collatedData.xlsx" \
    --sheet 0 \
    --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L3" \
    --suffix "_filledIntervals_normUtil_L3.csv" \
    --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FullIntervals_L3" \
    --id-cols participantID pairID testingDate sessionType main_RR currentRole \
    --order-col testingOrder \
    --cleanedfile-col cleanedFile \
    --manifest --group-json --check-columns --check-order --check-empty \
    >> "$LOG_FILE" 2>&1
echo "✅ group_and_append.py Lambda 3 completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting group_and_append.py Lambda 2 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/group_and_append.py" \
    --meta "${TRUE_BASE_DIR}/collatedData.xlsx" \
    --sheet 0 \
    --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L2" \
    --suffix "_filledIntervals_normUtil_L2.csv" \
    --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FullIntervals_L2" \
    --id-cols participantID pairID testingDate sessionType main_RR currentRole \
    --order-col testingOrder \
    --cleanedfile-col cleanedFile \
    --manifest --group-json --check-columns --check-order --check-empty \
    >> "$LOG_FILE" 2>&1
echo "✅ group_and_append.py Lambda 2 completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting group_and_append.py Lambda 1 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/group_and_append.py" \
    --meta "${TRUE_BASE_DIR}/collatedData.xlsx" \
    --sheet 0 \
    --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L1" \
    --suffix "_filledIntervals_normUtil_L1.csv" \
    --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FullIntervals_L1" \
    --id-cols participantID pairID testingDate sessionType main_RR currentRole \
    --order-col testingOrder \
    --cleanedfile-col cleanedFile \
    --manifest --group-json --check-columns --check-order --check-empty \
    >> "$LOG_FILE" 2>&1
echo "✅ group_and_append.py Lambda 1 completed at $(date)" | tee -a "$LOG_FILE"