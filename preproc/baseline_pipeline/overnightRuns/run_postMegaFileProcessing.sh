#!/usr/bin/env bash
set -Eeuo pipefail

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/postMegaFileProcessing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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
PROC_DIR="FreshStart_redoAgainAgainAgain_PO_redo"
#PROC_DIR="FreshStart_redoAgainSingle"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Pos"
TRIANGLES_DIR="/Users/mairahmac/Desktop/TriangleSets"
MEGA_DIR="${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles"
LAMBDA="1"
ROUND_DUR_FILTER=2.0
IQR_MULTI=1.5

# ######### Patch

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting patch_dropDist.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/patch_dropDist.py" \
  --input "${MEGA_DIR}/allIntervalData_AN.csv" \
  --output "${MEGA_DIR}_temp/allIntervalData_AN_dropDist.csv" \
>> "$LOG_FILE" 2>&1
echo "✅ patch_dropDist.py completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting patch_dropDist.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/patch_dropDist.py" \
  --input "${MEGA_DIR}/allIntervalData_PO.csv" \
  --output "${MEGA_DIR}_temp/allIntervalData_PO_dropDist.csv" \
>> "$LOG_FILE" 2>&1
echo "✅ patch_dropDist.py completed at $(date)" | tee -a "$LOG_FILE"


### EV event tagging 
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting annotate_within_round_swap_behavior.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/annotate_within_round_swap_behavior.py" \
  --input "${MEGA_DIR}_temp/allIntervalData_AN_dropDist.csv" \
  --output "${MEGA_DIR}_temp/allIntervalData_AN_EVBeh.csv" \
  >> "$LOG_FILE" 2>&1
echo "✅ annotate_within_round_swap_behavior.py completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting annotate_within_round_swap_behavior.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/annotate_within_round_swap_behavior.py" \
  --input "${MEGA_DIR}_temp/allIntervalData_PO_dropDist.csv" \
  --output "${MEGA_DIR}_temp/allIntervalData_PO_EVBeh.csv" \
  >> "$LOG_FILE" 2>&1
echo "✅ annotate_within_round_swap_behavior.py completed at $(date)" | tee -a "$LOG_FILE"

# ##########

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting generate_base_qc_flags.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/generate_base_qc_flags.py" \
  --input "${MEGA_DIR}_temp/allIntervalData_AN_EVBeh.csv" \
  --outdir "${MEGA_DIR}_temp" \
  >> "$LOG_FILE" 2>&1
echo "✅ generate_base_qc_flags.py completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting generate_base_qc_flags.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/generate_base_qc_flags.py" \
  --input "${MEGA_DIR}_temp/allIntervalData_PO_EVBeh.csv" \
  --outdir "${MEGA_DIR}_temp" \
  >> "$LOG_FILE" 2>&1
echo "✅ generate_base_qc_flags.py completed at $(date)" | tee -a "$LOG_FILE"




# ##########

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting cappingTotalRounds.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/extraction/cappingTotalRounds.py" \
    --inFile "${MEGA_DIR}_temp/allIntervalData_AN_EVBeh_baseQC.csv" \
    --output "${MEGA_DIR}_temp/allIntervalData_AN_EVBeh_baseQC_1st50Rds.csv" \
    --summary "${MEGA_DIR}_temp/session_round_counts_AN.csv" \
    >> "$LOG_FILE" 2>&1
echo "✅ cappingTotalRounds.py completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting cappingTotalRounds.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/extraction/cappingTotalRounds.py" \
    --inFile "${MEGA_DIR}_temp/allIntervalData_PO_EVBeh_baseQC.csv" \
    --output "${MEGA_DIR}_temp/allIntervalData_PO_EVBeh_baseQC_1st50Rds.csv" \
    --summary "${MEGA_DIR}_temp/session_round_counts_PO.csv" \
    >> "$LOG_FILE" 2>&1
echo "✅ cappingTotalRounds.py completed at $(date)" | tee -a "$LOG_FILE"





############ All Rounds 
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting generate_round_dur_optionA.py for all rounds at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/generate_round_dur_optionA.py" \
  --input "${MEGA_DIR}_temp/allIntervalData_AN_EVBeh_baseQC.csv" \
  --outdir "${MEGA_DIR}_temp/all/mad_${ROUND_DUR_FILTER}_AN" \
  --preferred-group rr_sess \
  --preferred-method mad \
  --mad-threshold ${ROUND_DUR_FILTER} \
  --iqr-multiplier ${IQR_MULTI} \
  --min-group-n 8 \
  >> "$LOG_FILE" 2>&1
echo "✅ generate_round_dur_optionA.py for all rounds completed at $(date)" | tee -a "$LOG_FILE"


echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting generate_round_dur_optionA.py for all rounds at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/generate_round_dur_optionA.py" \
  --input "${MEGA_DIR}_temp/allIntervalData_PO_EVBeh_baseQC.csv" \
  --outdir "${MEGA_DIR}_temp/all/mad_${ROUND_DUR_FILTER}_PO" \
  --preferred-group rr_sess \
  --preferred-method mad \
  --mad-threshold ${ROUND_DUR_FILTER} \
  --iqr-multiplier ${IQR_MULTI} \
  --min-group-n 8 \
  >> "$LOG_FILE" 2>&1
echo "✅ generate_round_dur_optionA.py for all rounds completed at $(date)" | tee -a "$LOG_FILE"


############ 1st 50 Rounds 

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting generate_round_dur_optionA.py for the 1st 50 rounds at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/generate_round_dur_optionA.py" \
  --input "${MEGA_DIR}_temp/allIntervalData_AN_EVBeh_baseQC_1st50Rds.csv" \
  --outdir "${MEGA_DIR}_temp/1st50/mad_${ROUND_DUR_FILTER}_AN" \
  --preferred-group rr_sess \
  --preferred-method mad \
  --mad-threshold ${ROUND_DUR_FILTER} \
  --min-group-n 8 \
  >> "$LOG_FILE" 2>&1
echo "✅ generate_round_dur_optionA.py for the 1st 50 rounds completed at $(date)" | tee -a "$LOG_FILE"


echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting generate_round_dur_optionA.py for the 1st 50 rounds at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/generate_round_dur_optionA.py" \
  --input "${MEGA_DIR}_temp/allIntervalData_PO_EVBeh_baseQC_1st50Rds.csv" \
  --outdir "${MEGA_DIR}_temp/1st50/mad_${ROUND_DUR_FILTER}_PO" \
  --preferred-group rr_sess \
  --preferred-method mad \
  --mad-threshold ${ROUND_DUR_FILTER} \
  --min-group-n 8 \
  >> "$LOG_FILE" 2>&1
echo "✅ generate_round_dur_optionA.py for the 1st 50 rounds completed at $(date)" | tee -a "$LOG_FILE"



##################################### All Rounds 

### Adding Learning Knots
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting addKnotsForDecisionModel.py Lambda ${LAMBDA} for the 1st 50 rounds at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/extraction/addKnotsForDecisionModel.py" \
  --in_csv "${MEGA_DIR}_temp/all/mad_${ROUND_DUR_FILTER}_AN/allIntervalData_AN_EVBeh_baseQC_roundDurFiltered.csv" \
  --out_csv "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/allDataKnotted_AN.csv" \
  --out_pruned "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/allDataKnotted_Prune_AN.csv" \
  --knots 15 20 25 \
  >> "$LOG_FILE" 2>&1
echo "✅ addKnotsForDecisionModel.py Lambda ${LAMBDA} for the 1st 50 rounds completed at $(date)" | tee -a "$LOG_FILE"

### Adding Learning Knots
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting addKnotsForDecisionModel.py Lambda ${LAMBDA} for the 1st 50 rounds at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/extraction/addKnotsForDecisionModel.py" \
  --in_csv "${MEGA_DIR}_temp/all/mad_${ROUND_DUR_FILTER}_PO/allIntervalData_PO_EVBeh_baseQC_roundDurFiltered.csv" \
  --out_csv "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/allDataKnotted_PO.csv" \
  --out_pruned "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/allDataKnotted_Prune_PO.csv" \
  --knots 15 20 25 \
  >> "$LOG_FILE" 2>&1
echo "✅ addKnotsForDecisionModel.py Lambda ${LAMBDA} for the 1st 50 rounds completed at $(date)" | tee -a "$LOG_FILE"


##################################### 1st 50

### Adding Learning Knots
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting addKnotsForDecisionModel.py Lambda ${LAMBDA} for the 1st 50 rounds at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/extraction/addKnotsForDecisionModel.py" \
  --in_csv "${MEGA_DIR}_temp/1st50/mad_${ROUND_DUR_FILTER}_AN/allIntervalData_AN_EVBeh_baseQC_1st50Rds_roundDurFiltered.csv" \
  --out_csv "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/1st50DataKnotted_AN.csv" \
  --out_pruned "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/1st50DataKnotted_AN.csv" \
  --knots 15 20 25 \
  >> "$LOG_FILE" 2>&1
echo "✅ addKnotsForDecisionModel.py Lambda ${LAMBDA} for the 1st 50 rounds completed at $(date)" | tee -a "$LOG_FILE"

### Adding Learning Knots
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting addKnotsForDecisionModel.py Lambda ${LAMBDA} for the 1st 50 rounds at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/extraction/addKnotsForDecisionModel.py" \
  --in_csv "${MEGA_DIR}_temp/1st50/mad_${ROUND_DUR_FILTER}_PO/allIntervalData_PO_EVBeh_baseQC_1st50Rds_roundDurFiltered.csv" \
  --out_csv "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/1st50DataKnotted_PO.csv" \
  --out_pruned "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/1st50DataKnotted_Prune_PO.csv" \
  --knots 15 20 25 \
  >> "$LOG_FILE" 2>&1
echo "✅ addKnotsForDecisionModel.py Lambda ${LAMBDA} for the 1st 50 rounds completed at $(date)" | tee -a "$LOG_FILE"






# ########## 1st50 ###########
### Making my decision expansion file
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting prepDataForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/extraction/prepDataForDecisionModel.py" \
  --interval_csv "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/1st50DataKnotted_AN.csv" \
  --utility_csv "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_All/pathUtility_lambda${LAMBDA}.csv" \
  --out_csv "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/1st50_decisionExpanded_AN.csv" \
  >> "$LOG_FILE" 2>&1
echo "✅ prepDataForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"


echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting prepDataForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/extraction/prepDataForDecisionModel.py" \
  --interval_csv "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/1st50DataKnotted_PO.csv" \
  --utility_csv "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_All/pathUtility_lambda${LAMBDA}.csv" \
  --out_csv "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/1st50_decisionExpanded_PO.csv" \
  >> "$LOG_FILE" 2>&1
echo "✅ prepDataForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"
# ####################

# ########## all ###########
### Making my decision expansion file
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting prepDataForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/extraction/prepDataForDecisionModel.py" \
  --interval_csv "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/allDataKnotted_AN.csv" \
  --utility_csv "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_All/pathUtility_lambda${LAMBDA}.csv" \
  --out_csv "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/all_decisionExpanded_AN.csv" \
  >> "$LOG_FILE" 2>&1
echo "✅ prepDataForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"


echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting prepDataForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/extraction/prepDataForDecisionModel.py" \
  --interval_csv "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/allDataKnotted_PO.csv" \
  --utility_csv "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_All/pathUtility_lambda${LAMBDA}.csv" \
  --out_csv "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/all_decisionExpanded_PO.csv" \
  >> "$LOG_FILE" 2>&1
echo "✅ prepDataForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"



#######################

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting exclude_coinsets_cd.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/exclude_coinsets_cd.py" \
  --input "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/allDataKnotted_AN.csv" \
  --output "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/allIntervalDataKnotted_AN.csv" \
  --exclude C D \
>> "$LOG_FILE" 2>&1
echo "✅ exclude_coinsets_bcd.py completed at $(date)" | tee -a "$LOG_FILE"



#######################

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting exclude_coinsets_cd.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/exclude_coinsets_cd.py" \
  --input "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/allDataKnotted_PO.csv" \
  --output "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/allIntervalDataKnotted_PO.csv" \
  --exclude C D \
>> "$LOG_FILE" 2>&1
echo "✅ exclude_coinsets_bcd.py completed at $(date)" | tee -a "$LOG_FILE"




#######################

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting exclude_coinsets_cd.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/exclude_coinsets_cd.py" \
  --input "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/1st50DataKnotted_AN.csv" \
  --output "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/1st50IntervalDataKnotted_AN.csv" \
  --exclude C D \
>> "$LOG_FILE" 2>&1
echo "✅ exclude_coinsets_bcd.py completed at $(date)" | tee -a "$LOG_FILE"



#######################

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting exclude_coinsets_cd.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/exclude_coinsets_cd.py" \
  --input "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/1st50DataKnotted_PO.csv" \
  --output "${MEGA_DIR}_final/mad_${ROUND_DUR_FILTER}/1st50IntervalDataKnotted_PO.csv" \
  --exclude C D \
>> "$LOG_FILE" 2>&1
echo "✅ exclude_coinsets_bcd.py completed at $(date)" | tee -a "$LOG_FILE"

