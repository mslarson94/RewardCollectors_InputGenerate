#!/usr/bin/env bash
set -Eeuo pipefail

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/OddsAndEnds_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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

LAMBDA="1"
ROUND_DUR_FILTER="2.0"

MEGA_DIR="${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles_final"
MEGA_FILE="${MEGA_DIR}/mad_${ROUND_DUR_FILTER}/1st50IntervalDataKnotted_AN_noCD_all.csv"
MEGA_SUMMARY="${MEGA_DIR}/participantSummaryData_1st50_AN_noCD.csv"
OUTDIR="${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/mad_${ROUND_DUR_FILTER}/1st50Rounds/all/noCD"

# ## Getting Round Num reports 
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting report_roundnums_lt100.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/baseline_pipeline/overnightRuns/report_roundnums_lt100.py" \
#   --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Flat_csv" \
#   --pattern "*_processed_events.csv" \
#   --include-counts \
#   --out "/Users/mairahmac/Desktop/roundnums_lt100_report.csv" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ report_roundnums_lt100.py completed at $(date)" | tee -a "$LOG_FILE"


# ### Plotting All the Triangles Together with Centroids 
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting plot_triangles_from_list.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/generatingUnityInput/plot_triangles_from_list.py" \
#   --triangles-csv "${TRIANGLES_DIR}/triangle_positions-formatted__A_D_.csv" \
#   --output "${TRIANGLES_DIR}/MultiTrianglePlots/CentroidPlot.png" \
#   --xlim -5.5 5.5 \
#   --ylim -5.5 5.5 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ plot_triangles_from_list.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting plottingSelectedCombos.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/generatingUnityInput/plottingSelectedCombos.py" \
# >> "$LOG_FILE" 2>&1
# echo "✅ plottingSelectedCombos.py completed at $(date)" | tee -a "$LOG_FILE"

# # echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting report_criterion.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/baseline_pipeline/extraction/report_criterion.py" \
#   --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/intervalsFinal" \
#   --pattern "*main*AN*__withDemo.csv" \
#   --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/CriterionMaxReports" \
#   --master-out "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/criterionReporting.csv"
#   >> "$LOG_FILE" 2>&1
# echo "✅ report_criterion.py completed at $(date)" | tee -a "$LOG_FILE"


echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting plot_round_duration_flags.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/plotting/plot_round_duration_flags.py" \
  "${MEGA_FILE}" \
  --output-dir "${OUTDIR}/round_duration_results" \
  >> "$LOG_FILE" 2>&1
echo "✅ plot_round_duration_flags.py completed at $(date)" | tee -a "$LOG_FILE"

# ### Adding Learning Knots
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting addKnotsForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/extraction/addKnotsForDecisionModel.py" \
#   --in_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalData_AN.csv" \
#   --out_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_AN.csv" \
#   --out_pruned "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_Prune_AN.csv" \
#   --knots 15 20 25 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ addKnotsForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"

# ### Adding Learning Knots
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting addKnotsForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/extraction/addKnotsForDecisionModel.py" \
#   --in_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalData_PO.csv" \
#   --out_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_PO.csv" \
#   --out_pruned "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_Prune_PO.csv" \
#   --knots 15 20 25 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ addKnotsForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"

# ######### Patch
# ### Myra needs to patch the dropQual column earlier in the pipeline, this is a temporary patch 
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting patch_dropqual_corrected.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/patch_dropqual_corrected.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_AN.csv" \
#   --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_AN_patched.csv" \
# >> "$LOG_FILE" 2>&1
# echo "✅ patch_dropqual_corrected.py completed at $(date)" | tee -a "$LOG_FILE"

# ### Myra needs to patch the dropQual column earlier in the pipeline, this is a temporary patch 
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting patch_dropqual_corrected.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/patch_dropqual_corrected.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_PO.csv" \
#   --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_PO_patched.csv" \
# >> "$LOG_FILE" 2>&1
# echo "✅ patch_dropqual_corrected.py completed at $(date)" | tee -a "$LOG_FILE"


# ### EV event tagging 
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting annotate_within_round_swap_behavior.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/annotate_within_round_swap_behavior.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_AN_patched.csv" \
#   --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_AN_EVBeh.csv" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ annotate_within_round_swap_behavior.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting annotate_within_round_swap_behavior.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/annotate_within_round_swap_behavior.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_PO_patched.csv" \
#   --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_PO_EVBeh.csv" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ annotate_within_round_swap_behavior.py completed at $(date)" | tee -a "$LOG_FILE"

# ##########

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting generate_base_qc_flags.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/generate_base_qc_flags.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_AN_EVBeh.csv" \
#   --outdir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ generate_base_qc_flags.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting generate_round_dur_optionA.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/generate_round_dur_optionA.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_AN_EVBeh_baseQC.csv" \
#   --outdir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/mad_2" \
#   --preferred-group rr_sess \
#   --preferred-method mad \
#   --mad-threshold 3.0 \
#   --min-group-n 8 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ generate_round_dur_optionA.py completed at $(date)" | tee -a "$LOG_FILE"


# ########## All Rounds ###########
# ### Making my decision expansion file
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting prepDataForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/extraction/prepDataForDecisionModel.py" \
#   --interval_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_AN_EVBeh_baseQC_roundDur_optionA.csv" \
#   --utility_csv "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_All/pathUtility_lambda${LAMBDA}.csv" \
#   --out_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/decisionExpanded_AN.csv" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ prepDataForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"


# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting prepDataForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/extraction/prepDataForDecisionModel.py" \
#   --interval_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_Prune_PO.csv" \
#   --utility_csv "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_All/pathUtility_lambda${LAMBDA}.csv" \
#   --out_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/decisionExpanded_PO.csv" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ prepDataForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"
# ####################

# ######### First 50 rounds only ###########

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting cappingTotalRounds.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/extraction/cappingTotalRounds.py" \
#     --inFile "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_AN_EVBeh.csv" \
#     --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_AN_EVBeh_1st50Rds.csv" \
#     --summary "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/session_round_counts.csv" \
#     >> "$LOG_FILE" 2>&1
# echo "✅ cappingTotalRounds.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting cappingTotalRounds.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/extraction/cappingTotalRounds.py" \
#     --inFile "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_Prune_PO.csv" \
#     --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_Prune_PO_1st50Rds.csv" \
#     --summary "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/session_round_counts.csv" \
#     >> "$LOG_FILE" 2>&1
# echo "✅ cappingTotalRounds.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting generate_base_qc_flags.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/generate_base_qc_flags.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_AN_EVBeh_1st50Rds.csv" \
#   --outdir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ generate_base_qc_flags.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting generate_round_dur_optionA.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/generate_round_dur_optionA.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_AN_EVBeh_1st50Rds_baseQC.csv" \
#   --outdir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/mad_2" \
#   --preferred-group rr_sess \
#   --preferred-method mad \
#   --mad-threshold 2.0 \
#   --min-group-n 8 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ generate_round_dur_optionA.py completed at $(date)" | tee -a "$LOG_FILE"



# ### Making my decision expansion file
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting prepDataForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/extraction/prepDataForDecisionModel.py" \
#   --interval_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/mad_2/allIntervalDataKnotted_AN_EVBeh_1st50Rds_baseQC_roundDur_optionA.csv" \
#   --utility_csv "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_All/pathUtility_lambda${LAMBDA}.csv" \
#   --out_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/mad_2/decisionExpanded_AN.csv" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ prepDataForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"

# ### Making my decision expansion file
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting prepDataForDecisionModel.py Lambda ${LAMBDA} at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/extraction/prepDataForDecisionModel.py" \
#   --interval_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_Prune_PO_1st50Rds.csv" \
#   --utility_csv "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_All/pathUtility_lambda${LAMBDA}.csv" \
#   --out_csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/mad_2/decisionExpanded_PO.csv" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ prepDataForDecisionModel.py Lambda ${LAMBDA} completed at $(date)" | tee -a "$LOG_FILE"
# #######################

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting exclude_coinsets_cd.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/baseline_pipeline/eventAugmentation/exclude_coinsets_cd.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/mad_2/allIntervalDataKnotted_AN_EVBeh_1st50Rds_baseQC_roundDur_optionA.csv" \
#   --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/mad_2/AN_PinDropsKnottedFiltered_1st50Rds.csv" \
#   --exclude C D \
# >> "$LOG_FILE" 2>&1
# echo "✅ exclude_coinsets_bcd.py completed at $(date)" | tee -a "$LOG_FILE"


# ### Plotting Ideal Distances Stuff 
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting plot_idealDistByCoinLayout.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/plotting/plot_idealDistByCoinLayout.py" \
#   --input_glob "${TRIANGLES_DIR}/RoutePlanWeightUtility/idealRoutes/ideal_routes_*.csv" \
#   --out_dir "${TRIANGLES_DIR}/RoutePlanWeightUtility/idealRoutes_Plots" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ plot_idealDistByCoinLayout.py completed at $(date)" | tee -a "$LOG_FILE"



# # #############################################################################################################################################################
# # Generating Ideal Distances, Path Utility, & Path Efficiency Stuff 
# #############################################################################################################################################################

# python "${CODE_DIR}/overnightRuns/calcIdealDistances.py"

# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/walkDataAnalysis/theoPaths_Classifiers/greedy_v2.py"

# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extraction/normalize_path_utility.py" \
#   --root "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda5" \
#   --pattern "all_orders__layout_*_L5.csv" \
#   --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L5" \
#   --require-order-col \
#   --write-summary \
#   --overwrite \
#   >> "$LOG_FILE" 2>&1

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