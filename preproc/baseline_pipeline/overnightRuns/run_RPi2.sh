#!/usr/bin/env bash
set -Eeuo pipefail

on_exit() {
    exit_code=$?

    if [ "$exit_code" -eq 0 ]; then
        afplay /System/Library/Sounds/Blow.aiff
    else
        afplay /System/Library/Sounds/Sosumi.aiff
    fi
}

trap on_exit EXIT

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
TIMECOL="mLT_orig"

####################################
# RPi Mark PreProc
####################################

# echo "🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸" | tee -a "$LOG_FILE"
# echo "🚀 Starting batch_read_rpi_logs_to_csv.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/baseRPiHandling/batch_read_rpi_logs_to_csv.py" \
#   --collated_xlsx "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --sheet_name "MagicLeapFiles" \
#   --raw_data_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RawData" \
#   --rpi_preproc_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}" \
#   --read_script "${CODE_DIR}/alignment/baseRPiHandling/read_rpi_logs_to_csv.py" \
#   --output_mode flat \
#   >> "$LOG_FILE" 2>&1
# echo "✅ batch_read_rpi_logs_to_csv.py completed at $(date)" | tee -a "$LOG_FILE" 

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting rpi_preproc_pipeline3.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/rpi_preproc/rpi_preproc_pipeline3.py \
#   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --device-ip-map "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/configs/DeviceIPAddresses.txt" \
#   --code-dir "${CODE_DIR}/alignment/rpi_preproc" \
#   --base-dir "${TRUE_BASE_DIR}" \
#   --proc-dir "${PROC_DIR}" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}"  \
#   --events-dir-name "EventSegmentation/EarliestRoundStart" \
#   --sheet "MagicLeapFiles" \
#   --only-rows-with-rpi \
#   --stage-report-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/rpi_preproc_stage_report.csv" \
#   --marks-timestamp-col "RPi_Time_verb" \
#   --strip-ml-suffixes "_earliestRoundStart,_processed" \
#   --dedupe-sec 0.05 \
#   --timezone-offset auto \
#   --timeCol "${TIMECOL}" \
# >> "$LOG_FILE" 2>&1
# echo "✅ rpi_preproc_pipeline.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡" | tee -a "$LOG_FILE"
# echo "🚀 Starting mark_matching_suite at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/markMatchApps/augment_mark_provenance.py" \
#   --mark-matches "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/markMatches_BioPac/ObsReward_A_02_08_2025_13_33_BioPac_mark_matches.csv" \
#   --mark-singles "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/markMatches_BioPac/ObsReward_A_02_08_2025_13_33_BioPac_mark_singles.csv" \
#   --rpi-unified "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/BioPac/RPi_unified/ObsReward_A_02_08_2025_13_33_BioPac_RPi_unified.csv" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/markMatches_BioPac_Redo" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mark_matching_suite.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡" | tee -a "$LOG_FILE"
# echo "🚀 Starting mark_matching_suite at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/markMatchingSuite/mark_matching_suite.py" \
#   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --device-ip-map "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/configs/DeviceIPAddresses.txt" \
#   --code-dir "${CODE_DIR}/alignment/markMatchingSuite" \
#   --base-dir "${TRUE_BASE_DIR}" \
#   --proc-dir "${PROC_DIR}" \
#   --rpi-preproc-dir "RPi_preproc_${TIMECOL}" \
#   --events-dir-name "EventSegmentation/EarliestRoundStart" \
#   --out-dir "AlignedSplit_${TIMECOL}" \
#   --rpi_time_type "RPi_Time_simple" \
#   --csv-timestamp-column "${TIMECOL}"  \
#   --event-type-column "lo_eventType" \
#   --sheet "MagicLeapFiles" \
#   --strip_ml_suffixes "_earliestRoundStart,_processed" \
#   --only-rows-with-rpi \
#   --initial_match_gap_s 1.0 \
#   --final_match_gap_s 0.35 \
#   --coarse_search_window_s 30.0 \
#   --burst_gap_s 30.0 \
#   --manual_filter_time_tolerance_s 0.005 \
#   --sigma_clip 4.0 \
# >> "$LOG_FILE" 2>&1
# echo "✅ mark_matching_suite.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡" | tee -a "$LOG_FILE"
# echo "🚀 Starting affine_fitting_suite.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/affineFittingSuite/affine_fitting_suite.py" \
#   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --sheet "MagicLeapFiles" \
#   --device-ip-map "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/configs/DeviceIPAddresses.txt" \
#   --code-dir "${CODE_DIR}/alignment/affineFittingSuite" \
#   --base-dir "${TRUE_BASE_DIR}" \
#   --proc-dir "${PROC_DIR}" \
#   --events-dir-name "EventSegmentation/EarliestRoundStart" \
#   --rpi-preproc-dir "RPi_preproc_${TIMECOL}" \
#   --out-dir "AlignedSplit_${TIMECOL}" \
#   --csv-timestamp-column "${TIMECOL}"  \
#   --event-type-column "lo_eventType" \
#   --blankRowTemplate "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/alignHelpers/NewRowInfo.csv" \
#   --strip_ml_suffixes "_earliestRoundStart,_processed" \
#   --only-rows-with-rpi \
#   --sigma_clip 4.0 \
# >> "$LOG_FILE" 2>&1
# echo "✅ affine_fitting_suite.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻" | tee -a "$LOG_FILE"
# echo "🚀 Starting alignment_qc_suite.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/alignmentQCSuite/alignment_qc_suite.py" \
#   --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignedSplit_${TIMECOL}/affineFitData/auto" \
#   --code-dir "${CODE_DIR}/alignment/alignmentQCSuite" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignmentQC_${TIMECOL}/affineFitQC/auto" \
#   --max-rmse-ms 50 \
#   --max-mad-ms 30 \
#   --min-inlier-fraction 0.80 \
#   --min-drift-span-s 60 \
#   --no-use-accumulated-drift \
#   --high-burst-range-ms 100 \
#   --high-burst-shift-ms 50 \
# >> "$LOG_FILE" 2>&1
# echo "✅ alignment_qc_suite.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻" | tee -a "$LOG_FILE"
# echo "🚀 Starting alignment_qc_suite.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/alignmentQCSuite/alignment_qc_suite.py" \
#   --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignedSplit_${TIMECOL}/affineFitData/hybrid" \
#   --code-dir "${CODE_DIR}/alignment/alignmentQCSuite" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignmentQC_${TIMECOL}/affineFitQC/hybrid" \
#   --max-rmse-ms 50 \
#   --max-mad-ms 30 \
#   --min-inlier-fraction 0.80 \
#   --min-drift-span-s 60 \
#   --no-use-accumulated-drift \
#   --high-burst-range-ms 100 \
#   --high-burst-shift-ms 50 \
# >> "$LOG_FILE" 2>&1
# echo "✅ alignment_qc_suite.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻🌻" | tee -a "$LOG_FILE"
# echo "🚀 Starting alignment_qc_suite.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/alignmentQCSuite/alignment_qc_suite.py" \
#   --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignedSplit_${TIMECOL}/affineFitData/manual" \
#   --code-dir "${CODE_DIR}/alignment/alignmentQCSuite" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignmentQC_${TIMECOL}/affineFitQC/manual" \
#   --max-rmse-ms 50 \
#   --max-mad-ms 30 \
#   --min-inlier-fraction 0.80 \
#   --min-drift-span-s 60 \
#   --no-use-accumulated-drift \
#   --high-burst-range-ms 100 \
#   --high-burst-shift-ms 50 \
# >> "$LOG_FILE" 2>&1
# echo "✅ alignment_qc_suite.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥" | tee -a "$LOG_FILE"
# echo "🚀 Starting characterize_fit_robustness.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/alignmentQCSuite/characterize_fit_robustness.py" \
#     --root "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignedSplit_${TIMECOL}/affineFitData/auto" \
#     --manifest-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignmentQC_${TIMECOL}/affineFitQC/auto/Categories/global_fail_low_local_movement.csv" \
#     --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignmentQC_${TIMECOL}/affineFitQC/auto/FitRobustnessQC" \
#     --recursive \
# >> "$LOG_FILE" 2>&1
# echo "✅ characterize_fit_robustness.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥" | tee -a "$LOG_FILE"
# echo "🚀 Starting characterize_fit_robustness.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/alignmentQCSuite/characterize_fit_robustness.py" \
#     --root "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignedSplit_${TIMECOL}/affineFitData/hybrid" \
#     --manifest-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignmentQC_${TIMECOL}/affineFitQC/hybrid/Categories/global_fail_low_local_movement.csv" \
#     --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignmentQC_${TIMECOL}/affineFitQC/hybrid/FitRobustnessQC" \
#     --recursive \
# >> "$LOG_FILE" 2>&1
# echo "✅ characterize_fit_robustness.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥" | tee -a "$LOG_FILE"
# echo "🚀 Starting characterize_fit_robustness.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/alignmentQCSuite/characterize_fit_robustness.py" \
#     --root "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignedSplit_${TIMECOL}/affineFitData/manual" \
#     --manifest-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignmentQC_${TIMECOL}/affineFitQC/manual/Categories/global_fail_low_local_movement.csv" \
#     --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/AlignmentQC_${TIMECOL}/affineFitQC/manual/FitRobustnessQC" \
#     --recursive \
# >> "$LOG_FILE" 2>&1
# echo "✅ characterize_fit_robustness.py completed at $(date)" | tee -a "$LOG_FILE"

# # #########################
# # #    Mark Matching      #
# # #########################

# echo "🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄" | tee -a "$LOG_FILE"
# echo "🚀 Starting mark_match_app.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/markMatchApps/mark_match_app.py" \
#   --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart" \
#   --rpi-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/RNS/RPi_unified/" \
#   --rpi-time-type "" \
#   --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/markMatches_RNS" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mark_match_app.py completed at $(date)" | tee -a "$LOG_FILE"

echo "🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄" | tee -a "$LOG_FILE"
echo "🚀 Starting mark_match_app.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/markMatchApps/mark_match_app.py" \
  --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart" \
  --rpi-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/BioPac/RPi_unified/" \
  --rpi-time-type "" \
  --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/markMatches_BioPac" \
  >> "$LOG_FILE" 2>&1
echo "✅ mark_match_app.py completed at $(date)" | tee -a "$LOG_FILE"

# ######################### 
# #   Mark Pair Review    #
# #########################

# echo "🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔" | tee -a "$LOG_FILE"
# echo "🚀 Starting mark_pair_review_app.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/markCluster_apps/mark_pair_review_app.py" \
#   --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart" \
#   --rpi-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/RNS/RPi_unified/" \
#   --match-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/markMatches_RNS" \
#   --review-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/pairReviews_RNS" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mark_pair_review_app.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔🦔" | tee -a "$LOG_FILE"
# echo "🚀 Starting mark_pair_review_app.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/markCluster_apps/mark_pair_review_app.py" \
#   --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart" \
#   --rpi-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/BioPac/RPi_unified/" \
#   --match-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/markMatches_BioPac" \
#   --review-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/pairReviews_BioPac" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mark_pair_review_app.py completed at $(date)" | tee -a "$LOG_FILE"

# ######################### 
# #     Mark Review       #
# ######################### 

# echo "🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈" | tee -a "$LOG_FILE"
# echo "🚀 Starting mark_review_app.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/markCluster_apps/mark_review_app.py" \
#   --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Flattened" \
#   --rpi-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/BioPac/RPi_unified/" \
#   --review-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/reviewDecisions_BioPac" \
#   --skip-reviewed-pairs \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mark_review_app.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈🦈" | tee -a "$LOG_FILE"
# echo "🚀 Starting mark_review_app.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/markCluster_apps/mark_review_app.py" \
#   --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Flattened" \
#   --rpi-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/RNS/RPi_unified/" \
#   --review-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/reviewDecisions_RNS" \
#   --skip-reviewed-pairs \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mark_review_app.py completed at $(date)" | tee -a "$LOG_FILE"


# ######################### 
# #   Mark Chunk Review   #
# ######################### 

# echo "🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍🫍" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/mark_chunk_review_tool.py" propose \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart/ObsReward_B_01_25_2025_17_07_earliestRoundStart.csv" \
#   --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_B_01_25_2025_17_07" \
#   --time-col "${TIMECOL}" \
#   --event-col lo_eventType \
#   --block-col BlockNum \
#   --mark-gap-seconds 30 \
#   --context-window-seconds 300 \
#   --boundary-pair-max-seconds 300 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mark_chunk_review_tool.py propose completed at $(date)" | tee -a "$LOG_FILE"


# ######################### 
# #    Mark Review Tk     #
# ######################### 

# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
# echo "🚀 Starting Streamlit review app at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/mark_review_tk.py"\
#   --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_B_01_25_2025_17_07/mark_clusters.csv" \
#   --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_B_01_25_2025_17_07/mark_cluster_context.csv" \
#   --source-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart/ObsReward_B_01_25_2025_17_07_earliestRoundStart.csv" \
#   --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_B_01_25_2025_17_07/review_edits.csv" \
#   --time-col "${TIMECOL}" \
#   --event-col lo_eventType \
#   --block-col BlockNum \
#   --context-seconds 300 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ Streamlit review app closed at $(date)" | tee -a "$LOG_FILE"


# ######################################
# #   Mark Chunk Review Apply Edits    #
# ######################################

# echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
# echo "🚀 Starting mark_chunk_review_tool.py apply-edits at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/mark_chunk_review_tool.py" apply-edits \
#   --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_B_01_25_2025_17_07/mark_clusters.csv" \
#   --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_B_01_25_2025_17_07/review_edits.csv" \
#   --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_B_01_25_2025_17_07/mark_cluster_context.csv" \
#   --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview/ObsReward_B_01_25_2025_17_07/Finalized" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mark_chunk_review_tool.py apply-edits completed at $(date)" | tee -a "$LOG_FILE"


########################################################################################################################################################################## 
################################## 
# Old Stuff 
##################################

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting export_filtered_copies.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/export_filtered_copies.py" \
#   --review-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/reviewDecisions_BioPac" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/filteredMarks_BioPac" \
#   --overwrite \
# >> "$LOG_FILE" 2>&1
# echo "✅ export_filtered_copies.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting export_filtered_copies.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/export_filtered_copies.py" \
#   --review-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/reviewDecisions_RNS" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/filteredMarks_RNS" \
#   --overwrite \
# >> "$LOG_FILE" 2>&1
# echo "✅ export_filtered_copies.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting rerun_filtered_alignment_batch.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/rerun_filtered_alignment_batch.py" \
#   --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/filteredMarks_BioPac/BioPac" \
#   --rpi-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/filteredMarks_BioPac/BioPac" \
#   --code-dir "${CODE_DIR}/alignment" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/filtered_aligned_BioPac" \
#   --blank-row-template "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/alignHelpers/NewRowInfo.csv" \
#   --csv-timestamp-column "${TIMECOL}" \
#   --event-type-column "lo_eventType" \
#   --event-type-values "Mark" \
#   --timezone-offset "auto" \
#   --strip-ml-suffixes "_earliestRoundStart,_processed" \
#   --max-match-gap-s "1.0" \
#   --only-label "BioPac" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ rerun_filtered_alignment_batch.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting rerun_filtered_alignment_batch.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/rerun_filtered_alignment_batch.py" \
#   --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/filteredMarks_RNS/RNS" \
#   --rpi-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/filteredMarks_RNS/RNS" \
#   --code-dir "${CODE_DIR}/alignment" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/filtered_aligned_RNS" \
#   --blank-row-template "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/alignHelpers/NewRowInfo.csv" \
#   --csv-timestamp-column "${TIMECOL}" \
#   --event-type-column "lo_eventType" \
#   --event-type-values "Mark" \
#   --timezone-offset "auto" \
#   --strip-ml-suffixes "_earliestRoundStart,_processed" \
#   --max-match-gap-s "1.0" \
#   --only-label "RNS" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ rerun_filtered_alignment_batch.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting summarize_drift_qc_batch.py BioPac at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/summarize_drift_qc_batch.py" \
#   --in-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filtered_aligned_BioPac" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filtered_aligned_BioPac/QC" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ summarize_drift_qc_batch.py BioPac completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting summarize_drift_qc_batch.py RNS at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/summarize_drift_qc_batch.py" \
#   --in-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filtered_aligned_RNS" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filtered_aligned_RNS/QC" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ summarize_drift_qc_batch.py RNS completed at $(date)" | tee -a "$LOG_FILE"







# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting marks_timeline_lfp.py for R019at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/marks_timeline_lfp.py \
#   --lfp "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R019_LFP_merged.csv" \
#   --rpi "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R019_RPi_merged.csv" \
#   --out "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R019_LFP2RPi.png" \
#   --dpi 150 \
#   --show \
#   >> "$LOG_FILE" 2>&1
# echo "✅ marks_timeline_lfp.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting marks_timeline_lfp.py for R037 at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/marks_timeline_lfp.py \
#   --lfp "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R037_LFP_merged.csv" \
#   --rpi "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R037_RPi_merged.csv" \
#   --out "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R037_LFP2RPi.png" \
#   --dpi 150 \
#   --show \
#   >> "$LOG_FILE" 2>&1
# echo "✅ marks_timeline_lfp.py for R037 completed at $(date)" | tee -a "$LOG_FILE"



# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting marks_elapsed_compare.py for R019 at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/marks_elapsed_compare.py \
#   --lfp "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R019_LFP_merged.csv" \
#   --lfp-col time_abs \
#   --rpi "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R019_RPi_merged.csv" \
#   --rpi-col RPi_Time_verb \
#   --out "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R019_LFP2RPi_elapsed.png" \
#   --export "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/elapsed_values.csv" \
#   --plot-iei \
#   --show \
#   >> "$LOG_FILE" 2>&1
# echo "✅ marks_timeline_lfp.py for R019 completed at $(date)" | tee -a "$LOG_FILE"


# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting marks_elapsed_compare.py for R037 at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/marks_elapsed_compare.py \
#   --lfp "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R037_LFP_merged.csv" \
#   --lfp-col time_abs \
#   --rpi "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R037_RPi_merged.csv" \
#   --rpi-col RPi_Time_verb \
#   --out "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R037_LFP2RPi_elapsed.png" \
#   --export "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/elapsed_values.csv" \
#   --plot-iei \
#   --show \
#   >> "$LOG_FILE" 2>&1
# echo "✅ marks_timeline_lfp.py for R037 completed at $(date)" | tee -a "$LOG_FILE"


# #  # ####################################
# # # # Magic Leap Alignment to RPi Marks 
# # # ####################################
# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting batch_split_pipeline.py at $(date)" | tee -a "$LOG_FILE"

# python ${CODE_DIR}/alignment/batch_split_pipeline3.py \
#   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --device-ip-map "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/configs/DeviceIPAddresses.txt" \
#   --code-dir "${CODE_DIR}/alignment" \
#   --base-dir "${TRUE_BASE_DIR}" \
#   --proc-dir "${PROC_DIR}" \
#   --events-dir-name "${EVENTS_DIR}" \
#   --csv-timestamp-column "${TIMECOL}" \
#   --event-type-column lo_eventType \
#   --timezone-offset auto \
#   --sheet MagicLeapFiles \
#   --out-dir ML_RPi_Aligned \
#   --strip-ml-suffixes "_events_final,_processed,_events_final.csv,_processed.csv" \
#   --only-rows-with-rpi \
#   --blankRowTemplate "${CODE_DIR}/alignment/NewRowInfo.csv" \
#   --debug \
#   >> "$LOG_FILE" 2>&1
# echo "✅ batch_split_pipeline.py completed at $(date)" | tee -a "$LOG_FILE"

