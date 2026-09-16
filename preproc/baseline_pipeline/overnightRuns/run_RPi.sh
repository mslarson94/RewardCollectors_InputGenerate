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
#PROC_DIR="FreshStart_redoAgainSingle"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Pos"

# # ####################################
# # # RPi Mark PreProc
# # ####################################

# echo "🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸" | tee -a "$LOG_FILE"
# echo "🚀 Starting read_rpi_logs_to_csv.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/read_rpi_logs_to_csv.py \
#   --log_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RawData/pair_200/03_17_2025/Morning/RPi/BioPac_RPi" \
#   --out_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/BioPac/RPi_simple_raw" \
#  >> "$LOG_FILE" 2>&1
# echo "✅ read_rpi_logs_to_csv.py completed at $(date)" | tee -a "$LOG_FILE" 


# echo "🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸" | tee -a "$LOG_FILE"
# echo "🚀 Starting batch_read_rpi_logs_to_csv.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/batch_read_rpi_logs_to_csv.py" \
#   --collated_xlsx "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --sheet_name "MagicLeapFiles" \
#   --raw_data_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RawData" \
#   --rpi_preproc_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc" \
#   --read_script "${CODE_DIR}/alignment/read_rpi_logs_to_csv.py" \
#   --output_mode flat \
#   >> "$LOG_FILE" 2>&1
# echo "✅ batch_read_rpi_logs_to_csv.py completed at $(date)" | tee -a "$LOG_FILE" 

# # echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# # echo "🚀 Starting rpi_preproc_pipeline3.py at $(date)" | tee -a "$LOG_FILE"

# # python ${CODE_DIR}/alignment/rpi_preproc_pipeline3.py \
# #   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
# #   --device-ip-map "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/configs/DeviceIPAddresses.txt" \
# #   --code-dir "${CODE_DIR}/alignment" \
# #   --base-dir "${TRUE_BASE_DIR}" \
# #   --proc-dir "${PROC_DIR}" \
# #   --timezone-offset auto \
# #   --sheet MagicLeapFiles \
# #   --events-dir-name "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Flattened" \
# #   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/"  \
# #   --marks-timestamp-col "RPi_Time_verb" \
# #   --strip-ml-suffixes "_eventsFlat,_processed" \
# #   --only-rows-with-rpi \
# #   --dedupe-sec 0.05 \
# #   --debug \
# #   >> "$LOG_FILE" 2>&1
# # echo "✅ rpi_preproc_pipeline.py completed at $(date)" | tee -a "$LOG_FILE"
# #ACTUAL_PROC="${TRUE_BASE_DIR}/${PROC_DIR}/full"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting rpi_preproc_pipeline3.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/rpi_preproc_pipeline3.py \
#   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --device-ip-map "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/configs/DeviceIPAddresses.txt" \
#   --code-dir "${CODE_DIR}/alignment" \
#   --base-dir "${TRUE_BASE_DIR}" \
#   --proc-dir "${PROC_DIR}" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/"  \
#   --events-dir-name "EventSegmentation/EarliestRoundStart" \
#   --sheet "MagicLeapFiles" \
#   --only-rows-with-rpi \
#   --stage-report-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/rpi_preproc_stage_report.csv" \
#   --marks-timestamp-col "RPi_Time_verb" \
#   --strip-ml-suffixes "_earliestRoundStart,_processed" \
#   --dedupe-sec 0.05 \
#   --timezone-offset auto \
# >> "$LOG_FILE" 2>&1
# echo "✅ rpi_preproc_pipeline.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸" | tee -a "$LOG_FILE"
# echo "🚀 Starting batch_split_pipeline3.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/batch_split_pipeline3.py \
#   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --device-ip-map "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/configs/DeviceIPAddresses.txt" \
#   --code-dir "${CODE_DIR}/alignment" \
#   --base-dir "${TRUE_BASE_DIR}" \
#   --proc-dir "${PROC_DIR}" \
#   --events-dir-name "EventSegmentation/Events_Flattened" \
#   --csv-timestamp-column "eMLT_orig" \
#   --event-type-column "lo_eventType" \
#   --timezone-offset "auto" \
#   --sheet "MagicLeapFiles" \
#   --out-dir "AlignedSplit" \
#   --strip-ml-suffixes "_eventsFlat,_processed" \
#   --only-rows-with-rpi \
#   --blankRowTemplate "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/alignHelpers/NewRowInfo.csv" \
#   --dedupesec "0.05" \
#   --maxmatchgaps "1.0" \
# >> "$LOG_FILE" 2>&1
# echo "✅ batch_split_pipeline3.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸" | tee -a "$LOG_FILE"
# echo "🚀 Starting multi_stream_drift.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/multi_stream_drift.py \
#         --mark-col markNum_aligned \
#         --lfp-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R037/R037_mergedLFP_trim.csv" \
#         --lfp-time-col time_abs \
#         --rpi-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R037/R037_mergedRPi_trim.csv" \
#         --rpi-time-col RPi_Time_verb \
#         --out-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R037/R037_LFP_RPi_drift.csv" \
#         --chunk-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R037/R037_mergedRPi_trim.csv" \
#         --chunk-col con_chunk_RPi \
#         --midi-chunks 1,2,3,4,5,6,7 \
#         --print-summary \
#    >> "$LOG_FILE" 2>&1

# echo "✅ multi_stream_drift.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄" | tee -a "$LOG_FILE"
# echo "🚀 Starting multi_stream_drift.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/multi_stream_drift.py \
#         --mark-col markNum_aligned \
#         --lfp-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R019/R019_mergedLFP_trim.csv"  \
#         --lfp-time-col time_abs \
#         --rpi-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R019/R019_mergedRPi_trim.csv" \
#         --rpi-time-col RPi_Time_verb \
#         --out-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R019/R019_LFP_RPi_drift.csv" \
#         --chunk-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R019/R019_mergedRPi_trim.csv" \
#         --chunk-col con_chunk_RPi \
#         --midi-chunks 1,2,3,4,5,6,7 \
#         --print-summary \
#    >> "$LOG_FILE" 2>&1

# echo "✅ multi_stream_drift.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸" | tee -a "$LOG_FILE"
# echo "🚀 Starting multi_stream_drift.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/multi_stream_drift.py \
#         --mark-col markNum_aligned \
#         --ml-csv  R037_ML.csv  --ml-time-col  mLTimestamp \
#         --lfp-csv R037_LFP.csv  --lfp-time-col time_abs \
#         --rpi-csv R037_mergedRPi_trim.csv --rpi-time-col RPi_Time_verb \
#         --out-csv R037_LFP_RPi_drift.csv
#    >> "$LOG_FILE" 2>&1

# echo "✅ multi_stream_drift.py completed at $(date)" | tee -a "$LOG_FILE"
#  # ####################################
# # # # Magic Leap Alignment to RPi Marks 
# # # ####################################
# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting marks_timeline_mlts.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/marks_timeline_mlts.py \
#     --events "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Flattened/ObsReward_B_03_17_2025_14_16_eventsFlat.csv" \
#     --rpi    "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/ObsReward_B_03_17_2025_14_16_RNS_RPi_unified.csv" \
#     --block All \
#     --out   "${TRUE_BASE_DIR}/${PROC_DIR}/markTimelines/R019_14_16_marks_timeline.png" \
#     --dpi   150 \
#     --show \
#     >> "$LOG_FILE" 2>&1
# echo "✅ marks_timeline_mlts.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting marks_timeline_mlts.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/marks_timeline_mlts.py \
#     --events "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Flattened/ObsReward_B_03_17_2025_14_16_eventsFlat.csv" \
#     --rpi    "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/ObsReward_B_03_17_2025_14_16_RNS_RPi_unified.csv" \
#     --block All \
#     --out   "${TRUE_BASE_DIR}/${PROC_DIR}/markTimelines/R019_14_16_marks_timeline.png" \
#     --dpi   150 \
#     --show \
#     >> "$LOG_FILE" 2>&1
# echo "✅ marks_timeline_mlts.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting marks_timeline_mlts.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/marks_timeline_mlts.py \
#     --events "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Flattened/ObsReward_A_01_25_2025_17_07_eventsFlat.csv" \
#     --rpi    "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/ObsReward_A_01_25_2025_17_07_RNS_RPi_unified.csv" \
#     --block All \
#     --out   "${TRUE_BASE_DIR}/${PROC_DIR}/markTimelines/R019_14_16_marks_timeline.png" \
#     --dpi   150 \
#     --show \
#     >> "$LOG_FILE" 2>&1
# echo "✅ marks_timeline_mlts.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting mark_review_app.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/mark_review_app.py" \
#   --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Flattened" \
#   --rpi-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/BioPac/RPi_unified/" \
#   --review-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/reviewDecisions_BioPac" \
#   --skip-reviewed-pairs \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mark_review_app.py completed at $(date)" | tee -a "$LOG_FILE"

echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting mark_review_app.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/mark_review_app.py" \
  --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Flattened" \
  --rpi-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/" \
  --review-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/reviewDecisions_RNS" \
  --skip-reviewed-pairs \
  >> "$LOG_FILE" 2>&1
echo "✅ mark_review_app.py completed at $(date)" | tee -a "$LOG_FILE"

# # echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# # echo "🚀 Starting export_filtered_copies.py at $(date)" | tee -a "$LOG_FILE"
# # python "${CODE_DIR}/alignment/export_filtered_copies.py" \
# #   --review-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/reviewDecisions_BioPac" \
# #   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filteredMarks_BioPac" \
# #   --overwrite \
# # >> "$LOG_FILE" 2>&1
# # echo "✅ export_filtered_copies.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting export_filtered_copies.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/export_filtered_copies.py" \
#   --review-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/reviewDecisions_RNS3" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filteredMarks_RNS3" \
#   --overwrite \
# >> "$LOG_FILE" 2>&1
# echo "✅ export_filtered_copies.py completed at $(date)" | tee -a "$LOG_FILE"

# # echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# # echo "🚀 Starting rerun_filtered_alignment_batch.py at $(date)" | tee -a "$LOG_FILE"
# # python "${CODE_DIR}/alignment/rerun_filtered_alignment_batch.py" \
# #   --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filteredMarks_BioPac/BioPac" \
# #   --rpi-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filteredMarks_BioPac/BioPac" \
# #   --code-dir "${CODE_DIR}/alignment" \
# #   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filtered_aligned_BioPac" \
# #   --blank-row-template "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/alignHelpers/NewRowInfo.csv" \
# #   --csv-timestamp-column "eMLT_orig" \
# #   --event-type-column "lo_eventType" \
# #   --event-type-values "Mark" \
# #   --timezone-offset "auto" \
# #   --strip-ml-suffixes "_eventsFlat_filtered,_processed" \
# #   --max-match-gap-s "1.0" \
# #   --only-label "BioPac" \
# #   >> "$LOG_FILE" 2>&1
# # echo "✅ rerun_filtered_alignment_batch.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting rerun_filtered_alignment_batch.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/rerun_filtered_alignment_batch.py" \
#   --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filteredMarks_RNS3/RNS" \
#   --rpi-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filteredMarks_RNS3/RNS" \
#   --code-dir "${CODE_DIR}/alignment" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filtered_aligned_RNS3" \
#   --blank-row-template "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/alignHelpers/NewRowInfo.csv" \
#   --csv-timestamp-column "eMLT_orig" \
#   --event-type-column "lo_eventType" \
#   --event-type-values "Mark" \
#   --timezone-offset "auto" \
#   --strip-ml-suffixes "_eventsFlat_filtered,_processed" \
#   --max-match-gap-s "1.0" \
#   --only-label "RNS" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ rerun_filtered_alignment_batch.py completed at $(date)" | tee -a "$LOG_FILE"


# # echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# # echo "🚀 Starting summarize_drift_qc_batch.py BioPac at $(date)" | tee -a "$LOG_FILE"
# # python "${CODE_DIR}/alignment/summarize_drift_qc_batch.py" \
# #   --in-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filtered_aligned_BioPac" \
# #   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filtered_aligned_BioPac/QC" \
# #   >> "$LOG_FILE" 2>&1
# # echo "✅ summarize_drift_qc_batch.py BioPac completed at $(date)" | tee -a "$LOG_FILE"

# # echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# # echo "🚀 Starting summarize_drift_qc_batch.py RNS at $(date)" | tee -a "$LOG_FILE"
# # python "${CODE_DIR}/alignment/summarize_drift_qc_batch.py" \
# #   --in-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filtered_aligned_RNS3" \
# #   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/filtered_aligned_RNS3/QC" \
# #   >> "$LOG_FILE" 2>&1
# # echo "✅ summarize_drift_qc_batch.py RNS completed at $(date)" | tee -a "$LOG_FILE"



# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting mark_chunk_prototype.py RNS at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/mark_chunk_prototype.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MostlyFilledEvents/ObsReward_A_02_09_2025_10_41_filled_intervalProps.csv" \
#   --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview" \
#   --time-col eMLT_orig \
#   --event-col lo_eventType \
#   --block-col BlockNum \
#   --mark-gap-seconds 60 \
#   --context-window-seconds 300 \
#   --boundary-pair-max-seconds 300 \
# >> "$LOG_FILE" 2>&1
# echo "✅ mark_chunk_prototype.py RNS completed at $(date)" | tee -a "$LOG_FILE"

######################
# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/mark_chunk_review_tool.py" propose \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart/ObsReward_A_03_17_2025_14_16_earliestRoundStart.csv" \
#   --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview_R019_14_16" \
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
#   --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview_R019_14_16/mark_clusters.csv" \
#   --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview_R019_14_16/mark_cluster_context.csv" \
#   --source-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart/ObsReward_A_03_17_2025_14_16_earliestRoundStart.csv" \
#   --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview_R019_14_16/review_edits.csv" \
#   --time-col eMLT_orig \
#   --event-col lo_eventType \
#   --block-col BlockNum \
#   --context-seconds 300 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ Streamlit review app closed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting mark_chunk_review_tool.py apply-edits at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/mark_chunk_review_tool.py" apply-edits \
#   --cluster-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview_R019_14_16/mark_clusters.csv" \
#   --review-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview_R019_14_16/review_edits.csv" \
#   --context-csv "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview_R019_14_16/mark_cluster_context.csv" \
#   --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview_R019_14_16/Finalized" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mark_chunk_review_tool.py apply-edits completed at $(date)" | tee -a "$LOG_FILE"



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
#   --csv-timestamp-column mLTimestamp \
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

