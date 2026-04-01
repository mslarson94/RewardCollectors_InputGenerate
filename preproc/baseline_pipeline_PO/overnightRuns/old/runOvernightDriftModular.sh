
# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/drift_processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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
CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
PROC_DIR="FreshStart"
EVENTS_DIR="Events_Final_NoWalks"

ML_FILE="ObsReward_A_03_17_2025_14_16_events_final.csv"
RPI_FILE="2025-02-17_15_08_32_071285381.log"


DEVICE_IP="192.168.50.109"
TESTDATE_FORMATTED="02_17_2025"
DEVICE="ML2A"
SESSION="Morning"
PAIR="pair_008"
SESSION_DATE="2025-02-17"
TIMEZONE_OFFSET=8


# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting compile_log_time_offsets.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}baseline_pipeline/compile_log_time_offsets.py" \
#   --root "${TRUE_BASE_DIR}/${PROC_DIR}" \
#   --xlsx "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --sheet MagicLeapFiles \
#   --out "${TRUE_BASE_DIR}/timezoneOffsets.csv" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ compile_log_time_offsets.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting batch_split_pipeline.py at $(date)" | tee -a "$LOG_FILE"

# python ${CODE_DIR}baseline_pipeline/alignment/extract_rpi_marks2.py \
#   --log_file "${TRUE_BASE_DIR}/${PROC_DIR}/RawData/pair_200/03_17_2025/Afternoon/RPi/RNS_RPi/2025-03-17_14_13_24_417382511.log" \
#   --log_file "${TRUE_BASE_DIR}/${PROC_DIR}/RawData/pair_200/03_17_2025/Afternoon/RPi/RNS_RPi/2025-03-17_14_41_04_514886047.log" \
#   --log_file "${TRUE_BASE_DIR}/${PROC_DIR}/RawData/pair_200/03_17_2025/Afternoon/RPi/RNS_RPi/2025-03-17_14_45_44_099056293.log" \
#   --session_date 2025-03-17 \
#   --device ML2G \
#   --device_ip 192.168.50.128 \
#   --label RNS \
#   --ml_csv_file "${TRUE_BASE_DIR}/${PROC_DIR}/full/${EVENTS_DIR}/augmented/ObsReward_A_03_17_2025_14_16_events_final.csv" \
#   --allow_day_rollover --dedupe-sec 0.05 \
#   --out_dir "${TRUE_BASE_DIR}/${PROC_DIR}/full/ML_RPi_Aligned" \
#   >> "$LOG_FILE" 2>&1
#  #/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/RawData/pair_200/03_17_2025/Afternoon/RPi/RNS_RPi
#   echo "✅ batch_split_pipeline.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting merge_ml_with_rpi_marks.py at $(date)" | tee -a "$LOG_FILE"

# python ${CODE_DIR}baseline_pipeline/alignment/merge_ml_with_rpi_marks.py \
#   --ml_csv_file "${TRUE_BASE_DIR}/${PROC_DIR}/full/${EVENTS_DIR}/augmented/${ML_FILE}" \
#   --rpi_marks_csv "${TRUE_BASE_DIR}/${PROC_DIR}/full/${EVENTS_DIR}/augmented/ObsReward_A_02_17_2025_15_11_ML2A_BioPac_RPiMarks.csv" \
#   --csv_timestamp_column mLTimestamp \
#   --event_type_column lo_eventType \
#   --event_type_values Mark \
#   --label BioPac \
#   --device ML2A \
#   --timezone_offset_hours auto \
#   --max_match_gap_s 1.0

# echo "✅ merge_ml_with_rpi_marks.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting summarize_drift.py at $(date)" | tee -a "$LOG_FILE"

# python ${CODE_DIR}baseline_pipeline/alignment/summarize_drift.py \
#   --merged_ml_csv "${TRUE_BASE_DIR}/${PROC_DIR}/full/${EVENTS_DIR}/augmented/ObsReward_A_02_17_2025_15_11_ML2A_BioPac_events.csv" \
#   --label BioPac

# echo "✅ summarize_drift.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting merge_ml_with_rpi_marks.py at $(date)" | tee -a "$LOG_FILE"

# python ${CODE_DIR}baseline_pipeline/alignment/merge_ml_with_rpi_marks2.py \
#   --ml_csv_file "${TRUE_BASE_DIR}/${PROC_DIR}/full/${EVENTS_DIR}/augmented/${ML_FILE}" \
#   --rpi_marks_csv "${TRUE_BASE_DIR}/${PROC_DIR}/full/ML_RPi_Aligned/ObsReward_A_03_17_2025_14_16_ML2G_RNS_RPiMarks.csv" \
#   --csv_timestamp_column mLTimestamp \
#   --event_type_column lo_eventType \
#   --event_type_values Mark \
#   --label RNS \
#   --device ML2G \
#   --timezone_offset_hours 7 \
#   --out_dir "${TRUE_BASE_DIR}/${PROC_DIR}/full/ML_RPi_Aligned" \

# echo "✅ merge_ml_with_rpi_marks.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting batch_split_pipeline.py at $(date)" | tee -a "$LOG_FILE"

# python ${CODE_DIR}baseline_pipeline/alignment/batch_split_pipeline2.py \
#   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --device-ip-map "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/configs/DeviceIPAddresses.txt" \
#   --code-dir "${CODE_DIR}baseline_pipeline/alignment" \
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
#   --debug \
#   >> "$LOG_FILE" 2>&1

# echo "✅ batch_split_pipeline.py completed at $(date)" | tee -a "$LOG_FILE"


python ${CODE_DIR}baseline_pipeline/alignment/mergeANPOevents.py \
    --a "${TRUE_BASE_DIR}/${PROC_DIR}/full/${EVENTS_DIR}/augmented/ObsReward_A_02_17_2025_15_11_events_final.csv" \
    --b "${TRUE_BASE_DIR}/${PROC_DIR}/full/${EVENTS_DIR}/augmented/ObsReward_B_02_17_2025_15_11_events_final.csv" \
    --outdir ./out \
    --match-window 60