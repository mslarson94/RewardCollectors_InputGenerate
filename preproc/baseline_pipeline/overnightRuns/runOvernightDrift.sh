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

ML_FILE="ObsReward_A_02_17_2025_15_11_events_final.csv"
RPI_FILE="2025-02-17_15_08_32_071285381.log"


DEVICE_IP="192.168.50.109"
TESTDATE_FORMATTED="02_17_2025"
DEVICE="ML2A"
SESSION="Morning"
PAIR="pair_008"
SESSION_DATE="2025-02-17"
TIMEZONE_OFFSET=8



echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting plot_drift_with_metadata.py for PO FULL setting at $(date)" | tee -a "$LOG_FILE"

python "${CODE_DIR}/plot_drift_with_metadata.py" \
  --ml_csv_file "${TRUE_BASE_DIR}/${PROC_DIR}/full/${EVENTS_DIR}/augmented/${ML_FILE}" \
  --log_file "${TRUE_BASE_DIR}/${PROC_DIR}/RawData/${PAIR}/${TESTDATE_FORMATTED}/${SESSION}/RPi/BioPac_RPi/${RPI_FILE}" \
  --csv_timestamp_column mLTimestamp \
  --event_type_column lo_eventType \
  --event_type_values Mark \
  --log_device_ip "$DEVICE_IP" \
  --timezone_offset_hours "$TIMEZONE_OFFSET" \
  --session_date "${SESSION_DATE}" \
  --annotate_threshold 0.5 \
  --max_match_gap_s 1.0 \
  >> "$LOG_FILE" 2>&1

echo "✅ plot_drift_with_metadata.py for PO FULL setting completed at $(date)" | tee -a "$LOG_FILE"
