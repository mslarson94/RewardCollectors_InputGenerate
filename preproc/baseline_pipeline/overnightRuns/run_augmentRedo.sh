#!/usr/bin/env bash

set -uo pipefail

on_exit() {
    exit_code=$?

    if [ "$exit_code" -eq 0 ]; then
        afplay /System/Library/Sounds/Blow.aiff
    else
        afplay /System/Library/Sounds/Sosumi.aiff
    fi
}

trap on_exit EXIT


LOG_FILE="/Users/mairahmac/Desktop/MarkLabelingRedo_processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
PROC_DIR="FreshStart_redoAgainAgainAgain_PO_redo_new"
META_FILE="collatedData.xlsx"
LABEL=BioPac
TIMECOL=mLT_orig


SOURCE_DIR="${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/markMatches_${LABEL}_orig"
RPI_UNIFIED_DIR="${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/${LABEL}/RPi_unified"
REVIEW_ROOT="${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc_${TIMECOL}/markMatches_${LABEL}"


MARK_TOOL="${CODE_DIR}/alignment/markMatchApps/augment_mark_provenance.py"

echo "🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡🐡" | tee -a "$LOG_FILE"
echo "🚀 Starting augment_mark_provenance.py at $(date)" | tee -a "$LOG_FILE"

mkdir -p "$REVIEW_ROOT"

shopt -s nullglob
SOURCE_FILES=("${SOURCE_DIR}"/*_mark_matches.csv)

if (( ${#SOURCE_FILES[@]} == 0 )); then
    echo "❌ No *_earliestRoundStart.csv files found in ${SOURCE_DIR}" \
        | tee -a "$LOG_FILE"
    exit 1
fi

TOTAL_FILES=${#SOURCE_FILES[@]}
CURRENT_FILE=0


for file in "${SOURCE_FILES[@]}"; do
    ((CURRENT_FILE += 1))
    echo "Processing # ${CURRENT_FILE} of ${TOTAL_FILES} files at $(date)"   | tee -a "$LOG_FILE"
    base=$(basename "$file" "_mark_matches.csv")
    echo "$base"
    singles_file="${SOURCE_DIR}/${base}_mark_singles.csv"
    matches_file="${SOURCE_DIR}/${base}_mark_matches.csv"
    rpi_unified_file="${RPI_UNIFIED_DIR}/${base}_RPi_unified.csv"

    if ! python "${CODE_DIR}/alignment/markMatchApps/augment_mark_provenance.py" \
          --mark-matches "${matches_file}" \
          --mark-singles "${singles_file}" \
          --rpi-unified "${rpi_unified_file}" \
          --out-dir "${REVIEW_ROOT}" \
          --suffix "" \
          >> "$LOG_FILE" 2>&1
    then
        echo "❌ propose failed for ${file}" \
            | tee -a "$LOG_FILE"
        continue
    fi

done
echo "✅ augment_mark_provenance.py completed for ${TOTAL_FILES} at $(date)" | tee -a "$LOG_FILE"
