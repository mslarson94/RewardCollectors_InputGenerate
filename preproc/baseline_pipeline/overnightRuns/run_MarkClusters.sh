#!/usr/bin/env bash

set -uo pipefail
LOG_FILE="/Users/mairahmac/Desktop/RPi_Clustering_processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
PROC_DIR="FreshStart_redoAgainAgainAgain_PO_redo_new"
META_FILE="collatedData.xlsx"

SOURCE_DIR="${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EarliestRoundStart"
REVIEW_ROOT="${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MarkReview"


MARK_TOOL="${CODE_DIR}/alignment/mark_chunk_review_tool.py"
REVIEW_TOOL="${CODE_DIR}/alignment/mark_review_tk.py"

mkdir -p "$REVIEW_ROOT"

shopt -s nullglob
SOURCE_FILES=("${SOURCE_DIR}"/*_earliestRoundStart.csv)

if (( ${#SOURCE_FILES[@]} == 0 )); then
    echo "❌ No *_earliestRoundStart.csv files found in ${SOURCE_DIR}" \
        | tee -a "$LOG_FILE"
    exit 1
fi

TOTAL_FILES=${#SOURCE_FILES[@]}
CURRENT_FILE=0

for SOURCE_CSV in "${SOURCE_FILES[@]}"; do
    ((CURRENT_FILE += 1))

    FILE_NAME="$(basename "$SOURCE_CSV")"
    DATASET_NAME="${FILE_NAME%_earliestRoundStart.csv}"

    OUTPUT_DIR="${REVIEW_ROOT}/${DATASET_NAME}"
    FINALIZED_DIR="${OUTPUT_DIR}/Finalized"

    CLUSTER_CSV="${OUTPUT_DIR}/mark_clusters.csv"
    CONTEXT_CSV="${OUTPUT_DIR}/mark_cluster_context.csv"
    REVIEW_CSV="${OUTPUT_DIR}/review_edits.csv"
    REVIEW_TEMPLATE="${OUTPUT_DIR}/mark_cluster_review_template.csv"

    mkdir -p "$OUTPUT_DIR"

    echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" \
        | tee -a "$LOG_FILE"

    echo "📁 [$CURRENT_FILE/$TOTAL_FILES] Processing ${DATASET_NAME}" \
        | tee -a "$LOG_FILE"

    # -------------------------------------------------------------------------
    # 1. PROPOSE
    # -------------------------------------------------------------------------

    if ! python "$MARK_TOOL" propose \
        --input "$SOURCE_CSV" \
        --output-dir "$OUTPUT_DIR" \
        --time-col mLT_orig \
        --event-col lo_eventType \
        --block-col BlockNum \
        --mark-gap-seconds 30 \
        --context-window-seconds 300 \
        --boundary-pair-max-seconds 300 \
        >> "$LOG_FILE" 2>&1
    then
        echo "❌ propose failed for ${DATASET_NAME}" \
            | tee -a "$LOG_FILE"
        continue
    fi

    # -------------------------------------------------------------------------
    # 2. CHECK WHETHER THERE IS ANYTHING TO REVIEW
    # -------------------------------------------------------------------------

    if [[ ! -s "$CLUSTER_CSV" ]]; then
        echo "⏭️ No Mark clusters found for ${DATASET_NAME}; nothing to review." \
            | tee -a "$LOG_FILE"

        mkdir -p "$FINALIZED_DIR"

        printf '%s\n' \
            "No Mark events were found in this source file." \
            > "${FINALIZED_DIR}/NO_MARKS.txt"

        continue
    fi

    # A CSV containing only a header also means there are no cluster rows.
    CLUSTER_LINE_COUNT=$(wc -l < "$CLUSTER_CSV")

    if (( CLUSTER_LINE_COUNT <= 1 )); then
        echo "⏭️ No Mark clusters found for ${DATASET_NAME}; nothing to review." \
            | tee -a "$LOG_FILE"

        mkdir -p "$FINALIZED_DIR"

        printf '%s\n' \
            "No Mark events were found in this source file." \
            > "${FINALIZED_DIR}/NO_MARKS.txt"

        continue
    fi

    # -------------------------------------------------------------------------
    # 3. INITIALIZE REVIEW CSV
    # -------------------------------------------------------------------------

    if [[ ! -f "$REVIEW_CSV" ]]; then
        cp "$REVIEW_TEMPLATE" "$REVIEW_CSV"
    fi

    # -------------------------------------------------------------------------
    # 4. REVIEW
    # -------------------------------------------------------------------------

    echo "🚀 Starting Tkinter review app for ${DATASET_NAME}" \
        | tee -a "$LOG_FILE"

    if ! python "$REVIEW_TOOL" \
        --cluster-csv "$CLUSTER_CSV" \
        --context-csv "$CONTEXT_CSV" \
        --source-csv "$SOURCE_CSV" \
        --review-csv "$REVIEW_CSV" \
        --time-col mLT_orig \
        --event-col lo_eventType \
        --block-col BlockNum \
        --context-seconds 300 \
        >> "$LOG_FILE" 2>&1
    then
        echo "❌ Review app failed for ${DATASET_NAME}" \
            | tee -a "$LOG_FILE"
        continue
    fi

    # -------------------------------------------------------------------------
    # 5. APPLY EDITS
    # -------------------------------------------------------------------------

    mkdir -p "$FINALIZED_DIR"

    if ! python "$MARK_TOOL" apply-edits \
        --cluster-csv "$CLUSTER_CSV" \
        --review-csv "$REVIEW_CSV" \
        --context-csv "$CONTEXT_CSV" \
        --output-dir "$FINALIZED_DIR" \
        >> "$LOG_FILE" 2>&1
    then
        echo "❌ apply-edits failed for ${DATASET_NAME}" \
            | tee -a "$LOG_FILE"
        continue
    fi

    echo "✅ Finished ${DATASET_NAME}" \
        | tee -a "$LOG_FILE"
done

echo "🎉 Finished processing ${TOTAL_FILES} files at $(date)" \
    | tee -a "$LOG_FILE"