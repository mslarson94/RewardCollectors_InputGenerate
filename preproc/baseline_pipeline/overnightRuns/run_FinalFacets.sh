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
LOG_FILE="/Users/mairahmac/Desktop/Rep_plotting_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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
CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
PROC_DIR="FreshStart_redoAgainAgainAgain_PO_redo"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Final_NoWalks"
LAMBDA="1"
ROUND_DUR_FILTER=2.0
MEGA_DIR="${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles_final"

MEGA_FILE="${MEGA_DIR}/mad_${ROUND_DUR_FILTER}/1st50IntervalDataKnotted_AN_noCD_all.csv"
MEGA_SUMMARY="${MEGA_DIR}/participantSummaryData_1st50_AN_noCD.csv"
OUTDIR="${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/mad_${ROUND_DUR_FILTER}/1st50Rounds/all/noCD"

# MEGA_FILE="${MEGA_DIR}/mad_${ROUND_DUR_FILTER}/1st50IntervalDataKnotted_AN_all.csv"
# MEGA_SUMMARY="${MEGA_DIR}/participantSummaryData_1st50_AN.csv"
# OUTDIR="${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/mad_${ROUND_DUR_FILTER}/1st50Rounds/all/all"

# MEGA_FILE="${MEGA_DIR}/mad_${ROUND_DUR_FILTER}/allIntervalDataKnotted_AN_noCD_all.csv"
# MEGA_SUMMARY="${MEGA_DIR}/participantSummaryData_AN_noCD.csv"
# OUTDIR="${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/mad_${ROUND_DUR_FILTER}/AllRounds/all/noCD"

# MEGA_FILE="${MEGA_DIR}/mad_${ROUND_DUR_FILTER}/allIntervalDataKnotted_AN_all.csv"
# MEGA_SUMMARY="${MEGA_DIR}/participantSummaryData_AN.csv"
# OUTDIR="${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/mad_${ROUND_DUR_FILTER}/AllRounds/all/all"


# ##################
# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting facetedPinDropWrapper_v3 for roundElapsed_s for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/repPlots2/facetedPinDropWrapper_v3.py"\
#   --input "${MEGA_FILE}" \
#   --roles "${MEGA_DIR}/randomizedRoles_42.csv" \
#   --out-root "${OUTDIR}/repPlots/roundElapsed_s" \
#   --voi roundElapsed_s \
#   --voi-str "Round Elapsed Time" \
#   --voi-unit "(s)" \
#   --require-cols isEligibleBase \
#   --exclude-true-cols roundDur_iqr_rr_sess_out \
#   --formats png \
#   --dot-mode "none" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ facetedPinDropWrapper_v3 for roundElapsed_s for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

# ##################
# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting facetedPinDropWrapper_v3 for roundElapsed_s for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/repPlots2/facetedPinDropWrapper_v3.py"\
#   --input "${MEGA_FILE}" \
#   --roles "${MEGA_DIR}/randomizedRoles_42.csv" \
#   --out-root "${OUTDIR}/repPlots/roundFrac" \
#   --voi roundFrac \
#   --voi-str "Round Fraction" \
#   --voi-unit "(elapsed time / total round duration)" \
#   --require-cols isEligibleBase \
#   --exclude-true-cols roundDur_iqr_rr_sess_out \
#   --formats png \
#   --dot-mode "none" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ facetedPinDropWrapper_v3 for roundElapsed_s for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"


# ##################
# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting facetedPinDropWrapper_v3 for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/repPlots2/facetedPinDropWrapper_v3.py"\
#   --input "${MEGA_FILE}" \
#   --roles "${MEGA_DIR}/randomizedRoles_42.csv" \
#   --out-root "${OUTDIR}/repPlots/dropDist" \
#   --voi dropDist \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "meters" \
#   --require-cols isEligibleBase isPerfectRound \
#   --formats png \
#   --dot-mode "none" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ facetedPinDropWrapper_v3 for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"


# ##################
# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting facetedPinDropWrapper_v3 for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/repPlots2/facetedPinDropWrapper_v3.py"\
#   --input "${MEGA_FILE}" \
#   --roles "${MEGA_DIR}/randomizedRoles_42.csv" \
#   --out-root "${OUTDIR}/repPlots/ln_dropDist" \
#   --voi ln_dropDist \
#   --voi-str "Natural Log Transformed Pin Drop Distance" \
#   --formats png \
#   --dot-mode "none" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ facetedPinDropWrapper_v3 for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

# ##################
# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting regressionWrapper.py for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/timeRegressionRepPlots/regressionWrapper.py" \
#     --input "${MEGA_FILE}" \
#     --roles "${MEGA_DIR}/randomizedRoles_42.csv" \
#     --output-dir "${OUTDIR}/repPlots/dropDist_reg_facetX" \
#     --outcome-column dropDist \
#     --representative-mode both \
#     --figure-mode both \
#     --include-tp1 \
#     --x-axis-mode facet \
#     --ylim 0 2 \
#     >> "$LOG_FILE" 2>&1
# echo "✅ regressionWrapper.py for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"


# ##################
# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting regressionWrapper.py for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/timeRegressionRepPlots/regressionWrapper.py" \
#     --input "${MEGA_FILE}" \
#     --roles "${MEGA_DIR}/randomizedRoles_42.csv" \
#     --output-dir "${OUTDIR}/repPlots/dropDist_reg_facetX_corr" \
#     --outcome-column dropDist \
#     --representative-mode both \
#     --figure-mode both \
#     --include-tp1 \
#     --x-axis-mode facet \
#     --correct-only \
#     >> "$LOG_FILE" 2>&1
# echo "✅ regressionWrapper.py for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

# ##################
# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting regressionWrapper.py for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/timeRegressionRepPlots/regressionWrapper.py" \
#     --input "${MEGA_FILE}" \
#     --roles "${MEGA_DIR}/randomizedRoles_42.csv" \
#     --output-dir "${OUTDIR}/repPlots/dropDist_reg_facetX_full" \
#     --outcome-column dropDist \
#     --representative-mode both \
#     --figure-mode both \
#     --include-tp1 \
#     --x-axis-mode facet \
#     >> "$LOG_FILE" 2>&1
# echo "✅ regressionWrapper.py for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"



# ##################
# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting regressionWrapper.py for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/timeRegressionRepPlots/regressionWrapper.py" \
#     --input "${MEGA_FILE}" \
#     --roles "${MEGA_DIR}/randomizedRoles_42.csv" \
#     --output-dir "${OUTDIR}/repPlots/ln_dropDist_reg_facetX" \
#     --outcome-column ln_dropDist \
#     --representative-mode both \
#     --figure-mode both \
#     --include-tp1 \
#     --x-axis-mode facet \
#     >> "$LOG_FILE" 2>&1
# echo "✅ regressionWrapper.py for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"


# ##################
# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting regressionWrapper.py for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/timeRegressionRepPlots/regressionWrapper.py" \
#     --input "${MEGA_FILE}" \
#     --roles "${MEGA_DIR}/randomizedRoles_42.csv" \
#     --output-dir "${OUTDIR}/repPlots/ln_dropDist_reg_facetX_corr" \
#     --outcome-column ln_dropDist \
#     --representative-mode both \
#     --figure-mode both \
#     --include-tp1 \
#     --x-axis-mode facet \
#     --correct-only \
#     >> "$LOG_FILE" 2>&1
# echo "✅ regressionWrapper.py for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

##################
echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting pathChoiceWrapper.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/path_choice_multipanel/pathChoiceWrapper.py" \
    --input "${MEGA_FILE}" \
    --summary "${MEGA_SUMMARY}" \
    --roles "${MEGA_DIR}/randomizedRoles_42.csv" \
    --out-dir "${OUTDIR}/pathChoice_Rep" \
    --formats png,pdf \
    --representative-mode general \
    --figure-mode both \
    --no-individual-plots \
    >> "$LOG_FILE" 2>&1
echo "✅ pathChoiceWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
