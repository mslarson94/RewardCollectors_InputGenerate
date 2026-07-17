#!/usr/bin/env bash
set -Eeuo pipefail

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/plotting_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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
MEGA_FILE="${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/AN_PinDropsKnottedFiltered_noABCD_all.csv"
OUTDIR="${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/all/noABCD"


##################
echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting layoutFacetWrapper for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/layoutFacetWrapper.py" \
  --input "${MEGA_FILE}" \
  --out-root "${OUTDIR}/LayoutFacet_ln_dropDist_EV_behMod" \
  --voi ln_dropDist \
  --voi-str "Natural log transformed Pin Drop Distance" \
  --voi-unit ln_dropDist \
  --layout-col EV_behMod \
  --require-cols isEligibleBase isPerfectRound \
  >> "$LOG_FILE" 2>&1
echo "✅ layoutFacetWrapper for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"


##################
echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting layoutFacetWrapper for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/layoutFacetWrapper.py" \
  --input "${MEGA_FILE}" \
  --out-root "${OUTDIR}/LayoutFacet_dropDist_EV_behMod" \
  --voi dropDist \
  --voi-str "Pin Drop Distance" \
  --voi-unit meters \
  --layout-col EV_behMod \
  --require-cols isEligibleBase isPerfectRound \
  >> "$LOG_FILE" 2>&1
echo "✅ layoutFacetWrapper for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"


echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting layoutFacetWrapper for roundElapsed_s for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/layoutFacetWrapper.py" \
  --input "${MEGA_FILE}" \
  --out-root "${OUTDIR}/LayoutFacet_roundElapsedTime_EV_behMod" \
  --voi roundElapsed_s \
  --voi-str "Round Elapsed Time" \
  --voi-unit seconds \
  --layout-col EV_behMod \
  --require-cols isEligibleBase isEligibleRoundDur \
  >> "$LOG_FILE" 2>&1
echo "✅ layoutFacetWrapper for roundElapsed_s for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting layoutFacetWrapper for roundFrac for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/layoutFacetWrapper.py" \
  --input "${MEGA_FILE}" \
  --out-root "${OUTDIR}/LayoutFacet_roundFrac_EV_behMod" \
  --voi roundFrac \
  --voi-str "Round Fraction (elapsed time / total round duration)" \
  --voi-unit roundFrac \
  --layout-col EV_behMod \
  --require-cols isEligibleBase isEligibleRoundDur \
  >> "$LOG_FILE" 2>&1
echo "✅ layoutFacetWrapper for roundFrac for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"
echo '✨ done ✨' | tee -a "$LOG_FILE"
