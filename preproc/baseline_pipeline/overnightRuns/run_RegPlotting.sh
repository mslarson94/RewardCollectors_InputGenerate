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



echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper_v2 for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/pinDropWrapper_v2.py" \
  --input "${MEGA_FILE}" \
  --formats png \
  --out-root "${OUTDIR}/FlexPlotByCoinType/DropDist_all"\
  --voi dropDist \
  --voi-unit meters \
  --voi-str "Pin Drop Distance - All Pin Drops" \
  --require-cols isEligibleBase \
  --outlier-method "no outlier filter" \
  --dot-mode none \
  >> "$LOG_FILE" 2>&1
echo "✅ pinDropWrapper_v2 for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper_v2 for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/pinDropWrapper_v2.py" \
  --input "${MEGA_FILE}" \
  --formats png \
  --out-root "${OUTDIR}/FlexPlotByCoinType/DropDist_correct"\
  --voi dropDist \
  --voi-unit meters \
  --voi-str "Pin Drop Distance - Correct Pin Drops Only" \
  --require-cols isPerfectRound isEligibleBase \
  --outlier-method "not outlier, filtered for correct pin drops only" \
  --dot-mode none \
  >> "$LOG_FILE" 2>&1
echo "✅ pinDropWrapper_v2 for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"
#################

echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper_v2 for ln_dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/pinDropWrapper_v2.py" \
  --input "${MEGA_FILE}" \
  --formats png \
  --out-root "${OUTDIR}/FlexPlotByCoinType/ln_DropDist_all"\
  --voi ln_dropDist \
  --voi-unit ln_dropDist \
  --voi-str "Natural log transformed Pin Drop Distance - All Pin Drops" \
  --require-cols isEligibleBase \
  --outlier-method "no outlier filter" \
  --dot-mode none \
  >> "$LOG_FILE" 2>&1
echo "✅ pinDropWrapper_v2 for ln_dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper_v2 for ln_dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/pinDropWrapper_v2.py" \
  --input "${MEGA_FILE}" \
  --formats png \
  --out-root "${OUTDIR}/FlexPlotByCoinType/ln_DropDist_correct"\
  --voi ln_dropDist \
  --voi-unit ln_dropDist \
  --voi-str "Natural log transformed Pin Drop Distance - Correct Pin Drops Only" \
  --require-cols isPerfectRound isEligibleBase \
  --outlier-method "not outlier, filtered for correct pin drops only" \
  --dot-mode none \
  >> "$LOG_FILE" 2>&1
echo "✅ pinDropWrapper_v2 for ln_dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

#################
echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper_v2 for roundElapsed_s for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/pinDropWrapper_v2.py" \
  --input "${MEGA_FILE}" \
  --formats png \
  --out-root "${OUTDIR}/FlexPlotByCoinType/RoundElapsed_s"\
  --voi roundElapsed_s \
  --voi-unit seconds \
  --voi-str "Round Elapsed Time" \
  --require-cols isEligibleBase isEligibleRoundDur \
  --exclude-true-cols roundDur_pref_out \
  --outlier-method "MAD on round_dur_s within main_RR+sessionID, robust z = 3.0" \
  --dot-mode none \
  >> "$LOG_FILE" 2>&1

echo "✅ pinDropWrapper_v2 for roundElapsed_s for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"


echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper_v2 for roundFrac for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/pinDropWrapper_v2.py" \
  --input "${MEGA_FILE}" \
  --formats png \
  --out-root "${OUTDIR}/FlexPlotByCoinType/RoundFrac"\
  --voi roundFrac \
  --voi-unit "Round Fraction (elapsed time / total round duration)" \
  --voi-str "Round Fraction (elapsed time / total round duration)" \
  --require-cols isEligibleBase isEligibleRoundDur \
  --exclude-true-cols roundDur_pref_out \
  --outlier-method "MAD on round_dur_s within main_RR+sessionID, robust z = 3.0" \
  --dot-mode none \
  >> "$LOG_FILE" 2>&1
echo "✅ pinDropWrapper_v2 for roundFrac for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

##################
echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting layoutFacetWrapper for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/layoutFacetWrapper.py" \
  --input "${MEGA_FILE}" \
  --out-root "${OUTDIR}/LayoutFacet_dropDist_coinSet" \
  --voi dropDist \
  --voi-str "Pin Drop Distance" \
  --voi-unit meters \
  --layout-col coinSet \
  --require-cols isEligibleBase isPerfectRound \
  >> "$LOG_FILE" 2>&1
echo "✅ layoutFacetWrapper for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

##################
echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting layoutFacetWrapper for ln_dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/layoutFacetWrapper.py" \
  --input "${MEGA_FILE}" \
  --out-root "${OUTDIR}/LayoutFacet_ln_dropDist_coinSet" \
  --voi ln_dropDist \
  --voi-str "Natural log transformed Pin Drop Distance" \
  --voi-unit ln_dropDist \
  --layout-col coinSet \
  --require-cols isEligibleBase isPerfectRound \
  >> "$LOG_FILE" 2>&1
echo "✅ layoutFacetWrapper for ln_dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

##################
echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting layoutFacetWrapper for roundFrac for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/layoutFacetWrapper.py" \
  --input "${MEGA_FILE}" \
  --out-root "${OUTDIR}/LayoutFacet_roundFrac_coinSet" \
  --voi roundFrac \
  --voi-str "Round Fraction (elapsed time / total round duration)" \
  --voi-unit roundFrac \
  --layout-col coinSet \
  --require-cols isEligibleBase isEligibleRoundDur \
  >> "$LOG_FILE" 2>&1
echo "✅ layoutFacetWrapper for roundFrac for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

##################
echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting layoutFacetWrapper for roundElapsed_s for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/layoutFacetWrapper.py" \
  --input "${MEGA_FILE}" \
  --out-root "${OUTDIR}/LayoutFacet_roundElapsed_s_coinSet" \
  --voi roundElapsed_s \
  --voi-str "Round Elapsed Time" \
  --voi-unit seconds \
  --layout-col coinSet \
  --require-cols isEligibleBase isEligibleRoundDur \
  >> "$LOG_FILE" 2>&1
echo "✅ layoutFacetWrapper for roundElapsed_s for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

