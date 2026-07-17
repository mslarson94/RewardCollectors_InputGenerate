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
MEGA_FILE="${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/megaFiles/allIntervalDataKnotted_AN_patched_baseQC_roundDur_optionA_main.csv"



# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper_v2 for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropPlots/pinDropWrapper_v2.py" \
#   --input "${MEGA_FILE}" \
#   --formats png \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/FlexPlotByCoinType/ln_DropDist_all"\
#   --voi ln_dropDist \
#   --voi-unit ln_dropDist \
#   --voi-str "Natural log transformed Pin Drop Distance" \
#   --require-cols isEligibleBase \
#   --outlier-method "no outlier filter" \
#   --dot-mode none \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper_v2 for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper_v2 for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropPlots/pinDropWrapper_v2.py" \
#   --input "${MEGA_FILE}" \
#   --formats png \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/FlexPlotByCoinType/ln_DropDist_correct"\
#   --voi ln_dropDist \
#   --voi-unit ln_dropDist \
#   --voi-str "Natural log transformed Pin Drop Distance" \
#   --require-cols isPerfectRound isEligibleBase \
#   --outlier-method "not outlier, filtered for correct pin drops only" \
#   --dot-mode none \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper_v2 for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"


# echo "✅ pinDropWrapper_v2 for roundFrac for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

# ##################
# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting layoutFacetWrapper for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropPlots/layoutFacetWrapper.py" \
#   --input "${MEGA_FILE}" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/LayoutFacet_ln_dropDist_coinSet" \
#   --voi ln_dropDist \
#   --voi-str "Natural log transformed Pin Drop Distance" \
#   --voi-unit ln_dropDist \
#   --layout-col coinSet \
#   --require-cols isEligibleBase isPerfectRound \
#   >> "$LOG_FILE" 2>&1
# echo "✅ layoutFacetWrapper for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"


##################
echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting layoutFacetWrapper for dropDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropPlots/layoutFacetWrapper.py" \
  --input "${MEGA_FILE}" \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/LayoutFacet_ln_dropDist_EV_behMod" \
  --voi ln_dropDist \
  --voi-str "Natural log transformed Pin Drop Distance" \
  --voi-unit ln_dropDist \
  --layout-col EV_behMod \
  --require-cols isEligibleBase isPerfectRound \
  >> "$LOG_FILE" 2>&1
echo "✅ layoutFacetWrapper for dropDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper_v2 for WalkAvgSpeed for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper_v2.py" \
#   --input "${MEGA_FILE}" \
#   --formats png \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/FlexPlotByCoinType/AvgWalkSpeed"\
#   --voi WalkAvgSpeed \
#   --voi-unit MetersPerSecond \
#   --filter-cols isPerfectRound isEligibleBase isEligibleRoundDur \
#   --outlier-method "MAD on round_dur_s within main_RR+sessionID" \
#   --dot-mode none \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper_v2 for WalkAvgSpeed for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"



# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper_v2 for WalkDist for Coin Interactions at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper_v2.py" \
#   --input "${MEGA_FILE}" \
#   --formats png \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/FlexPlotByCoinType/WalkDist"\
#   --voi WalkDist \
#   --voi-unit meters \
#   --filter-cols isPerfectRound isEligibleBase isEligibleRoundDur \
#   --outlier-method "MAD on round_dur_s within main_RR+sessionID" \
#   --dot-mode none \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper_v2 for WalkDist for Coin Interactions completed at $(date)" | tee -a "$LOG_FILE"

################################################################################################################

# echo "🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting histoWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/histoWrapper.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/full/Merged_PtRoleCoinSet_Flat_csv/augmented" \
#   --pattern "*_events.csv" \
#   --formats png,pdf \
#   --recursive \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/DropDist"\
#   --no-group-subdirs \
#   --variable-of-interest dropDist \
#   --voi_str "Pin Drop Distance to Closest Coin Not Yet Collected" \
#   --voi_UnitStr "(m)" \
#   --blocks-per-facet 20 \
#   --use-outlier-filter \
#   --filter-columns truecontent_elapsed_s \
#   >> "$LOG_FILE" 2>&1

# echo "✅ histoWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# echo '✨ done ✨' | tee -a "$LOG_FILE"



# echo "🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽🐽" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting histoWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/histoWrapper.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/full/Merged_PtRoleCoinSet_Flat_csv" \
#   --pattern "*_events.csv" \
#   --formats png,pdf \
#   --recursive \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/Latency"\
#   --no-group-subdirs \
#   --variable-of-interest "trueSession_elapsed_s" \
#   --voi_str "Round Elapsed Time" \
#   --voi_UnitStr "(s)" \
#   --blocks-per-facet 20 \
#   --use-outlier-filter \
#   --filter-columns truecontent_elapsed_s \
#   >> "$LOG_FILE" 2>&1





# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_Main/FacetByCoinSetID_NoOutlier" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "CoinSetID" \
#   --use-outlier-filter \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_Main/FacetByCoinSetID_NoOutlier" \
#   --formats "pdf" \
#   --voi "truecontent_elapsed_s" \
#   --voi-str "Round Elapsed Time" \
#   --voi-unit "(s)" \
#   --facet-by "CoinSetID" \
#   --use-outlier-filter \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"


echo '✨ done ✨' | tee -a "$LOG_FILE"
