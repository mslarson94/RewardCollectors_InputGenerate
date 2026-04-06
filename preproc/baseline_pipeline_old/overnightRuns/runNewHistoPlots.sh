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
PROC_DIR="FreshStart_redoAgainAgainAgain"
# PROC_DIR="FreshStart_mini"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Final_NoWalks"
LAMBDA="1"


echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper for COIN INTERACTIONS at $(date)" | tee -a "$LOG_FILE"
# add to run_overnight_modular3.sh after add_coin_labels_from_collated.py
python "${CODE_DIR}/pinDropWrapper.py" \
  --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/intervalsFinal_L1" \
  --pattern "*__withDemo.csv" \
  --formats pdf \
  --recursive \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/FlexPlotByCoinType/RoundElapsed"\
  --no-group-subdirs \
  --variable-of-interest roundElapsed_s \
  --blocks-per-facet 40 \
  --use-outlier-filter \
  --filter-columns roundElapsed_s \
  >> "$LOG_FILE" 2>&1

echo "✅ flexiblePlotByCoinType2 for PARTICIPANT ROLE COIN INTERACTIONS completed at $(date)" | tee -a "$LOG_FILE"

echo '✨ done ✨' | tee -a "$LOG_FILE"


echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper for COIN INTERACTIONS at $(date)" | tee -a "$LOG_FILE"
# add to run_overnight_modular3.sh after add_coin_labels_from_collated.py
python "${CODE_DIR}/pinDropWrapper.py" \
  --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/intervalsFinal_L1" \
  --pattern "*__withDemo.csv" \
  --formats pdf \
  --recursive \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/FlexPlotByCoinType/DropDist"\
  --no-group-subdirs \
  --variable-of-interest dropDist \
  --blocks-per-facet 40 \
  --use-outlier-filter \
  --filter-columns roundElapsed_s \
  >> "$LOG_FILE" 2>&1

echo "✅ flexiblePlotByCoinType2 for PARTICIPANT ROLE COIN INTERACTIONS completed at $(date)" | tee -a "$LOG_FILE"

echo '✨ done ✨' | tee -a "$LOG_FILE"


echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper for COIN INTERACTIONS at $(date)" | tee -a "$LOG_FILE"
# add to run_overnight_modular3.sh after add_coin_labels_from_collated.py
python "${CODE_DIR}/pinDropWrapper.py" \
  --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/intervalsFinal_L1" \
  --pattern "*__withDemo.csv" \
  --formats pdf \
  --recursive \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/FlexPlotByCoinType/RoundFrac"\
  --no-group-subdirs \
  --variable-of-interest roundFrac \
  --blocks-per-facet 40 \
  --use-outlier-filter \
  --filter-columns roundFrac \
  >> "$LOG_FILE" 2>&1

echo "✅ flexiblePlotByCoinType2 for PARTICIPANT ROLE COIN INTERACTIONS completed at $(date)" | tee -a "$LOG_FILE"

echo '✨ done ✨' | tee -a "$LOG_FILE"


echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper for COIN INTERACTIONS at $(date)" | tee -a "$LOG_FILE"
# add to run_overnight_modular3.sh after add_coin_labels_from_collated.py
python "${CODE_DIR}/pinDropWrapper.py" \
  --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/intervalsFinal_L1" \
  --pattern "*__withDemo.csv" \
  --formats pdf \
  --recursive \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/FlexPlotByCoinType/AvgWalkSpeed"\
  --no-group-subdirs \
  --variable-of-interest WalkAvgSpeed \
  --blocks-per-facet 40 \
  --use-outlier-filter \
  --filter-columns WalkAvgSpeed \
  >> "$LOG_FILE" 2>&1

echo "✅ flexiblePlotByCoinType2 for PARTICIPANT ROLE COIN INTERACTIONS completed at $(date)" | tee -a "$LOG_FILE"

echo '✨ done ✨' | tee -a "$LOG_FILE"


echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper for COIN INTERACTIONS at $(date)" | tee -a "$LOG_FILE"
# add to run_overnight_modular3.sh after add_coin_labels_from_collated.py
python "${CODE_DIR}/pinDropWrapper.py" \
  --input "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/intervalsFinal_L1" \
  --pattern "*__withDemo.csv" \
  --formats pdf \
  --recursive \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/Plotting/FlexPlotByCoinType/PathEff"\
  --no-group-subdirs \
  --variable-of-interest path_eff_raw \
  --blocks-per-facet 40 \
  --use-outlier-filter \
  --filter-columns path_eff_raw \
  >> "$LOG_FILE" 2>&1

echo "✅ flexiblePlotByCoinType2 for PARTICIPANT ROLE COIN INTERACTIONS completed at $(date)" | tee -a "$LOG_FILE"

echo '✨ done ✨' | tee -a "$LOG_FILE"



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





echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropWrapper.py" \
  --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_Main/FacetByCoinSetID_NoOutlier" \
  --formats "pdf" \
  --voi "dropDist" \
  --voi-str "Pin Drop Distance" \
  --voi-unit "(m)" \
  --facet-by "CoinSetID" \
  --use-outlier-filter \
  >> "$LOG_FILE" 2>&1
echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"


echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropWrapper.py" \
  --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_Main/FacetByCoinSetID_NoOutlier" \
  --formats "pdf" \
  --voi "truecontent_elapsed_s" \
  --voi-str "Round Elapsed Time" \
  --voi-unit "(s)" \
  --facet-by "CoinSetID" \
  --use-outlier-filter \
  >> "$LOG_FILE" 2>&1
echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"

