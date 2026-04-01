#!/usr/bin/env bash
set -Eeuo pipefail

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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
PROC_DIR="FreshStart"
# PROC_DIR="FreshStart_mini"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Final_NoWalks"


# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting flexiblePlotByCoinType2 for COIN INTERACTIONS at $(date)" | tee -a "$LOG_FILE"
# # add to run_overnight_modular3.sh after add_coin_labels_from_collated.py
# python "${CODE_DIR}/flexiblePlotByCoinType2.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/full/testDir" \
#   --pattern "*_events.csv" \
#   --formats pdf \
#   --recursive \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/DropDist"\
#   --no-group-subdirs \
#   --variable-of-interest dropDist \
#   --blocks-per-facet 20 \
#   --use-outlier-filter \
#   --filter-columns truecontent_elapsed_s \
#   >> "$LOG_FILE" 2>&1


# echo "✅ flexiblePlotByCoinType2 for PARTICIPANT ROLE COIN INTERACTIONS completed at $(date)" | tee -a "$LOG_FILE"

# echo '✨ done ✨' | tee -a "$LOG_FILE"



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

# echo "✅ histoWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# echo '✨ done ✨' | tee -a "$LOG_FILE"

# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "coinSet" \
#   >> "$LOG_FILE" 2>&1

# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# echo '✨ done ✨' | tee -a "$LOG_FILE"

# echo "🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "" \
#   >> "$LOG_FILE" 2>&1

# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# echo '✨ done ✨' | tee -a "$LOG_FILE"



# echo "🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist_wOutlier_main_Correct" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "" \
#   --use-outlier-filter \
#   --filter-columns dropDist \
#   --xlim 0.0 1.1 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist_main" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "" \
#   --no-outlier-filter \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_Time_main_Short" \
#   --formats "pdf" \
#   --voi "truecontent_elapsed_s" \
#   --voi-str "Round Elapsed Time" \
#   --voi-unit "(s)" \
#   --facet-by "" \
#   --xlim 0.0 120.0 \
#   --use-outlier-filter \
#   --filter-columns "truecontent_elapsed_s" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# # #!/usr/bin/env bash
# set -euo pipefail



# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_Time_wOutliers_main" \
#   --formats "pdf" \
#   --voi "truecontent_elapsed_s" \
#   --voi-str "Round Elapsed Time" \
#   --voi-unit "(s)" \
#   --facet-by "" \
#   --no-outlier-filter \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# # #!/usr/bin/env bash
# set -euo pipefail


# echo "🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_RR.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist_wOutlier_RR_Correct" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "" \
#   --use-outlier-filter \
#   --xlim 0.0 1.1 \
#   --filter-columns dropDist \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈🌈" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_RR.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist_RR" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "" \
#   --no-outlier-filter \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚🐚" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_RR.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_Time_RR_Short" \
#   --formats "pdf" \
#   --voi "truecontent_elapsed_s" \
#   --voi-str "Round Elapsed Time" \
#   --voi-unit "(s)" \
#   --facet-by "" \
#   --use-outlier-filter \
#   --xlim 0.0 120.0 \
#   --filter-columns "truecontent_elapsed_s" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# # #!/usr/bin/env bash
# set -euo pipefail



# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_RR.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_Time_wOutliers_RR" \
#   --formats "pdf" \
#   --voi "truecontent_elapsed_s" \
#   --voi-str "Round Elapsed Time" \
#   --voi-unit "(s)" \
#   --facet-by "" \
#   --no-outlier-filter \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# # #!/usr/bin/env bash
# set -euo pipefail


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
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_Main/FacetByCoinSetID_Outlier" \
  --formats "pdf" \
  --voi "dropDist" \
  --voi-str "Pin Drop Distance" \
  --voi-unit "(m)" \
  --facet-by "CoinSetID" \
  --no-outlier-filter \
  >> "$LOG_FILE" 2>&1
echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropWrapper.py" \
  --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_RR.csv" \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_RR/FacetByCoinSetID_NoOutlier" \
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
  --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_RR.csv" \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_RR/FacetByCoinSetID_Outlier" \
  --formats "pdf" \
  --voi "dropDist" \
  --voi-str "Pin Drop Distance" \
  --voi-unit "(m)" \
  --facet-by "CoinSetID" \
  --no-outlier-filter \
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


echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropWrapper.py" \
  --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_Main/FacetByCoinSetID_Outlier" \
  --formats "pdf" \
  --voi "truecontent_elapsed_s" \
  --voi-str "Round Elapsed Time" \
  --voi-unit "(s)" \
  --facet-by "CoinSetID" \
  --no-outlier-filter \
  >> "$LOG_FILE" 2>&1
echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropWrapper.py" \
  --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_RR.csv" \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_RR/FacetByCoinSetID_NoOutlier" \
  --formats "pdf" \
  --voi "truecontent_elapsed_s" \
  --voi-str "Round Elapsed Time" \
  --voi-unit "(s)" \
  --facet-by "CoinSetID" \
  --use-outlier-filter \
  >> "$LOG_FILE" 2>&1
echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"


echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/pinDropWrapper.py" \
  --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_RR.csv" \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_RR/FacetByCoinSetID_Outlier" \
  --formats "pdf" \
  --voi "truecontent_elapsed_s" \
  --voi-str "Round Elapsed Time" \
  --voi-unit "(s)" \
  --facet-by "CoinSetID" \
  --no-outlier-filter \
  >> "$LOG_FILE" 2>&1
echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_Main/TPBlock_CoinSetID/BlockNum_Other/df_CoinSetID_1.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_Main/TP2_All_Outlier" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "" \
#   --no-outlier-filter \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_Main/TPBlock_CoinSetID/BlockNum_Other/df_CoinSetID_1.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_Main/TP2_All_NoOutlier" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "" \
#   --use-outlier-filter \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"

# LOG_FILE="/tmp/pinDrop_allsubjects_$(date +%Y%m%d_%H%M%S).log"

# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"

# PY="/usr/bin/python3"   # or just 'python' if your env is set

# $PY "${CODE_DIR}/pinDropWrapper.py" \
#   --input "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/PinDrops_All/PinDrops_ALL.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/AllSubjects_Out" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "coinSet" \
#   >> "$LOG_FILE" 2>&1

# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# echo '✨ done ✨' | tee -a "$LOG_FILE"
