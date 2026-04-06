#!/usr/bin/env bash
set -Eeuo pipefail

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/OddsAndEnds_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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
CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
PROC_DIR="FreshStart_redoAgain"
#PROC_DIR="FreshStart_redoAgainSingle"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Pos"
TRIANGLES_DIR="/Users/mairahmac/Desktop/TriangleSets"

# ## Getting Round Num reports 
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting report_roundnums_lt100.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/baseline_pipeline/overnightRuns/report_roundnums_lt100.py" \
#   --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Flat_csv" \
#   --pattern "*_processed_events.csv" \
#   --include-counts \
#   --out "/Users/mairahmac/Desktop/roundnums_lt100_report.csv" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ report_roundnums_lt100.py completed at $(date)" | tee -a "$LOG_FILE"


# ### Plotting All the Triangles Together with Centroids 
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting plot_triangles_from_list.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/generatingUnityInput/plot_triangles_from_list.py" \
#   --triangles-csv "${TRIANGLES_DIR}/triangle_positions-formatted__A_D_.csv" \
#   --output "${TRIANGLES_DIR}/MultiTrianglePlots/CentroidPlot.png" \
#   --xlim -5.5 5.5 \
#   --ylim -5.5 5.5 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ plot_triangles_from_list.py completed at $(date)" | tee -a "$LOG_FILE"



### Plotting Ideal Distances Stuff 
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting plot_idealDistByCoinLayout.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/plotting/plot_idealDistByCoinLayout.py" \
  --input_glob "${TRIANGLES_DIR}/RoutePlanWeightUtility/idealRoutes/ideal_routes_*.csv" \
  --out_dir "${TRIANGLES_DIR}/RoutePlanWeightUtility/idealRoutes_Plots" \
  >> "$LOG_FILE" 2>&1
echo "✅ plot_idealDistByCoinLayout.py completed at $(date)" | tee -a "$LOG_FILE"



# # #############################################################################################################################################################
# # Generating Ideal Distances, Path Utility, & Path Efficiency Stuff 
# #############################################################################################################################################################

# python "${CODE_DIR}/overnightRuns/calcIdealDistances.py"

# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/walkDataAnalysis/theoPaths_Classifiers/greedy_v2.py"

# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extraction/normalize_path_utility.py" \
#   --root "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda5" \
#   --pattern "all_orders__layout_*_L5.csv" \
#   --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L5" \
#   --require-order-col \
#   --write-summary \
#   --overwrite \
#   >> "$LOG_FILE" 2>&1

# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extraction/normalize_path_utility.py" \
#   --root "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda2" \
#   --pattern "all_orders__layout_*_L2.csv" \
#   --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L2" \
#   --require-order-col \
#   --write-summary \
#   --overwrite \
#   >> "$LOG_FILE" 2>&1

# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extraction/normalize_path_utility.py" \
#   --root "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_lambda1" \
#   --pattern "all_orders__layout_*_L1.csv" \
#   --output "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L1" \
#   --require-order-col \
#   --write-summary \
#   --overwrite \
#   >> "$LOG_FILE" 2>&1