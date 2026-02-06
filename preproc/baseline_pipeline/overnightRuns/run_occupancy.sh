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
CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
PROC_DIR="FreshStart_redo"
#PROC_DIR="FreshStart_multi"
META_FILE="collatedData.xlsx"
EVENTS_DIR="EventSegmentation/EventsFinal"


# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
# echo "🚀 Starting occupancy_PreBlockCylinder_v2.py for PO  setting at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/plotting/occupancy_PreBlockCylinder_v2.py" \
#   --startprop "${TRUE_BASE_DIR}/${PROC_DIR}/${EVENTS_DIR}/ObsReward_A_02_17_2025_15_11_startPosPropagated.csv" \
#   --reprocessed "${TRUE_BASE_DIR}/${PROC_DIR}/ReProcessedData_Flat/ObsReward_A_02_17_2025_15_11_reprocessed.csv" \
#   --event PreBlock_CylinderWalk_segment \
#   --mode time \
#   --tcol eMLT_orig \
#   --timestamp-unit auto \
#   --max-gap-sec 0.5 \
#   --bin-size 0.10 \
#   --xmin -10 --xmax 10 --ymin -10 --ymax 10 \
#   --outDir "${TRUE_BASE_DIR}/${PROC_DIR}/OccupancyHeatMaps" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ occupancy_PreBlockCylinder_v2.py for PO completed at $(date)" | tee -a "$LOG_FILE" 
# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"

# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
# echo "🚀 Starting instantDistCalc.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/preproc/baseline_pipeline/reproc/instantDistCalc.py \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/ReProcessedData_Flat/ObsReward_A_02_17_2025_15_11_reprocessed.csv" \
#   --output "${TRUE_BASE_DIR}/${PROC_DIR}/ReProc_withDist/" \
# >> "$LOG_FILE" 2>&1
# echo "✅ instantDistCalc.py completed at $(date)" | tee -a "$LOG_FILE" 
# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"

# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
# echo "🚀 Starting instantDistCalc.py at $(date)" | tee -a "$LOG_FILE"

# IN_DIR="${TRUE_BASE_DIR}/${PROC_DIR}/ReProcessedData_Flat"
# OUT_DIR="${TRUE_BASE_DIR}/${PROC_DIR}/ReProc_withDist"

# mkdir -p "$OUT_DIR"

# shopt -s nullglob
# for in_file in "$IN_DIR"/*.csv; do
#   echo "➡️  Processing: $in_file" | tee -a "$LOG_FILE"
#   python "${CODE_DIR}/preproc/baseline_pipeline/reproc/instantDistCalc.py" \
#     --input "$in_file" \
#     --output "$OUT_DIR" \
#     --overwrite \
#     >> "$LOG_FILE" 2>&1

#   rc=$?
#   if [[ $rc -ne 0 ]]; then
#     echo "❌ Failed (exit $rc): $in_file" | tee -a "$LOG_FILE"
#     exit $rc
#   fi
#   echo "✅ Done: $in_file" | tee -a "$LOG_FILE"
# done
# shopt -u nullglob

# echo "✅ plot_orders.py completed at $(date)" | tee -a "$LOG_FILE"
# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/plotting/plot_orders.py" "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/all_orders__layout_Ax.csv" \
# --outdir "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/CoinSetAx"

# echo "✅ plot_orders.py completed at $(date)" | tee -a "$LOG_FILE"
# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/plotting/plot_orders.py" "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/all_orders__layout_Bx.csv" \
# --outdir "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/CoinSetBx"

# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
# echo "🚀 Starting instantDistCalc.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/plotting/plot_coinsetsPaths.py" \
#   --csv "${TRUE_BASE_DIR}/CoinSets.csv" \
#   --out "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/CoinSetPaths" \

# echo "✅ extract_pin_drops.py completed at $(date)" | tee -a "$LOG_FILE"
# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preproc/plotting/extract_pin_drops.py" \
#   --root-dir "${TRUE_BASE_DIR}" \
#   --proc-dir "${PROC_DIR}" \
#   --input-dir-name "${TRUE_BASE_DIR}/${PROC_DIR}/${EVENTS_DIR}" \
#   --out-dir-name PinDrops_All \
#   >> "$LOG_FILE" 2>&1
# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"

echo "✅ extract_pin_drops.py completed at $(date)" | tee -a "$LOG_FILE"
echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventSeg/justAddTestingOrder.py" \
  --trueRootDir "${TRUE_BASE_DIR}" \
  --procDir "${TRUE_BASE_DIR}/${PROC_DIR}/${EVENTS_DIR}" \
  --role "AN" \
  --outDir "${TRUE_BASE_DIR}/${PROC_DIR}/EventsFinal_TestingOrder" \
  >> "$LOG_FILE" 2>&1
echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"

echo "✅ extract_pin_drops.py completed at $(date)" | tee -a "$LOG_FILE"
echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preproc/baseline_pipeline/eventSeg/justAddTestingOrder.py" \
  --trueRootDir "${TRUE_BASE_DIR}" \
  --procDir "${TRUE_BASE_DIR}/${PROC_DIR}/${EVENTS_DIR}" \
  --role "PO" \
  --outDir "${TRUE_BASE_DIR}/${PROC_DIR}/EventsFinal_TestingOrder" \
  >> "$LOG_FILE" 2>&1
echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"