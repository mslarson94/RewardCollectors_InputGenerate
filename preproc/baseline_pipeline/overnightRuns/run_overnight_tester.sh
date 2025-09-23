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
CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
# PROC_DIR="FreshStart"
PROC_DIR="FreshStart_mini"
# META_FILE="collatedData.xlsx"
CSV_LOC="/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_mini/full/data"
CSV_OUTDIR="/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_mini/full/data_MergedANandPO"
###################
# Raw Preprocessing
###################

# echo "🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting preprocRaw_AN.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preprocRaw/preprocRaw_AN.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ preprocRaw_AN.py  completed at $(date)" | tee -a "$LOG_FILE"


# echo "🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting preprocRaw_PO.py  at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preprocRaw/preprocRaw_PO.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ preprocRaw_PO.py completed at $(date)" | tee -a "$LOG_FILE"

# ##################################################################################################################
# # Initial Event Segmentation (Glia Setting) Used for PO Alignment to AN data & All data to Raspberry Pi .log files 
# ##################################################################################################################

# echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
# echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for AN GLIA setting at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py" \
#   --trueRootDir "$TRUE_BASE_DIR" \
#   --procDir "$PROC_DIR" \
#   --role AN \
#   --segment-type glia \
#   --allowed-status complete \
#   --allowed-status truncated \
#   >> "$LOG_FILE" 2>&1
# echo "✅ preFrontalCortex_unifiedEventSeg.py for AN GLIA setting completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
# echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for PO GLIA setting at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py" \
#   --trueRootDir "$TRUE_BASE_DIR" \
#   --procDir "$PROC_DIR" \
#   --role PO \
#   --segment-type glia \
#   --allowed-status complete \
#   --allowed-status truncated \
#   >> "$LOG_FILE" 2>&1
# echo "✅ preFrontalCortex_unifiedEventSeg.py for PO GLIA setting completed at $(date)" | tee -a "$LOG_FILE"


# ###########################
# # PO Alignment to AN data 
# ###########################

# echo "🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢🦢" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 alignPO2AN_part1.py  at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/alignPO2AN.py" \
#     --root-dir "$TRUE_BASE_DIR" \
#     --base-dir "$PROC_DIR" \
#     >> "$LOG_FILE" 2>&1
# echo "✅ alignPO2AN_part1.py  completed at $(date)" | tee -a "$LOG_FILE"


#########################################################################################################
#                              Possibly Needed Section for Mark Alignment                              ##

# ##################################################################
# # Back Propagation of Physio Alignment Times to Magic Leap Files 
# ##################################################################


# New script here 


##                             End of Possibly Needed Section for Mark Alignment                        ##
##########################################################################################################




#######################################################################################################
# Event Segmentation (Full Setting) Used for segmenting of major events for both PO & AN participants 
#######################################################################################################

# echo "🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞🐞" | tee -a "$LOG_FILE"
# echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for AN FULL setting at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py" \
#   --trueRootDir "$TRUE_BASE_DIR" \
#   --procDir "$PROC_DIR" \
#   --role AN \
#   --segment-type full \
#   --allowed-status complete \
#   --allowed-status truncated \
#   >> "$LOG_FILE" 2>&1
# echo "✅ preFrontalCortex_unifiedEventSeg.py for AN FULL setting completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for PO FULL setting at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py" \
#   --trueRootDir "$TRUE_BASE_DIR" \
#   --procDir "$PROC_DIR" \
#   --role PO \
#   --segment-type full \
#   --allowed-status complete \
#   --allowed-status truncated \
#   >> "$LOG_FILE" 2>&1
# echo "✅ preFrontalCortex_unifiedEventSeg.py for PO FULL setting completed at $(date)" | tee -a "$LOG_FILE"


#######################################################################################################################
# Event Augmentation Pipeline (Flattening, Adding Positions, Elapsed Times, Walk Duration, Coin Labels, & Route Types)
#######################################################################################################################
# echo "🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting justFlatten.py  at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/justFlatten.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ justFlatten.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting add_coin_labels_from_collated.py  at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/add_coin_labels_from_collated.py" \
#   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --coin-sets "${TRUE_BASE_DIR}/CoinSets.csv" \
#   --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/full/Events_Flattened" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/full/Events_CoinsLabeled" \
#   --pattern "eventsFlat" \
#   --sheet "MagicLeapFiles" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ add_coin_labels_from_collated.py completed at $(date)" | tee -a "$LOG_FILE"


# ## Fix so that all walks are computed.

# echo "🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting computeWalks.py  at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/computeWalks.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --events-dir-name "Events_CoinsLabeled" \
#   --meta-dir-name "MetaData_Flat" \
#   --output-dir-name "Events_ComputedWalks" \
#   --eventsEnding "events_coinLabel" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ computeWalks.py completed at $(date)" | tee -a "$LOG_FILE"



# echo "🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆🦆" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting mergeWalks.py  at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/mergeWalks.py" \
#     --root-dir "$TRUE_BASE_DIR" \
#     --proc-dir "$PROC_DIR" \
#     --events-dir Events_CoinsLabeled \
#     --output-dir Events_MergedWalks \
#     --eventsEnding "events_coinLabel" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mergeWalks.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting getPositionsAndElapsedTime.py for NO WALKS at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/getPositionsAndElapsedTime.py" \
#   --events-dir "$TRUE_BASE_DIR/$PROC_DIR/full/Events_CoinsLabeled" \
#   --processed-dir "$TRUE_BASE_DIR/$PROC_DIR/ProcessedData_Flat" \
#   --out-dir "$TRUE_BASE_DIR/$PROC_DIR/full/Events_Final_NoWalks" \
#   --pattern '*_events_coinLabel.csv' \
#   >> "$LOG_FILE" 2>&1
# echo "✅ getPositionsAndElapsedTime.py for NO WALKS completed at $(date)" | tee -a "$LOG_FILE"



# echo "🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧🐧" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting getPositionsAndElapsedTime.py for WALKS at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/getPositionsAndElapsedTime.py" \
#   --events-dir "$TRUE_BASE_DIR/$PROC_DIR/full/Events_MergedWalks" \
#   --processed-dir "$TRUE_BASE_DIR/$PROC_DIR/ProcessedData_Flat" \
#   --out-dir "$TRUE_BASE_DIR/$PROC_DIR/full/Events_Final_MergedWalks" \
#   --pattern '*_events_with_walks.csv' \
#   >> "$LOG_FILE" 2>&1
# echo "✅ getPositionsAndElapsedTime.py for WALKS completed at $(date)" | tee -a "$LOG_FILE"


## add in stacking paths by chestPinNum per Round
## add in match position to start positions 
## add in classification of walks 

# ####################################
# # Magic Leap Alignment to RPi Marks 
# ####################################
# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting batch_split_pipeline.py at $(date)" | tee -a "$LOG_FILE"

# python ${CODE_DIR}baseline_pipeline/alignment/batch_split_pipeline2.py \
#   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --device-ip-map "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/configs/DeviceIPAddresses.txt" \
#   --code-dir "${CODE_DIR}baseline_pipeline/alignment" \
#   --base-dir "${TRUE_BASE_DIR}" \
#   --proc-dir "${PROC_DIR}" \
#   --events-dir-name "${EVENTS_DIR}" \
#   --csv-timestamp-column mLTimestamp \
#   --event-type-column lo_eventType \
#   --timezone-offset auto \
#   --sheet MagicLeapFiles \
#   --out-dir ML_RPi_Aligned \
#   --strip-ml-suffixes "_events_final,_processed,_events_final.csv,_processed.csv" \
#   --only-rows-with-rpi \
#   --debug \
#   >> "$LOG_FILE" 2>&1
# echo "✅ batch_split_pipeline.py completed at $(date)" | tee -a "$LOG_FILE"

echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting mergeANPOevents2.py at $(date)" | tee -a "$LOG_FILE"

python ${CODE_DIR}/eventAugmentation/mergeANPOevents2.py \
  --an "${CSV_LOC}/ObsReward_A_02_17_2025_15_11_events_final.csv" \
  --po "${CSV_LOC}/ObsReward_B_02_17_2025_15_11_events_final.csv" \
  --outdir "${CSV_OUTDIR}" \
  --estimate-missing-po \
  >> "$LOG_FILE" 2>&1
echo "✅ mergeANPOevents2.py completed at $(date)" | tee -a "$LOG_FILE"
