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
PROC_DIR="FreshStart"
# PROC_DIR="FreshStart_mini"
# META_FILE="collatedData.xlsx"

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

echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for PO FULL setting at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py" \
  --trueRootDir "$TRUE_BASE_DIR" \
  --procDir "$PROC_DIR" \
  --role PO \
  --segment-type full \
  --allowed-status complete \
  --allowed-status truncated \
  >> "$LOG_FILE" 2>&1
echo "✅ preFrontalCortex_unifiedEventSeg.py for PO FULL setting completed at $(date)" | tee -a "$LOG_FILE"


#######################################################################################################################
# Event Augmentation Pipeline (Flattening, Adding Positions, Elapsed Times, Walk Duration, Coin Labels, & Route Types)
#######################################################################################################################
echo "🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting justFlatten.py  at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/justFlatten.py" \
  --root-dir "$TRUE_BASE_DIR" \
  --proc-dir "$PROC_DIR" \
  >> "$LOG_FILE" 2>&1
echo "✅ justFlatten.py completed at $(date)" | tee -a "$LOG_FILE"

echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting add_coin_labels_from_collated.py  at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/add_coin_labels_from_collated.py" \
  --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
  --coin-sets "${TRUE_BASE_DIR}/CoinSets.csv" \
  --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/full/Events_Flattened" \
  --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/full/Events_CoinsLabeled" \
  --pattern "eventsFlat" \
  --sheet "MagicLeapFiles" \
  >> "$LOG_FILE" 2>&1
echo "✅ add_coin_labels_from_collated.py completed at $(date)" | tee -a "$LOG_FILE"


## Fix so that all walks are computed.

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



### Too far
# ################
# # Postprocessing
# ################

## Add in post match to Marks align 

####################
## Analysis Merges
####################
# echo "🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄🦄" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting mergeEventsV3.py for PARTICIPANT-ROLE-COINSET at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/mergeEventsV3.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --event-dir "Events_Final_NoWalks" \
#   --eventEnding "_events_final.csv" \
#   --outEnding "_events.csv" \
#   --output-dir "Merged_PtRoleCoinSet" \
#   --group-key-fields participantID \
#   --group-key-fields currentRole \
#   --group-key-fields coinSet \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mergeEventsV3.py for PARTICIPANT-ROLE-COINSET completed at $(date)" | tee -a "$LOG_FILE"

# echo "💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting mergeEventsV3.py for PAIR-TEST_DATE-SESSION at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/mergeEventsV3.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --event-dir "Events_AugFinal_withWalks" \
#   --eventEnding "_events_final.csv" \
#   --outEnding "_events.csv" \
#   --output-dir "Merged_PairTestDateSession" \
#   --group-key-fields pairID \
#   --group-key-fields testingDate \
#   --group-key-fields sessionType \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mergeEventsV3.py for PAIR-TEST_DATE-SESSION completed at $(date)" | tee -a "$LOG_FILE"

# echo "🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting mergeEventsV3.py for BASIC-GRANULAR at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/mergeEventsV3.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --event-dir "Events_AugFinal_withWalks" \
#   --eventEnding "_events_final.csv" \
#   --outEnding "_events.csv" \
#   --output-dir "Merged_BasicGranular" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mergeEventsV3.py for BASIC-GRANULAR completed at $(date)" | tee -a "$LOG_FILE"


# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting mergeEventsV3.py for PARTICIPANT ROLE COIN INTERACTIONS at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/mergeEventsV3.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --event-dir "Events_Final_NoWalks" \
#   --eventEnding "_events_final.csv" \
#   --outEnding "_events.csv" \
#   --output-dir "Merged_PtRoleCoinTypes" \
#   --group-key-fields participantID \
#   --group-key-fields currentRole \
#   --group-key-fields coinLabel \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mergeEventsV3.py for PARTICIPANT ROLE COIN INTERACTIONS completed at $(date)" | tee -a "$LOG_FILE"

# echo '✨ done ✨' | tee -a "$LOG_FILE"


# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting mergeEventsV3.py for COIN INTERACTIONS at $(date)" | tee -a "$LOG_FILE"
# # add to run_overnight_modular3.sh after add_coin_labels_from_collated.py
# python "${CODE_DIR}/plotting/extract_pin_drops.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --input-dir-name "Events_CoinsLabeled" \
#   --pattern "*_events_coinLabel.csv" \
#   --pin-event "PinDrop_Moment" \
#   --out-dir-name "PinDrops_All" \
#   --split-on coinLabel

# echo "✅ mergeEventsV3.py for PARTICIPANT ROLE COIN INTERACTIONS completed at $(date)" | tee -a "$LOG_FILE"

# echo '✨ done ✨' | tee -a "$LOG_FILE"


# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting flexiblePlotByCoinType2 for COIN INTERACTIONS at $(date)" | tee -a "$LOG_FILE"
# # add to run_overnight_modular3.sh after add_coin_labels_from_collated.py
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/flexiblePlotByCoinType2.py" \
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



# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting histoWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/histoWrapper.py" \
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



# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting histoWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/histoWrapper.py" \
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

# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/pinDropWrapper.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL.csv" \
#   --pattern "*_events.csv" \
#   --recursive \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "coinSet" \
#   >> "$LOG_FILE" 2>&1

# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# echo '✨ done ✨' | tee -a "$LOG_FILE"

# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/pinDropWrapper.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL.csv" \
#   --pattern "*_events.csv" \
#   --recursive \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "" \
#   >> "$LOG_FILE" 2>&1

# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# echo '✨ done ✨' | tee -a "$LOG_FILE"



# echo "💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "" \
#   --use-outlier-filter \
#   --filter-columns dropDist \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"



# echo "💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_Time" \
#   --formats "pdf" \
#   --voi "trueSession_elapsed_s" \
#   --voi-str "Round Elapsed Time" \
#   --voi-unit "(s)" \
#   --facet-by "" \
#   --use-outlier-filter \
#   --filter-columns trueSession_elapsed_s \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# # #!/usr/bin/env bash
# set -euo pipefail

# LOG_FILE="/tmp/pinDrop_allsubjects_$(date +%Y%m%d_%H%M%S).log"

# echo "💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"

# PY="/usr/bin/python3"   # or just 'python' if your env is set

# $PY "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/pinDropWrapper.py" \
#   --input "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/PinDrops_All/PinDrops_ALL.csv" \
#   --out-root "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/AllSubjects_Out" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "coinSet" \
#   >> "$LOG_FILE" 2>&1

# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"
# echo '✨ done ✨' | tee -a "$LOG_FILE"
