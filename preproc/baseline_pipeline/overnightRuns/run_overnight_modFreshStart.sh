#!/usr/bin/env bash
set -Eeuo pipefail

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/modFreshStart_processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

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
PROC_DIR="FreshStart_redoAgainAgain"
#PROC_DIR="FreshStart_redoAgainSingle"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Pos"

# # ###################
# # # Raw Preprocessing
# # ###################

# echo "🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑" | tee -a "$LOG_FILE"
# echo "🚀 Starting preprocRaw_AN.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preprocRaw/preprocRaw_AN.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ preprocRaw_AN.py  completed at $(date)" | tee -a "$LOG_FILE"


# # echo "🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼" | tee -a "$LOG_FILE"
# # echo "" | tee -a "$LOG_FILE"
# # echo "🚀 Starting preprocRaw_PO.py  at $(date)" | tee -a "$LOG_FILE"
# # python "${CODE_DIR}/preprocRaw/preprocRaw_PO.py" \
# #   --root-dir "$TRUE_BASE_DIR" \
# #   --proc-dir "$PROC_DIR" \
# #   >> "$LOG_FILE" 2>&1
# # echo "✅ preprocRaw_PO.py completed at $(date)" | tee -a "$LOG_FILE"

# # #################################################################################################################
# # Initial Event Segmentation (Glia Setting) Used for PO Alignment to AN data & All data to Raspberry Pi .log files 
# # #################################################################################################################

# echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
# echo "Look Right Here Myra" | tee -a "$LOG_FILE"
# echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for AN at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py" \
#   --trueRootDir "$TRUE_BASE_DIR" \
#   --procDir "$PROC_DIR" \
#   --role AN \
#   --allowed-status complete \
#   --allowed-status truncated \
#   >> "$LOG_FILE" 2>&1
# echo "✅ preFrontalCortex_unifiedEventSeg.py for AN completed at $(date)" | tee -a "$LOG_FILE"

# # echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
# # echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for PO  setting at $(date)" | tee -a "$LOG_FILE"
# # python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py" \
# #   --trueRootDir "$TRUE_BASE_DIR" \
# #   --procDir "$PROC_DIR" \
# #   --role PO \
# #   --allowed-status complete \
# #   --allowed-status truncated \
# #   >> "$LOG_FILE" 2>&1
# # echo "✅ preFrontalCortex_unifiedEventSeg.py for PO completed at $(date)" | tee -a "$LOG_FILE" 


# ###########################################################################
# # Event Augmentation Pipeline | Flattening Events' Details Column of Dict's 
# ###########################################################################
# echo "🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting justFlatten.py  at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/justFlatten.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ justFlatten.py completed at $(date)" | tee -a "$LOG_FILE"

# ##################################################
# # Event Augmentation Pipeline | Adding Coin Labels
# ##################################################
# echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting add_coin_labels_from_collated.py  at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/add_coin_labels_from_collated.py" \
#   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --coin-sets "${TRUE_BASE_DIR}/CoinSets.csv" \
#   --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Flattened" \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_CoinsLabeled" \
#   --pattern "eventsFlat" \
#   --sheet "MagicLeapFiles" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ add_coin_labels_from_collated.py completed at $(date)" | tee -a "$LOG_FILE"

# ##########################
# # Create effectiveRoundNum 
# ##########################
# echo "🚀 add_effective_roundnum.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/add_effective_roundnum.py" \
#   --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_CoinsLabeled" \
#   --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_EffRoundNum" \
#   --in-suffix "_events_coinLabel.csv" \
#   --out-suffix "_events_effRoundNum.csv" \
#   --pattern "*_events_coinLabel.csv" \
#   --overwrite \
#   >> "$LOG_FILE" 2>&1
# echo "✅ add_effective_roundnum.py completed at $(date)" | tee -a "$LOG_FILE" 

# ######################################################################
# # Merge/reshape lo_eventTypes (CoinVis/ChestVis/SwapVoteTextVis pairs)
# ######################################################################
# echo "🚀 combine_event_pairs.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/combine_event_pairs.py" \
#   --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_EffRoundNum" \
#   --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_MergedPairs" \
#   --pattern "*_events_effRoundNum.csv" \
#   --in-suffix "_events_effRoundNum.csv" \
#   --out-suffix "_events_pairsMerged.csv" \
#   --overwrite \
# >> "$LOG_FILE" 2>&1
# echo "✅ combine_event_pairs.py completed at $(date)" | tee -a "$LOG_FILE" 


# ####################################################################################
# # Event Augmentation Pipeline | Adding HeadPosAnchored & HeadForthAnchored Positions
# ####################################################################################
# echo "🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting getPositions.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/getPositions.py" \
#   --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_MergedPairs" \
#   --processed-dir "$TRUE_BASE_DIR/$PROC_DIR/ProcessedData_Flat" \
#   --out-dir "$TRUE_BASE_DIR/$PROC_DIR/EventSegmentation/Events_Pos" \
#   --pattern '*_events_pairsMerged.csv' \
#   >> "$LOG_FILE" 2>&1
# echo "✅ getPositions.py completed at $(date)" | tee -a "$LOG_FILE"

# ####################################################################
# # ReProcessing Pipeline |  00_ Batch Running Reproc Scripts 01 - 04
# ####################################################################
# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting 00_batch_reproc_steps_01_04.py at $(date)" | tee -a "$LOG_FILE"
# echo "            01_build_intervals : Build Block/Round Interval Tables - Single Row for Every True Round"
# echo "            02_reproc_processed_add_elapsed_and_distance : Add Elapsed Time & Distance to _processed.csv to make _PreLimReprocessed.csv files"
# echo "            03_compute_speed : Compute Speed to Make Final _reprocessed.csv files"
# echo "            04_finalize_events_and_intervals_with_pindrops (no startPos here): Fleshing out Block/Round Interval Tables & Events Files with More Dist & Time Info"
# python "${CODE_DIR}/reproc/00_batch_reproc_steps_01_04.py" \
#   --events-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Pos" \
#   --processed-root "${TRUE_BASE_DIR}/${PROC_DIR}/ProcessedData_Flat" \
#   --intervals-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Intervals" \
#   --prelim-reproc-dir "${TRUE_BASE_DIR}/${PROC_DIR}/PreLimReProcessedData_Flat" \
#   --reproc-dir "${TRUE_BASE_DIR}/${PROC_DIR}/ReProcessedData_Flat" \
#   --events-pre-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsPreReproc" \
#   --events-reproc-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsReproc" \
#   --pattern "*_events_pos.csv" \
#   --max-round 100 \
#   --round-mode auto \
#   >> "$LOG_FILE" 2>&1
# echo "✅ 00_batch_reproc_steps_01_04.py completed at $(date)" | tee -a "$LOG_FILE"

#######################################################################################
# Reprocessing Pipeline | Adding Instantaneous Distance Calcs to _reprocessed.csv files
#######################################################################################
echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
echo "🚀 Starting instantDistCalc.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/reproc/instantDistCalc.py" \
  --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/ReProcessedData_Flat" \
  --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/ReProc_withDist" \
  --coinsets "${TRUE_BASE_DIR}/CoinSets.csv" \
  --events-dir ${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsReproc\
  --events-suffix "_event_reproc.csv" \
  --overwrite \
  >> "$LOG_FILE" 2>&1
echo "✅ instantDistCalc.py completed at $(date)" | tee -a "$LOG_FILE"
# directory mode
#python add_distances.py --input-dir processed/ --output-dir out/ --events-dir events/ --events-suffix "_events.csv" --coinsets CoinSets.csv

#######################################################################
# ReProcessing Pipeline |  Adding StartPos to all PinDrop_Moment events
#######################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting 05_batch_add_startpos.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/reproc/05_batch_add_startpos.py" \
  --root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsReproc" \
  --pattern "*_event_reproc.csv" \
  --interval-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventIntervals" \
  --also-update-intervals \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsAlmostFinal" \
  >> "$LOG_FILE" 2>&1
echo "✅ 05_batch_add_startpos.py completed at $(date)" | tee -a "$LOG_FILE"

######################################################################################
# ReProcessing Pipeline |  Propagating PinDrop Start Positions throughout Events files
######################################################################################
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🚀 Starting 06b_propagate_pindrop_startpos.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/reproc/06b_propagate_pindrop_startpos.py" \
  --root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsAlmostFinal" \
  --pattern "*_withStartPos.csv"  \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsStartPos" \
  --interval-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventIntervals" \
  --also-update-intervals
  >> "$LOG_FILE" 2>&1
echo "✅ 06b_propagate_pindrop_startpos.py completed at $(date)" | tee -a "$LOG_FILE"


##########################################################################################################
# Event Augmentation Pipeline | Generating New Walk Events within a Round (i.e. Walk_PinDrop & Walk_Chest)
##########################################################################################################
echo "🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting computeWalks.py  at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/computeWalks.py" \
  --root-dir "$TRUE_BASE_DIR" \
  --proc-dir "$PROC_DIR" \
  --events-dir-name "EventsStartPos" \
  --meta-dir-name "MetaData_Flat" \
  --output-dir-name "Events_ComputedWalks" \
  --eventsEnding "startPosPropagated" \
  >> "$LOG_FILE" 2>&1
echo "✅ computeWalks.py completed at $(date)" | tee -a "$LOG_FILE"

######################################################################################################################################
# Event/Interval Augmentation Pipeline | Adding Missing pos/time/dist cols for _start/_end events in *_eventsWalks using *_reprocessed
######################################################################################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting fillEventsPosWalks.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/fillEventsPosWalks.py" \
  --events-walks-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsMergedWalks" \
  --reprocessed-dir "${TRUE_BASE_DIR}/${PROC_DIR}/ReProc_withDist" \
  --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventWalksFilled" \
  --pattern "*_eventsWalks.csv" \
  --debug \
  --overwrite \
  >> "$LOG_FILE" 2>&1
echo "✅ fillEventsPosWalks.py completed at $(date)" | tee -a "$LOG_FILE"


###################################################################################################################################################
# Event/Interval Augmentation Pipeline | Augmenting Intervals with more Static Block/Round Info from Events Files (i.e. CoinSetID, coinSet, etc...)
###################################################################################################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting buildIntervalsFromEvents.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/buildIntervalsFromEvents.py" \
  --interval-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Intervals" \
  --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventWalksFilled" \
  --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Event_Intervals_Almost" \
  --pattern "*_finalInterval_vert.csv" \
  --overwrite \
  >> "$LOG_FILE" 2>&1
echo "✅ buildIntervalsFromEvents.py completed at $(date)" | tee -a "$LOG_FILE"

#############################################################################################################################################################
# Event/Interval Augmentation Pipeline | Propagate Interval-level columns into *_filled.csv Event Files (i.e. round_start_origRow, round_end_AppTime, etc...)
#############################################################################################################################################################

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting propagateIntervalCols.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/propagateIntervalCols.py" \
  --filled-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventWalksFilled" \
  --interval-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Event_Intervals_Almost" \
  --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MostlyFilledEvents" \
  --pattern "*_filled.csv" \
  --overwrite \
 >> "$LOG_FILE" 2>&1
echo "✅ propagateIntervalCols.py completed at $(date)" | tee -a "$LOG_FILE"

#############################################################################################################################################################
# Event/Interval Augmentation Pipeline | Propagate Interval-level columns into *_filled.csv Event Files (i.e. round_start_origRow, round_end_AppTime, etc...)
#############################################################################################################################################################

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting attach_walk_pindrop_metrics.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/attach_walk_pindrop_metrics.py" \
  --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MostlyFilledEvents" \
  --events-pattern "*_filled_intervalProps.csv" \
  --interval-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Event_Intervals_Almost" \
  --interval-pattern "*_interval_fromEvents.csv" \
  --outdir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/NearlyFilledEventsIntervals" \
  --out-suffix "_nearlyFilledInterval.csv" \
  --overwrite \
 >> "$LOG_FILE" 2>&1
echo "✅ attach_walk_pindrop_metrics.py completed at $(date)" | tee -a "$LOG_FILE"

echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting attach_walk_chest_metrics.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/attach_walk_chest_metrics.py" \
  --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/MostlyFilledEvents" \
  --events-pattern "*_filled_intervalProps.csv" \
  --interval-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/NearlyFilledEventsIntervals" \
  --interval-pattern "*_nearlyFilledInterval.csv" \
  --outdir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FilledEventsIntervals" \
  --out-suffix "_filledIntervals.csv" \
  --overwrite \
 >> "$LOG_FILE" 2>&1
echo "✅ attach_walk_chest_metrics.py completed at $(date)" | tee -a "$LOG_FILE"



##########################################################################################
# Event/Interval Augmentation Pipeline | Assigning Path Utility & Path Efficiency to Walks
##########################################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting assign_norm_util_and_efficiency.py Lambda 1 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/assign_norm_util_and_efficiency.py" \
  --main-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FilledEventsIntervals" \
  --main-pattern "*_filledIntervals.csv" \
  --ref-dir "/Users/mairahmac/Desktop/TriangleSets/RoutePlanWeightUtility/pathUtility_norm_L1" \
  --ref-pattern "all_orders__layout_*_L1_normUtil.csv" \
  --output "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L1" \
  --out-mode dir \
  --suffix "_normUtil_L1" \
  --overwrite \
  >> "$LOG_FILE" 2>&1
echo "✅ assign_norm_util_and_efficiency.py completed at $(date)" | tee -a "$LOG_FILE"


#########################################################################################
# Event/Interval Augmentation Pipeline | Assigning Path Utility & Path Efficiency to Walks
#########################################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting group_and_append.py Lambda 1 at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/group_and_append.py" \
    --meta "${TRUE_BASE_DIR}/collatedData.xlsx" \
    --sheet 0 \
    --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsUtil_L1" \
    --suffix "_filledIntervals_normUtil_L1.csv" \
    --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FullIntervals_L1" \
    --id-cols participantID pairID testingDate sessionType main_RR currentRole \
    --order-col testingOrder \
    --cleanedfile-col cleanedFile \
    --manifest --group-json --check-columns --check-order --check-empty \
    >> "$LOG_FILE" 2>&1
echo "✅ group_and_append.py Lambda 1 completed at $(date)" | tee -a "$LOG_FILE"


# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting add_session_running_totals.py Lambda 1 at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/add_session_running_totals.py" \
#   --input_dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/FullIntervals_L1" \
#   --output_dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/totBlockRounds_L1" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ add_session_running_totals.py Lambda 1 completed at $(date)" | tee -a "$LOG_FILE"


# # ### python "${CODE_DIR}/eventAugmentation/scan_missing_keys.py" --input_dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/totBlockRounds_L1" --output_dir "${TRUE_BASE_DIR}/${PROC_DIR}"

# # # ## Had to delete stray rows where they silently failed the truncated screening earlier upstream 
# # # ## - 101 - Looks like it was because we accidentally pushed too far in the blocks
# # ## - 8888 - No notes on it on surface level
# # ## - R019 - It happened in the tutorial section, I think we accidentally pushed too far and had to restart


# echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
# echo "🚀 Starting add_swap_rates.py Lambda 1 at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/add_swap_rates.py" \
#   --input_dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/totBlockRounds_L1" \
#   --output_dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/swapRate_L1" \
#   --recent_trials 9 \
# >> "$LOG_FILE" 2>&1
# echo "✅ add_swap_rates.py Lambda 1 completed at $(date)" | tee -a "$LOG_FILE"