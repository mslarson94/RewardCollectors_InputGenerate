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
PROC_DIR="FreshStart_redoAgain"
#PROC_DIR="FreshStart_redoAgainSingle"
META_FILE="collatedData.xlsx"
EVENTS_DIR="Events_Pos"
###################
# Raw Preprocessing
###################

echo "🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑" | tee -a "$LOG_FILE"
echo "🚀 Starting preprocRaw_AN.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preprocRaw/preprocRaw_AN.py" \
  --root-dir "$TRUE_BASE_DIR" \
  --proc-dir "$PROC_DIR" \
  >> "$LOG_FILE" 2>&1
echo "✅ preprocRaw_AN.py  completed at $(date)" | tee -a "$LOG_FILE"


echo "🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting preprocRaw_PO.py  at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/preprocRaw/preprocRaw_PO.py" \
  --root-dir "$TRUE_BASE_DIR" \
  --proc-dir "$PROC_DIR" \
  >> "$LOG_FILE" 2>&1
echo "✅ preprocRaw_PO.py completed at $(date)" | tee -a "$LOG_FILE"

# #################################################################################################################
# Initial Event Segmentation (Glia Setting) Used for PO Alignment to AN data & All data to Raspberry Pi .log files 
# #################################################################################################################

echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
echo "Look Right Here Myra" | tee -a "$LOG_FILE"
echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for AN at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py" \
  --trueRootDir "$TRUE_BASE_DIR" \
  --procDir "$PROC_DIR" \
  --role AN \
  --allowed-status complete \
  --allowed-status truncated \
  >> "$LOG_FILE" 2>&1
echo "✅ preFrontalCortex_unifiedEventSeg.py for AN completed at $(date)" | tee -a "$LOG_FILE"

echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for PO  setting at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py" \
  --trueRootDir "$TRUE_BASE_DIR" \
  --procDir "$PROC_DIR" \
  --role PO \
  --allowed-status complete \
  --allowed-status truncated \
  >> "$LOG_FILE" 2>&1
echo "✅ preFrontalCortex_unifiedEventSeg.py for PO completed at $(date)" | tee -a "$LOG_FILE" 


###########################################################################
# Event Augmentation Pipeline | Flattening Events' Details Column of Dict's 
###########################################################################
echo "🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting justFlatten.py  at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/justFlatten.py" \
  --root-dir "$TRUE_BASE_DIR" \
  --proc-dir "$PROC_DIR" \
  >> "$LOG_FILE" 2>&1
echo "✅ justFlatten.py completed at $(date)" | tee -a "$LOG_FILE"

##################################################
# Event Augmentation Pipeline | Adding Coin Labels
##################################################
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting add_coin_labels_from_collated.py  at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/add_coin_labels_from_collated.py" \
  --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
  --coin-sets "${TRUE_BASE_DIR}/CoinSets.csv" \
  --events-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Flattened" \
  --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_CoinsLabeled" \
  --pattern "eventsFlat" \
  --sheet "MagicLeapFiles" \
  >> "$LOG_FILE" 2>&1
echo "✅ add_coin_labels_from_collated.py completed at $(date)" | tee -a "$LOG_FILE"

##########################
# Create effectiveRoundNum 
##########################
echo "🚀 add_effective_roundnum.py at $(date)" | tee -a "$LOG_FILE"

######################################################################
# Merge/reshape lo_eventTypes (CoinVis/ChestVis/SwapVoteTextVis pairs)
######################################################################
echo "🚀 combine_event_pairs.py at $(date)" | tee -a "$LOG_FILE"

####################################################################################
# Event Augmentation Pipeline | Adding HeadPosAnchored & HeadForthAnchored Positions
####################################################################################
echo "🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦🦦" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting getPositions.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/getPositions.py" \
  --events-dir "$TRUE_BASE_DIR/$PROC_DIR/EventSegmentation/Events_CoinsLabeled" \
  --processed-dir "$TRUE_BASE_DIR/$PROC_DIR/ProcessedData_Flat" \
  --out-dir "$TRUE_BASE_DIR/$PROC_DIR/EventSegmentation/Events_Pos" \
  --pattern '*_events_coinLabel.csv' \
  >> "$LOG_FILE" 2>&1
echo "✅ getPositions.py completed at $(date)" | tee -a "$LOG_FILE"

####################################################################
# ReProcessing Pipeline |  00_ Batch Running Reproc Scripts 01 - 04
####################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting 00_batch_reproc_steps_01_04.py at $(date)" | tee -a "$LOG_FILE"
echo "            01_build_intervals : Build Block/Round Interval Tables - Single Row for Every True Round"
echo "            02_reproc_processed_add_elapsed_and_distance : Add Elapsed Time & Distance to _processed.csv to make _PreLimReprocessed.csv files"
echo "            03_compute_speed : Compute Speed to Make Final _reprocessed.csv files"
echo "            04_finalize_events_and_intervals_with_pindrops (no startPos here): Fleshing out Block/Round Interval Tables & Events Files with More Dist & Time Info"
python "${CODE_DIR}/reproc/00_batch_reproc_steps_01_04.py" \
  --events-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_CoinsLabeled" \
  --processed-root "${TRUE_BASE_DIR}/${PROC_DIR}/ProcessedData_Flat" \
  --intervals-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/Events_Pos" \
  --prelim-reproc-dir "${TRUE_BASE_DIR}/${PROC_DIR}/PreLimReProcessedData_Flat" \
  --reproc-dir "${TRUE_BASE_DIR}/${PROC_DIR}/ReProcessedData_Flat" \
  --events-pre-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsPreReproc" \
  --events-reproc-dir "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsReproc" \
  --pattern "*_events_pos.csv" \
  --max-round 100 \
  --round-mode auto \
  >> "$LOG_FILE" 2>&1
echo "✅ 00_batch_reproc_steps_01_04.py completed at $(date)" | tee -a "$LOG_FILE"

#######################################################################################
# Reprocessing Pipeline | Adding Instantaneous Distance Calcs to _reprocessed.csv files
#######################################################################################
echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
echo "🚀 Starting instantDistCalc.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/reproc/instantDistCalc.py" \
  --input-dir "${TRUE_BASE_DIR}/${PROC_DIR}/ReProcessedData_Flat" \
  --output-dir "${TRUE_BASE_DIR}/${PROC_DIR}/ReProc_withDist" \
  --overwrite \
  >> "$LOG_FILE" 2>&1
echo "✅ instantDistCalc.py completed at $(date)" | tee -a "$LOG_FILE"

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


echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🚀 Starting 06b_propagate_pindrop_startpos.py at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/reproc/06b_propagate_pindrop_startpos.py" \
  --root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsAlmostFinal" \
  --pattern "*_withEffectiveRound.csv"  \
  --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventsStartPos" \
  --interval-root "${TRUE_BASE_DIR}/${PROC_DIR}/EventSegmentation/EventIntervals" \
  --also-update-intervals
  >> "$LOG_FILE" 2>&1
echo "✅ 06b_propagate_pindrop_startpos.py completed at $(date)" | tee -a "$LOG_FILE"

##### Run after running runReProc_1.sh

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

## add in stacking paths by chestPinNum per Round
## add in match position to start positions 
## add in classification of walks 

# # ####################################
# # # RPi Mark PreProc
# # ####################################

# echo "🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸" | tee -a "$LOG_FILE"
# echo "🚀 Starting read_rpi_logs_to_csv.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/read_rpi_logs_to_csv.py \
#   --log_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RawData/pair_200/03_17_2025/Morning/RPi/BioPac_RPi" \
#   --out_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/BioPac/RPi_simple_raw" \
#  >> "$LOG_FILE" 2>&1
# echo "✅ read_rpi_logs_to_csv.py completed at $(date)" | tee -a "$LOG_FILE" 

# echo "🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸" | tee -a "$LOG_FILE"
# echo "🚀 Starting read_rpi_logs_to_csv.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/read_rpi_logs_to_csv.py \
#   --log_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RawData/pair_200/03_17_2025/Afternoon/RPi/BioPac_RPi" \
#   --out_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/BioPac/RPi_simple_raw" \
#  >> "$LOG_FILE" 2>&1
# echo "✅ read_rpi_logs_to_csv.py completed at $(date)" | tee -a "$LOG_FILE" 

# echo "🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸" | tee -a "$LOG_FILE"
# echo "🚀 Starting read_rpi_logs_to_csv.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/read_rpi_logs_to_csv.py \
#   --log_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RawData/pair_200/03_17_2025/Morning/RPi/RNS_RPi" \
#   --out_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_simple_raw" \
#  >> "$LOG_FILE" 2>&1
# echo "✅ read_rpi_logs_to_csv.py completed at $(date)" | tee -a "$LOG_FILE" 

# echo "🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸" | tee -a "$LOG_FILE"
# echo "🚀 Starting read_rpi_logs_to_csv.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/read_rpi_logs_to_csv.py \
#   --log_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RawData/pair_200/03_17_2025/Afternoon/RPi/RNS_RPi" \
#   --out_dir "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_simple_raw" \
#  >> "$LOG_FILE" 2>&1
# echo "✅ read_rpi_logs_to_csv.py completed at $(date)" | tee -a "$LOG_FILE" 

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting rpi_preproc_pipeline.py at $(date)" | tee -a "$LOG_FILE"

# python ${CODE_DIR}/alignment/rpi_preproc_pipeline.py \
#   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --device-ip-map "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/configs/DeviceIPAddresses.txt" \
#   --code-dir "${CODE_DIR}/alignment" \
#   --base-dir "${TRUE_BASE_DIR}" \
#   --proc-dir "${PROC_DIR}" \
#   --timezone-offset auto \
#   --sheet MagicLeapFiles \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/"  \
#   --marks-timestamp-col "RPi_Time_verb" \
#   --strip-ml-suffixes "_events_final,_processed,_events_final.csv,_processed.csv" \
#   --only-rows-with-rpi \
#   --dedupe-sec 0.05 \
#   --debug \
#   >> "$LOG_FILE" 2>&1
# echo "✅ rpi_preproc_pipeline.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting rpi_preproc_pipeline3.py at $(date)" | tee -a "$LOG_FILE"

# python ${CODE_DIR}/alignment/rpi_preproc_pipeline3.py \
#   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --device-ip-map "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/configs/DeviceIPAddresses.txt" \
#   --code-dir "${CODE_DIR}/alignment" \
#   --base-dir "${TRUE_BASE_DIR}" \
#   --proc-dir "${PROC_DIR}" \
#   --timezone-offset auto \
#   --sheet MagicLeapFiles \
#   --out-dir "${TRUE_BASE_DIR}/${PROC_DIR}/"  \
#   --marks-timestamp-col "RPi_Time_verb" \
#   --strip-ml-suffixes "_events_final,_processed,_events_final.csv,_processed.csv" \
#   --only-rows-with-rpi \
#   --dedupe-sec 0.05 \
#   --debug \
#   >> "$LOG_FILE" 2>&1
# echo "✅ rpi_preproc_pipeline.py completed at $(date)" | tee -a "$LOG_FILE"
# ACTUAL_PROC="${TRUE_BASE_DIR}/${PROC_DIR}/full"


# echo "🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸" | tee -a "$LOG_FILE"
# echo "🚀 Starting multi_stream_drift.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/multi_stream_drift.py \
#         --mark-col markNum_aligned \
#         --lfp-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R037/R037_mergedLFP_trim.csv" \
#         --lfp-time-col time_abs \
#         --rpi-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R037/R037_mergedRPi_trim.csv" \
#         --rpi-time-col RPi_Time_verb \
#         --out-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R037/R037_LFP_RPi_drift.csv" \
#         --chunk-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R037/R037_mergedRPi_trim.csv" \
#         --chunk-col con_chunk_RPi \
#         --midi-chunks 1,2,3,4,5,6,7 \
#         --print-summary \
#    >> "$LOG_FILE" 2>&1

# echo "✅ multi_stream_drift.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄🍄" | tee -a "$LOG_FILE"
# echo "🚀 Starting multi_stream_drift.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/multi_stream_drift.py \
#         --mark-col markNum_aligned \
#         --lfp-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R019/R019_mergedLFP_trim.csv"  \
#         --lfp-time-col time_abs \
#         --rpi-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R019/R019_mergedRPi_trim.csv" \
#         --rpi-time-col RPi_Time_verb \
#         --out-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R019/R019_LFP_RPi_drift.csv" \
#         --chunk-csv "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/R019/R019_mergedRPi_trim.csv" \
#         --chunk-col con_chunk_RPi \
#         --midi-chunks 1,2,3,4,5,6,7 \
#         --print-summary \
#    >> "$LOG_FILE" 2>&1

# echo "✅ multi_stream_drift.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸🌸" | tee -a "$LOG_FILE"
# echo "🚀 Starting multi_stream_drift.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/multi_stream_drift.py \
#         --mark-col markNum_aligned \
#         --ml-csv  R037_ML.csv  --ml-time-col  mLTimestamp \
#         --lfp-csv R037_LFP.csv  --lfp-time-col time_abs \
#         --rpi-csv R037_mergedRPi_trim.csv --rpi-time-col RPi_Time_verb \
#         --out-csv R037_LFP_RPi_drift.csv
#    >> "$LOG_FILE" 2>&1

# echo "✅ multi_stream_drift.py completed at $(date)" | tee -a "$LOG_FILE"
#  # ####################################
# # # # Magic Leap Alignment to RPi Marks 
# # # ####################################
# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting marks_timeline_mlts.py at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/marks_timeline_mlts.py \
#     --events "${ACTUAL_PROC}/Events_Final_NoWalks/augmented/ObsReward_B_03_17_2025_14_16_events_final.csv" \
#     --rpi    "${TRUE_BASE_DIR}/${PROC_DIR}/RPi_preproc/RNS/RPi_unified/ObsReward_B_03_17_2025_14_16_RNS_RPi_unified.csv" \
#     --block All \
#     --out   "${ACTUAL_PROC}/markTimelines/R019_14_16_marks_timeline.png" \
#     --dpi   150 \
#     --show \
#     >> "$LOG_FILE" 2>&1
# echo "✅ marks_timeline_mlts.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting marks_timeline_lfp.py for R019at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/marks_timeline_lfp.py \
#   --lfp "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R019_LFP_merged.csv" \
#   --rpi "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R019_RPi_merged.csv" \
#   --out "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R019_LFP2RPi.png" \
#   --dpi 150 \
#   --show \
#   >> "$LOG_FILE" 2>&1
# echo "✅ marks_timeline_lfp.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting marks_timeline_lfp.py for R037 at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/marks_timeline_lfp.py \
#   --lfp "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R037_LFP_merged.csv" \
#   --rpi "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R037_RPi_merged.csv" \
#   --out "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R037_LFP2RPi.png" \
#   --dpi 150 \
#   --show \
#   >> "$LOG_FILE" 2>&1
# echo "✅ marks_timeline_lfp.py for R037 completed at $(date)" | tee -a "$LOG_FILE"



# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting marks_elapsed_compare.py for R019 at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/marks_elapsed_compare.py \
#   --lfp "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R019_LFP_merged.csv" \
#   --lfp-col time_abs \
#   --rpi "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R019_RPi_merged.csv" \
#   --rpi-col RPi_Time_verb \
#   --out "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R019_LFP2RPi_elapsed.png" \
#   --export "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/elapsed_values.csv" \
#   --plot-iei \
#   --show \
#   >> "$LOG_FILE" 2>&1
# echo "✅ marks_timeline_lfp.py for R019 completed at $(date)" | tee -a "$LOG_FILE"


# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting marks_elapsed_compare.py for R037 at $(date)" | tee -a "$LOG_FILE"
# python ${CODE_DIR}/alignment/marks_elapsed_compare.py \
#   --lfp "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R037_LFP_merged.csv" \
#   --lfp-col time_abs \
#   --rpi "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R037_RPi_merged.csv" \
#   --rpi-col RPi_Time_verb \
#   --out "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/R037_LFP2RPi_elapsed.png" \
#   --export "${TRUE_BASE_DIR}/${PROC_DIR}/MarkAlignTemp/AfternoonOnly/elapsed_values.csv" \
#   --plot-iei \
#   --show \
#   >> "$LOG_FILE" 2>&1
# echo "✅ marks_timeline_lfp.py for R037 completed at $(date)" | tee -a "$LOG_FILE"


# #  # ####################################
# # # # Magic Leap Alignment to RPi Marks 
# # # ####################################
# echo "🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛🐦‍⬛" | tee -a "$LOG_FILE"
# echo "🚀 Starting batch_split_pipeline.py at $(date)" | tee -a "$LOG_FILE"

# python ${CODE_DIR}/alignment/batch_split_pipeline3.py \
#   --collated "${TRUE_BASE_DIR}/collatedData.xlsx" \
#   --device-ip-map "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/RC_utilities/configs/DeviceIPAddresses.txt" \
#   --code-dir "${CODE_DIR}/alignment" \
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
#   --blankRowTemplate "${CODE_DIR}/alignment/NewRowInfo.csv" \
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
# echo "🚀 Starting mergeEvents.py for PARTICIPANT-ROLE-COINSET at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/mergeEvents.py" \
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
# echo "✅ mergeEvents.py for PARTICIPANT-ROLE-COINSET completed at $(date)" | tee -a "$LOG_FILE"

# echo "💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞💞" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting mergeEvents.py for PAIR-TEST_DATE-SESSION at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/mergeEvents.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --event-dir "ML_RPi_Aligned" \
#   --breaker "ML2 " \
#   --eventEnding "_BioPacRNS_events.csv" \
#   --outEnding "_events.csv" \
#   --output-dir "Merged_PairTestDateSession" \
#   --group-key-fields pairID \
#   --group-key-fields testingDate \
#   --group-key-fields sessionType \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mergeEvents.py for PAIR-TEST_DATE-SESSION completed at $(date)" | tee -a "$LOG_FILE"

# echo "🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬🔬" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting mergeEvents.py for BASIC-GRANULAR at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/mergeEvents.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --event-dir "Events_AugFinal_withWalks" \
#   --eventEnding "_events_final.csv" \
#   --outEnding "_events.csv" \
#   --output-dir "Merged_BasicGranular" \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mergeEvents.py for BASIC-GRANULAR completed at $(date)" | tee -a "$LOG_FILE"


# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting mergeEvents.py for PARTICIPANT ROLE COIN INTERACTIONS at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/mergeEvents.py" \
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
# echo "✅ mergeEvents.py for PARTICIPANT ROLE COIN INTERACTIONS completed at $(date)" | tee -a "$LOG_FILE"

# echo '✨ done ✨' | tee -a "$LOG_FILE"
# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting mergeEvents.py for PARTICIPANT ROLE COIN INTERACTIONS at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/mergeEvents.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --event-dir "Events_Final_NoWalks" \
#   --eventEnding "_events_final.csv" \
#   --outEnding "_events.csv" \
#   --output-dir "Merged_RoleDate" \
#   --group-key-fields pairID \
#   >> "$LOG_FILE" 2>&1
# echo "✅ mergeEvents.py for PARTICIPANT ROLE COIN INTERACTIONS completed at $(date)" | tee -a "$LOG_FILE"

# # Myra this 
# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting mergeEvents.py for COIN INTERACTIONS at $(date)" | tee -a "$LOG_FILE"
# # add to run_overnight_modular3.sh after add_coin_labels_from_collated.py
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/extract_pin_drops.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --input-dir-name "Events_Final_NoWalks" \
#   --pattern "*_events_final.csv" \
#   --pin-event "PinDrop_Moment" \
#   --out-dir-name "PinDrops_All" \
#   --split-on coinLabel \
#   >> "$LOG_FILE" 2>&1

# echo "✅ mergeEvents.py for PARTICIPANT ROLE COIN INTERACTIONS completed at $(date)" | tee -a "$LOG_FILE"

# echo '✨ done ✨' | tee -a "$LOG_FILE"

# echo "💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting mergeEvents.py for COIN INTERACTIONS at $(date)" | tee -a "$LOG_FILE"
# # add to run_overnight_modular3.sh after add_coin_labels_from_collated.py
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/extract_pin_votes.py" \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --input-dir-name "Events_Final_NoWalks" \
#   --pattern "*_events_final.csv" \
#   --out-dir-name "PinDropVotes_All" \
#   --split-on "coinLabel" \
#   >> "$LOG_FILE" 2>&1

# echo "✅ mergeEvents.py for PARTICIPANT ROLE COIN INTERACTIONS completed at $(date)" | tee -a "$LOG_FILE"

# echo '✨ done ✨' | tee -a "$LOG_FILE"


# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/score_pinDropVotes_PO.py" \
#   --input "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDropVotes_All/PinDropVotes_ALL.csv" \
#   --out-summary "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDropVotes_All/PinDropVotes_PO_summary.csv" \
#   --out-split-dir "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDropVotes_All/PinDropVotes_PO_splits" \
#   --chance-level 0.50    # optional, if you want binomial p vs chance

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
#   >> "$LOG_FILE" 2>&1f


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
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist_Coins" \
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
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist_wOutliers" \
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
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist_coins" \
#   --formats "pdf" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "coinSet" \
#   --use-outlier-filter \
#   --filter-columns dropDist \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist_main" \
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
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_RR.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist_RR" \
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
#   --formats "png" \
#   --voi "truecontent_elapsed_s" \
#   --voi-str "Round Elapsed Time" \
#   --voi-unit "(s)" \
#   --facet-by "" \
#   --use-outlier-filter \
#   --filter-columns truecontent_elapsed_s \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_Time_coins" \
#   --formats "png" \
#   --voi "truecontent_elapsed_s" \
#   --voi-str "Round Elapsed Time" \
#   --voi-unit "(s)" \
#   --facet-by "coinSet" \
#   --use-outlier-filter \
#   --filter-columns truecontent_elapsed_s \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_Time_main" \
#   --formats "png" \
#   --voi "truecontent_elapsed_s" \
#   --voi-str "Round Elapsed Time" \
#   --voi-unit "(s)" \
#   --facet-by "" \
#   --use-outlier-filter \
#   --filter-columns truecontent_elapsed_s \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_RR.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_Time_RR" \
#   --formats "png" \
#   --voi "truecontent_elapsed_s" \
#   --voi-str "Round Elapsed Time" \
#   --voi-unit "(s)" \
#   --facet-by "" \
#   --use-outlier-filter \
#   --filter-columns truecontent_elapsed_s \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_main.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist_main_new" \
#   --formats "png" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "" \
#   --use-outlier-filter \
#   --filter-columns dropDist \
#   --ylim 1.4 \
#   --blockmax 14 \
#   >> "$LOG_FILE" 2>&1

#   echo "💰💰💰💰💰💰💰💰💰💰" | tee -a "$LOG_FILE"
# echo "🚀 Starting pinDropWrapper.py at $(date)" | tee -a "$LOG_FILE"
# python "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plotting/pinDropWrapper.py" \
#   --input  "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All/PinDrops_ALL_RR.csv" \
#   --out-root "${TRUE_BASE_DIR}/${PROC_DIR}/full/PinDrops_All_DropDist_RR_new" \
#   --formats "png" \
#   --voi "dropDist" \
#   --voi-str "Pin Drop Distance" \
#   --voi-unit "(m)" \
#   --facet-by "" \
#   --use-outlier-filter \
#   --filter-columns dropDist \
#   --ylim 1.4 \
#   --blockmin 0 \
#   --blockmax 10 \
#   >> "$LOG_FILE" 2>&1
# echo "✅ pinDropWrapper.py completed at $(date)" | tee -a "$LOG_FILE"