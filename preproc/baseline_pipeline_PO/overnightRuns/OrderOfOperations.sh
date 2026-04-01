
###################
# Raw Preprocessing
###################
echo "🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑" | tee -a "$LOG_FILE"
echo "🚀 Starting preprocRaw_AN.py at $(date)" | tee -a "$LOG_FILE"


############################
# Initial Event Segmentation 
############################

echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for AN at $(date)" | tee -a "$LOG_FILE"

###########################################################################
# Event Augmentation Pipeline | Flattening Events' Details Column of Dict's 
###########################################################################
echo "🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭🦭" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting justFlatten.py  at $(date)" | tee -a "$LOG_FILE"

##################################################
# Event Augmentation Pipeline | Adding Coin Labels
##################################################
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting add_coin_labels_from_collated.py  at $(date)" | tee -a "$LOG_FILE"

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


####################################################################
# ReProcessing Pipeline |  00_ Batch Running Reproc Scripts 01 - 04
####################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting 00_batch_reproc_steps_01_04.py at $(date)" | tee -a "$LOG_FILE"
echo "            01_build_intervals : Build Block/Round Interval Tables - Single Row for Every True Round"
echo "            02_reproc_processed_add_elapsed_and_distance : Add Elapsed Time & Distance to _processed.csv to make _PreLimReprocessed.csv files"
echo "            03_compute_speed : Compute Speed to Make Final _reprocessed.csv files"
echo "            04_finalize_events_and_intervals_with_pindrops (no startPos here): Fleshing out Block/Round Interval Tables & Events Files with More Dist & Time Info"


#######################################################################################
# Reprocessing Pipeline | Adding Instantaneous Distance Calcs to _reprocessed.csv files
#######################################################################################
echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
echo "🚀 Starting instantDistCalc.py at $(date)" | tee -a "$LOG_FILE"

#######################################################################
# ReProcessing Pipeline |  Adding StartPos to all PinDrop_Moment events
#######################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting 05_batch_add_startpos.py at $(date)" | tee -a "$LOG_FILE"

######################################################################################
# ReProcessing Pipeline |  Propagating PinDrop Start Positions throughout Events files
######################################################################################
echo "🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷🐷" | tee -a "$LOG_FILE"
echo "🚀 Starting 06b_propagate_pindrop_startpos.py at $(date)" | tee -a "$LOG_FILE"

##########################################################################################################
# Event Augmentation Pipeline | Generating New Walk Events within a Round (i.e. Walk_PinDrop & Walk_Chest)
##########################################################################################################
echo "🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️🚶🏻‍♀️‍➡️" | tee -a "$LOG_FILE"
echo "🚀 Starting computeWalks.py  at $(date)" | tee -a "$LOG_FILE"

######################################################################################################################################
# Event/Interval Augmentation Pipeline | Adding Missing pos/time/dist cols for _start/_end events in *_eventsWalks using *_reprocessed
######################################################################################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting fillEventsPosWalks.py at $(date)" | tee -a "$LOG_FILE"

###################################################################################################################################################
# Event/Interval Augmentation Pipeline | Augmenting Intervals with more Static Block/Round Info from Events Files (i.e. CoinSetID, coinSet, etc...)
###################################################################################################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting buildIntervalsFromEvents.py at $(date)" | tee -a "$LOG_FILE"

#############################################################################################################################################################
# Event/Interval Augmentation Pipeline | Propagate Interval-level columns into *_filled.csv Event Files (i.e. round_start_origRow, round_end_AppTime, etc...)
#############################################################################################################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting propagateIntervalCols.py at $(date)" | tee -a "$LOG_FILE"

##########################################################################################
# Event/Interval Augmentation Pipeline | Assigning Path Utility & Path Efficiency to Walks
##########################################################################################
echo "✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨" | tee -a "$LOG_FILE"
echo "🚀 Starting assign_norm_util_and_efficiency.py at $(date)" | tee -a "$LOG_FILE"
