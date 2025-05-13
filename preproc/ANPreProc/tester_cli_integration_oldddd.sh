#!/bin/bash

######################
##     Configs      ##
######################

# Metadata file location (full path)
metadata="/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"

# Root directory for data (full path)
dataDir="/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/idealTestFile"
#dataDir="/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/threepairs"
# Acceptable values: complete, truncated
allowed_statuses=("complete" "truncated")

# Set mode to one of: all | subdirs | filelist
mode="all"

# Define subDirs for 'subdirs' mode
subDirs=("pair_201" "pair_008")

# Define file_list for 'filelist' mode
file_list=(
  "$dataDir/ProcessedData/pair_006/02_08_2025/Morning/MagicLeaps/ML2A/ObsReward_A_02_08_2025_13_30_processed.csv"
  "$dataDir/ProcessedData/pair_008/02_19_2025/Morning/MagicLeaps/ML2G/ObsReward_A_02_19_2025_18_14_processed.csv"
  "$dataDir/ProcessedData/pair_008/02_19_2025/Morning/MagicLeaps/ML2G/ObsReward_A_02_19_2025_18_10_processed.csv"
  "$dataDir/ProcessedData/pair_200/03_17_2025/Afternoon/MagicLeaps/ML2G/ObsReward_A_03_17_2025_14_16_processed.csv"
)

# Directory for nohup log output
nohupLogFileOutDir="/Users/mairahmac/Desktop"



#######################################################################################################
##                              🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥                                ##
##                              🔥🔥🔥🔥🔥🔥 DON'T TOUCH 🔥🔥🔥🔥🔥🔥                                 ##
##                              🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥                                ##
#######################################################################################################

#############################
##  Logging nohup output   ##
#############################

mkdir -p "$nohupLogFileOutDir"
LOG_FILE="$nohupLogFileOutDir/processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"
echo "📄 Logging to $LOG_FILE"

##########################
##  Environment Setup   ##
##########################

# Activate Conda environment if available
if command -v conda &> /dev/null; then
  conda activate RewardCollectors
fi

##################################
##     Executing Script         ##
##################################

echo "🚀 AN cascading: Starting eventCascades_AN.py at $(date)" | tee -a "$LOG_FILE"

# if [[ "$mode" == "all" ]]; then
#     echo "▶️ Mode: ALL — processing entire dataDir" | tee -a "$LOG_FILE"
#     echo "$dataDir"
#     python eventCascades_AN.py \
#       --dataDir "$dataDir" \
#       --metadata "$metadata" \
#       --allowed_statuses "${allowed_statuses[@]}" >> "$LOG_FILE" 2>&1
if [[ "$mode" == "all" ]]; then
  {
    echo "🚀 AN cascading: Starting eventCascades_AN.py at $(date)"
    echo "▶️ Mode: ALL — processing entire dataDir"
    echo "📂 Data dir is: $dataDir"
    python eventCascades_AN.py \
      --dataDir "$dataDir" \
      --metadata "$metadata" \
      --allowed_statuses "${allowed_statuses[@]}"
    echo "✅ AN cascading: eventCascades_AN.py completed at $(date)"
  } >> "$LOG_FILE" 2>&1
elif [[ "$mode" == "subdirs" ]]; then
    echo "▶️ Mode: SUBDIRS — processing selected subdirectories" | tee -a "$LOG_FILE"
    python eventCascades_AN.py \
      --dataDir "$dataDir" \
      --metadata "$metadata" \
      --allowed_statuses "${allowed_statuses[@]}" \
      --subDirs "${subDirs[@]}" >> "$LOG_FILE" 2>&1

elif [[ "$mode" == "filelist" ]]; then
    echo "▶️ Mode: FILELIST — processing specific file list" | tee -a "$LOG_FILE"
    python eventCascades_AN.py \
      --dataDir "$dataDir" \
      --metadata "$metadata" \
      --allowed_statuses "${allowed_statuses[@]}" \
      --file_list "${file_list[@]}" >> "$LOG_FILE" 2>&1

else
    echo "❌ Invalid mode: $mode. Use 'all', 'subdirs', or 'filelist'." | tee -a "$LOG_FILE"
    exit 1
fi

echo "✅ AN cascading: eventCascades_AN.py completed at $(date)" | tee -a "$LOG_FILE"
