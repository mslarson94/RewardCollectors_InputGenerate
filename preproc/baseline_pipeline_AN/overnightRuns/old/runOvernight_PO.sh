#!/bin/bash

######################
##     Configs      ##
######################

# Metadata file location (full path)
metadata="/Users/mairahmac/Desktop/RC_TestingNotes/collatedData.xlsx"

# Root directory for data (full path)
dataDir="/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData"

# Acceptable values: complete, truncated
allowed_statuses=("complete" "truncated")

# Set mode to one of: all | subdirs | filelist
mode="all"

# Define subDirs for 'subdirs' mode
subDirs=("pair_201" "pair_008")

# Define file_list for 'filelist' mode
file_list=(
  "$dataDir/ProcessedData/pair_006/02_08_2025/Morning/MagicLeaps/ML2D/ObsReward_B_02_08_2025_13_30_processed.csv"
  "$dataDir/ProcessedData/pair_008/02_19_2025/Morning/MagicLeaps/ML2C/ObsReward_B_02_19_2025_18_14_processed.csv"
  "$dataDir/ProcessedData/pair_008/02_19_2025/Morning/MagicLeaps/ML2C/ObsReward_B_02_19_2025_18_10_processed.csv"
  "$dataDir/ProcessedData/pair_200/03_17_2025/Afternoon/MagicLeaps/ML2C/ObsReward_B_03_17_2025_14_16_processed.csv"
)

# Directory for nohup log output
nohupLogFileOutDir="/Users/mairahmac/Desktop"

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

# Optional: mode validation
if [[ "$mode" != "all" && "$mode" != "subdirs" && "$mode" != "filelist" ]]; then
  echo "❌ Invalid mode: $mode. Use 'all', 'subdirs', or 'filelist'." | tee -a "$LOG_FILE"
  exit 1
fi

if [[ "$mode" == "all" ]]; then
  {
    echo -e "🚀 PO cascading: Starting eventCascades_PO.py at $(date)\n"
    echo "▶️ Mode: ALL — processing entire dataDir"
    echo "📂 Data dir is: $dataDir"
    python eventCascades_PO.py \
      --dataDir "$dataDir" \
      --metadata "$metadata" \
      --allowed_statuses "${allowed_statuses[@]}"
    echo -e "\n🌟 PO cascading: eventCascades_PO.py completed at $(date)"
  } >> "$LOG_FILE" 2>&1

elif [[ "$mode" == "subdirs" ]]; then
  {
    echo -e "🚀 PO cascading: Starting eventCascades_PO.py at $(date)\n"
    echo "▶️ Mode: SUBDIRS — processing selected subdirectories"
    python eventCascades_PO.py \
      --dataDir "$dataDir" \
      --metadata "$metadata" \
      --allowed_statuses "${allowed_statuses[@]}" \
      --subDirs "${subDirs[@]}"
    echo -e "\n🌟 PO cascading: eventCascades_PO.py completed at $(date)"
  } >> "$LOG_FILE" 2>&1

elif [[ "$mode" == "filelist" ]]; then
  {
    echo -e "🚀 PO cascading: Starting eventCascades_PO.py at $(date)\n"
    echo "▶️ Mode: FILELIST — processing specific file list"
    python eventCascades_PO.py \
      --dataDir "$dataDir" \
      --metadata "$metadata" \
      --allowed_statuses "${allowed_statuses[@]}" \
      --file_list "${file_list[@]}"
    echo -e "\n🌟 PO cascading: eventCascades_PO.py completed at $(date)"
  } >> "$LOG_FILE" 2>&1
fi
