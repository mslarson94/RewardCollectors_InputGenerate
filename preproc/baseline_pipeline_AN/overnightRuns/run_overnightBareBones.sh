#!/bin/bash

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

# Activate virtual environment if needed
conda activate RewardCollectors

##############################################

# ##### Bare Bones
echo "" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 AN & PO processing: Starting python unifiedEventSeg.py for Bare Bones processing at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/unifiedEventSeg.py  >> "$LOG_FILE" 2>&1

echo "✅ AN & PO processing: python unifiedEventSeg.py  completed at $(date)" | tee -a "$LOG_FILE"
