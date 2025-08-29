#!/bin/bash

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

# Activate virtual environment if needed
conda activate RewardCollectors

##### AN proc

echo "🚀 AN cascading: Starting eventCascades_AN.py  at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/eventCascades_AN.py  >> "$LOG_FILE" 2>&1

echo "✅ AN cascading: python eventCascades_AN.py  completed at $(date)" | tee -a "$LOG_FILE"
