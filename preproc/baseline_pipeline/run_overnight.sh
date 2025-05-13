#!/bin/bash

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

# Activate virtual environment if needed
conda activate RewardCollectors

##############################################

# ##### AN proc

# echo "🚀 AN processing: Starting python preproc_AN.py  at $(date)" | tee -a "$LOG_FILE"

# python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/preproc_AN.py  >> "$LOG_FILE" 2>&1

# echo "✅ AN processing: python preproc_AN.py  completed at $(date)" | tee -a "$LOG_FILE"

# ##### AN Events 

echo "🚀 AN cascading: Starting integration_with_logger.py at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/integration_with_logger.py  >> "$LOG_FILE" 2>&1

echo "✅ AN cascading: python integration_with_logger.py  completed at $(date)" | tee -a "$LOG_FILE"


##############################################


# # ##### PO proc

# echo "🚀 PO processing: Starting preproc_PO.py completed at $(date)" | tee -a "$LOG_FILE"

# # Run fullerPreProc.py and log output
# python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/preproc_PO.py >> "$LOG_FILE" 2>&1

# echo "✅ PO processing: preproc_PO.py completed at $(date)" | tee -a "$LOG_FILE"

# ##### PO Events 

# echo "🚀 PO cascading: Starting batch_extract_with_summary.py  at $(date)" | tee -a "$LOG_FILE"

# python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/batch_extract_with_summary.py  >> "$LOG_FILE" 2>&1

# echo "✅ PO cascading: python batch_extract_with_summary.py  completed at $(date)" | tee -a "$LOG_FILE"