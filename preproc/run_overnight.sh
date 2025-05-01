#!/bin/bash

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

# Activate virtual environment if needed
conda activate RewardCollectors

##### AN proc

echo "🚀 AN processing: Starting python preproc_AN.py  at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/preproc_AN.py  >> "$LOG_FILE" 2>&1

echo "✅ AN processing: python preproc_AN.py  completed at $(date)" | tee -a "$LOG_FILE"

# ##### AN Events 

echo "🚀 AN cascading: Starting eventCascades_AN.py  at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/eventCascades_AN.py  >> "$LOG_FILE" 2>&1

echo "✅ AN cascading: python eventCascades_AN.py  completed at $(date)" | tee -a "$LOG_FILE"



# ##### PO proc

# echo "🚀 PO processing: Starting fullerPreProc_updated_2pass.py completed at $(date)" | tee -a "$LOG_FILE"

# # Run fullerPreProc.py and log output
# python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/fullerPreProc_updated_2pass.py >> "$LOG_FILE" 2>&1

# echo "✅ PO processing: fullerPreProc_updated_2pass.py completed at $(date)" | tee -a "$LOG_FILE"

# echo "🚀 AN processing: Starting correctML2G.py at $(date)" | tee -a "$LOG_FILE"

# # Run correctML2G.py and log output
# python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/correctML2G.py >> "$LOG_FILE" 2>&1
# echo "✅ AN processsing: correctML2G.py completed at $(date)" | tee -a "$LOG_FILE"

# # echo "🚀 Starting AN_summarizeTestingBlocks.py at $(date)" | tee -a "$LOG_FILE"

# # python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/AN_summarizeTestingBlocks.py  >> "$LOG_FILE" 2>&1

# # echo "✅ AN_summarizeTestingBlocks.py completed at $(date)" | tee -a "$LOG_FILE"

# # echo "🚀 PO processing: Starting PO_summarizeTestingBlocks.py at $(date)" | tee -a "$LOG_FILE"

# # python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/PO_summarizeTestingBlocks.py  >> "$LOG_FILE" 2>&1

# # echo "✅ PO processing: PO_summarizeTestingBlocks.py completed at $(date)" | tee -a "$LOG_FILE"


# # # echo "✅ correctML2G.py completed at $(date)" | tee -a "$LOG_FILE"
# # echo "🎉 All processing completed successfully at $(date)" | tee -a "$LOG_FILE"
