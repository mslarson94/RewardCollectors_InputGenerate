#!/bin/bash

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

# Activate virtual environment if needed
conda activate RewardCollectors

# ##############################################

#### AN proc
echo "🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 AN processing: Starting python preproc_AN.py  at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline/preproc_AN_augment_v5.py  >> "$LOG_FILE" 2>&1

echo "✅ AN processing: python preproc_AN.py  completed at $(date)" | tee -a "$LOG_FILE"

# ##### AN Events 

# # echo "🚀 AN flat proc moving: Starting move_uncorrected.sh at $(date)" | tee -a "$LOG_FILE"

# # sh /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/move_uncorrected.sh >> "$LOG_FILE" 2>&1
# # bash /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/move_uncorrected.sh >> "$LOG_FILE" 2>&1 || { echo "❌ move_uncorrected.sh failed"; exit 1; }
# # sh /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/move_uncorrected.sh >> "$LOG_FILE" 2>&1
# # if [ $? -ne 0 ]; then
# #   echo "❌ move_uncorrected.sh failed at $(date)" | tee -a "$LOG_FILE"
# #   exit 1
# # fi

# # echo "✅ AN cascading: move_uncorrected.sh completed at $(date)" | tee -a "$LOG_FILE"`


# ##############################################


##### PO proc
echo "" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 PO processing: Starting preproc_PO.py completed at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline/preproc_PO_augment_v4.py >> "$LOG_FILE" 2>&1

echo "✅ PO processing: preproc_PO.py completed at $(date)" | tee -a "$LOG_FILE"

# # find out about free stats consulting 
# # existing collabs with stat's people 
# # EMU dress code -> talk random bullshit 
# # 

# # ##############################################

# # ##### AN events
# echo "" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 AN & PO processing: Starting python peventCascadeBuilder_AN_patched.py  at $(date)" | tee -a "$LOG_FILE"

# python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/eventCascadeBuilder_AN_patched.py  >> "$LOG_FILE" 2>&1

# #python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/eventCascadeBuilder_AN_patched.py >> "$LOG_FILE" 2>&1

# echo "✅ AN & PO processing: python eventCascadeBuilder_AN_patched.py  completed at $(date)" | tee -a "$LOG_FILE"

# ##### PO Events 

# ##### Merge Events
# echo "" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 AN & PO processing: Starting python mergeEvents.py  at $(date)" | tee -a "$LOG_FILE"

# python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/mergeEventsV2.py  >> "$LOG_FILE" 2>&1

# #python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/eventCascadeBuilder_AN_patched.py >> "$LOG_FILE" 2>&1

# echo "✅ AN & PO processing: python mergeEvents.py  completed at $(date)" | tee -a "$LOG_FILE"


# ##### Merge Events
# echo "" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 AN & PO processing: Starting python postCascadeAugment.py  at $(date)" | tee -a "$LOG_FILE"

# python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/postCascadeAugment.py

# echo "✅ AN & PO processing: python postCascadeAugment.py  completed at $(date)" | tee -a "$LOG_FILE"


# echo "" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼🦑🪼" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 AN & PO processing: Starting python behaviorAnalyses.py  at $(date)" | tee -a "$LOG_FILE"
# python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/behaviorAnalyses.py 
# echo "✅ AN & PO processing: python behaviorAnalyses.py  completed at $(date)" | tee -a "$LOG_FILE"
