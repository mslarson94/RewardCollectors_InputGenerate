#!/bin/bash

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

# Activate virtual environment if needed
conda activate RewardCollectors

##############################################

# #### preprocess_events_for_alignment.py
# echo "🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting preprocess_events_for_alignment.py  at $(date)" | tee -a "$LOG_FILE"

# python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/preprocess_events_for_alignment.py  >> "$LOG_FILE" 2>&1

# echo "✅ preprocess_events_for_alignment.py  completed at $(date)" | tee -a "$LOG_FILE"


# #### alignPO2AN_part1.py
# echo "🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting alignPO2AN_part1.py  at $(date)" | tee -a "$LOG_FILE"

# python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/alignPO2AN_part1.py  >> "$LOG_FILE" 2>&1

# echo "✅ alignPO2AN_part1.py completed at $(date)" | tee -a "$LOG_FILE"


#### flattenAndLabel.py
echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 flattenAndLabel.py  at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline/flattenAndLabel.py  >> "$LOG_FILE" 2>&1

echo "✅ flattenAndLabel.py completed at $(date)" | tee -a "$LOG_FILE"


#### computeWalks_v2.py
echo "🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳🐳" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting computeWalks_v3.py  at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline/computeWalks_v3.py  >> "$LOG_FILE" 2>&1

echo "✅ computeWalks_v3.py completed at $(date)" | tee -a "$LOG_FILE"


#### mergeWalks.py
echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting mergeWalks.py  at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline/mergeWalks.py  >> "$LOG_FILE" 2>&1

echo "✅ mergeWalks.py completed at $(date)" | tee -a "$LOG_FILE"



#### compile_all_subject_summaries.py
echo "🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting compile_all_subject_summaries.py  at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/compile_all_subject_summaries.py  >> "$LOG_FILE" 2>&1

echo "✅ compile_all_subject_summaries.py completed at $(date)" | tee -a "$LOG_FILE"


#### extract_pin_drops_per_participant.py
echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting extract_pin_drops_per_participant.py  at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/extract_pin_drops_per_participant.py  >> "$LOG_FILE" 2>&1

echo "✅ extract_pin_drops_per_participant.py completed at $(date)" | tee -a "$LOG_FILE"

#### generate_grouped_summaries.py
echo "🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting generate_grouped_summaries.py  at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/generate_grouped_summaries.py >> "$LOG_FILE" 2>&1

echo "✅ generate_grouped_summaries.py completed at $(date)" | tee -a "$LOG_FILE"

#### plot_dropDist_with_coin_value_and_blocks.py
echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting plot_dropDist_with_coin_value_and_blocks.py  at $(date)" | tee -a "$LOG_FILE"

python /Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/plot_dropDist_with_coin_value_and_blocks.py  >> "$LOG_FILE" 2>&1

echo "✅ plot_dropDist_with_coin_value_and_blocks.py completed at $(date)" | tee -a "$LOG_FILE"