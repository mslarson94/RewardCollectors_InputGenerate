#!/bin/bash

# Set up log file
LOG_FILE="/Users/mairahmac/Desktop/processing_log_$(date +'%Y-%m-%d_%H-%M-%S').log"

# Activate virtual environment if needed
conda activate RewardCollectors
# Segment barebones

CODE_DIR="/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/preproc/baseline_pipeline"
TRUE_BASE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes"
DATA_DIR="${TRUE_BASE_DIR}/FreshStart"
META_FILE="${TRUE_BASE_DIR}/collatedData.xlsx"

# ###################
# # Raw Preprocessing
# ###################

# echo "🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting preprocRaw_AN.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preprocRaw/preprocRaw_AN.py" >> "$LOG_FILE" 2>&1
# echo "✅ preprocRaw_AN.py  completed at $(date)" | tee -a "$LOG_FILE"


# echo "🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting preprocRaw_PO.py  at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/preprocRaw/preprocRaw_PO.py"  >> "$LOG_FILE" 2>&1
# echo "✅ preprocRaw_PO.py completed at $(date)" | tee -a "$LOG_FILE"


# echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py  at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py"  >> "$LOG_FILE" 2>&1
# echo "✅ preFrontalCortex_unifiedEventSeg.py completed at $(date)" | tee -a "$LOG_FILE"


echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 alignPO2AN_part1.py  at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/alignment/alignPO2AN.py" --dataDir "$DATA_DIR" --metadata "$META_FILE"
echo "✅ alignPO2AN_part1.py  completed at $(date)" | tee -a "$LOG_FILE"


echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py  at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py"  >> "$LOG_FILE" 2>&1
echo "✅ preFrontalCortex_unifiedEventSeg.py completed at $(date)" | tee -a "$LOG_FILE"



echo "🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting flattenAndLabel.py  at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/flattenAndLabel.py"  >> "$LOG_FILE" 2>&1
echo "✅ flattenAndLabel.py completed at $(date)" | tee -a "$LOG_FILE"


echo "🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting computeWalks.py  at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/computeWalks.py"  >> "$LOG_FILE" 2>&1
echo "✅ computeWalks.py completed at $(date)" | tee -a "$LOG_FILE"


echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "🚀 Starting mergeWalks.py  at $(date)" | tee -a "$LOG_FILE"
python "${CODE_DIR}/eventAugmentation/mergeWalks.py"  >> "$LOG_FILE" 2>&1
echo "✅ mergeWalks.py completed at $(date)" | tee -a "$LOG_FILE"
# ####################
# # Segment Bare Bones
# ####################

# echo "🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for 🦑 AN 🦑 at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py" --role AN --segmentType barebones --dataDir "$DATA_DIR" --metadata "$META_FILE"
# echo "✅ preFrontalCortex_unifiedEventSeg.py ☠️ bare bones ☠️ for 🦑 AN 🦑 completed at $(date)" | tee -a "$LOG_FILE"


# echo "🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for 🪼 PO 🪼 at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py" --role PO --segmentType barebones --dataDir "$DATA_DIR" --metadata "$META_FILE"
# echo "✅ preFrontalCortex_unifiedEventSeg.py ☠️ bare bones ☠️ for 🪼 PO 🪼 completed at $(date)" | tee -a "$LOG_FILE"


# ###########
# # Alignment
# ###########

# echo "🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝🐝" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 wrapper_alignment.py  at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/alignment/wrapper_alignment.py" --dataDir "$DATA_DIR" --metadata "$META_FILE"
# echo "✅ wrapper_alignment.py  completed at $(date)" | tee -a "$LOG_FILE"


# #################
# # Segment Muscles
# #################

# echo "🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑🦑" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for 🦑 AN 🦑 at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py" --role AN --segmentType full --dataDir "$DATA_DIR" --metadata "$META_FILE"
# echo "✅ preFrontalCortex_unifiedEventSeg.py 💪🏻 full muscles 💪🏻 for 🦑 AN 🦑 completed at $(date)" | tee -a "$LOG_FILE"


# echo "🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼🪼" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting preFrontalCortex_unifiedEventSeg.py for 🪼 PO 🪼 at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventSeg/preFrontalCortex_unifiedEventSeg.py" --role PO --segmentType full --dataDir "$DATA_DIR" --metadata "$META_FILE"
# echo "✅ preFrontalCortex_unifiedEventSeg.py 💪🏻 full muscles 💪🏻 for 🪼 PO 🪼 completed at $(date)" | tee -a "$LOG_FILE"


# ################
# # Postprocessing
# ################

# echo "🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿🪿" | tee -a "$LOG_FILE"
# echo "" | tee -a "$LOG_FILE"
# echo "🚀 Starting wrapper_postprocessing.py at $(date)" | tee -a "$LOG_FILE"
# python "${CODE_DIR}/eventAugmentation/wrapper_postprocessing.py" --dataDir "$DATA_DIR"
# echo "✅ wrapper_postprocessing.py completed at $(date)" | tee -a "$LOG_FILE"

