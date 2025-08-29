#!/bin/bash

# cd "/Users/mairahmac/Desktop/RC_TestingNotes/ResurrectedData/ExtractedEvents_csv_Flat" || exit
#
# # Rename all ObsReward_B_*_processed_events.csv ➝ *_processed_events_unaligned.csv
# for file in ObsReward_B_*_processed_events.csv; do
#     # Skip if no matching files
#     [ -e "$file" ] || continue
#
#     # Generate new filename
#     new_name="${file/_processed_events.csv/_processed_events_unaligned.csv}"
#
#     # Rename the file
#     mv "$file" "$new_name"
#     echo "✅ Renamed $file ➝ $new_name"
# done

cd "/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/ProcessedData_Flat" || exit

# Rename all ObsReward_B_*_processed.csv ➝ *_processed_unaligned.csv
for file in ObsReward_B_*_processed.csv; do
    # Skip if no matching files
    [ -e "$file" ] || continue

    # Generate new filename
    new_name="${file/_processed.csv/_processed_unaligned.csv}"

    # Rename the file
    mv "$file" "$new_name"
    echo "✅ Renamed $file ➝ $new_name"
done
