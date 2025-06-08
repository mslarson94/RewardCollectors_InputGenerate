#!/bin/bash

# Set the root directory to start the search
SOURCE_DIR="/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/ProcessedData"
TARGET_DIR="/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/ProcessedData_Flat"

# Create the target directory if it doesn't exist
mkdir -p "$TARGET_DIR"

# Find and copy matching files
find "$SOURCE_DIR" -type f -print0 | while IFS= read -r -d '' file; do
  filename=$(basename "$file")
  if [[ "$filename" =~ ^ObsReward_A_[0-9]{2}_[0-9]{2}_[0-9]{4}_[0-9]{2}_[0-9]{2}_processed\.(csv|json)$ ]]; then
    cp "$file" "$TARGET_DIR/"
  fi
done
