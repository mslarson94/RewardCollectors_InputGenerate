#!/bin/bash

# Define the directory to store flattened JSON files
OUTPUT_DIR="/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/MergedEvents_V1_flat"
INPUT_DIR="/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/MergedEvents_V1"

# Create the output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Loop through all subdirectories
for subdir in "$INPUT_DIR"/*/ ; do
    echo "$subdir"
    subdir_name="$(basename "$subdir")"
    meta_file="$subdir/merged_meta.json"

    if [ -f "$meta_file" ]; then
        # Compose new filename
        new_filename="${subdir_name}_meta.json"
        # Copy to output directory
        cp "$meta_file" "$OUTPUT_DIR/$new_filename"
        echo "Copied: $meta_file -> $OUTPUT_DIR/$new_filename"
    else
        echo "Skipping: $meta_file not found."
    fi
done

echo "All meta files copied!"
