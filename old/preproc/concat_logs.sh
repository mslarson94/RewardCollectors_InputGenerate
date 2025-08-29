#!/bin/bash

# Usage: ./concat_logs.sh /path/to/logs /path/to/output_file.txt

# Check arguments
# if [ "$#" -ne 2 ]; then
#     echo "Usage: $0 /path/to/logs /path/to/output_file.txt"
#     exit 1
# fi

# log_dir="$1"
# output_file="$2"
log_dir="/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData/pair_200/03_17_2025/Afternoon/RPi/RNS_RPi"
output_file="/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Marks/AfternoonRPiLogs.txt"
# Remove existing output file if it exists
rm -f "$output_file"

# Loop through all .log files, sorted alphabetically
for file in "$log_dir"/*.log; do
    if [ -f "$file" ]; then
        echo "Adding $file to $output_file"
        cat "$file" >> "$output_file"
        echo -e "\n" >> "$output_file"  # Add newline separator between files
    fi
done

echo "All .log files from $log_dir concatenated into $output_file."
