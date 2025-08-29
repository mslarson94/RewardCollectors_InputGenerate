#!/bin/bash

# File lists and directories
FILELIST_R019="fileList_R019.txt"
SRCDIR_R019="/Users/mairahmac/Desktop/RC_TestingNotes/03172025/R019/R019_DatFiles"
DEST_R019="/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData/pair_200/03_17_2025/R019_DatFiles"

FILELIST_R037="fileList_R037.txt"
SRCDIR_R037="/Users/mairahmac/Desktop/RC_TestingNotes/03172025/R037/R037_DatFiles"
DEST_R037="/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData/pair_200/03_17_2025/R037_DatFiles"

# Copy R019 files
mkdir -p "$DEST_R019"
while IFS= read -r filename; do
    srcfile="$SRCDIR_R019/$filename"
    if [[ -f "$srcfile" ]]; then
        cp "$srcfile" "$DEST_R019/"
        echo "Copied $filename to $DEST_R019"
    else
        echo "File not found: $srcfile"
    fi
done < "$FILELIST_R019"

# Copy R037 files
mkdir -p "$DEST_R037"
while IFS= read -r filename; do
    srcfile="$SRCDIR_R037/$filename"
    if [[ -f "$srcfile" ]]; then
        cp "$srcfile" "$DEST_R037/"
        echo "Copied $filename to $DEST_R037"
    else
        echo "File not found: $srcfile"
    fi
done < "$FILELIST_R037"
