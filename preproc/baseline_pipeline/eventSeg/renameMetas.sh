#!/usr/bin/env bash
# renameMetas.sh
# Removes the leading "MetaData_Flat_" from every file/dir name in a target directory.
# Usage: ./rename_strip_prefix.sh /path/to/dir
#        ./rename_strip_prefix.sh            # defaults to current directory

set -euo pipefail

dir="/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/glia/MetaData_Flat"
if [[ ! -d "$dir" ]]; then
  echo "Error: '$dir' is not a directory" >&2
  exit 1
fi

shopt -s nullglob

for path in "$dir"/MetaData_Flat*; do
  name="$(basename "$path")"
  new="${name#MetaData_Flat}"

  # Skip if nothing to change or result would be empty
  [[ "$name" == "$new" || -z "$new" ]] && continue

  dest="$(dirname "$path")/$new"

  if [[ -e "$dest" ]]; then
    echo "Skipping: '$path' -> '$dest' (destination exists)" >&2
    continue
  fi

  mv -- "$path" "$dest"
done