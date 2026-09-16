#!/usr/bin/env bash
# fix_events_suffix.sh
# Renames "*_processed.csv_events.csv" -> "*_processed_events.csv" in a target directory.
# Usage: ./fix_events_suffix.sh [directory]   # defaults to the path below if omitted

set -euo pipefail


dir="${1:-/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/MetaData_Flat}"  # <-- adjust if needed
suffix="_processed.csv_meta.json"
target="_processed_meta.json"

if [[ ! -d "$dir" ]]; then
  echo "Error: '$dir' is not a directory" >&2
  exit 1
fi

shopt -s nullglob

for path in "$dir"/*"$suffix"; do
  name="${path##*/}"                 # basename
  base="${name%"$suffix"}"           # strip the exact trailing suffix
  new="${base}${target}"
  dest="${path%/*}/$new"

  if [[ -e "$dest" ]]; then
    echo "Skipping: '$path' -> '$dest' (destination exists)" >&2
    continue
  fi

  mv -- "$path" "$dest"
done
