#!/usr/bin/env bash
# compare_event_augmentation.sh
set -euo pipefail

REPO_ROOT="${1:-.}"
OUT_DIR="${2:-$REPO_ROOT/compare_ANPO}"
mkdir -p "$OUT_DIR"
AN_DIR="$REPO_ROOT/preproc/baseline_pipeline_AN/reproc"
PO_DIR="$REPO_ROOT/preproc/baseline_pipeline_PO/reproc"

if [[ ! -d "$AN_DIR" ]]; then
  echo "Missing directory: $AN_DIR" >&2
  exit 1
fi

if [[ ! -d "$PO_DIR" ]]; then
  echo "Missing directory: $PO_DIR" >&2
  exit 1
fi

mkdir -p "$OUT_DIR/diffs/reproc"

SUMMARY_CSV="$OUT_DIR/file_comparison_summary_reproc.csv"

python3 - "$AN_DIR" "$PO_DIR" "$OUT_DIR" "$SUMMARY_CSV" <<'PY'
import csv
import difflib
import hashlib
import sys
from pathlib import Path

an_dir = Path(sys.argv[1])
po_dir = Path(sys.argv[2])
out_dir = Path(sys.argv[3])
summary_csv = Path(sys.argv[4])
diff_dir = out_dir / "diffs"

def file_map(root: Path) -> dict[str, Path]:
    return {p.name: p for p in root.rglob("*") if p.is_file()}

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def safe_diff_name(name: str) -> str:
    return f"{name}.diff.txt"

an_files = file_map(an_dir)
po_files = file_map(po_dir)

an_names = set(an_files)
po_names = set(po_files)

shared = sorted(an_names & po_names)
an_only = sorted(an_names - po_names)
po_only = sorted(po_names - an_names)

rows: list[dict[str, str]] = []

for name in shared:
    an_path = an_files[name]
    po_path = po_files[name]

    an_hash = sha256(an_path)
    po_hash = sha256(po_path)

    status = "identical" if an_hash == po_hash else "different"
    diff_file = ""

    if status == "different":
        an_text = an_path.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
        po_text = po_path.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)

        diff_lines = difflib.unified_diff(
            an_text,
            po_text,
            fromfile=str(an_path),
            tofile=str(po_path),
            n=3,
        )

        diff_path = diff_dir / safe_diff_name(name)
        diff_path.write_text("".join(diff_lines), encoding="utf-8")
        diff_file = str(diff_path)

    rows.append({
        "filename": name,
        "status": status,
        "an_path": str(an_path),
        "po_path": str(po_path),
        "diff_file": diff_file,
    })

for name in an_only:
    rows.append({
        "filename": name,
        "status": "AN_only",
        "an_path": str(an_files[name]),
        "po_path": "",
        "diff_file": "",
    })

for name in po_only:
    rows.append({
        "filename": name,
        "status": "PO_only",
        "an_path": "",
        "po_path": str(po_files[name]),
        "diff_file": "",
    })

rows.sort(key=lambda r: (r["status"], r["filename"]))

summary_csv.parent.mkdir(parents=True, exist_ok=True)
with summary_csv.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["filename", "status", "an_path", "po_path", "diff_file"],
    )
    writer.writeheader()
    writer.writerows(rows)

shared_count = len(shared)
different_count = sum(1 for r in rows if r["status"] == "different")
identical_count = sum(1 for r in rows if r["status"] == "identical")

print(f"Shared filenames:   {shared_count}")
print(f"Different files:    {different_count}")
print(f"Identical files:    {identical_count}")
print(f"AN-only files:      {len(an_only)}")
print(f"PO-only files:      {len(po_only)}")
print(f"Summary CSV:        {summary_csv}")
print(f"Diff directory:     {diff_dir}")
PY