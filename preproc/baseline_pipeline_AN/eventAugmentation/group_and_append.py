#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Tuple

import pandas as pd


DEFAULT_ID_COLS = [
    "participantID",
    "pairID",
    "testingDate",
    "sessionType",
    "main_RR",
    "currentRole",
    "sessionID"
]


@dataclass(frozen=True)
class GroupSpec:
    key_cols: Tuple[str, ...]
    order_col: str
    cleanedfile_col: str
    processed_suffix: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Group rows on composite keys and concatenate per-group CSVs in testingOrder."
    )
    p.add_argument("--meta", required=True, help="Metadata file: .xlsx, .csv, or .parquet")
    p.add_argument("--sheet", default="0", help="Excel sheet name or index (default: 0). Ignored for non-Excel.")
    p.add_argument("--input-dir", required=True, help="Directory containing CSV files to append")
    p.add_argument("--suffix", required=True, help='Input suffix to locate files, e.g. "_filledIntervals_normUtil_L5.csv"')
    p.add_argument("--out-dir", required=True, help="Output directory for merged per-group CSVs")
    p.add_argument("--out-prefix", default="merged", help="Prefix for output files (default: merged)")

    p.add_argument("--id-cols", nargs="+", default=DEFAULT_ID_COLS,
                   help=f"Composite key columns (default: {' '.join(DEFAULT_ID_COLS)})")
    p.add_argument("--order-col", default="testingOrder", help="Ordering column (default: testingOrder)")
    p.add_argument("--cleanedfile-col", default="cleanedFile", help="Column holding cleaned file name (default: cleanedFile)")
    p.add_argument("--processed-suffix", default="_processed.csv",
                   help='Suffix to strip from cleanedFile to get baseName (default: "_processed.csv")')

    p.add_argument("--recursive", action="store_true", help="Search input-dir recursively for matching filenames")
    p.add_argument("--strict", action="store_true", help="Fail if any expected file is missing or duplicated")
    p.add_argument("--allow-duplicates", action="store_true",
                   help="If multiple matches found, allow and pick first in lexical order")
    p.add_argument("--glob", action="store_true",
                   help="Treat baseName+suffix as a glob pattern instead of exact filename match")
    p.add_argument("--encoding", default=None, help="Optional encoding for reading CSVs (default: pandas auto)")
    p.add_argument("--read-kwargs-json", default=None,
                   help='Optional JSON string of kwargs passed to pandas.read_csv, e.g. \'{"sep":","}\'')

    p.add_argument("--write-index", action="store_true", help="Write index column to output CSVs")
    p.add_argument("--manifest", action="store_true", help="Write manifest CSVs")
    p.add_argument("--dry-run", action="store_true",
                   help="Resolve files and write manifests, but do not read/merge CSV contents")

    p.add_argument("--group-json", action="store_true", help="Write per-group JSON describing the merge inputs and checks")
    p.add_argument("--check-columns", action="store_true", help="Warn if input CSV columns differ within a group")
    p.add_argument("--check-order", action="store_true", help="Warn if testingOrder has duplicates or non-numeric values")
    p.add_argument("--check-empty", action="store_true", help="Warn if any input file is empty (0 rows)")

    return p.parse_args()


def read_meta(meta_path: Path, sheet: str) -> pd.DataFrame:
    ext = meta_path.suffix.lower()
    if ext in [".xlsx", ".xlsm", ".xls"]:
        try:
            sheet_arg = int(sheet)
        except ValueError:
            sheet_arg = sheet
        return pd.read_excel(meta_path, sheet_name=sheet_arg)
    if ext == ".csv":
        return pd.read_csv(meta_path)
    if ext == ".parquet":
        return pd.read_parquet(meta_path)
    raise ValueError(f"Unsupported meta file extension: {ext}")


def normalize_basename(cleaned_file: str, processed_suffix: str) -> str:
    if cleaned_file is None or (isinstance(cleaned_file, float) and pd.isna(cleaned_file)):
        return ""
    s = str(cleaned_file)
    return s[:-len(processed_suffix)] if s.endswith(processed_suffix) else s


def list_files(input_dir: Path, recursive: bool) -> List[Path]:
    if recursive:
        return [p for p in input_dir.rglob("*") if p.is_file()]
    return [p for p in input_dir.glob("*") if p.is_file()]


def resolve_expected_file(files: List[Path], input_dir: Path, base_name: str, suffix: str, use_glob: bool) -> List[Path]:
    if not base_name:
        return []
    target = f"{base_name}{suffix}"
    if use_glob:
        return sorted(input_dir.glob(target))
    matches = [p for p in files if p.name == target]
    return sorted(matches, key=lambda p: str(p))


def stable_group_id(key_dict: Dict[str, object]) -> str:
    payload = json.dumps(key_dict, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()[:12]


def safe_part(s: object) -> str:
    txt = str(s).strip().replace(" ", "_")
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
    return "".join(c if c in allowed else "-" for c in txt)[:80] or "NA"


def make_output_name(prefix: str, key_dict: Dict[str, object], group_id: str) -> str:
    parts = [prefix]
    for k, v in key_dict.items():
        parts.append(f"{k}={safe_part(v)}")
    parts.append(f"id={group_id}")
    return "__".join(parts) + ".csv"


def main() -> None:
    args = parse_args()

    meta_path = Path(args.meta).expanduser().resolve()
    input_dir = Path(args.input_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_meta_dir = out_dir / "meta"
    out_meta_dir.mkdir(parents=True)

    df = read_meta(meta_path, args.sheet)

    spec = GroupSpec(
        key_cols=tuple(args.id_cols),
        order_col=args.order_col,
        cleanedfile_col=args.cleanedfile_col,
        processed_suffix=args.processed_suffix,
    )

    missing = [c for c in list(spec.key_cols) + [spec.order_col, spec.cleanedfile_col] if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns in meta: {missing}")

    df = df.copy()
    df["baseName"] = df[spec.cleanedfile_col].apply(lambda x: normalize_basename(x, spec.processed_suffix))
    # Robust numeric coercion for ordering:
    # - numeric values become floats
    # - non-numeric become NaN
    # We'll sort with NaNs last, and keep a stable tie-breaker.
    df["_order_num"] = pd.to_numeric(df[spec.order_col], errors="coerce")

    # Stable tie-breaker preserves original row order within identical testingOrder
    df["_row_idx"] = range(len(df))

    all_files: List[Path] = []
    if not args.glob:
        all_files = list_files(input_dir, args.recursive)

    read_kwargs = json.loads(args.read_kwargs_json) if args.read_kwargs_json else {}

    group_manifest_rows = []
    file_manifest_rows = []

    # Sort: numeric order first, NaNs last, then original row order as tie-breaker
    df_sorted = df.sort_values(
        by=["_order_num", "_row_idx"],
        ascending=[True, True],
        na_position="last",
        kind="mergesort",  # stable sort
    )

    grouped = df_sorted.groupby(list(spec.key_cols), dropna=False, sort=False)

    for key_vals, g in grouped:
        if len(spec.key_cols) == 1:
            key_vals = (key_vals,)

        key_dict = {col: val for col, val in zip(spec.key_cols, key_vals)}
        gid = stable_group_id(key_dict)

        out_name = make_output_name(args.out_prefix, key_dict, gid)
        out_path = out_dir / out_name

        # Order sanity checks (safe even in dry-run)
        if args.check_order:
            order_vals = pd.to_numeric(g[spec.order_col], errors="coerce")
            if order_vals.isna().any():
                print(f"[ORDER-WARN] Non-numeric {spec.order_col} values in group {gid}")
            dup_count = order_vals.dropna().duplicated().sum()
            if dup_count > 0:
                print(f"[ORDER-WARN] Duplicate {spec.order_col} values ({dup_count}) in group {gid}")

        resolved: List[Path] = []
        missing_rows = 0
        dup_rows = 0

        for _, row in g.iterrows():
            base = row["baseName"]
            matches = resolve_expected_file(
                files=all_files,
                input_dir=input_dir,
                base_name=base,
                suffix=args.suffix,
                use_glob=args.glob,
            )

            if len(matches) == 0:
                missing_rows += 1
                msg = f"[MISSING] {base}{args.suffix} (group {gid})"
                if args.strict:
                    raise SystemExit(msg)
                print(msg)
                continue

            if len(matches) > 1 and not args.allow_duplicates:
                dup_rows += 1
                msg = f"[DUPLICATE] {base}{args.suffix} matched {len(matches)} files (group {gid}): {matches}"
                if args.strict:
                    raise SystemExit(msg)
                print(msg)
                continue

            resolved.append(matches[0])

        # Always add per-file manifest rows (even in dry-run)
        for pth in resolved:
            file_manifest_rows.append({
                "group_id": gid,
                **{f"key_{k}": str(v) for k, v in key_dict.items()},
                "suffix": args.suffix,
                "file_path": str(pth),
            })

        if args.dry_run:
            print(f"[DRY-RUN] Would merge {len(resolved)} files -> {out_path}")
            group_manifest_rows.append({
                "group_id": gid,
                "out_file": str(out_path),
                "n_files": len(resolved),
                "missing_expected": missing_rows,
                "duplicate_expected": dup_rows,
                "rows_merged": "",
                "rows_by_file": "",
                "files": ";".join(str(p) for p in resolved),
            })
            if args.group_json:
                group_json_path = out_dir / f"{args.out_prefix}__{gid}.json"
                payload = {
                    "group_id": gid,
                    "key": {k: str(v) for k, v in key_dict.items()},
                    "suffix": args.suffix,
                    "out_file": str(out_path),
                    "missing_expected": missing_rows,
                    "duplicate_expected": dup_rows,
                    "ordered_files": [str(p) for p in resolved],
                    "rows_by_file": [],
                }
                group_json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            continue

        if not resolved:
            print(f"[SKIP] No files to merge for group {gid} -> {out_path}")
            group_manifest_rows.append({
                "group_id": gid,
                "out_file": str(out_path),
                "n_files": 0,
                "missing_expected": missing_rows,
                "duplicate_expected": dup_rows,
                "rows_merged": 0,
                "rows_by_file": "[]",
                "files": "",
            })
            continue

        frames = []
        rows_total = 0
        col_sets = []
        per_file_rows = []

        for pth in resolved:
            try:
                f = pd.read_csv(pth, encoding=args.encoding, **read_kwargs)
            except Exception as e:
                msg = f"[READ-ERROR] {pth}: {e}"
                if args.strict:
                    raise
                print(msg)
                continue

            per_file_rows.append((str(pth), len(f)))
            col_sets.append(set(map(str, f.columns)))

            if args.check_empty and len(f) == 0:
                print(f"[EMPTY-WARN] {pth} has 0 rows (group {gid})")

            frames.append(f)
            rows_total += len(f)

        if args.check_columns and col_sets:
            base_cols = col_sets[0]
            for i, cs in enumerate(col_sets[1:], start=1):
                if cs != base_cols:
                    print(f"[COL-WARN] Column mismatch in group {gid} between files 0 and {i}")
                    break

        if not frames:
            print(f"[SKIP] All reads failed for group {gid} -> {out_path}")
            group_manifest_rows.append({
                "group_id": gid,
                "out_file": str(out_path),
                "n_files": 0,
                "missing_expected": missing_rows,
                "duplicate_expected": dup_rows,
                "rows_merged": 0,
                "rows_by_file": json.dumps(per_file_rows),
                "files": ";".join(str(p) for p in resolved),
            })
            continue

        merged = pd.concat(frames, ignore_index=True)
        merged.to_csv(out_path, index=args.write_index)
        print(f"[OK] {gid}: merged {len(frames)} files ({rows_total} rows) -> {out_path}")

        group_manifest_rows.append({
            "group_id": gid,
            "out_file": str(out_path),
            "n_files": len(frames),
            "missing_expected": missing_rows,
            "duplicate_expected": dup_rows,
            "rows_merged": len(merged),
            "rows_by_file": json.dumps(per_file_rows),
            "files": ";".join(str(p) for p in resolved),
        })

        if args.group_json:
            group_json_path = out_meta_dir / f"{args.out_prefix}__{gid}.json"
            payload = {
                "group_id": gid,
                "key": {k: str(v) for k, v in key_dict.items()},
                "suffix": args.suffix,
                "out_file": str(out_path),
                "missing_expected": missing_rows,
                "duplicate_expected": dup_rows,
                "ordered_files": [str(p) for p in resolved],
                "rows_by_file": per_file_rows,
            }
            group_json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if args.manifest:
        group_manifest_path = out_meta_dir / f"{args.out_prefix}__group_manifest.csv"
        pd.DataFrame(group_manifest_rows).to_csv(group_manifest_path, index=False)

        file_manifest_path = out_meta_dir / f"{args.out_prefix}__file_manifest.csv"
        pd.DataFrame(file_manifest_rows).to_csv(file_manifest_path, index=False)

        print(f"[OK] Wrote group manifest -> {group_manifest_path}")
        print(f"[OK] Wrote file manifest  -> {file_manifest_path}")


if __name__ == "__main__":
    main()