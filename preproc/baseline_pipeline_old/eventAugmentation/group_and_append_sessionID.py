#!/usr/bin/env python3
"""
group_and_append_sessionID.py

Group rows in a metadata table into "test sessions" and concatenate per-row CSVs in testingOrder.

This version supports short, informative output filenames built from:
  participantID + sessionID + currentRole + suffix

Default behavior:
  - Reads Excel sheet "MagicLeapFiles"
  - Builds a grouping key from:
      sessionID + who
    where who = participantID if present else currentRole
  - Writes output filenames like:
      <participantID>_<sessionID>_<currentRole>_<suffix>.csv

"""
from __future__ import annotations

import argparse
import json
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd


DEFAULT_SHEET = "MagicLeapFiles"
DEFAULT_ORDER_COL = "testingOrder"
DEFAULT_CLEANEDFILE_COL = "cleanedFile"
DEFAULT_PROCESSED_SUFFIX = "_processed.csv"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Group rows on composite keys and concatenate per-group CSVs in testingOrder."
    )
    p.add_argument("--meta", required=True, help="Metadata file: .xlsx, .csv, or .parquet")
    p.add_argument(
        "--sheet",
        default=DEFAULT_SHEET,
        help=f'Excel sheet name or index (default: "{DEFAULT_SHEET}"). Ignored for non-Excel.',
    )
    p.add_argument("--input-dir", required=True, help="Directory containing CSV files to append")
    p.add_argument(
        "--suffix",
        required=True,
        help='Input suffix to locate files, e.g. "_filledIntervals_normUtil_L5.csv"',
    )
    p.add_argument("--out-dir", required=True, help="Output directory for merged per-group CSVs")
    p.add_argument("--out-prefix", default="merged", help="Prefix for output files (default: merged)")
    p.add_argument("--out-meta-dir", required=True, help="Output directory for the manifest files")

    # sessionID-based grouping / naming
    p.add_argument("--session-col", default="sessionID", help='Session id column (default: "sessionID")')
    p.add_argument(
        "--who-mode",
        choices=["participant", "role", "participant_or_role"],
        default="participant_or_role",
        help="How to pick the per-session participant/role identifier (default: participant_or_role)",
    )
    p.add_argument(
        "--participant-col",
        default="participantID",
        help='Participant id column (default: "participantID")',
    )
    p.add_argument(
        "--role-col",
        default="currentRole",
        help='Role column (default: "currentRole")',
    )
    p.add_argument(
        "--id-cols",
        nargs="+",
        default=None,
        help=(
            "Optional explicit composite key columns. "
            "If provided, overrides sessionID/who-mode grouping."
        ),
    )
    p.add_argument(
        "--include-hash",
        action="store_true",
        help="Append a short stable hash to output filenames (helps if collisions are possible).",
    )

    p.add_argument("--order-col", default=DEFAULT_ORDER_COL, help=f"Ordering column (default: {DEFAULT_ORDER_COL})")
    p.add_argument(
        "--cleanedfile-col",
        default=DEFAULT_CLEANEDFILE_COL,
        help=f"Column holding cleaned file name (default: {DEFAULT_CLEANEDFILE_COL})",
    )
    p.add_argument(
        "--processed-suffix",
        default=DEFAULT_PROCESSED_SUFFIX,
        help=f'Suffix to strip from cleanedFile to get baseName (default: "{DEFAULT_PROCESSED_SUFFIX}")',
    )

    p.add_argument("--recursive", action="store_true", help="Search input-dir recursively for matching filenames")
    p.add_argument("--strict", action="store_true", help="Fail if any expected file is missing or duplicated")
    p.add_argument(
        "--allow-duplicates",
        action="store_true",
        help="If multiple matches found, allow and pick first in lexical order",
    )
    p.add_argument("--glob", action="store_true", help="Treat baseName+suffix as a glob pattern instead of exact match")
    p.add_argument("--encoding", default=None, help="Optional encoding for reading CSVs (default: pandas auto)")
    p.add_argument(
        "--read-kwargs-json",
        default=None,
        help='Optional JSON kwargs for pandas.read_csv, e.g. \'{"sep":","}\'',
    )

    p.add_argument("--write-index", action="store_true", help="Write index column to output CSVs")
    p.add_argument("--manifest", action="store_true", help="Write manifest CSVs")
    p.add_argument("--dry-run", action="store_true", help="Resolve files and write manifests, but do not merge CSVs")

    p.add_argument("--group-json", action="store_true", help="Write per-group JSON describing merge inputs/checks")
    p.add_argument("--check-columns", action="store_true", help="Warn if input CSV columns differ within a group")
    p.add_argument("--check-order", action="store_true", help="Warn if testingOrder has duplicates or non-numeric values")
    p.add_argument("--check-empty", action="store_true", help="Warn if any input file is empty (0 rows)")

    return p.parse_args()


@dataclass(frozen=True)
class GroupSpec:
    key_cols: Tuple[str, ...]
    order_col: str
    cleanedfile_col: str
    processed_suffix: str


def read_meta(meta_path: Path, sheet: str) -> pd.DataFrame:
    ext = meta_path.suffix.lower()
    if ext in [".xlsx", ".xlsm", ".xls"]:
        try:
            sheet_arg: object = int(sheet)
        except ValueError:
            sheet_arg = sheet
        return pd.read_excel(meta_path, sheet_name=sheet_arg)
    if ext == ".csv":
        return pd.read_csv(meta_path)
    if ext == ".parquet":
        return pd.read_parquet(meta_path)
    raise ValueError(f"Unsupported meta file extension: {ext}")


def normalize_basename(cleaned_file: object, processed_suffix: str) -> str:
    if cleaned_file is None or (isinstance(cleaned_file, float) and pd.isna(cleaned_file)):
        return ""
    s = str(cleaned_file)
    return s[: -len(processed_suffix)] if s.endswith(processed_suffix) else s


def list_files(input_dir: Path, recursive: bool) -> List[Path]:
    if recursive:
        return [p for p in input_dir.rglob("*") if p.is_file()]
    return [p for p in input_dir.glob("*") if p.is_file()]


def resolve_expected_file(
    files: List[Path],
    input_dir: Path,
    base_name: str,
    suffix: str,
    use_glob: bool,
) -> List[Path]:
    if not base_name:
        return []
    target = f"{base_name}{suffix}"
    if use_glob:
        return sorted(input_dir.glob(target))
    matches = [p for p in files if p.name == target]
    return sorted(matches, key=lambda p: str(p))


def stable_group_hash(key_dict: Dict[str, object]) -> str:
    payload = json.dumps(key_dict, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()[:10]


def safe_part(val: object, max_len: int = 80) -> str:
    txt = str(val).strip().replace(" ", "_")
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
    out = "".join(c if c in allowed else "-" for c in txt)
    return (out[:max_len] if out else "NA") or "NA"


def pick_who(df: pd.DataFrame, who_mode: str, participant_col: str, role_col: str) -> pd.Series:
    if who_mode == "participant":
        return df.get(participant_col)
    if who_mode == "role":
        return df.get(role_col)
    # participant_or_role
    p = df.get(participant_col)
    r = df.get(role_col)
    p_ok = ~pd.isna(p) & (p.astype(str).str.strip() != "")
    out = pd.Series([None] * len(df), index=df.index, dtype="object")
    out[p_ok] = p[p_ok]
    out[~p_ok] = r[~p_ok]
    return out


def suffix_label(suffix: str) -> str:
    s = suffix.strip()
    if s.lower().endswith(".csv"):
        s = s[:-4]
    while s and s[0] in "._-":
        s = s[1:]
    return safe_part(s, 120)


def group_value_single_or_mixed(series: pd.Series) -> str:
    vals = (
        series.dropna()
        .astype(str)
        .map(str.strip)
    )
    vals = vals[vals != ""]
    uniq = vals.unique().tolist()
    if len(uniq) == 0:
        return "NA"
    if len(uniq) == 1:
        return safe_part(uniq[0], 60)
    return "MIXED"


def first_nonempty(series: pd.Series) -> str:
    vals = (
        series.dropna()
        .astype(str)
        .map(str.strip)
    )
    vals = vals[vals != ""]
    if len(vals) == 0:
        return "NA"
    return safe_part(vals.iloc[0], 60)


def make_output_name_pattern(
    participant_id: str,
    session_id: object,
    current_role: str,
    suffix: str,
    include_hash: bool,
    gid_hash: str,
) -> str:
    parts = [
        safe_part(session_id, 60),
        safe_part(current_role, 60),
        safe_part(participant_id, 60)
    ]
    if include_hash:
        parts.append(gid_hash)
    return "_".join(parts) + ".csv"


def uniquify_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    i = 2
    while True:
        cand = parent / f"{stem}__{i}{suffix}"
        if not cand.exists():
            return cand
        i += 1


def main() -> None:
    args = parse_args()

    meta_path = Path(args.meta).expanduser().resolve()
    input_dir = Path(args.input_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_meta_dir = Path(args.out_meta_dir).expanduser().resolve()
    out_meta_dir.mkdir(parents=True, exist_ok=True)

    df = read_meta(meta_path, args.sheet).copy()

    # Default grouping if --id-cols not provided:
    if args.id_cols is None:
        required = [args.session_col, args.cleanedfile_col, args.order_col]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise SystemExit(f"Missing required columns in meta: {missing}")

        if args.participant_col not in df.columns:
            raise SystemExit(f'Missing participant column "{args.participant_col}" in meta.')
        if args.role_col not in df.columns:
            raise SystemExit(f'Missing role column "{args.role_col}" in meta.')

        df["_who"] = pick_who(df, args.who_mode, args.participant_col, args.role_col)
        spec = GroupSpec(
            key_cols=(args.session_col, "_who"),
            order_col=args.order_col,
            cleanedfile_col=args.cleanedfile_col,
            processed_suffix=args.processed_suffix,
        )
    else:
        missing = [c for c in list(args.id_cols) + [args.cleanedfile_col, args.order_col] if c not in df.columns]
        if missing:
            raise SystemExit(f"Missing required columns in meta: {missing}")
        spec = GroupSpec(
            key_cols=tuple(args.id_cols),
            order_col=args.order_col,
            cleanedfile_col=args.cleanedfile_col,
            processed_suffix=args.processed_suffix,
        )

    df["baseName"] = df[spec.cleanedfile_col].apply(lambda x: normalize_basename(x, spec.processed_suffix))
    df["_order_num"] = pd.to_numeric(df[spec.order_col], errors="coerce")
    df["_row_idx"] = range(len(df))

    all_files: List[Path] = []
    if not args.glob:
        all_files = list_files(input_dir, args.recursive)

    read_kwargs = json.loads(args.read_kwargs_json) if args.read_kwargs_json else {}

    group_manifest_rows: List[Dict[str, object]] = []
    file_manifest_rows: List[Dict[str, object]] = []

    df_sorted = df.sort_values(
        by=["_order_num", "_row_idx"],
        ascending=[True, True],
        na_position="last",
        kind="mergesort",
    )

    grouped = df_sorted.groupby(list(spec.key_cols), dropna=False, sort=False)

    for key_vals, g in grouped:
        if len(spec.key_cols) == 1:
            key_vals = (key_vals,)
        key_dict = {col: val for col, val in zip(spec.key_cols, key_vals)}
        gid_hash = stable_group_hash(key_dict)

        if args.check_order:
            order_vals = pd.to_numeric(g[spec.order_col], errors="coerce")
            if order_vals.isna().any():
                print(f"[ORDER-WARN] Non-numeric {spec.order_col} values in group {gid_hash}")
            dup_count = order_vals.dropna().duplicated().sum()
            if dup_count > 0:
                print(f"[ORDER-WARN] Duplicate {spec.order_col} values ({dup_count}) in group {gid_hash}")

        # Output naming
        if args.id_cols is None:
            participant_id = first_nonempty(g.get(args.participant_col, pd.Series([], dtype="object")))
            current_role = group_value_single_or_mixed(g.get(args.role_col, pd.Series([], dtype="object")))
            session_id = g[args.session_col].iloc[0] if args.session_col in g.columns else key_dict.get(args.session_col)

            out_name = make_output_name_pattern(
                participant_id=participant_id,
                session_id=session_id,
                current_role=current_role,
                suffix=args.suffix,
                include_hash=args.include_hash,
                gid_hash=gid_hash,
            )
        else:
            # Legacy verbose naming
            parts = [args.out_prefix] + [f"{k}={safe_part(v)}" for k, v in key_dict.items()]
            parts.append(f"id={gid_hash}")
            out_name = "__".join(parts) + ".csv"

        #out_path = uniquify_path(out_dir / out_name)
        out_path = out_dir / out_name

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
                msg = f"[MISSING] {base}{args.suffix} (group {gid_hash})"
                if args.strict:
                    raise SystemExit(msg)
                print(msg)
                continue

            if len(matches) > 1 and not args.allow_duplicates:
                dup_rows += 1
                msg = f"[DUPLICATE] {base}{args.suffix} matched {len(matches)} files (group {gid_hash}): {matches}"
                if args.strict:
                    raise SystemExit(msg)
                print(msg)
                continue

            resolved.append(matches[0])

        for pth in resolved:
            file_manifest_rows.append(
                {
                    "group_hash": gid_hash,
                    **{f"key_{k}": str(v) for k, v in key_dict.items()},
                    "suffix": args.suffix,
                    "file_path": str(pth),
                }
            )

        if args.dry_run:
            print(f"[DRY-RUN] Would merge {len(resolved)} files -> {out_path}")
            group_manifest_rows.append(
                {
                    "group_hash": gid_hash,
                    "out_file": str(out_path),
                    "n_files": len(resolved),
                    "missing_expected": missing_rows,
                    "duplicate_expected": dup_rows,
                    "rows_merged": "",
                    "rows_by_file": "",
                    "files": ";".join(str(p) for p in resolved),
                }
            )
            if args.group_json:
                group_json_path = out_meta_dir / f"{args.out_prefix}__{gid_hash}.json"
                payload = {
                    "group_hash": gid_hash,
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
            print(f"[SKIP] No files to merge for group {gid_hash} -> {out_path}")
            group_manifest_rows.append(
                {
                    "group_hash": gid_hash,
                    "out_file": str(out_path),
                    "n_files": 0,
                    "missing_expected": missing_rows,
                    "duplicate_expected": dup_rows,
                    "rows_merged": 0,
                    "rows_by_file": "[]",
                    "files": "",
                }
            )
            continue

        frames: List[pd.DataFrame] = []
        rows_total = 0
        col_sets: List[set[str]] = []
        per_file_rows: List[Tuple[str, int]] = []

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
                print(f"[EMPTY-WARN] {pth} has 0 rows (group {gid_hash})")

            frames.append(f)
            rows_total += len(f)

        if args.check_columns and col_sets:
            base_cols = col_sets[0]
            for i, cs in enumerate(col_sets[1:], start=1):
                if cs != base_cols:
                    print(f"[COL-WARN] Column mismatch in group {gid_hash} between files 0 and {i}")
                    break

        if not frames:
            print(f"[SKIP] All reads failed for group {gid_hash} -> {out_path}")
            group_manifest_rows.append(
                {
                    "group_hash": gid_hash,
                    "out_file": str(out_path),
                    "n_files": 0,
                    "missing_expected": missing_rows,
                    "duplicate_expected": dup_rows,
                    "rows_merged": 0,
                    "rows_by_file": json.dumps(per_file_rows),
                    "files": ";".join(str(p) for p in resolved),
                }
            )
            continue

        merged = pd.concat(frames, ignore_index=True)
        merged.to_csv(out_path, index=args.write_index)
        print(f"[OK] {gid_hash}: merged {len(frames)} files ({rows_total} rows) -> {out_path}")

        group_manifest_rows.append(
            {
                "group_hash": gid_hash,
                "out_file": str(out_path),
                "n_files": len(frames),
                "missing_expected": missing_rows,
                "duplicate_expected": dup_rows,
                "rows_merged": len(merged),
                "rows_by_file": json.dumps(per_file_rows),
                "files": ";".join(str(p) for p in resolved),
            }
        )

        if args.group_json:
            group_json_path = out_meta_dir / f"{args.out_prefix}__{gid_hash}.json"
            payload = {
                "group_hash": gid_hash,
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