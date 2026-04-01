from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


@dataclass(frozen=True)
class SplitSpec:
    col: str
    values: tuple[Any, ...]
    other_label: str = "Other"


def _safe_token(x: Any) -> str:
    s = str(x)
    s = s.strip().replace(" ", "_").replace("/", "_").replace("\\", "_")
    s = s.replace(":", "_").replace("|", "_")
    return s


def _build_mask(series: pd.Series, allowed: Iterable[Any], pick: Any, include_na_in_other: bool) -> pd.Series:
    allowed_set = set(allowed)
    if pick == "Other":
        mask = ~series.isin(allowed_set)
        if not include_na_in_other:
            mask &= series.notna()
        return mask
    return series == pick


def quickieSplitie(
    *,
    df: pd.DataFrame,
    splitDict: dict[str, list[Any] | tuple[Any, ...]],
    outDir: str | Path,
    childNestDir: list[str] | tuple[str, ...] = (),
    other_label: str = "Other",
    include_na_in_other: bool = True,
    include_nested_in_filename: bool = False,
    write_empty: bool = False,
    filename_prefix: str = "df",
    csv_kwargs: dict[str, Any] | None = None,
    write_manifest: bool = True,
    manifest_name: str = "manifest.csv",
) -> pd.DataFrame:
    """
    Split df into CSVs based on splitDict and optional nested folders (childNestDir).

    If write_manifest=True, writes a manifest CSV into outDir that records:
      - output_path (relative to outDir)
      - n_rows, n_cols
      - the chosen bucket value for each split column

    Returns the manifest dataframe (even if write_manifest=False).
    """
    outDir = Path(outDir)
    outDir.mkdir(parents=True, exist_ok=True)
    csv_kwargs = csv_kwargs or {}

    missing_cols = [c for c in splitDict.keys() if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Columns not found in df: {missing_cols}")

    specs: list[SplitSpec] = [
        SplitSpec(col=col, values=tuple(vals), other_label=other_label)
        for col, vals in splitDict.items()
    ]

    childNestDir = list(childNestDir)
    unknown_nesters = [c for c in childNestDir if c not in splitDict]
    if unknown_nesters:
        raise ValueError(f"childNestDir contains columns not in splitDict: {unknown_nesters}")

    # buckets per column: provided + Other
    buckets_per_col: list[list[Any]] = []
    for sp in specs:
        buckets_per_col.append(list(sp.values) + [sp.other_label])

    manifest_rows: list[dict[str, Any]] = []

    for combo in product(*buckets_per_col):
        combo_map = {specs[i].col: combo[i] for i in range(len(specs))}

        mask = pd.Series(True, index=df.index)
        for sp in specs:
            col = sp.col
            pick = combo_map[col]
            mask &= _build_mask(df[col], sp.values, pick, include_na_in_other)

        out_df = df.loc[mask].copy()
        if out_df.empty and not write_empty:
            continue

        # nested folders
        folder = outDir
        for nest_col in childNestDir:
            nest_val = combo_map[nest_col]
            folder = folder / f"{_safe_token(nest_col)}_{_safe_token(nest_val)}"
        folder.mkdir(parents=True, exist_ok=True)

        # filename
        parts = [filename_prefix]
        for sp in specs:
            col = sp.col
            val = combo_map[col]
            if (col in childNestDir) and (not include_nested_in_filename):
                continue
            parts.append(f"{_safe_token(col)}_{_safe_token(val)}")

        filename = "_".join(parts) + ".csv"
        path = folder / filename

        out_df.to_csv(path, index=False, **csv_kwargs)

        # record manifest entry (store relative path for portability)
        rel = path.relative_to(outDir)
        entry = {
            "output_path": str(rel),
            "n_rows": int(out_df.shape[0]),
            "n_cols": int(out_df.shape[1]),
        }
        # store bucket choices
        for sp in specs:
            entry[sp.col] = combo_map[sp.col]
        manifest_rows.append(entry)

    manifest_df = pd.DataFrame(manifest_rows).sort_values("output_path").reset_index(drop=True)

    if write_manifest:
        manifest_path = outDir / manifest_name
        manifest_df.to_csv(manifest_path, index=False)

    return manifest_df


splitDict_1 = {
    "main_RR": ['main', 'RR'],
    "CoinSetID": [4, 5, 6],
    "coinSet": ['A']
    #"dropQual": ["bad", "good"],
}

inpath = Path('/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redo/PinDrops_All')
in_csv = inpath / 'PinDrops_ALL.csv'
outDir = '/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redo/PinDrops_Sorted3'

df = pd.read_csv(in_csv, low_memory=False)
manifest = quickieSplitie(
    df=df,
    splitDict=splitDict_1,
    outDir=outDir,
    childNestDir=["main_RR"],
    write_manifest=True,
)

print(manifest)
