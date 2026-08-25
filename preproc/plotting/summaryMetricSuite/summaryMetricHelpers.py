#!/usr/bin/env python3
"""Input, validation, filtering, and output helpers for summary-metric plots."""

from __future__ import annotations

import glob
import re
from pathlib import Path
from typing import Iterable

import pandas as pd


SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".txt", ".xlsx", ".xls", ".parquet", ".pq"}


def read_table(path: str | Path, *, sheet_name: str | int | None = None) -> pd.DataFrame:
    """Read a supported tabular participant-summary file."""
    input_path = Path(path)
    suffix = input_path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(input_path)
    if suffix in {".tsv", ".txt"}:
        return pd.read_csv(input_path, sep="\t")
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(input_path, sheet_name=sheet_name)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(input_path)

    raise ValueError(
        f"Unsupported input format '{suffix}' for {input_path}. "
        f"Supported: {sorted(SUPPORTED_EXTENSIONS)}"
    )


def expand_inputs(patterns: Iterable[str]) -> list[Path]:
    """Expand file paths and glob patterns while preserving input order."""
    paths: list[Path] = []
    seen: set[Path] = set()

    for pattern in patterns:
        matches = [Path(item) for item in glob.glob(pattern)]
        if not matches and Path(pattern).exists():
            matches = [Path(pattern)]

        for path in sorted(matches):
            resolved = path.resolve()
            if resolved not in seen:
                seen.add(resolved)
                paths.append(path)

    if not paths:
        raise FileNotFoundError("No input files matched --input.")

    return paths


def validate_metric(df: pd.DataFrame, variable: str) -> pd.Series:
    """Return the variable as numeric data, raising on missing/non-numeric input."""
    if variable not in df.columns:
        preview = ", ".join(map(str, df.columns[:25]))
        raise ValueError(
            f"Variable '{variable}' is not present in the input. "
            f"Available columns begin with: {preview}"
        )

    numeric = pd.to_numeric(df[variable], errors="coerce")
    nonmissing_original = df[variable].notna().sum()

    if nonmissing_original > 0 and numeric.notna().sum() == 0:
        raise ValueError(f"Variable '{variable}' contains no numeric values.")

    return numeric


def apply_filters(df: pd.DataFrame, filters: Iterable[str]) -> pd.DataFrame:
    """Apply repeatable COLUMN=VALUE equality filters."""
    result = df.copy()

    for expression in filters:
        if "=" not in expression:
            raise ValueError(
                f"Invalid --where value '{expression}'. Expected COLUMN=VALUE."
            )

        column, raw_value = expression.split("=", 1)
        column = column.strip()
        raw_value = raw_value.strip()

        if column not in result.columns:
            raise ValueError(f"Filter column '{column}' is not present in the input.")

        series = result[column]
        numeric_value = pd.to_numeric(pd.Series([raw_value]), errors="coerce").iloc[0]

        if pd.api.types.is_numeric_dtype(series) and pd.notna(numeric_value):
            mask = pd.to_numeric(series, errors="coerce").eq(float(numeric_value))
        else:
            mask = series.astype("string").str.strip().eq(raw_value)

        result = result.loc[mask].copy()

    return result


def slugify(value: object) -> str:
    """Return a filesystem-safe short label."""
    text = str(value).strip()
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"[^A-Za-z0-9_.-]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "NA"


def metric_axis_label(label: str, unit: str) -> str:
    """Build an axis label from a human-readable label and optional units."""
    clean_label = label.strip()
    clean_unit = unit.strip()
    return f"{clean_label} {clean_unit}".strip()


def save_figure(
    figure,
    *,
    output_dir: Path,
    stem: str,
    formats: Iterable[str],
    dpi: int = 220,
) -> list[str]:
    """Save one figure in each requested format and return output filenames."""
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs: list[str] = []

    for extension in formats:
        ext = extension.lower().lstrip(".")
        output_path = output_dir / f"{stem}.{ext}"
        figure.savefig(output_path, dpi=dpi, bbox_inches="tight")
        outputs.append(output_path.name)

    return outputs
