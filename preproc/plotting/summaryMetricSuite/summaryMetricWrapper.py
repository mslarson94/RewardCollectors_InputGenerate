#!/usr/bin/env python3
"""CLI runner for participant-summary metric histograms, violins, and descriptives."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from summaryMetricHelpers import (
    apply_filters,
    expand_inputs,
    read_table,
    save_figure,
    slugify,
    validate_metric,
)
from summaryMetricPlots import (
    plot_faceted_histogram_overlay,
    plot_faceted_histogram_panels,
    plot_faceted_violin,
    plot_histogram,
    plot_violin,
)
from summaryMetricStats import summarize_metric


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Plot any numeric variable from participantSummaryData_* files using "
            "pin-drop-style histogram/KDE and violin aesthetics."
        )
    )
    parser.add_argument(
        "--input",
        nargs="+",
        required=True,
        help="One or more files/globs, e.g. participantSummaryData_*.csv",
    )
    parser.add_argument("--voi", required=True, help="Variable-of-interest column name.")
    parser.add_argument(
        "--voi-str",
        default=None,
        help="Human-readable label. Defaults to the --voi column name.",
    )
    parser.add_argument("--voi-unit", default="", help="Optional units, e.g. '(rounds)'.")
    parser.add_argument("--out-dir", required=True, help="Output root directory.")
    parser.add_argument(
        "--formats",
        default="png,pdf",
        help="Comma-separated plot formats.",
    )
    parser.add_argument(
        "--facet-by",
        default="",
        help=(
            "Optional grouping column. Produces per-facet plots plus combined "
            "overlay, panel, and violin comparison figures."
        ),
    )
    parser.add_argument(
        "--facet-panel-vertical-threshold",
        type=int,
        default=4,
        help=(
            "Stack histogram facet panels vertically when the number of facets "
            "is at least this value. Default: 4."
        ),
    )
    parser.add_argument(
        "--where",
        action="append",
        default=[],
        metavar="COLUMN=VALUE",
        help="Repeatable exact-match filter, e.g. --where main_RR=main.",
    )
    parser.add_argument(
        "--bin-width",
        type=float,
        default=None,
        help="Optional fixed histogram bin width; defaults to FD bins.",
    )
    parser.add_argument(
        "--xlim",
        type=float,
        nargs=2,
        metavar=("MIN", "MAX"),
        default=None,
        help="Optional shared x-axis limits.",
    )
    parser.add_argument(
        "--dot-mode",
        choices=("panel", "baseline", "none"),
        default="panel",
        help="Participant-dot display for individual histograms.",
    )
    parser.add_argument(
        "--summary-sheet",
        default=None,
        help="Excel sheet name; ignored for non-Excel inputs.",
    )
    return parser.parse_args()


def _group_frames(
    data: pd.DataFrame,
    facet_by: str,
) -> list[tuple[object | None, pd.DataFrame]]:
    """Return whole-data or facet-specific frames."""
    if not facet_by:
        return [(None, data)]

    if facet_by not in data.columns:
        raise ValueError(f"Facet column '{facet_by}' is not present in the input.")

    return list(data.groupby(facet_by, dropna=False, sort=True))


def _save_combined_facet_plots(
    *,
    data: pd.DataFrame,
    input_stem: str,
    file_dir: Path,
    args: argparse.Namespace,
    variable_label: str,
    xlim: tuple[float, float] | None,
    formats: tuple[str, ...],
) -> list[str]:
    """Generate comparison plots containing all facets."""
    if not args.facet_by:
        return []

    outputs: list[str] = []
    facet_slug = slugify(args.facet_by)
    variable_slug = slugify(args.voi)

    for show_stats, suffix in ((True, ""), (False, "__nostats")):
        overlay = plot_faceted_histogram_overlay(
            data,
            variable=args.voi,
            facet_by=args.facet_by,
            variable_label=variable_label,
            unit=args.voi_unit,
            title_prefix=input_stem,
            bin_width=args.bin_width,
            xlim=xlim,
            show_stats=show_stats,
        )
        outputs.extend(
            save_figure(
                overlay,
                output_dir=file_dir,
                stem=(
                    f"hist_overlay__{variable_slug}"
                    f"__by-{facet_slug}{suffix}"
                ),
                formats=formats,
            )
        )
        plt.close(overlay)

        panels = plot_faceted_histogram_panels(
            data,
            variable=args.voi,
            facet_by=args.facet_by,
            variable_label=variable_label,
            unit=args.voi_unit,
            title_prefix=input_stem,
            bin_width=args.bin_width,
            xlim=xlim,
            vertical_threshold=args.facet_panel_vertical_threshold,
            show_stats=show_stats,
        )
        outputs.extend(
            save_figure(
                panels,
                output_dir=file_dir,
                stem=(
                    f"hist_panels__{variable_slug}"
                    f"__by-{facet_slug}{suffix}"
                ),
                formats=formats,
            )
        )
        plt.close(panels)

    combined_violin = plot_faceted_violin(
        data,
        variable=args.voi,
        facet_by=args.facet_by,
        variable_label=variable_label,
        unit=args.voi_unit,
        title_prefix=input_stem,
        xlim=xlim,
    )
    outputs.extend(
        save_figure(
            combined_violin,
            output_dir=file_dir,
            stem=f"violin_combined__{variable_slug}__by-{facet_slug}",
            formats=formats,
        )
    )
    plt.close(combined_violin)

    return outputs


def main() -> None:
    """Run plots and descriptive summaries for each matched summary file."""
    args = parse_args()

    if args.facet_panel_vertical_threshold < 1:
        raise ValueError("--facet-panel-vertical-threshold must be at least 1.")

    output_root = Path(args.out_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    formats = tuple(
        item.strip().lower()
        for item in args.formats.split(",")
        if item.strip()
    )
    if not formats:
        raise ValueError("--formats must contain at least one extension.")

    variable_label = args.voi_str or args.voi
    xlim = tuple(args.xlim) if args.xlim is not None else None

    manifest: dict[str, object] = {
        "variable": args.voi,
        "variable_label": variable_label,
        "unit": args.voi_unit,
        "facet_by": args.facet_by or None,
        "facet_panel_vertical_threshold": args.facet_panel_vertical_threshold,
        "filters": args.where,
        "files": [],
    }

    for input_path in expand_inputs(args.input):
        raw = read_table(input_path, sheet_name=args.summary_sheet)
        data = apply_filters(raw, args.where)
        data = data.copy()
        data[args.voi] = validate_metric(data, args.voi)

        if data.empty:
            raise ValueError(f"No rows remain after filtering {input_path.name}.")

        if data[args.voi].notna().sum() == 0:
            raise ValueError(
                f"No numeric '{args.voi}' values remain after filtering "
                f"{input_path.name}."
            )

        if args.facet_by and args.facet_by not in data.columns:
            raise ValueError(
                f"Facet column '{args.facet_by}' is not present in {input_path.name}."
            )

        file_dir = output_root / input_path.stem / slugify(args.voi)
        file_dir.mkdir(parents=True, exist_ok=True)

        all_stats = summarize_metric(
            data,
            variable=args.voi,
            group_column=args.facet_by or None,
        )
        stats_path = file_dir / f"summary_stats__{slugify(args.voi)}.csv"
        all_stats.to_csv(stats_path, index=False)

        file_record: dict[str, object] = {
            "input": str(input_path),
            "rows_raw": int(len(raw)),
            "rows_after_filters": int(len(data)),
            "valid_metric_values": int(data[args.voi].notna().sum()),
            "outputs": [stats_path.name],
        }

        for color_index, (group_value, group) in enumerate(
            _group_frames(data, args.facet_by)
        ):
            suffix = ""
            title_prefix = input_path.stem
            target_dir = file_dir

            if group_value is not None:
                group_slug = slugify(group_value)
                suffix = f"__{slugify(args.facet_by)}-{group_slug}"
                title_prefix = (
                    f"{input_path.stem} | {args.facet_by}: {group_value}"
                )
                target_dir = file_dir / group_slug
                target_dir.mkdir(parents=True, exist_ok=True)

            if group[args.voi].notna().sum() == 0:
                continue

            hist = plot_histogram(
                group,
                variable=args.voi,
                variable_label=variable_label,
                unit=args.voi_unit,
                title_prefix=title_prefix,
                bin_width=args.bin_width,
                xlim=xlim,
                dot_mode=args.dot_mode,
                color_index=color_index,
                show_stats=True,
            )
            file_record["outputs"].extend(
                save_figure(
                    hist,
                    output_dir=target_dir,
                    stem=f"histkde__{slugify(args.voi)}{suffix}",
                    formats=formats,
                )
            )
            plt.close(hist)

            hist_no_stats = plot_histogram(
                group,
                variable=args.voi,
                variable_label=variable_label,
                unit=args.voi_unit,
                title_prefix=title_prefix,
                bin_width=args.bin_width,
                xlim=xlim,
                dot_mode=args.dot_mode,
                color_index=color_index,
                show_stats=False,
            )
            file_record["outputs"].extend(
                save_figure(
                    hist_no_stats,
                    output_dir=target_dir,
                    stem=f"histkde__{slugify(args.voi)}{suffix}__nostats",
                    formats=formats,
                )
            )
            plt.close(hist_no_stats)

            violin = plot_violin(
                group,
                variable=args.voi,
                variable_label=variable_label,
                unit=args.voi_unit,
                title_prefix=title_prefix,
                xlim=xlim,
                color_index=color_index,
                show_stats=True,
            )
            file_record["outputs"].extend(
                save_figure(
                    violin,
                    output_dir=target_dir,
                    stem=f"violin__{slugify(args.voi)}{suffix}",
                    formats=formats,
                )
            )
            plt.close(violin)

            violin_no_stats = plot_violin(
                group,
                variable=args.voi,
                variable_label=variable_label,
                unit=args.voi_unit,
                title_prefix=title_prefix,
                xlim=xlim,
                color_index=color_index,
                show_stats=False,
            )
            file_record["outputs"].extend(
                save_figure(
                    violin_no_stats,
                    output_dir=target_dir,
                    stem=f"violin__{slugify(args.voi)}{suffix}__nostats",
                    formats=formats,
                )
            )
            plt.close(violin_no_stats)

        file_record["outputs"].extend(
            _save_combined_facet_plots(
                data=data,
                input_stem=input_path.stem,
                file_dir=file_dir,
                args=args,
                variable_label=variable_label,
                xlim=xlim,
                formats=formats,
            )
        )

        manifest["files"].append(file_record)

    manifest_path = output_root / f"manifest__{slugify(args.voi)}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str))
    print(json.dumps(manifest, indent=2, default=str))


if __name__ == "__main__":
    main()
