#!/usr/bin/env python3
"""Command-line runner for participant path-choice violin plots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt

from pathChoiceHelpers import (
    merge_rounds_with_summary,
    prepare_round_level_data,
    prepare_summary_data,
    read_table,
)
from pathChoicePlots import (
    plot_faceted_path_proportions,
    plot_faceted_path_violins,
)
from pathChoiceStats import summarize_participants, summarize_path_choices


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create participant/session faceted path-choice violins with jittered "
            "round points and merged participant summary annotations."
        )
    )
    parser.add_argument("--input", required=True, help="Pin-drop CSV/TSV/Excel/Parquet file.")
    parser.add_argument("--summary", required=True, help="Participant summary CSV/TSV/Excel/Parquet file.")
    parser.add_argument(
        "--summary-sheet",
        default=None,
        help="Excel summary sheet name. Ignored for non-Excel files.",
    )
    parser.add_argument("--out-dir", required=True, help="Output directory.")
    parser.add_argument("--formats", default="png,pdf", help="Comma-separated plot formats.")
    parser.add_argument("--ncols", type=int, default=3, help="Facet columns.")
    parser.add_argument("--panel-width", type=float, default=5.1, help="Width of each facet in inches.")
    parser.add_argument("--panel-height", type=float, default=4.2, help="Height of each violin facet in inches.")
    parser.add_argument(
        "--annotation-fontsize",
        type=float,
        default=7.2,
        help="Font size for compact summary annotations.",
    )
    parser.add_argument(
        "--no-proportion-plot",
        action="store_true",
        help="Skip the companion categorical proportion plot.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_rounds = read_table(args.input)
    raw_summary = read_table(args.summary, sheet_name=args.summary_sheet)
    rounds = prepare_round_level_data(raw_rounds)
    summary = prepare_summary_data(raw_summary)
    merged = merge_rounds_with_summary(rounds, summary)

    merged.to_csv(output_dir / "path_choice_round_level_merged.csv", index=False)
    (
        merged[["path_code", "path_label"]]
        .drop_duplicates()
        .sort_values("path_code")
        .to_csv(output_dir / "path_choice_code_lookup.csv", index=False)
    )
    summarize_path_choices(merged).to_csv(
        output_dir / "path_choice_counts_proportions.csv", index=False
    )
    summarize_participants(merged).to_csv(
        output_dir / "path_choice_participant_summary.csv", index=False
    )

    formats = [item.strip().lower() for item in args.formats.split(",") if item.strip()]
    for extension in formats:
        figure = plot_faceted_path_violins(
            merged,
            output_path=output_dir / f"path_choice_faceted_violins.{extension}",
            ncols=args.ncols,
            panel_width=args.panel_width,
            panel_height=args.panel_height,
            annotation_fontsize=args.annotation_fontsize,
        )
        plt.close(figure)

        if not args.no_proportion_plot:
            figure = plot_faceted_path_proportions(
                merged,
                output_path=output_dir / f"path_choice_faceted_proportions.{extension}",
                ncols=args.ncols,
                panel_width=args.panel_width,
                panel_height=max(3.6, args.panel_height - 0.2),
                annotation_fontsize=args.annotation_fontsize,
            )
            plt.close(figure)

    manifest = {
        "input": str(Path(args.input)),
        "summary": str(Path(args.summary)),
        "summary_sheet": args.summary_sheet,
        "rows_raw": len(raw_rounds),
        "unique_valid_rounds": len(rounds),
        "participant_sessions": int(
            merged[["participantID", "sessionID"]].drop_duplicates().shape[0]
        ),
        "excluded_path_codes": [888, 999],
        "summary_merge_counts": merged["_summary_merge"].value_counts().to_dict(),
        "outputs": sorted(path.name for path in output_dir.iterdir()),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
    print(json.dumps(manifest, indent=2, default=str))


if __name__ == "__main__":
    main()
