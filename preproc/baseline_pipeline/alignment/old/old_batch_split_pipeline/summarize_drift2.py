# file: alignment/summarize_drift2.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from batchAlignHelpers import _available_labels, _plot_single, _series_for_label, _summarize


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Plot and summarize drift from merged/aligned ML CSV (supports multiple labels and combined overlay)"
    )
    ap.add_argument("--merged_ml_csv", required=True)
    ap.add_argument(
        "--label",
        default="",
        help="Single label (BioPac or RNS). Ignored if --labels is used.",
    )
    ap.add_argument(
        "--labels",
        default="",
        help="Comma-separated labels to include, e.g. 'BioPac,RNS,Combined'",
    )
    ap.add_argument(
        "--timeCol",
        default="mLT_orig",
        help="time column from the Magic Leap file that you're using")
    args = ap.parse_args()
    timeCol = args.timeCol
    in_csv = Path(args.merged_ml_csv)
    if not in_csv.exists():
        raise FileNotFoundError(f"Missing merged/aligned CSV: {in_csv}")

    df = pd.read_csv(in_csv)
    ml_base = in_csv.stem
    out_dir = in_csv.parent / "Drift"
    out_dir.mkdir(parents=True, exist_ok=True)

    requested = (
        [s.strip() for s in args.labels.split(",") if s.strip()]
        if args.labels
        else ([args.label.strip()] if args.label.strip() else [])
    )

    present = _available_labels(df)
    chosen = [label for label in requested if label != "Combined" and label in present] if requested else present

    per_label_rows: list[dict[str, object]] = []

    for label in chosen:
        series = _series_for_label(df, label, timeCol)
        if series is None:
            continue

        x, y, _ = series
        out_png = out_dir / f"{ml_base}_{label}_DriftPlot.png"
        out_csv = out_dir / f"{ml_base}_{label}_DriftSummary.csv"

        _plot_single(
            label,
            x,
            y,
            f"Drift vs Event Index — {ml_base} [{label}]",
            out_png,
        )

        stats = _summarize(y)
        pd.DataFrame([stats]).to_csv(out_csv, index=False)
        per_label_rows.append({"label": label, **stats})
        print(f"[ok] wrote drift plot    -> {out_png}")
        print(f"[ok] wrote drift summary -> {out_csv}")

    do_combined = ("Combined" in requested) or (len(chosen) >= 2)
    if do_combined:
        fig, ax = plt.subplots(figsize=(12, 4))
        combined_rows: list[dict[str, object]] = []

        for label in chosen:
            series = _series_for_label(df, label, timeCol)
            if series is None:
                continue
            x, y, _ = series
            ax.scatter(x, y, label=label)
            combined_rows.append({"label": label, **_summarize(y)})

        if combined_rows:
            ax.set_title(f"Drift vs Event Index — {ml_base} [Combined]")
            ax.set_xlabel("Event Index")
            ax.set_ylabel("Drift (s)")
            ax.grid(True, alpha=0.4)
            ax.legend()

            combined_png = out_dir / f"{ml_base}_Combined_DriftPlot.png"
            combined_csv = out_dir / f"{ml_base}_Combined_DriftSummary.csv"

            fig.savefig(combined_png, dpi=150)
            plt.close(fig)

            pd.DataFrame(combined_rows).to_csv(combined_csv, index=False)
            print(f"[ok] wrote combined plot    -> {combined_png}")
            print(f"[ok] wrote combined summary -> {combined_csv}")
        else:
            plt.close(fig)

    if not per_label_rows and not do_combined:
        print("[skip] no drift columns present for requested labels; nothing to summarize/plot")


if __name__ == "__main__":
    main()