#/Users/mairahmac/Desktop/tempVisual/ObsReward_A_03_17_2025_15_50_events_final.csv
# visualMarks.py
# Usage:
#   python visualMarks.py \
#     --ml_csv "/path/ObsReward_A_03_17_2025_15_50_events_final.csv" \
#     --rns_log "/path/2025-03-17_15_47_19_616403579.log" \
#     --outdir "./out" \
#     --annotate_k 5 \
#     [--biopac_log "/path/to/biopac.log"]  # optional, SAME format as RNS; only its own IP inside
#
# Notes:
# - One chart per device timeline (no subplots), matplotlib only, no explicit colors.
# - RNS: ONLY pulls times immediately after the line "[192.168.50.128]".
# - If --biopac_log is provided, it creates a third chart for that file (using *its* own [ip] tags).
# - ML labels (Block–Round) get transferred onto the first K RNS/BioPac marks by order.

import argparse
import os
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

RNS_IP = "192.168.50.128"  # per your clarification


def parse_ml_time(tstr: str):
    # Magic Leap format: "HH:MM:SS:milliseconds" (e.g., "15:50:00:352")
    try:
        hh, mm, ss, ms = tstr.split(":")
        return datetime(1900, 1, 1, int(hh), int(mm), int(ss), int(ms) * 1000)
    except Exception:
        return pd.NaT


def load_ml_marks(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    marks = df[df["lo_eventType"].astype(str).str.strip().str.lower() == "mark"].copy()
    marks["ml_dt"] = marks["mLTimestamp_raw"].astype(str).map(parse_ml_time)

    def fmt_label(b, r):
        if pd.notnull(b) and pd.notnull(r):
            return f"B{int(b)}-R{int(r)}"
        return ""

    marks["label"] = marks.apply(lambda r: fmt_label(r["BlockNum"], r["RoundNum"]), axis=1)
    return marks[["mLTimestamp_raw", "ml_dt", "BlockNum", "RoundNum", "label"]].reset_index(drop=True)


def extract_times_after_tag(log_path: str, tag: str) -> pd.DataFrame:
    # Get the timestamp line immediately after each exact line 'tag'
    with open(log_path, "r", errors="ignore") as f:
        lines = f.read().splitlines()
    out = []
    i = 0
    while i < len(lines):
        if lines[i].strip() == tag:
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines):
                out.append(lines[j].strip())
            i = j
        else:
            i += 1

    def parse_rpi_time(tstr: str):
        try:
            dt = datetime.strptime(tstr, "%H:%M:%S.%f")
            return datetime(1900, 1, 1, dt.hour, dt.minute, dt.second, dt.microsecond)
        except Exception:
            return pd.NaT

    df = pd.DataFrame({"time_str": out})
    df["dt"] = df["time_str"].map(parse_rpi_time)
    return df


def plot_timeline(dt_series: pd.Series, labels=None, title="", x_label="", outfile=None):
    fig = plt.figure(figsize=(10, 2.6))
    ax = plt.gca()
    ax.vlines(dt_series, 0, 1)
    ax.plot(dt_series, [1] * len(dt_series), "o")
    if labels is not None:
        for t, lab in zip(dt_series, labels):
            if lab:
                ax.text(t, 1.02, lab, rotation=90, va="bottom", ha="center", fontsize=8)
    ax.set_ylim(0, 1.2)
    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.get_yaxis().set_visible(False)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S.%f"))
    fig.autofmt_xdate()
    if outfile:
        os.makedirs(os.path.dirname(outfile), exist_ok=True)
        plt.savefig(outfile, bbox_inches="tight", dpi=150)
    plt.show()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ml_csv", required=True)
    ap.add_argument("--rns_log", required=True)
    ap.add_argument("--biopac_log", default=None, help="Optional separate BioPac Pi log (same format).")
    ap.add_argument("--outdir", default="./out")
    ap.add_argument("--annotate_k", type=int, default=0)
    args = ap.parse_args()

    ml = load_ml_marks(args.ml_csv)
    ml_labels = ml["label"].tolist()

    # RNS: only after [192.168.50.128]
    rns = extract_times_after_tag(args.rns_log, f"[{RNS_IP}]")

    # Prepare annotation transfer
    k = min(args.annotate_k, len(ml_labels)) if args.annotate_k > 0 else 0
    rns_labels = [ml_labels[i] if i < k else "" for i in range(len(rns))] if k else None

    # Plot ML
    plot_timeline(
        ml["ml_dt"],
        labels=ml["label"],
        title="Magic Leap — 'Mark' events (original ML time base)",
        x_label="ML time-of-day",
        outfile=os.path.join(args.outdir, "magic_leap_marks.png"),
    )

    # Plot RNS
    plot_timeline(
        rns["dt"],
        labels=rns_labels,
        title=f"RNS Raspberry Pi — times after [{RNS_IP}] (original RNS Pi time base)",
        x_label="RNS time-of-day",
        outfile=os.path.join(args.outdir, "rns_marks.png"),
    )

    # Optional, separate BioPac log (if you provide it)
    if args.biopac_log:
        # If the BioPac file uses a different IP tag, adjust here (e.g., "[192.168.50.156]").
        # If its log only contains one device, use the first tag found; otherwise, pass the desired tag.
        # For parity, we’ll assume the same tag line format and reuse extract_times_after_tag.
        # Example: change "BIOPAC_IP" to the correct string once known.
        BIOPAC_IP = "192.168.50.156"
        biopac = extract_times_after_tag(args.biopac_log, f"[{BIOPAC_IP}]")
        bio_labels = [ml_labels[i] if i < k else "" for i in range(len(biopac))] if k else None
        plot_timeline(
            biopac["dt"],
            labels=bio_labels,
            title=f"BioPac Raspberry Pi — times after [{BIOPAC_IP}] (original BioPac Pi time base)",
            x_label="BioPac time-of-day",
            outfile=os.path.join(args.outdir, "biopac_marks.png"),
        )


if __name__ == "__main__":
    main()
