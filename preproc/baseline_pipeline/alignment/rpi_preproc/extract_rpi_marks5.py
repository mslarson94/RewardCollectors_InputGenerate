#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
extract_rpi_marks5.py

Extract RPi mark timestamps from pre-parsed CSVs and apply a device-specific
whole-hour timezone offset.

Inputs (per CSV row):
    RPi_Time_Raw_simple  - time-of-day string, e.g. "08:50:01.148287"
    DeviceIP             - e.g. "192.168.50.128"
    EnclosingFolder      - folder name containing the log
    LogFileName          - original .log file name
    markNumber           - per-IP mark index (1-based) within that log

Outputs:
    Per-device/IP CSV:
        <ml_root>_<label>_RPi_simple.csv

    All-IP CSV:
        <ml_root>_<label>_allIP_RPi_simple.csv

The timezone offset used to create RPi_Time_simple is estimated only from
rows matching --device_ip. This prevents timestamps from other devices/IPs
from contaminating the ML↔RPi wall-clock offset estimate.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from RC_utilities.alignHelpers.batchAlignHelpers import (
    _to_datetime_on_date,
    _auto_offset_hours,
    _normalize_ml_stem,
    _round_offset_hours,
)


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Extract RPi marks from pre-parsed CSVs and apply a "
            "device-specific timezone offset."
        )
    )

    ap.add_argument(
        "--rpi_csv_file",
        required=True,
        action="append",
        help=(
            "RPi CSV file(s) produced by read_rpi_logs_to_csv_per_file.py; "
            "pass multiple times to combine."
        ),
    )
    ap.add_argument(
        "--session_date",
        required=True,
        help="Session date as YYYY-MM-DD.",
    )
    ap.add_argument(
        "--device",
        required=True,
        help="Device name, e.g. ML2G.",
    )
    ap.add_argument(
        "--device_ip",
        required=True,
        help="Device IPv4 to extract, e.g. 192.168.50.109.",
    )
    ap.add_argument(
        "--label",
        default="RPi",
        help="RPi source label: BioPac, RNS, or RPi.",
    )
    ap.add_argument(
        "--ml_csv_file",
        default="",
        help="ML CSV used for automatic timezone-offset estimation.",
    )
    ap.add_argument(
        "--timezone_offset_hours",
        default="auto",
        help=(
            "Whole-hour timezone offset, or 'auto' to estimate from the "
            "requested device IP and ML timestamps."
        ),
    )
    ap.add_argument(
        "--out_dir",
        default="",
        help="Directory for output CSVs.",
    )
    ap.add_argument(
        "--strip-ml-suffixes",
        default="_eventsFlat,_processed",
        help="Comma-separated suffixes to strip from ML stem.",
    )
    ap.add_argument(
        "--allow_day_rollover",
        action="store_true",
        help=(
            "If timestamps go backward across files, add one day to "
            "subsequent timestamps."
        ),
    )
    ap.add_argument(
        "--dedupe-sec",
        type=float,
        default=0.0,
        help="Drop marks closer than this many seconds; 0 disables.",
    )
    ap.add_argument(
        "--timeCol",
        default="mLT_orig",
        help="Magic Leap timestamp column.",
    )

    args = ap.parse_args()

    csv_paths = [Path(p) for p in args.rpi_csv_file]

    print("🐦‍⬛ starting CSV-based extract script")
    print("CSV paths:", csv_paths)

    for path in csv_paths:
        if not path.exists():
            raise FileNotFoundError(path)

    # ------------------------------------------------------------------
    # Load and combine raw RPi rows.
    # ------------------------------------------------------------------

    combined_rows: List[Tuple[str, datetime, str, str, int]] = []

    last_dt: datetime | None = None
    day_offset = timedelta(0)

    for path in csv_paths:
        df_in = pd.read_csv(path)

        required_cols = [
            "RPi_Time_Raw_simple",
            "DeviceIP",
            "EnclosingFolder",
            "LogFileName",
            "markNumber",
        ]

        missing = [
            col
            for col in required_cols
            if col not in df_in.columns
        ]

        if missing:
            raise ValueError(
                f"Missing required columns in {path}: {missing}"
            )

        times_str = (
            df_in["RPi_Time_Raw_simple"]
            .astype(str)
            .tolist()
        )

        dts0 = _to_datetime_on_date(
            times_str,
            args.session_date,
        )

        ips = (
            df_in["DeviceIP"]
            .astype(str)
            .str.strip()
            .tolist()
        )

        log_names = (
            df_in["LogFileName"]
            .astype(str)
            .tolist()
        )

        mark_nums = (
            pd.to_numeric(
                df_in["markNumber"],
                errors="raise",
            )
            .astype(int)
            .tolist()
        )

        for (
            raw_time_str,
            dt0,
            ip,
            log_name,
            mark_num,
        ) in zip(
            times_str,
            dts0,
            ips,
            log_names,
            mark_nums,
        ):
            dt = dt0 + day_offset

            if (
                args.allow_day_rollover
                and last_dt is not None
                and dt < last_dt
            ):
                while dt <= last_dt:
                    dt += timedelta(days=1)

                advance = dt - dt0
                day_offset += advance

            combined_rows.append(
                (
                    ip,
                    dt,
                    raw_time_str,
                    log_name,
                    mark_num,
                )
            )

            last_dt = dt

    if not combined_rows:
        raise SystemExit(
            "No rows found in provided RPi CSV file(s)."
        )

    combined_rows.sort(key=lambda row: row[1])

    # ------------------------------------------------------------------
    # Optional deduplication.
    # ------------------------------------------------------------------

    if args.dedupe_sec and args.dedupe_sec > 0:
        deduped: List[
            Tuple[str, datetime, str, str, int]
        ] = []

        prev_dt: datetime | None = None
        threshold = timedelta(
            seconds=float(args.dedupe_sec)
        )

        for row in combined_rows:
            dt = row[1]

            if (
                prev_dt is None
                or (dt - prev_dt) > threshold
            ):
                deduped.append(row)
                prev_dt = dt

        combined_rows = deduped

    # ------------------------------------------------------------------
    # Build all-IP raw dataframe.
    # ------------------------------------------------------------------

    df = pd.DataFrame(
        {
            "RPi_Time_Raw_simple": [
                row[1] for row in combined_rows
            ],
            "Raw_Time_String": [
                row[2] for row in combined_rows
            ],
            "Device": args.device,
            "DeviceIP": [
                row[0] for row in combined_rows
            ],
            "RPi_Source": args.label,
            "LogFile": [
                row[3] for row in combined_rows
            ],
            "LogLineText": [
                "" for _ in combined_rows
            ],
            "LogPairIndex": [
                int(row[4]) - 1
                for row in combined_rows
            ],
            "markNumber": [
                int(row[4])
                for row in combined_rows
            ],
        }
    )

    df["DeviceIP"] = (
        df["DeviceIP"]
        .astype(str)
        .str.strip()
    )

    requested_ip = str(args.device_ip).strip()

    # ------------------------------------------------------------------
    # DEVICE-SPECIFIC MASK
    # ------------------------------------------------------------------

    device_mask = df["DeviceIP"].eq(requested_ip)

    individual_ip_df = (
        df.loc[device_mask]
        .copy()
        .reset_index(drop=True)
    )

    if individual_ip_df.empty:
        raise ValueError(
            f"No RPi rows found for DeviceIP={requested_ip}"
        )

    # ------------------------------------------------------------------
    # Device-specific timezone offset estimation.
    # ------------------------------------------------------------------

    if str(args.timezone_offset_hours).strip().lower() == "auto":
        if (
            not args.ml_csv_file
            or not Path(args.ml_csv_file).exists()
        ):
            raise ValueError(
                "auto timezone offset requires --ml_csv_file"
            )

        ml_df = pd.read_csv(args.ml_csv_file)

        if args.timeCol not in ml_df.columns:
            raise ValueError(
                f"ML CSV must contain {args.timeCol!r} "
                "for auto timezone offset"
            )

        ml_times = pd.to_datetime(
            ml_df[args.timeCol],
            errors="coerce",
        ).dropna()

        if ml_times.empty:
            raise ValueError(
                "No valid timestamps found in ML CSV "
                "for auto timezone offset"
            )

        device_raw_times = pd.to_datetime(
            individual_ip_df[
                "RPi_Time_Raw_simple"
            ],
            errors="coerce",
        ).dropna()

        if device_raw_times.empty:
            raise ValueError(
                f"No valid RPi timestamps for "
                f"DeviceIP={requested_ip}"
            )

        raw_offset = float(
            _auto_offset_hours(
                ml_times,
                device_raw_times,
            )
        )

    else:
        raw_offset = float(
            args.timezone_offset_hours
        )

    tz_offset_hours = _round_offset_hours(
        raw_offset,
        step=1.0,
    )

    tz_offset = pd.to_timedelta(
        tz_offset_hours,
        unit="h",
    )

    # ------------------------------------------------------------------
    # Apply correction ONLY to requested device.
    # ------------------------------------------------------------------

    individual_ip_df["RPi_Time_simple"] = (
        pd.to_datetime(
            individual_ip_df[
                "RPi_Time_Raw_simple"
            ],
            errors="coerce",
        )
        + tz_offset
    )

    individual_ip_df[
        "Timezone_Offset_Hours"
    ] = tz_offset_hours

    # Keep all-IP output primarily diagnostic.
    #
    # Only the requested device gets the correction calculated above.
    # Other IPs are intentionally left without a corrected simple time
    # rather than incorrectly inheriting another device's offset.
    df["RPi_Time_simple"] = pd.NaT
    df["Timezone_Offset_Hours"] = pd.NA

    df.loc[
        device_mask,
        "RPi_Time_simple",
    ] = individual_ip_df[
        "RPi_Time_simple"
    ].to_numpy()

    df.loc[
        device_mask,
        "Timezone_Offset_Hours",
    ] = tz_offset_hours

    # ------------------------------------------------------------------
    # Output naming.
    # ------------------------------------------------------------------

    suffixes = [
        suffix
        for suffix in (
            args.strip_ml_suffixes or ""
        ).split(",")
        if suffix
    ]

    if args.ml_csv_file:
        ml_stem = Path(
            args.ml_csv_file
        ).stem

        base = _normalize_ml_stem(
            ml_stem,
            suffixes,
        )

        out_dir = (
            Path(args.out_dir)
            if args.out_dir
            else Path(args.ml_csv_file).parent
        )

    else:
        base = csv_paths[0].stem

        out_dir = (
            Path(args.out_dir)
            if args.out_dir
            else csv_paths[0].parent
        )

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    # ------------------------------------------------------------------
    # Write per-device simple marks.
    # ------------------------------------------------------------------

    out_csv_ip = (
        out_dir
        / f"{base}_{args.label}_RPi_simple.csv"
    )

    individual_ip_df.to_csv(
        out_csv_ip,
        index=False,
    )

    # ------------------------------------------------------------------
    # Write diagnostic all-IP table.
    # ------------------------------------------------------------------

    out_csv_all = (
        out_dir
        / f"{base}_{args.label}_allIP_RPi_simple.csv"
    )

    df.to_csv(
        out_csv_all,
        index=False,
    )

    # ------------------------------------------------------------------
    # Reporting.
    # ------------------------------------------------------------------

    raw_first = pd.to_datetime(
        individual_ip_df[
            "RPi_Time_Raw_simple"
        ],
        errors="coerce",
    ).dropna()

    corrected_first = pd.to_datetime(
        individual_ip_df[
            "RPi_Time_simple"
        ],
        errors="coerce",
    ).dropna()

    print(
        f"[info] DeviceIP={requested_ip}"
    )

    print(
        f"[info] device-specific RPi rows="
        f"{len(individual_ip_df)}"
    )

    print(
        f"[info] timezone offset applied="
        f"{tz_offset_hours:+.0f} hours"
    )

    if not raw_first.empty:
        print(
            f"[info] first raw simple time="
            f"{raw_first.iloc[0]}"
        )

    if not corrected_first.empty:
        print(
            f"[info] first corrected simple time="
            f"{corrected_first.iloc[0]}"
        )

    print(
        f"[info] wrote {len(individual_ip_df)} rows "
        f"for DeviceIP={requested_ip} -> "
        f"{out_csv_ip}"
    )

    print(
        f"[info] wrote {len(df)} rows "
        f"(all IPs; diagnostic) -> "
        f"{out_csv_all}"
    )


if __name__ == "__main__":
    main()
