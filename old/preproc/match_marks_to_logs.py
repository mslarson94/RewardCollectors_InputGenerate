#!/usr/bin/env python3

import os
import sys
import argparse
import re
import pandas as pd
from datetime import datetime

def parse_log_file(log_path):
    ip_pattern = re.compile(r"\[(.*?)\]")
    time_pattern = re.compile(r"(\d{2}:\d{2}:\d{2}\.\d{6})")

    with open(log_path, 'r') as f:
        lines = f.readlines()

    ips = []
    timestamps = []

    for line in lines:
        ip_match = ip_pattern.match(line.strip())
        if ip_match:
            ips.append(ip_match.group(1))
        else:
            ts_match = time_pattern.match(line.strip())
            if ts_match:
                timestamps.append(ts_match.group(1))

    if len(ips) != len(timestamps):
        raise ValueError(f"Mismatch between IPs and timestamps in log file {log_path}.")

    log_entries = []
    for ip, ts in zip(ips, timestamps):
        dt = datetime.strptime(ts, "%H:%M:%S.%f")
        log_entries.append({'IP': ip, 'Timestamp': ts, 'ParsedTimestamp': dt})

    return pd.DataFrame(log_entries)

def parse_log_directory(log_dir):
    log_files = sorted([
        os.path.join(log_dir, f)
        for f in os.listdir(log_dir)
        if f.lower().endswith('.log') and not f.lower().endswith('_verb.log')
    ])
    if not log_files:
        raise FileNotFoundError(f"No .log files found in directory {log_dir} (excluding *_verb.log files).")
    all_logs = pd.concat([parse_log_file(f) for f in log_files], ignore_index=True)
    return all_logs

def adjust_log_hours(df, target_hour):
    def adjust_hour(ts):
        return ts.replace(hour=target_hour)
    df['AdjustedTimestamp'] = df['ParsedTimestamp'].apply(adjust_hour)
    return df

def parse_mark_csv(csv_path):
    df = pd.read_csv(csv_path)
    df['ParsedTimestamp'] = df['Timestamp'].apply(
        lambda x: datetime.strptime(x, "%H:%M:%S:%f")
    )
    df['ParsedTimestampUTC'] = df['ParsedTimestamp'] + pd.Timedelta(hours=8)
    return df

def find_closest_log_timestamp(target_time, log_times):
    diffs = (log_times - target_time).abs().dt.total_seconds()
    min_idx = diffs.idxmin()
    return log_times.iloc[min_idx], diffs.iloc[min_idx]

def match_marks_to_logs(marks_df, log_df, ip, threshold_seconds):
    device_log = log_df[log_df['IP'] == ip]
    matched_times = []
    time_diffs = []
    exceeds_threshold = []

    for ts in marks_df['ParsedTimestampUTC']:
        closest_ts, diff = find_closest_log_timestamp(ts, device_log['AdjustedTimestamp'])
        matched_times.append(closest_ts)
        time_diffs.append(diff)
        exceeds_threshold.append(diff > threshold_seconds)

    marks_df['ClosestLogTimestamp'] = matched_times
    marks_df['TimeDifferenceSeconds'] = time_diffs
    marks_df['ExceedsThreshold'] = exceeds_threshold
    return marks_df

# def main():
#     parser = argparse.ArgumentParser(description="Match Mark events to log timestamps.")
#     parser.add_argument('--log-dir', required=True, help='Directory containing .log files')
#     parser.add_argument('--csv', required=True, help='Path to Mark CSV file')
#     parser.add_argument('--output', required=True, help='Output CSV file path')
#     parser.add_argument('--original-hour', type=int, required=True, help='Original log hour (e.g. 3 for 03:00)')
#     parser.add_argument('--target-hour', type=int, required=True, help='Target session start hour in UTC (e.g. 18 for 18:00 UTC)')
#     parser.add_argument('--threshold-seconds', type=float, default=5.0, help='Threshold in seconds to flag mismatches (default: 5.0)')

#     args = parser.parse_args()

#     print(f"Parsing .log files in directory: {args.log_dir}")
#     log_df = parse_log_directory(args.log_dir)
#     log_df = adjust_log_hours(log_df, args.target_hour)

#     print(f"Parsing behavior CSV file: {args.csv}")
#     marks_df = parse_mark_csv(args.csv)

#     if 'R019_AN' in args.csv:
#         ip = '192.168.50.109'
#     elif 'R037_PO' in args.csv:
#         ip = '192.168.50.127'
#     elif 'R037_AN' in args.csv:
#         ip = '192.168.50.128'
#     elif 'R019_PO' in args.csv:
#         ip = '192.168.50.156'
#     else:
#         raise ValueError("Could not determine IP from filename.")

#     print("Matching Mark events to log timestamps...")
#     matched_df = match_marks_to_logs(marks_df, log_df, ip, args.threshold_seconds)

#     matched_df.to_csv(args.output, index=False)
#     print(f"Matching complete! Output saved to {args.output}")

def main():
    # Session configurations: List of tuples with
    # (log_dir, csv_path, output_path, original_hour, target_hour, threshold_seconds)
    print('anything')
    sessions = [
        (
            "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/RawData/pair_200/03_17_2025/Afternoon/RPi/RNS_RPi/",
            "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Marks/R037_AN_Marks_Afternoon.csv",
            "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Marks/R037_AfternoonMatched.csv",
            3, 18, 5.0
        ),
        (
            "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/RawData/pair_200/03_17_2025/Afternoon/RPi/RNS_RPi/",
            "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Marks/R019_PO_Marks_Afternoon.csv",
            "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Marks/R019_PO_AfternoonMatched.csv",
            3, 18, 5.0
        ),
        (
            "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/RawData/pair_200/03_17_2025/Morning/RPi/RNS_RPi/",
            "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Marks/R019_AN_Marks_Morning.csv",
            "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Marks/R019_AN_MorningMatched.csv",
            3, 18, 5.0
        ),
        (
            "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/RawData/pair_200/03_17_2025/Morning/RPi/RNS_RPi",
            "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Marks/R037_PO_Marks_Morning.csv",
            "/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Marks/R037_PO_MorningMatched.csv",
            3, 18, 5.0
        ),
    ]

    for log_dir, csv_path, output_path, original_hour, target_hour, threshold_seconds in sessions:
        print(f"\nProcessing session with Mark file: {csv_path}")

        log_df = parse_log_directory(log_dir)
        log_df = adjust_log_hours(log_df, target_hour)

        print(f"Parsing behavior CSV file: {csv_path}")
        marks_df = parse_mark_csv(csv_path)

        # Determine IP from CSV filename
        if 'R019_AN' in csv_path:
            ip = '192.168.50.109'
        elif 'R037_PO' in csv_path:
            ip = '192.168.50.127'
        elif 'R037_AN' in csv_path:
            ip = '192.168.50.128'
        elif 'R019_PO' in csv_path:
            ip = '192.168.50.156'
        else:
            raise ValueError(f"Could not determine IP from filename: {csv_path}")

        print(f"Matching Mark events to log timestamps for IP: {ip}")
        matched_df = match_marks_to_logs(marks_df, log_df, ip, threshold_seconds)

        matched_df.to_csv(output_path, index=False)
        print(f"Matching complete! Output saved to {output_path}")


if __name__ == "__main__":
    print("Script is running!")
    main()