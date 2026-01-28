% run_driftAnalysis.m
% -----------------------------------------
% Script to visualize drift between Magic Leap event timestamps
% and Raspberry Pi log timestamps for iEEG alignment.
%
% Author: Myra Saraí Larson
% Lab: Suthana Lab, UCLA
% -----------------------------------------

clearvars;
close all;
clc;

% ========= Modify These for Each New Participant/Session =========

% 📁 Path to the event CSV file from Magic Leap (where marks were *sent*)
csv_file = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS_old/SocialVsAsocial/Marks/R037_AfternoonMatched.csv';

% 📁 Path to the Raspberry Pi log file (combined log showing when marks were *received*)
% NOTE: Can be .txt or .log — just needs to be plain text with timestamps
log_file = '/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData/pair_200/03_17_2025/Afternoon/RPi/RNS_RPi/CombinedRaspiLogs.txt';

% 📌 Column in the CSV that contains the actual mark timestamps (usually 'Timestamp')
timestamp_column = 'Timestamp';

% ⏰ Timezone offset (in hours) to convert Raspberry Pi log (UTC) to local time (PDT → 7, EST → 5)
timezone_offset = hours(-7);

% 🧠 A descriptive label for the session, used in plot titles
source_name = 'R037_AfternoonMatched';




% ========= Call the Drift Visualization Function =========

% plot_drift_from_logs( ...
%     'ml_csv_file', csv_file, ...
%     'log_file', log_file, ...
%     'timezone_offset', hours(7), ...
%     'csv_timestamp_column', 'Timestamp', ...
%     'log_device_ip', '192.168.50.128', ...
%     'session_date', '2025-03-17');

plot_drift_with_metadata( ...
    'ml_csv_file', csv_file, ...
    'log_file', log_file, ...
    'timezone_offset', hours(-7), ...
    'csv_timestamp_column', 'Timestamp', ...
    'log_device_ip', '192.168.50.128', ...
    'session_date', '2025-03-17');

%annotate_drift_jumps(ml_times, drift, 'Threshold', 0.5);
