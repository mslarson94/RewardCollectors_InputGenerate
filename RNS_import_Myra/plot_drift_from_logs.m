
function plot_drift_from_logs(varargin)
% plot_drift_from_logs
% Visualizes drift between Magic Leap and Raspberry Pi event timestamps.

% ================================
% 🧪 Function Version Tracker
% ================================
version_number = 9;
fprintf('\n📦 Running plot_drift_from_logs (Version %d)\n', version_number);

% -------------------------------
% Input Parsing
% -------------------------------
p = inputParser;
addParameter(p, 'ml_csv_file', '', @ischar);
addParameter(p, 'log_file', '', @ischar);
addParameter(p, 'csv_timestamp_column', 'Timestamp', @ischar);
addParameter(p, 'log_device_ip', '', @ischar);
addParameter(p, 'timezone_offset', hours(7), @(x) isduration(x) || isnumeric(x));
addParameter(p, 'session_date', '', @ischar);
parse(p, varargin{:});

ml_file = p.Results.ml_csv_file;
log_file = p.Results.log_file;
ts_col = p.Results.csv_timestamp_column;
target_ip = p.Results.log_device_ip;
tz_offset = p.Results.timezone_offset;
session_date = datetime(p.Results.session_date, 'InputFormat', 'yyyy-MM-dd');

% -------------------------------
% Load Magic Leap Data
% -------------------------------
% -------------------------------
% Load Magic Leap Data
% -------------------------------
opts = detectImportOptions(ml_file, 'Delimiter', ',');
opts = setvartype(opts, 'string');  % Read all as string to avoid NaNs

ml_data = readtable(ml_file, opts);

% Confirm the column exists
disp("🧠 Column names found in CSV:");
disp(ml_data.Properties.VariableNames);

if ~ismember(ts_col, ml_data.Properties.VariableNames)
    error('❌ Could not find column "%s" in the CSV file.', ts_col);
end

% Extract and trim timestamp column
raw_ts = strtrim(ml_data.(ts_col));

disp("🔍 Sample Magic Leap timestamps:");
disp(raw_ts(1:min(10,end)));


% Check and filter valid timestamps
colon_count = count(raw_ts, ':');
valid_rows = colon_count == 3;
raw_ts = raw_ts(valid_rows);
ml_data = ml_data(valid_rows, :);

% Convert to 'HH:mm:ss.SSSSSS'
fixed_ts = regexprep(raw_ts, '^(\d{2}:\d{2}:\d{2}):(\d+)$', '$1.$2');

% Convert to datetime
try
    ml_times = datetime(fixed_ts, 'InputFormat', 'HH:mm:ss.SSSSSS');
catch
    error('❌ Failed to parse Magic Leap timestamps. Check formatting.');
end

% Combine with session date
ml_times = session_date + timeofday(ml_times);
% 🔎 Remove Magic Leap timestamps starting at or after a known cutoff
cutoff_time = datetime('14:35:37', 'Format', 'HH:mm:ss');
ml_timeofday = timeofday(ml_times);
keep_rows = ml_timeofday < timeofday(cutoff_time);

% Apply filter
ml_times = ml_times(keep_rows);
fprintf('[Debug] Trimmed ML timestamps after %s — now %d remaining.\n', ...
    datestr(cutoff_time, 'HH:MM:SS'), numel(ml_times));

% -------------------------------
% Load and Parse Raspberry Pi Log
% -------------------------------
log_raw = read_logfile(log_file);

% Clean log lines
log_raw = log_raw(~cellfun(@isempty, log_raw));
log_raw = log_raw(~contains(log_raw, '-e'));

% Ensure even count for IP/timestamp pairs
if mod(length(log_raw), 2) ~= 0
    warning('⚠️ Log file contains an odd number of lines — trimming last line.');
    log_raw(end) = [];
end

try
    log_raw = reshape(log_raw, 2, [])';  % Nx2
catch
    error('❌ Could not reshape log into IP-timestamp pairs.');
end

% Show what device IPs are present
fprintf('\n📡 Unique device IPs in log file:\n');
disp(unique(log_raw(:,1)));

% Match log rows by bracketed IP
search_ip = ['[' target_ip ']'];
id_match = contains(log_raw(:,1), search_ip);
fprintf('[Debug] Matching rows for %s: %d\n', search_ip, sum(id_match));
disp(log_raw(id_match, :));

if ~any(id_match)
    error(['❌ No matching timestamps found for device IP: ' target_ip]);
end

% Extract and parse timestamps
log_times_utc = log_raw(id_match, 2);
try
    utc_dt = datetime(log_times_utc, 'InputFormat', 'HH:mm:ss.SSSSSS');
    log_times = session_date + timeofday(utc_dt) + tz_offset;
catch
    error('❌ Failed to parse Raspberry Pi timestamps.');
end

% -------------------------------
% Match Events and Compute Drift
% -------------------------------
fprintf('[Debug] # ML times: %d | # RPi times: %d\n', length(ml_times), length(log_times));

n_events = min(length(ml_times), length(log_times));
if n_events == 0
    warning('⚠️ No events found to analyze.');
    return;
end

ml_times = ml_times(1:n_events);
log_times = log_times(1:n_events);
drift = seconds(log_times - ml_times);

% -------------------------------
% Plot Results
% -------------------------------
figure;
plot(1:n_events, drift, '-o');
xlabel('Event Index');
ylabel('Drift (seconds)');
title(['Timestamp Drift: ' strrep(ml_file, '_', '\_')]);
grid on;

% -------------------------------
% Print Summary
% -------------------------------
fprintf('\n--- Drift Analysis (%s) ---\n', ml_file);
fprintf('Number of events: %d\n', n_events);
fprintf('Mean drift: %.3f s\n', mean(drift));
fprintf('Median drift: %.3f s\n', median(drift));
fprintf('Std deviation: %.3f s\n', std(drift));
fprintf('Max drift: %.3f s\n', max(abs(drift)));
fprintf('------------------------------\n');

% -------------------------------
% Save Drift Info
% -------------------------------
[~, name, ~] = fileparts(ml_file);
outname = [name '_DriftAnalysis.csv'];
% Optional: show large jumps
drift_diff = [0; diff(drift)];
large_jump_threshold = 30;  % seconds
jump_indices = find(abs(drift_diff) > large_jump_threshold);
if ~isempty(jump_indices)
    fprintf('\n⚠️ Large drift jumps detected at indices:\n');
    disp(jump_indices);
    disp(table(jump_indices, ...
        ml_times(jump_indices), ...
        log_times(jump_indices), ...
        drift(jump_indices), ...
        'VariableNames', {'Index', 'ML_Time', 'RPi_Time', 'Drift_sec'}));
end

T = table((1:n_events)', ml_times, log_times, drift, ...
    'VariableNames', {'Index', 'ML_Timestamp', 'RPi_Timestamp', 'Drift_sec'});
writetable(T, outname);
fprintf('✅ Drift data exported to: %s\n', outname);
end
