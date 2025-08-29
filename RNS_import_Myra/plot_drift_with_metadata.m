function plot_drift_with_metadata(varargin)
% plot_drift_with_metadata (Version 2)
% Visualizes timestamp drift + derivative view, and tracks RPi file origins.
% Exports drift table and plots.

% ===============================
% 📦 Version Info
% ===============================
version_number = 2;
fprintf('\n📦 Running plot_drift_with_metadata (Version %d)\n', version_number);

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
% Load Magic Leap Timestamps
% -------------------------------
opts = detectImportOptions(ml_file, 'Delimiter', ',');
opts = setvartype(opts, ts_col, 'string');
ml_data = readtable(ml_file, opts);
raw_ts = strtrim(ml_data.(ts_col));

% Format timestamps
colon_count = count(raw_ts, ':');
valid_rows = colon_count == 3;
raw_ts = raw_ts(valid_rows);
ml_data = ml_data(valid_rows, :);
fixed_ts = regexprep(raw_ts, '^(\d{2}:\d{2}:\d{2}):(\d+)$', '$1.$2');
ml_times = datetime(fixed_ts, 'InputFormat', 'HH:mm:ss.SSSSSS');
ml_times = session_date + timeofday(ml_times);

% -------------------------------
% Load and Parse RPi Log File
% -------------------------------
log_lines = read_logfile(log_file);
log_lines = log_lines(~cellfun(@isempty, log_lines));
log_lines = log_lines(~contains(log_lines, '-e'));

% Parse filename time from RPi log
[~, log_base, ~] = fileparts(log_file);
log_start = parse_raspi_filename_time(log_base);  % local time

% Reshape log into [IP, timestamp] rows
if mod(length(log_lines), 2) ~= 0
    warning('⚠️ Odd number of lines in log file, trimming.');
    log_lines(end) = [];
end
log_raw = reshape(log_lines, 2, [])';

% Identify relevant device marks
search_ip = ['[' target_ip ']'];
ip_match = contains(log_raw(:,1), search_ip);
timestamps = log_raw(ip_match, 2);
log_tags = repmat(string(log_base), sum(ip_match), 1);

% Convert RPi timestamps to datetime (local → UTC)
log_times = datetime(timestamps, 'InputFormat', 'HH:mm:ss.SSSSSS');
log_times = session_date + timeofday(log_times) + tz_offset;

% -------------------------------
% Match and Compute Drift
% -------------------------------
n_events = min(length(ml_times), length(log_times));
ml_times = ml_times(1:n_events);
log_times = log_times(1:n_events);
log_tags = log_tags(1:n_events);

drift = seconds(log_times - ml_times);
delta_drift = [NaN; diff(drift)];

% -------------------------------
% Plotting
% -------------------------------
[~, base, ~] = fileparts(ml_file);
out_plot = [base '_DriftPlot.png'];

figure('Name','Drift Analysis','Position',[100,100,1200,600]);
subplot(2,1,1);
gscatter(1:n_events, drift, log_tags, [], 'o', 8);
title(['Drift vs Event Index — ' strrep(base, '_', '\_')]);
xlabel('Event Index'); ylabel('Drift (s)');
legend('Location','bestoutside'); grid on;

subplot(2,1,2);
plot(2:n_events, delta_drift(2:end), '-x');
xlabel('Event Index'); ylabel('Δ Drift (s)');
title('Derivative of Drift'); grid on;
yline(1, '--r', '1s Threshold');

saveas(gcf, out_plot);
fprintf('🖼️ Plot saved to: %s\n', out_plot);
annotate_drift_jumps(ml_times, drift, 'Threshold', 0.5);

% -------------------------------
% Export Results
% -------------------------------
out_csv = [base '_DriftExtended.csv'];
T = table((1:n_events)', ml_times, log_times, drift, delta_drift, log_tags, ...
    'VariableNames', {'Index', 'ML_Timestamp', 'RPi_Timestamp', 'Drift_sec', 'DeltaDrift', 'RPi_SourceFile'});
writetable(T, out_csv);
fprintf('📄 CSV saved to: %s\n', out_csv);

% -------------------------------
% Summary
% -------------------------------
fprintf('\n--- Summary ---\n');
fprintf('Number of events: %d\n', n_events);
fprintf('Mean drift: %.3f s\n', mean(drift));
fprintf('Max drift: %.3f s\n', max(drift));
fprintf('Max Δ drift: %.3f s\n', max(abs(delta_drift)));
fprintf('----------------\n');

end

function dt = parse_raspi_filename_time(fname)
% Parses filenames like: 2025-03-17_13_19_53_658434492.log
try
    expr = '(\d{4})-(\d{2})-(\d{2})_(\d{2})_(\d{2})_(\d{2})';
    tokens = regexp(fname, expr, 'tokens', 'once');
    if isempty(tokens)
        dt = NaT;
    else
        dt = datetime([tokens{1:3}], 'InputFormat', 'yyyy MM dd') + ...
             hours(str2double(tokens{4})) + minutes(str2double(tokens{5})) + seconds(str2double(tokens{6}));
    end
catch
    dt = NaT;
end
end


function annotate_drift_jumps(ml_times, drift, varargin)
% annotate_drift_jumps
% Detects large jumps in drift and annotates them on the current plot.
%
% Inputs:
%   ml_times : datetime array of Magic Leap timestamps
%   drift    : numeric array of drift values (in seconds)
%
% Optional:
%   'Threshold' : numeric, jump threshold in seconds (default = 0.5)
%   'ShowText'  : logical, whether to show text labels (default = true)
%
% Example:
%   annotate_drift_jumps(ml_times, drift, 'Threshold', 1.0);

% -------------------------------
% Input Handling
% -------------------------------
p = inputParser;
addParameter(p, 'Threshold', 0.5, @isnumeric);
addParameter(p, 'ShowText', true, @islogical);
parse(p, varargin{:});
threshold = p.Results.Threshold;
show_text = p.Results.ShowText;

% -------------------------------
% Compute Drift Differences
% -------------------------------
drift_diff = [0; diff(drift)];
suspicious_idx = find(abs(drift_diff) > threshold);

if isempty(suspicious_idx)
    fprintf('✅ No suspicious drift jumps exceeding %.2f sec found.\n', threshold);
    return;
end

% -------------------------------
% Report in Console
% -------------------------------
fprintf('\n🚨 Suspected Drift Jumps (Threshold: %.2fs)\n', threshold);
for i = 1:length(suspicious_idx)
    idx = suspicious_idx(i);
    fprintf('🔹 Index %d | Time = %s | Δ Drift = %.3f sec\n', ...
        idx, string(ml_times(idx)), drift_diff(idx));
end

% -------------------------------
% Annotate Plot
% -------------------------------
hold on;
plot(suspicious_idx, drift(suspicious_idx), 'ro', 'MarkerSize', 8, 'LineWidth', 2);

if show_text
    for i = 1:length(suspicious_idx)
        x = suspicious_idx(i);
        label = sprintf('\\leftarrow %.1fs', drift_diff(x));
        text(x + 1, drift(x), label, ...
             'Color', 'red', 'FontSize', 8, 'Interpreter', 'tex');
    end
end
end
