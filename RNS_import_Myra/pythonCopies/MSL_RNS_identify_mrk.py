function Ephys = MSL_RNS_identify_mrk(Ephys, csvFile, sessionDate, desired_start_time, desired_end_time)
% Adapted from the original RNS_identify_mrk to use user-provided timestamps from .csv

mark_temp1 = [0 -1 -1 0 0 -1 -1 0 0 0 0 0 0 -1 -1 0];
mark_temp2 = [0 -1 -1 0 0 0 0 0 0 -1 -1 0];

single_chan = Ephys.raw(1,:);
N = size(single_chan,2);

% Cross-correlation
[mrk_corr1, ~] = xcorr(single_chan, mark_temp1);
mrk_corr1 = mrk_corr1(N:end);

[mrk_corr2, ~] = xcorr(single_chan, mark_temp2);
mrk_corr2 = mrk_corr2(N:end);
mrk_corr2 = circshift(mrk_corr2, -4);

I = or(mrk_corr1 > 0.9 * max(mrk_corr1), mrk_corr2 > 0.9 * max(mrk_corr2));
I = [0, diff(I) == -1];
idx = find(I);

% refine marker detection
val = zeros(sum(I)-1,1);
for cnt = 1:sum(I)-1
    val(cnt) = sum((single_chan(idx(cnt)-1:idx(cnt)+size(mark_temp1,2)-2) == -512) == (mark_temp1==-1));
end
idx = idx(val>=10);
I = false(size(I,2),1);
I(idx) = -1;

% Load timestamps from CSV with proper parsing
opts = detectImportOptions(csvFile);
opts = setvaropts(opts, {'AlignedTimestamp'}, 'Type', 'char');
timestamps = readtable(csvFile, opts);

disp('--- CSV Timestamps ---');
disp(head(timestamps));
disp('-----------------------');

% Parse timestamps
date_part = datetime(sessionDate, 'InputFormat', 'yyyy-MM-dd');
date_time = arrayfun(@(x) parseTimeString(x, date_part), timestamps.AlignedTimestamp);

if all(isnat(date_time))
    error('All timestamps parsed as NaT — please check the column name and format.');
end

RNS_stamp = seconds(date_time - date_time(1));
RNS_stamp = RNS_stamp - RNS_stamp(1);
N_stamp = size(RNS_stamp,1);

mrk_det = (idx-1).'/Ephys.Fs;
n_shift = (mrk_det + RNS_stamp(end)) < N/Ephys.Fs;

disp('--- DEBUG INFO ---');
disp(['Earliest EEG time: ' datestr(min(date_time))]);
disp(['Latest EEG time: ' datestr(max(date_time))]);
disp(['EEG Duration (seconds): ' num2str(N/Ephys.Fs)]);
disp(['# EEG Markers Detected: ' num2str(length(idx))]);
disp(['First EEG marker time (mrk_det(1)): ' num2str(mrk_det(1)) ' sec']);
disp(['Last RNS_stamp: ' num2str(RNS_stamp(end)) ' sec']);
disp('-------------------');

if sum(n_shift) == 0
    error('No valid n_shift detected — timestamps may not align with EEG data.');
end

for cnt = 1:sum(n_shift)
    ptrn_det(:,cnt) = mrk_det(cnt) + RNS_stamp; 
    [err(:,cnt), ix_det(:,cnt)] = min(abs(mrk_det - ptrn_det(:,cnt)'));
end

[~, id] = min(mean(abs(err)));
max_err = max(abs(err(:,id)));
disp(['pattern detection: ' num2str(round(max_err*1e3)/1e3) ' seconds max shift'])

Tsh_delay = 5;
valid_mrk = err(:,id) < Tsh_delay;
Mrk = (mrk_det(ix_det(valid_mrk,id)));

if any(err(:,id) >= Tsh_delay)
    warning([num2str(sum(valid_mrk==0)) ' markers were not detected'])
    disp('predicting missing markers...')
    [P, S] = polyfit(ptrn_det(valid_mrk,id), Mrk,1);
    [Mrk_mdl, delta] = polyval(P, ptrn_det(:,id), S);
    disp(['prediction error std: ' num2str(max(delta)*1e3) ' ms'])
    Mrk(~valid_mrk,1) = Mrk_mdl(~valid_mrk,1);
end

Ephys.mrk = round(Mrk*Ephys.Fs) + 1;

time = ([1:N]-1)/Ephys.Fs;
Ephys.daytime = date_time(1) + seconds(time-ptrn_det(1,id));

% Post-process markers to trim to desired time window
mrk_times = Ephys.daytime(Ephys.mrk);
keep_mrks = (mrk_times >= desired_start_time) & (mrk_times <= desired_end_time);
Ephys.mrk = Ephys.mrk(keep_mrks);

% Plotting
figure;
plot(Ephys.daytime, single_chan, 'r')
hold on
plot(Ephys.daytime, I*(-512), 'b')
plot(Ephys.daytime(1) + seconds(ptrn_det(:,id)), ones(N_stamp,1)*-512, 'ro')
plot(Ephys.daytime(Ephys.mrk), ones(length(Ephys.mrk),1)*-512, 'bo')
xlim([min(Ephys.daytime) max(Ephys.daytime)])
set(gcf,'Position',[0,20,1800,500])
set(gca,'Fontsize',12)
xlabel('Day Time')
ylabel('iEEG')

% Artifact correction
I = false(1,N);
I(Ephys.mrk) = true;
J = logical([0 I(1:end-1)]);
mark_ind = find(J == 1);
for i_mark = 1:sum(J)
    J(mark_ind(i_mark) - 2:mark_ind(i_mark) + size(mark_temp1,2)-3) = -1 * mark_temp1;
end
valid_samples = find(J == 0);
lfp_valid = Ephys.raw(:,valid_samples);
all_samples = 1:N;
lfp_new = spline(valid_samples, lfp_valid, all_samples);

Ephys.raw = lfp_new;

end


function parsedTime = parseTimeString(time_str, date_part)
    % Check if time_str is already datetime or NaT
    if isdatetime(time_str)
        parsedTime = time_str;
        return;
    end

    % Check for missing or empty strings
    if isempty(time_str) || any(strcmp(time_str, {'NaN', 'NaT'}))
        parsedTime = NaT;
        return;
    end

    % Convert numeric input to string if needed
    if isnumeric(time_str)
        time_str = string(time_str);
    elseif iscell(time_str)
        time_str = string(time_str{1});
    end

    % Ensure time_str is a string
    time_str = string(time_str);

    % Split time string into hour, minute, second, microsecond
    time_parts = split(time_str, ':');
    if numel(time_parts) < 3
        parsedTime = NaT;
        return;
    end

    hour = str2double(time_parts{1});
    minute = str2double(time_parts{2});
    second_parts = split(time_parts{3}, '.');
    second = str2double(second_parts{1});
    microsecond = 0;
    if length(second_parts) > 1
        microsecond = str2double(second_parts{2});
    end

    parsedTime = date_part + hours(hour) + minutes(minute) + ...
                 seconds(second) + milliseconds(microsecond / 1000);
end
