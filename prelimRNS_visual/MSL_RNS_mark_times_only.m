function [mark_times, mrk_idx, daytime] = MSL_RNS_mark_times_only(Ephys, csvFile, sessionDate, desired_start_time, desired_end_time)
    % --- detection & alignment identical to Script 4 up through Ephys.mrk/daytime ---
    % (paste your detection code here unchanged, up to Ephys.mrk and Ephys.daytime)
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
    
    % disp('All Mark Times sec');
    % disp([num2str(mrk_det) ' sec']);
    
    % T = table(mrk_det, 'VariableNames', {'MarkTime'});
    % writetable(T, fullfile('/Users/mairahmac/Desktop/', 'mark_times.csv'));
    % R = table(RNS_stamp, 'VariableNames', {'RNS_Timestamp'});
    % writetable(R, fullfile('/Users/mairahmac/Desktop/', 'RNS_timestamps.csv'));
    
    
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
    
    mrk_times_all = Ephys.daytime(Ephys.mrk);
    keep = (mrk_times_all >= desired_start_time) & (mrk_times_all <= desired_end_time);
    mrk_idx   = Ephys.mrk(keep);          % sample indices of kept marks
    mark_times = Ephys.daytime(mrk_idx);  % absolute times of kept marks
    daytime   = Ephys.daytime;            % for reference if needed
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
