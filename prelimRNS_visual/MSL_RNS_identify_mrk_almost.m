%% 
function Ephys = MSL_RNS_identify_mrk(Ephys, csvFile, sessionDate, desired_start_time, desired_end_time)
% Detect RNS "mark" events, align to CSV timestamps with partial overlap,
% build absolute daytime, trim to desired window, and spline-inpaint artifacts.
% INPUTS:
%   Ephys.raw  : (chans x samples), numeric
%   Ephys.Fs   : sampling rate (Hz)
%   csvFile    : path to CSV with a usable time column:
%                  'RNS_RPi_Timestamp' (absolute, possibly AM/PM) or
%                  'AlignedTimestamp'   (time-of-day) or
%                  'ParsedTimestamp'    (absolute/time-of-day)
%   sessionDate: 'yyyy-MM-dd' string or datetime
%   desired_start_time / desired_end_time : datetimes (absolute)
% OUTPUT (updates Ephys):
%   Ephys.mrk     : sample indices of aligned marks (post-trim)
%   Ephys.daytime : datetime per sample
%   Ephys.raw     : artifact-corrected LFP (spline-inpainted @ marks)


%% -------- Templates & raw channel --------

mark_temp1 = [0 -1 -1 0 0 -1 -1 0 0 0 0 0 0 -1 -1 0];
mark_temp2 = [0 -1 -1 0 0 0 0 0 0 -1 -1 0];

% Use channel 2 by request; change to (1,:) if needed
single_chan = double(Ephys.raw(2,:));
N = size(single_chan,2);


%% -------- Mark detection by xcorr --------
[mrk_corr1, ~] = xcorr(single_chan, mark_temp1); % in this channel where is the mark - initial guess 
mrk_corr1 = mrk_corr1(N:end);

[mrk_corr2, ~] = xcorr(single_chan, mark_temp2); % same thing but the second mark pattern 
mrk_corr2 = mrk_corr2(N:end);
mrk_corr2 = circshift(mrk_corr2, -4);

I = (mrk_corr1 > 0.9 * max(mrk_corr1)) | (mrk_corr2 > 0.9 * max(mrk_corr2)); % checking for maxima in detection, looking for moments in time where the corr of the signal w/ marktemp 1 or 2 is higher than 0.9 
I = [0, diff(I) == -1];
idx = find(I); % find the indices where the likely matches are for those patterns

%% -------- Refine marker detection (bounds-safe) --------
if ~isempty(idx)
    val = zeros(max(numel(idx)-1,0),1);
    for cnt = 1:numel(val)
        a = max(idx(cnt)-1,1);
        b = min(idx(cnt)+numel(mark_temp1)-2, N);
        tmpl = mark_temp1(1:(b-a+1));
        val(cnt) = sum((single_chan(a:b) == -512) == (tmpl==-1)); % basically checking to see that we aren't accidentally matching to IEDs. 512 is 2^9 (device does 2^10 divided by 2 maximum increments, -512 is the minimum increment) - very rarely possible organically to be actually precisely -512 
    end
    keep = false(numel(idx),1);
    keep(1:numel(val)) = (val>=10);
    idx = idx(keep);
end

if isempty(idx)
    error('No hardware marks detected in EEG.');
end

%% -------- Reading and Parsing CSVs --------
% supports AM/PM absolute or time-of-day
opts = detectImportOptions(csvFile, 'VariableNamingRule','preserve');

% Only set varopts for timestamp columns that actually exist
candidates = {'RNS_RPi_Timestamp','AlignedTimestamp','ParsedTimestamp'};
have = candidates(ismember(candidates, opts.VariableNames));
if isempty(have)
    error('No usable timestamp column found. Expected one of: %s. Found: %s', ...
          strjoin(candidates, ', '), strjoin(opts.VariableNames, ', '));
end
opts = setvaropts(opts, have, 'Type', 'char');

timestamps = readtable(csvFile, opts);

% Pick the first available column in preferred order
useCol = have{1};
raw_ts = string(timestamps.(useCol));

% Pre-clean: collapse spaces, trim, drop obvious junk (no colon at all)
raw_ts = strtrim(regexprep(raw_ts, '\s+', ' '));
looksLikeTime = contains(raw_ts, ':');
raw_ts = raw_ts(looksLikeTime);

% session date anchor
if ~isa(sessionDate,'datetime')
    date_part = datetime(sessionDate, 'InputFormat', 'yyyy-MM-dd');
else
    date_part = dateshift(sessionDate,'start','day');
end

% detect absolute vs time-of-day
hasDate = any(contains(raw_ts, "/")) || any(contains(raw_ts, "-"));
if hasDate
    % Try AM/PM first, then 24h, with/without milliseconds
    fmts = { ...
        'yyyy-MM-dd HH:mm:ss.SSS', ...
    };
    dt = NaT(size(raw_ts));
    for f = 1:numel(fmts)
        try
            tmp = datetime(raw_ts, 'InputFormat', fmts{f});
            fill = isnat(dt) & ~isnat(tmp);
            dt(fill) = tmp(fill);
            if all(~isnat(dt)), break; end
        catch
        end
    end
    still = isnat(dt);
    if any(still)
        try
            dt_guess = datetime(raw_ts(still)); % last resort
            dt(still) = dt_guess;
        catch
        end
    end
    date_time = dt;
else
    % time-of-day only → robust parser (handles HH:MM:SS.SSS and HH:MM:SS:ffffff)
    date_time = parseAlignedTimestamps(raw_ts, date_part);
end

%% -------- Filtering Imported CSV file --------
% Drop NaT rows from malformed timestamps (if any)
date_time = date_time(~isnat(date_time));

% Filter CSV to desired window BEFORE building RNS_stamp
in_win = (date_time >= desired_start_time) & (date_time <= desired_end_time);
date_time = date_time(in_win);

if numel(date_time) < 2
    error('Not enough CSV marks inside the requested window (%d found). Widen the window.', numel(date_time));
end

%% -------- Build CSV-relative stamp times (seconds since first kept mark) --------
RNS_stamp = seconds(date_time - date_time(1));
N_stamp   = numel(RNS_stamp);
fprintf('Using column "%s". Kept %d marks spanning %.3f s (from %s to %s)\n', ...
    useCol, N_stamp, RNS_stamp(end), datestr(min(date_time)), datestr(max(date_time)));
mrk_det = (idx-1).'/Ephys.Fs; % the times when these marks happened in seconds 
n_shift = (mrk_det + RNS_stamp(end)) < N/Ephys.Fs; %this was to be flexible with the mark pattern detection for ex: if there were only 8 marks detected but the rasp pi log file says there were 9 marks, this is handling that. 

for cnt = 1:sum(n_shift)
    
    ptrn_det(:,cnt) =  mrk_det(cnt) + RNS_stamp; 
    [err(:,cnt), ix_det(:,cnt)] = min(abs(mrk_det - ptrn_det(:,cnt)')) ;
end

[~, id] = min(mean(abs(err)));
max_err = max(abs(err(:,id)));

disp(['pattern detection: ' num2str(round(max_err*1e3)/1e3) ' seconds max shift'])

Tsh_delay = 5;

valid_mrk = err(:,id) < Tsh_delay ; 
Mrk = (mrk_det(ix_det(valid_mrk,id)));

%% Resuming Normal Processing
Ephys.mrk = round(Mrk*Ephys.Fs) + 1;

time = ([1:N]-1)/Ephys.Fs;
Ephys.daytime = date_time(1) + seconds(time-ptrn_det(1,id));
% plots! Checking whether the RNS times match the detected markers 
% red dots => predicted times baed on rasp pi timestamps 
% blue dots => detected RNS marks 
figure;
plot(Ephys.daytime, single_chan, 'r')
hold on
plot(Ephys.daytime, I*(-512), 'b')
plot(Ephys.daytime(1) + seconds(ptrn_det(:,id)), ones(N_stamp,1)*-512, 'ro')
plot(Ephys.daytime(1) + seconds((Ephys.mrk-1)/Ephys.Fs), ones(N_stamp,1)*-512, 'bo' )
% if any(err(:,id) >= Tsh_delay )
%     plot(Ephys.daytime(1) + seconds(Mrk_mdl), ones(N_stamp,1)*-512, 'mo')
%     plot(Ephys.daytime(1) + seconds(Mrk_mdl), ones(N_stamp,1)*-512, 'mx')
% end
xlim([min(Ephys.daytime) max(Ephys.daytime)])
set(gcf,'Position',[0,20,1800,500])
set(gca,'Fontsize',12)
xlabel('day time')
ylabel('iEEG')

I  = false(1,N);
I(Ephys.mrk) = true;

% I = ptrn_corr > 0.75 * (max(abs(ptrn_corr))) | ptrn_corr < 0.9*min(ptrn_corr);

%         I1 = a1 > 0.9 * (max(abs(a1)));
%         I1 = logical([I1(5:end) zeros(1,4)]);
%         I2 = a2 > 0.9 * (max(abs(a2)));
%         I3 = a3 > 0.9 * (max(abs(a3)));
%         I4 = a4 > 0.9 * (max(abs(a4)));

J = logical([0 I(1:end-1)]);
mark_ind = find(J == 1);
for i_mark = 1 : sum(J)
    J(mark_ind(i_mark) - 2:mark_ind(i_mark) + size(mark_temp1,2)-3) = -1 * mark_temp1;
end
valid_samples = find(J == 0);
lfp_valid = Ephys.raw(:,valid_samples);
all_samples = 1:N;
lfp_new = spline(valid_samples, lfp_valid, all_samples); % spline interpolation of marks 

%%
% plot(time,lfp_new(1,:),'m')

%% update struct
Ephys.raw = lfp_new;

end



%% ========= Helpers =========

function dt = parseAlignedTimestamps(raw_ts, date_part)
% Parse times of day from strings:
%   - "HH:MM:SS.SSS"      (milliseconds or more digits)
%   - "HH:MM:SS:ffffff"   (microseconds after a colon)
% Any malformed/empty rows -> NaT.

    raw_ts = string(raw_ts);
    raw_ts = strtrim(regexprep(raw_ts, '\s+', ' ')); % normalize spaces

    dt = NaT(size(raw_ts));
    for i = 1:numel(raw_ts)
        s = raw_ts(i);
        if s=="" || strcmpi(s,"nan") || strcmpi(s,"nat")
            dt(i) = NaT; continue;
        end

        tok = split(s, ':');   % string array
        if numel(tok) < 3
            dt(i) = NaT; continue;   % guard against short/invalid strings
        end

        hh = str2double(tok(1));
        mm = str2double(tok(2));

        if numel(tok) == 4
            % HH : MM : SS : FRACTION  (e.g., "10:49:40:941000")
            ss   = str2double(tok(3));
            frac = str2double("0." + regexprep(tok(4), "\D", ""));
        else
            % HH : MM : SS[.fraction]  (e.g., "10:49:40.941")
            sp   = split(tok(3), '.');
            ss   = str2double(sp(1));
            frac = 0;
            if numel(sp) > 1
                frac = str2double("0." + regexprep(sp(2), "\D", ""));
            end
        end

        if any(isnan([hh mm ss frac]))
            dt(i) = NaT;
        else
            dt(i) = date_part + hours(hh) + minutes(mm) + seconds(ss + frac);
        end
    end
end

