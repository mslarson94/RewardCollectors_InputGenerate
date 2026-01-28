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

% -----------------------
% Templates & raw channel
% -----------------------
mark_temp1 = [0 -1 -1 0 0 -1 -1 0 0 0 0 0 0 -1 -1 0];
mark_temp2 = [0 -1 -1 0 0 0 0 0 0 -1 -1 0];

% Use channel 2 by request; change to (1,:) if needed
single_chan = double(Ephys.raw(2,:));
N = size(single_chan,2);

% -----------------------
% Mark detection by xcorr
% -----------------------
[mrk_corr1, ~] = xcorr(single_chan, mark_temp1); % in this channel where is the mark - initial guess 
mrk_corr1 = mrk_corr1(N:end);

[mrk_corr2, ~] = xcorr(single_chan, mark_temp2); % same thing but the second mark pattern 
mrk_corr2 = mrk_corr2(N:end);
mrk_corr2 = circshift(mrk_corr2, -4);

I = (mrk_corr1 > 0.9 * max(mrk_corr1)) | (mrk_corr2 > 0.9 * max(mrk_corr2)); % checking for maxima in detection, looking for moments in time where the corr of the signal w/ marktemp 1 or 2 is higher than 0.9 
I = [0, diff(I) == -1];
idx = find(I); % find the indices where the likely matches are for those patterns

% refine marker detection (bounds-safe)
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

% -----------------------
% Read & parse CSV (supports AM/PM absolute or time-of-day)
% -----------------------
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
        'M/d/yyyy h:mm:ss.SSS a', ...
        'M/d/yyyy h:mm:ss a', ...
        'MM/dd/yyyy hh:mm:ss.SSS a', ...
        'MM/dd/yyyy hh:mm:ss a', ...
        'yyyy-MM-dd h:mm:ss.SSS a', ...
        'yyyy-MM-dd h:mm:ss a', ...
        'yyyy-MM-dd HH:mm:ss.SSS', ...
        'yyyy-MM-dd HH:mm:ss', ...
        'M/d/yyyy HH:mm:ss.SSS', ...
        'M/d/yyyy HH:mm:ss' ...
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

% Drop NaT rows from malformed timestamps (if any)
date_time = date_time(~isnat(date_time));

% Filter CSV to desired window BEFORE building RNS_stamp
in_win = (date_time >= desired_start_time) & (date_time <= desired_end_time);
date_time = date_time(in_win);

if numel(date_time) < 2
    error('Not enough CSV marks inside the requested window (%d found). Widen the window.', numel(date_time));
end

% Build CSV-relative stamp times (seconds since first kept mark)
RNS_stamp = seconds(date_time - date_time(1));
N_stamp   = numel(RNS_stamp);
fprintf('Using column "%s". Kept %d marks spanning %.3f s (from %s to %s)\n', ...
    useCol, N_stamp, RNS_stamp(end), datestr(min(date_time)), datestr(max(date_time)));

% -----------------------
% Partial-overlap alignment
% -----------------------
mrk_det = (idx-1).' / Ephys.Fs;   % Mx1 detected mark times [s]
dur     = N / Ephys.Fs;

cand = mrk_det(mrk_det < dur);
if isempty(cand)
    error('No detected marks fall inside the EEG segment.');
end

err_cell = {}; ix_cell = {}; ptrn_cell = {}; have_any = false;

for c = 1:numel(cand)
    pred    = cand(c) + RNS_stamp;          % 1 x N_stamp predicted times
    inside  = (pred >= 0) & (pred <= dur);
    pred_in = pred(inside);                  % 1 x K

    if numel(pred_in) < 2, continue; end
    have_any = true;

    % Distance matrix: (M x K)
    D = abs(mrk_det - pred_in');
    % For each predicted stamp (column), pick nearest detected mark (row)
    [err_min, ix_row] = min(D, [], 1);      % 1 x K

    err_cell{end+1}  = err_min.';           % K x 1
    ix_cell{end+1}   = ix_row.';            % K x 1 (indices into mrk_det)
    ptrn_cell{end+1} = pred_in.';           % K x 1 (predicted times used)
end

if ~have_any
    error('No valid n_shift detected — timestamps do not overlap your EEG segment.');
end

% Pick best candidate by mean absolute error
mean_err  = cellfun(@mean, err_cell);
[~, id]   = min(mean_err);
err_best  = err_cell{id};       % K_best x 1
ix_best   = ix_cell{id};        % K_best x 1
ptrn_best = ptrn_cell{id};      % K_best x 1

max_err = max(abs(err_best));
disp(['pattern detection: ' num2str(round(max_err*1e3)/1e3) ' seconds max shift'])

% Threshold and (optionally) predict missing
Tsh_delay = 5;                                  % seconds
valid_mrk = err_best < Tsh_delay;               % K_best x 1
Mrk       = mrk_det(ix_best(valid_mrk));        % matched detected mark times (s)
ptrn_kept = ptrn_best(valid_mrk);               % matched predicted times (s)

if any(~valid_mrk)
    warning([num2str(sum(~valid_mrk)) ' markers were not detected'])
    disp('predicting missing markers...')
    [P, S]         = polyfit(ptrn_kept, Mrk, 1);  % map predicted -> detected
    [Mrk_mdl, del] = polyval(P, ptrn_best, S);
    disp(['prediction error std: ' num2str(max(del)*1e3) ' ms'])
    Mrk(~valid_mrk) = Mrk_mdl(~valid_mrk);
end

Ephys.mrk = round(Mrk * Ephys.Fs) + 1;

% -----------------------
% Build absolute daytime
% -----------------------
time = ([1:N]-1) / Ephys.Fs;
Ephys.daytime = date_time(1) + seconds(time - ptrn_best(1));

% -----------------------
% Trim markers to desired absolute window
% -----------------------
mrk_times = Ephys.daytime(Ephys.mrk);
keep_mrks = (mrk_times >= desired_start_time) & (mrk_times <= desired_end_time);
Ephys.mrk = Ephys.mrk(keep_mrks);

% -----------------------
% Plot (quick QA)
% -----------------------
figure;
plot(Ephys.daytime, single_chan, 'r'); hold on
plot(Ephys.daytime, I*(-512), 'b')
plot(Ephys.daytime(1) + seconds(ptrn_best), ones(numel(ptrn_best),1)*-512, 'ro')
plot(Ephys.daytime(Ephys.mrk), ones(numel(Ephys.mrk),1)*-512, 'bo')
xlim([min(Ephys.daytime) max(Ephys.daytime)])
set(gcf,'Position',[0,20,1800,500])
set(gca,'Fontsize',12)
xlabel('Day Time'); ylabel('iEEG')

% -----------------------
% Artifact correction via spline-inpainting
% -----------------------
I_art = false(1,N);
I_art(Ephys.mrk) = true;
J = logical([0 I_art(1:end-1)]);
mark_ind = find(J == 1);
for i_mark = 1:numel(mark_ind)
    a = max(mark_ind(i_mark) - 2, 1);
    b = min(mark_ind(i_mark) + numel(mark_temp1) - 3, N);
    J(a:b) = -1 * mark_temp1(1:(b-a+1)); % bounds-safe
end
valid_samples = find(J == 0);
lfp_valid = Ephys.raw(:,valid_samples);
all_samples = 1:N;
lfp_new = spline(valid_samples, lfp_valid, all_samples);
Ephys.raw = lfp_new;

end


% =======================
% Helpers
% =======================
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
