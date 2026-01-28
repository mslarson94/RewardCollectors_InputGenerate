% Suthana Lab, UCLA
    % adapted by Martin Seeber, 2022

%function MSL_RNS_identify_mrk(Ephys, csvFile)
%function Ephys = RNS_identify_mrk(Ephys, txtFile)
function Ephys = MSL_RNS_IDMarkOnly(Ephys, csvFile, sessionDate, desired_start_time, desired_end_time, RPi_Time, outMarkersFile)

mark_temp1 = [0 -1 -1 0 0 -1 -1 0 0 0 0 0 0 -1 -1 0];
mark_temp2 = [0 -1 -1 0 0 0 0 0 0 -1 -1 0];

% mark_temp1 = [0 -1 -1 0 0 0 0 0 0  -1 -1 0];
% mark_temp2 = [0 -1 -1 0 0 0 0 0 0 0 0 0 0  -1 -1 0];
% mark_temp3 = [0 -1 -1 0 0 -1 -1 0];
% mark_temp4 = [0 -1 -1 0 ];

single_chan = Ephys.raw(1,:); % {i_trial, i_channel};
N = size(single_chan,2);

fprintf('***********************\n')
fprintf('N\n')
disp(N)
fprintf('***********************\n')

fprintf('***********************\n')
fprintf('N/Ephys.Fs\n')
disp(N/Ephys.Fs)
fprintf('***********************\n')

fprintf('***********************\n')
fprintf('Ephys.time\n')
disp(Ephys.time{1})
fprintf('***********************\n')



[mrk_corr1, ~] = xcorr(single_chan, mark_temp1);
mrk_corr1 = mrk_corr1(N:end);

[mrk_corr2, ~] = xcorr(single_chan, mark_temp2);
mrk_corr2 = mrk_corr2(N:end);
% ptrn2 starts 4 samples later
mrk_corr2 = circshift(mrk_corr2,-4);

% --- candidate detection (use rising edges, not falling) ---
I = (mrk_corr1 > 0.9*max(mrk_corr1)) | (mrk_corr2 > 0.9*max(mrk_corr2));

% rising-edge starts of high-corr regions; prepend false to catch a start at sample 1
idx0 = find(diff([false; I(:)]) == 1);

% ensure the template window fits completely in the signal (prevents dropping last mark)
tplLen = numel(mark_temp1);
fitMask = (idx0 - 1 >= 1) & (idx0 + tplLen - 2 <= N);
idx0 = idx0(fitMask);

% --- refine marker detection (score how well the -512 positions match the template) ---
val = zeros(numel(idx0), 1);
tplMask = (mark_temp1 == -1);
for k = 1:numel(idx0)
    seg = single_chan(idx0(k)-1 : idx0(k) + tplLen - 2);
    val(k) = sum( (seg == -512) == tplMask );
end

% final accepted indices
idx = idx0(val >= 10);

% idx seems to be the index in Ephys.raw where there is a really high likelihood that
% there was an actual mark detected. 

% fprintf('***********************\n')
% fprintf('idx\n')
% disp(idx)
% fprintf('***********************\n')


%% Myra's new section 
% start-of-record absolute time (already a datetime in format yyyy-MM-dd HH:mm:ss.SSS)
t0 = Ephys.time{1};

% relative and absolute time axes
Ephys.t_sec = (0:N-1).' / Ephys.Fs;     % seconds from start, column vector
Ephys.t_abs = t0 + seconds(Ephys.t_sec);% absolute datetimes per sample

% refine detections and attach their times
idx = idx(val>=10);
Ephys.idx_time_sec = Ephys.t_sec(idx);
Ephys.idx_time_abs = Ephys.t_abs(idx);

fprintf('***********************\n')
fprintf('Ephys.idx_time_abs\n')
disp(Ephys.idx_time_abs)
fprintf('***********************\n')

% --- Plot LFP over absolute time with marker overlays (no desired_start_time) ---

% assumes you already set:
%   t0           = Ephys.time{1};                % datetime start
%   Ephys.t_sec  = (0:N-1).' / Ephys.Fs;
%   Ephys.t_abs  = t0 + seconds(Ephys.t_sec);
%   idx          = idx(val>=10);
%   Ephys.idx_time_abs = Ephys.t_abs(idx);

t   = Ephys.t_abs;          % absolute timeline
lfp = Ephys.raw(1,:);       % single-channel LFP
mrk = Ephys.idx_time_abs;   % absolute times of detected marks

% choose the absolute time window (use full record by default)
t_start = t(1);
t_end   = t(end);

% window the data
win_mask = (t >= t_start) & (t <= t_end);
t_win    = t(win_mask);
lfp_win  = lfp(win_mask);

% window the markers
mrk_win  = mrk(mrk >= t_start & mrk <= t_end);

% plot
figure;
plot(t_win, lfp_win);                % LFP vs absolute time
hold on

% draw markers as vertical lines (black). Use 'b' for blue if preferred.
if ~isempty(mrk_win)
    try
        % modern MATLAB
        xline(mrk_win, 'k-');        % change 'k-' -> 'b-' for blue
    catch
        % fallback for older MATLAB without xline
        yl = ylim;
        for k = 1:numel(mrk_win)
            line([mrk_win(k) mrk_win(k)], yl, 'Color', 'k'); % change to [0 0 1] for blue
        end
    end
end

xlabel('Absolute time');
ylabel('LFP (uV)');
title('LFP aligned to absolute time with detected markers');
grid on

Ephys.idx_time_abs.Format = 'yyyy-MM-dd HH:mm:ss.SSS';
T = table( ...
    idx(:), ...
    Ephys.idx_time_sec(:), ...
    Ephys.idx_time_abs, ...
    'VariableNames', {'sample_idx','time_sec','time_abs'});
writetable(T, outMarkersFile);


end
