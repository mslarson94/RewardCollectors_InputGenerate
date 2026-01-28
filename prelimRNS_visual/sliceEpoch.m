% slice_epoch.m
% Slice the EEG data to the window 10:56:16.791000 - 10:56:36.468000

clearvars;
close all;
clc;

%% Load the processed Ephys data
% (Make sure you've saved your .mat file after running MSL_importRNS.m)

% Adjust the file path as needed:
%load('/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS_preprocs/Ephys_session.mat');
load('/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS_preprocs/Ephys_Session_R019CylinderWalk1.mat');
%% Define the mini-epoch time window
date_part = '2025-03-17';
% 10:56:19:791000	10:56:34:468000
% 10:56:38:210000	10:56:54:555000
% 10:56:58:319000	10:57:09:818000

%11:20:58:522	11:21:15:134
%11:23:08:994	11:23:23:620
%11:28:31:472	11:28:45:540
% Define start and end times with millisecond precision
desired_start_time = datetime([date_part ' 11:28:29.472'], ...
                              'InputFormat', 'yyyy-MM-dd HH:mm:ss.SSS');
desired_end_time = datetime([date_part ' 11:28:45.540'], ...
                            'InputFormat', 'yyyy-MM-dd HH:mm:ss.SSS');

%% Slice the data based on Daytime
time_idx = (Ephys_session.daytime >= desired_start_time) & ...
           (Ephys_session.daytime <= desired_end_time);

Ephys_sliced.raw = Ephys_session.raw(:, time_idx);
Ephys_sliced.daytime = Ephys_session.daytime(time_idx);

%% Slice the markers (optional)
if isfield(Ephys_session, 'mrk')
    Ephys_sliced.mrk = Ephys_session.mrk( ...
        Ephys_session.daytime(Ephys_session.mrk) >= desired_start_time & ...
        Ephys_session.daytime(Ephys_session.mrk) <= desired_end_time ...
    );
else
    Ephys_sliced.mrk = [];
end

%% Plot the sliced data
figure;
plot(Ephys_sliced.daytime, Ephys_sliced.raw(1,:), 'b');
xlabel('Time');
ylabel('EEG Signal (Channel 1)');
title('Sliced EEG Data (Channel 1)');
grid on;

%% Print summary
disp('----- EEG Slice Summary -----');
disp(['Start time: ' datestr(min(Ephys_sliced.daytime))]);
disp(['End time:   ' datestr(max(Ephys_sliced.daytime))]);
disp(['Duration:   ' num2str(seconds(max(Ephys_sliced.daytime) - min(Ephys_sliced.daytime))) ' seconds']);
disp(['Number of markers in slice: ' num2str(length(Ephys_sliced.mrk))]);
disp('------------------------------');

%% Save the sliced data if needed
% save('Ephys_sliced.mat', 'Ephys_sliced');
