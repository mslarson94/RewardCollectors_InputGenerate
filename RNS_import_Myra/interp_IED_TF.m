% interp_IED.m
% Martin Seeber, 2025
% Suthana Lab, UCLA

clearvars
close all

folder = [pwd filesep 'Data'];
%sessionDate = '2025-03-17';  % Adjust as needed

Fs = 250;
f_line = 60;
f_tm = 62.5;

% voltage scale in uV (for high gain)
V_scale = 800/1024;   

% define frequency axis
f_axis = 2.^[0:0.1:7];
f_axis = f_axis(f_axis<90);
N_f = length(f_axis);

catalog = table();  % Placeholder if not using a catalog here

iEEG_runs = concat_RNS(folder, catalog, sessionDate, desired_start_time, desired_end_time);  % Added sessionDate here
iEEG = iEEG_runs{1}.' * V_scale;

[N, N_chan] = size(iEEG);

% (rest unchanged)
