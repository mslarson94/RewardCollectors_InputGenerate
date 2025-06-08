%% PSD and Headmap Analysis: Channels 1 & 2
% Myra Saraí Larson, 2025
% Suthana Lab, UCLA

clearvars;
close all;
clc;

% Define channels to analyze
channels_of_interest = [1 2];

% Sampling rate
Fs = 250;

% Load concatenated chest and cylinder walk data
% (Use the same files from earlier analyses)
num_walks = 3;
chest_walks = cell(1, num_walks);
cylinder_walks = cell(1, num_walks);

% Load Chest Walks
for i = 1:num_walks
    filename = sprintf('R019_ChestWalk%d.mat', i);
    tmp = load(filename);
    if isfield(tmp, 'Ephys_sliced')
        data = tmp.Ephys_sliced.raw(channels_of_interest, :);
    elseif isfield(tmp, 'signal')
        data = tmp.signal.raw(channels_of_interest, :);
    elseif isfield(tmp, 'Ephys')
        data = tmp.Ephys.raw(channels_of_interest, :);
    else
        error('Unknown variable name in file %s', filename);
    end
    chest_walks{i} = data;
end

% Load Cylinder Walks
for i = 1:num_walks
    filename = sprintf('R019_CylinderWalk%d_Self.mat', i);
    tmp = load(filename);
    if isfield(tmp, 'Ephys_sliced')
        data = tmp.Ephys_sliced.raw(channels_of_interest, :);
    elseif isfield(tmp, 'signal')
        data = tmp.signal.raw(channels_of_interest, :);
    elseif isfield(tmp, 'Ephys')
        data = tmp.Ephys.raw(channels_of_interest, :);
    else
        error('Unknown variable name in file %s', filename);
    end
    cylinder_walks{i} = data;
end

% Concatenate Walks
chest_concat = [];
cylinder_concat = [];
for i = 1:num_walks
    chest_concat = [chest_concat, chest_walks{i}];
    cylinder_concat = [cylinder_concat, cylinder_walks{i}];
end

%% PSD Analysis
window = 2 * Fs;  % 2-second window
noverlap = round(0.5 * window);  % 50% overlap

for ch_idx = 1:length(channels_of_interest)
    channel = channels_of_interest(ch_idx);
    figure;
    
    % Chest Walk PSD
    [Pxx_chest, F_chest] = pwelch(chest_concat(ch_idx,:), window, noverlap, [], Fs);
    subplot(2,1,1);
    plot(F_chest, 10*log10(Pxx_chest));
    title(sprintf('PSD - Chest Walk - Channel %d', channel));
    xlabel('Frequency (Hz)');
    ylabel('Power/Frequency (dB/Hz)');
    xlim([0 100]);
    grid on;
    
    % Cylinder Walk PSD
    [Pxx_cyl, F_cyl] = pwelch(cylinder_concat(ch_idx,:), window, noverlap, [], Fs);
    subplot(2,1,2);
    plot(F_cyl, 10*log10(Pxx_cyl));
    title(sprintf('PSD - Cylinder Walk Self - Channel %d', channel));
    xlabel('Frequency (Hz)');
    ylabel('Power/Frequency (dB/Hz)');
    xlim([0 100]);
    grid on;
end

%% Headmap Example (topoplot)
% NOTE: This example uses made-up data; to generate realistic headmaps,
% you need channel locations (electrode positions). Here, we’ll simulate that.

% Average Power Calculation (0-100 Hz) per channel
chest_avg_power = zeros(length(channels_of_interest),1);
cylinder_avg_power = zeros(length(channels_of_interest),1);

for ch_idx = 1:length(channels_of_interest)
    % Chest
    [Pxx, ~] = pwelch(chest_concat(ch_idx,:), window, noverlap, [], Fs);
    chest_avg_power(ch_idx) = mean(10*log10(Pxx));
    
    % Cylinder
    [Pxx, ~] = pwelch(cylinder_concat(ch_idx,:), window, noverlap, [], Fs);
    cylinder_avg_power(ch_idx) = mean(10*log10(Pxx));
end

% Example headmap using topoplot (requires EEGLAB Toolbox)
% Create dummy electrode positions (you’ll need to replace with your actual layout)
% Assuming channels 1 and 2 are frontal left and right, respectively

chanlocs = struct('labels', {'F3','F4'}, 'X',{ -1, 1 }, 'Y',{ 1, 1 }, 'Z',{0,0});

% % Headmap for Chest Walk
% figure;
% topoplot(chest_avg_power, chanlocs, 'style', 'both', 'electrodes', 'labels');
% title('Chest Walk - Average Power');

% % Headmap for Cylinder Walk
% figure;
% topoplot(cylinder_avg_power, chanlocs, 'style', 'both', 'electrodes', 'labels');
% title('Cylinder Walk Self - Average Power');

disp('PSD and headmap analysis complete!');

% Concatenate Walks
chest_concat = [];
cylinder_concat = [];
for i = 1:num_walks
    chest_concat = [chest_concat, chest_walks{i}];
    cylinder_concat = [cylinder_concat, cylinder_walks{i}];
end

%% Generate Spectrogram Heatmaps
window_length = 2 * Fs;  % 2-second window
noverlap = round(0.5 * window_length);

for ch_idx = 1:length(channels_of_interest)
    channel = channels_of_interest(ch_idx);
    
    % Chest Walks
    figure;
    [S_chest, F_chest, T_chest, P_chest] = spectrogram(chest_concat(ch_idx,:), window_length, noverlap, [], Fs, 'yaxis');
    imagesc(T_chest, F_chest, 10*log10(P_chest));
    axis xy;
    colorbar;
    xlabel('Time (s)');
    ylabel('Frequency (Hz)');
    title(sprintf('Chest Walks - Spectrogram - Channel %d', channel));
    ylim([0 100]);
    set(gca, 'FontSize', 14);
    
    % Cylinder Walks
    figure;
    [S_cyl, F_cyl, T_cyl, P_cyl] = spectrogram(cylinder_concat(ch_idx,:), window_length, noverlap, [], Fs, 'yaxis');
    imagesc(T_cyl, F_cyl, 10*log10(P_cyl));
    axis xy;
    colorbar;
    xlabel('Time (s)');
    ylabel('Frequency (Hz)');
    title(sprintf('Cylinder Walks - Spectrogram - Channel %d', channel));
    ylim([0 100]);
    set(gca, 'FontSize', 14);
end

disp('Spectrogram analysis complete!');
