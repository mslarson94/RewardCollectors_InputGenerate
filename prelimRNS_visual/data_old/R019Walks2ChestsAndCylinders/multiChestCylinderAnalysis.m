%% Multi-Walk EEG Analysis: Channels 1 & 2
% Analyzing Chest vs. Cylinder Walks
% Myra Saraí Larson, 2025
% Suthana Lab, UCLA

clearvars;
close all;
clc;

% Define channels to analyze
channels_of_interest = [1 2];

% Define walk filenames (adjust as needed)
num_walks = 3;
chest_walks = cell(1, num_walks);
cylinder_walks = cell(1, num_walks);

%% Load Chest Walks
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

%% Load Cylinder Walks
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

%% Concatenate Walks
chest_walks_concat = [];
cylinder_walks_concat = [];
for i = 1:num_walks
    chest_walks_concat = [chest_walks_concat, chest_walks{i}];
    cylinder_walks_concat = [cylinder_walks_concat, cylinder_walks{i}];
end

%% Basic Analysis
Fs = 250; % Sampling rate

for ch_idx = 1:length(channels_of_interest)
    channel = channels_of_interest(ch_idx);

    % Power
    chest_power = mean(chest_walks_concat(ch_idx,:).^2);
    cylinder_power = mean(cylinder_walks_concat(ch_idx,:).^2);

    fprintf('Channel %d:\n', channel);
    fprintf('  Mean Chest Walk Power: %.4f\n', chest_power);
    fprintf('  Mean Cylinder Walk Power (Self): %.4f\n', cylinder_power);

    % Correlation between signals
    % Note: Signals must be the same length — truncate longer one
    min_len = min(size(chest_walks_concat,2), size(cylinder_walks_concat,2));
    r = corrcoef(chest_walks_concat(ch_idx,1:min_len), ...
                 cylinder_walks_concat(ch_idx,1:min_len));
    fprintf('  Correlation (r) between Chest and Cylinder Walks (Self): %.4f\n\n', r(1,2));

    % Optional: Plotting for Visual Comparison
    figure;
    subplot(2,1,1);
    plot((1:min_len)/Fs, chest_walks_concat(ch_idx,1:min_len));
    title(sprintf('Chest Walk - Channel %d', channel));
    xlabel('Time (s)');
    ylabel('Amplitude (uV)');

    subplot(2,1,2);
    plot((1:min_len)/Fs, cylinder_walks_concat(ch_idx,1:min_len));
    title(sprintf('Cylinder Walk (Self) - Channel %d', channel));
    xlabel('Time (s)');
    ylabel('Amplitude (uV)');
end

disp('Analysis complete!');
