% --------- Step 1: Load the data ---------
filename = 'Subject1.DAT';  % Update with your actual file name
[data, fs, channelLabels] = loadRNSData(filename); 
% Your lab might have a specific function for this. Replace as needed.

% --------- Step 2: Identify mark channel ---------
% Try plotting all channels briefly to visually find the mark channel
figure;
for ch = 1:size(data, 2)
    subplot(size(data, 2), 1, ch);
    plot(data(:, ch));
    title(['Channel ', num2str(ch)]);
end
% Look for a channel with discrete blips or binary-like spikes

% --------- Step 3: Focus on mark channel ---------
mark_ch = 5;  % Replace with the actual channel number once identified
mark_data = data(:, mark_ch);
time = (0:length(mark_data)-1) / fs;

figure;
plot(time, mark_data);
xlabel('Time (s)');
ylabel('Amplitude');
title('Mark Channel Visualization');
