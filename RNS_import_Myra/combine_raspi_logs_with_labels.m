 
% combine_raspi_logs_with_labels.m
% Concatenates Raspberry Pi .log files into one .txt file, preserving filenames.

log_dir = '/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData/pair_200/03_17_2025/Afternoon/RPi/RNS_RPi';  % 🔁 CHANGE THIS to your actual folder path
output_file = fullfile(log_dir, 'CombinedRaspiLogs.txt');
log_files = dir(fullfile(log_dir, '*.log'));

fid_out = fopen(output_file, 'w');

for k = 1:length(log_files)
    file_name = log_files(k).name;
    full_path = fullfile(log_dir, file_name);

    fprintf(fid_out, '[%s]\n', file_name);  % Add header with filename

    fid_in = fopen(full_path, 'r');
    tline = fgetl(fid_in);
    while ischar(tline)
        fprintf(fid_out, '%s\n', tline);
        tline = fgetl(fid_in);
    end
    fclose(fid_in);

    fprintf(fid_out, '\n');  % Add space between files
end

fclose(fid_out);
fprintf('✅ Combined log written to: %s\n', output_file);
