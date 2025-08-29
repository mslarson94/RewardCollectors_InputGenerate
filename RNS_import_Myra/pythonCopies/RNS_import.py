% Martin Seeber, 2025
% Suthana Lab, UCLA

clearvars;
close all;
clc;

Subj_ID = 'R019';

Ephys.Info.Subject = Subj_ID;
Ephys.Info.Task = "Observation Reward"; 
Ephys.Info.Session = "1"; 
Ephys.Info.outdir = 'Ephys_Struct/';
Ephys.Fs = 250;    

%folder = ['C:\Users\Martin Seeber\Documents\ObservationReward\' ]; 
%folder = ['/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData/pair_200/03_17_2025/Afternoon/RPi' ]; 
folder = ['/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData' ]; 
session = 'Afternoon';
pairID = 'pair_200';
testDate = '03_17_2025';
RNS_folder = [folder filesep pairID filesep testDate filesep 'RNSData' filesep Subj_ID filesep Subj_ID '_DatFiles' filesep];
baseRNS_folder = [folder filesep pairID filesep testDate filesep 'RNSData' filesep Subj_ID ];
timestamp_folder = [folder filesep pairID filesep testDate filesep session filesep 'RPi' filesep 'RNS_RPi' filesep];

%catalog_file = dir([folder Subj_ID filesep '/*.csv']);
catalog_file = dir([ baseRNS_folder filesep Subj_ID '*.csv']);
catalog_file = catalog_file(1);
%catalog = readtable([folder Subj_ID filesep catalog_file.name]);
catalog = readtable([baseRNS_folder filesep catalog_file.name]);
% concatenate continous data 
ECoG = concat_RNS(RNS_folder, catalog);


% .logs by Epochs

fullAfternoonRPi = { [timestamp_folder '2025-03-17_13_19_53_658434492.log']; ...
                [timestamp_folder '2025-03-17_13_59_48_249350486.log'] };

switch Subj_ID

       case 'R019'
        
        % Afternoon Chest Open 
        txtFiles = { [timestamp_folder '2025-03-17_13_19_53_658434492.log']; ...
                        [timestamp_folder '2025-03-17_13_59_48_249350486.log'] };
        


        
        for cnt = 3 % R019
        % for cnt = 2 % R037
            Ephys.raw = ECoG.data{ cnt };
            Ephys_session = RNS_identify_mrk(Ephys, txtFiles{1} );
                
            save([RNS_folder 'Ephys_session' num2str(cnt) '.mat'], '-struct', 'Ephys_session')
        end
end


% 1️⃣ Load your chest file into MATLAB
chest_file = 'R037_AN_Chest_Afternoon.csv';
chests_df = readtable(chest_file);

% 2️⃣ Parse timestamps as datetime (HH:mm:ss:SSSSSS)
start_times = datetime(chests_df.start_Timestamp, 'InputFormat', 'HH:mm:ss:SSSSSS');
end_times = datetime(chests_df.end_Timestamp, 'InputFormat', 'HH:mm:ss:SSSSSS');

% 3️⃣ Apply the 7-hour offset
offset_hours = 7;  % same offset as before
start_times_adj = start_times + hours(offset_hours);
end_times_adj = end_times + hours(offset_hours);

% 4️⃣ Find sample indices in EEG data
epochs = cell(height(chests_df), 1);
time_epochs = cell(height(chests_df), 1);

for i = 1:height(chests_df)
    [~, start_idx] = min(abs(Ephys.daytime - start_times_adj(i)));
    [~, end_idx] = min(abs(Ephys.daytime - end_times_adj(i)));
    
    % Extract EEG segment
    epochs{i} = Ephys.raw(:, start_idx:end_idx);
    time_epochs{i} = Ephys.daytime(start_idx:end_idx);
end

% 5️⃣ Save results
Ephys.epochs = epochs;
Ephys.time_epochs = time_epochs;

% Save optional .mat files per epoch
for i = 1:length(epochs)
    filename = sprintf('%s_ChestEpoch_%02d.mat', Ephys.Info.Subject, i);
    save(fullfile(Ephys.Info.outdir, filename), 'epochs', 'time_epochs', 'Ephys', '-v7.3');
end

disp('EEG epochs extracted successfully with time offset applied!');


plot(Ephys.daytime, Ephys.raw(1,:)); hold on;
for i = 1:length(start_times_adj)
    xline(start_times_adj(i), 'g--');
    xline(end_times_adj(i), 'r--');
end
