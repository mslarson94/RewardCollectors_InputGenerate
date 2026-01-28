
% import_RNS.m
% Adapted by Myra Saraí Larson, 2025
% Suthana Lab, UCLA

clearvars;
close all;
clc;

% -----------------------------
% Session and File Parameters
% -----------------------------
Subj_ID = 'R037';  
Ephys.Info.Subject = Subj_ID;
Ephys.Info.Task = "Observation Reward"; 
Ephys.Info.Session = "1"; 
Ephys.Info.outdir = 'Ephys_Struct/';
Ephys.Fs = 250;

% -----------------------------
% Folder Paths
% -----------------------------
% folder = '/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/RawData'; 
% marksFolder = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS_old/SocialVsAsocial/Marks';
% session = 'Morning';
% pairID = 'pair_200';
% testDate = '03_17_2025';
% marksFile = 'R019_AN_Marks_Morning.csv';


folder = '/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/RawData'; 
marksFolder = '/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_mini/RPi_preproc/RNS';
baseFolder = '/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_mini';
session = 'Morning';
pairID = 'pair_200';
testDate = '03_17_2025';
device = "ML2A";
testTime = "11_15";
marksFile = 'ObsReward_A_03_17_2025_10_44_RNS_RPi_unified.csv';
RPi_Time_var = 'RPi_Time_verb';

RNS_folder = [folder filesep pairID filesep testDate filesep 'RNSData' filesep Subj_ID filesep Subj_ID '_DatFiles' filesep];
baseRNS_folder = [folder filesep pairID filesep testDate filesep 'RNSData' filesep Subj_ID ];

marksOut   = [Subj_ID '_1420.csv'];                 % <-- add semicolon
outDir     = fullfile(baseFolder, 'LFP_marks');                  % directory for outputs
outMarkers = fullfile(outDir, marksOut);                         % full path to CSV
if ~exist(outDir, 'dir'), mkdir(outDir); end                     % ensure folder exists

csvFile = [marksFolder filesep 'RPi_unified' filesep marksFile];

% -----------------------------
% Catalog File
% -----------------------------
catalog_file = dir([baseRNS_folder filesep Subj_ID '*.csv']);
catalog_file = catalog_file(1);
catalog = readtable([baseRNS_folder filesep catalog_file.name]);

% -----------------------------
% Load ECoG Data
% -----------------------------
sessionDate = '2025-03-17';  
%ECoG = concat_RNS(RNS_folder, catalog, sessionDate, desired_start_time, desired_end_time);

% -----------------------------
% Define time window
% -----------------------------
desired_start_time = datetime(sessionDate + " 14:20:00", 'InputFormat', 'yyyy-MM-dd HH:mm:ss');
desired_end_time = datetime(sessionDate + " 15:10:00", 'InputFormat', 'yyyy-MM-dd HH:mm:ss');

fprintf(marksOut)
fprintf('\n***********************\n')
fprintf('desired_start_time')
disp(desired_start_time)
fprintf('desired_end_time')
disp(desired_end_time)
fprintf('***********************\n')

ECoG = concat_RNS(RNS_folder, catalog, sessionDate, desired_start_time, desired_end_time);
% -----------------------------
% Marker Detection
% -----------------------------
switch Subj_ID
    case Subj_ID
        for cnt = 1
            Ephys.raw = ECoG.data{cnt};
            Ephys.time = ECoG.time
            MSL_RNS_IDMarkOnly(Ephys, csvFile, sessionDate, desired_start_time, desired_end_time, RPi_Time_var, outMarkers);
            %Ephys_session = MSL_RNS_identify_mrk(Ephys, csvFile, sessionDate, desired_start_time, desired_end_time, RPi_Time_var);
            %MSL_RNS_IDMarkOnly
            %MSL_RNS_IDMarkOnly(Ephys, csvFile, sessionDate, desired_start_time, desired_end_time, RPi_Time_var, outMarkers);

            %save([RNS_folder 'Ephys_session' num2str(cnt) '.mat'], '-struct', 'Ephys_session');
            % 
            % [mark_times, mrk_idx] = MSL_RNS_mark_times_only(Ephys, csvFile, sessionDate, desired_start_time, desired_end_time);
            % T = table(mark_times, 'VariableNames', {'MarkTime'});
            % writetable(T, fullfile('/Users/mairahmac/Desktop/', 'R037_mark_times.csv'));
        end
end
