
% import_RNS.m
% Adapted by Myra Saraí Larson, 2025
% Suthana Lab, UCLA

clearvars;
close all;
clc;

% -----------------------------
% Session and File Parameters
% -----------------------------
Subj_ID = 'R019';  
Ephys.Info.Subject = Subj_ID;
Ephys.Info.Task = "Observation Reward"; 
Ephys.Info.Session = "1"; 
Ephys.Info.outdir = 'Ephys_Struct/';
Ephys.Fs = 250;

% -----------------------------
% Folder Paths
% -----------------------------
folder = '/Users/mairahmac/Desktop/RC_TestingNotes/SelectedData/RawData'; 
marksFolder = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Marks';
session = 'Morning';
pairID = 'pair_200';
testDate = '03_17_2025';
marksFile = 'R019_ChestWalkMarksOnly.csv';

RNS_folder = [folder filesep pairID filesep testDate filesep 'RNSData' filesep Subj_ID filesep Subj_ID '_DatFiles' filesep];
baseRNS_folder = [folder filesep pairID filesep testDate filesep 'RNSData' filesep Subj_ID ];

csvFile = [marksFolder filesep marksFile];

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
desired_start_time = datetime(sessionDate + " 10:50:00", 'InputFormat', 'yyyy-MM-dd HH:mm:ss');
desired_end_time = datetime(sessionDate + " 11:00:00", 'InputFormat', 'yyyy-MM-dd HH:mm:ss');


ECoG = concat_RNS(RNS_folder, catalog, sessionDate, desired_start_time, desired_end_time);
% -----------------------------
% Marker Detection
% -----------------------------
switch Subj_ID
    case 'R019'
        for cnt = 1
            Ephys.raw = ECoG.data{cnt};
            Ephys_session = MSL_RNS_identify_mrk(Ephys, csvFile, sessionDate, desired_start_time, desired_end_time);
            save([RNS_folder 'Ephys_session' num2str(cnt) '.mat'], '-struct', 'Ephys_session');
        end
end
