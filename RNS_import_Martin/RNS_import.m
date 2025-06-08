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

folder = ['C:\Users\Martin Seeber\Documents\ObservationReward\' ]; 
RNS_folder = [folder Subj_ID filesep filesep Subj_ID '_DatFiles' filesep];
timestamp_folder = [ folder 'RNS_RPi' filesep];

catalog_file = dir([folder Subj_ID filesep '/*.csv']);
catalog_file = catalog_file(1);
catalog = readtable([folder Subj_ID filesep catalog_file.name]);

% concatenate continous data 
ECoG = concat_RNS(RNS_folder, catalog);

switch Subj_ID

       case 'R019'
                
        txtFiles = { [timestamp_folder '2025-03-17_13_19_53_658434492.log']; ...
                        [timestamp_folder '2025-03-17_13_59_48_249350486.log'] };
        
        for cnt = 3

            Ephys.raw = ECoG.data{ cnt };
            Ephys_session = RNS_identify_mrk(Ephys, txtFiles{1} );
                
            save([RNS_folder 'Ephys_session' num2str(cnt) '.mat'], '-struct', 'Ephys_session')
        end
end
