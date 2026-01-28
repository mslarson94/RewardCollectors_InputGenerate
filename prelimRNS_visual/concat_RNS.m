%% Mauricio Vallejo, Martin Seeber
% RNS analysis 
% 01/01/22
% adapted to separate multiple sessions 07/14/2022
% added catalog time stamps 05/30/2025

%%%  IN
%%%% OUT
% EcoG_data = concatenated matrix. Chans x Samples. 
% 
        
function ECoG = concat_RNS(directory, catalog, sessionDate, desired_start_time, desired_end_time)
%% Select the folder with all .dat files for the session 

% directory=uigetdir;  % get directory
% addpath(directory) % add dir

files = dir([directory, '*.dat']); % disp files in dir

id_data = contains( [catalog.Filename],{files.name});
id_real = contains( [catalog.ECoGTrigger], {'Real_Time'});

time_stamps = catalog.Timestamp(id_data & id_real);
files = files(id_real(id_data));

% % Convert your desired window to datetime (e.g. 10:42 AM to 11:15 AM)
% desired_start_time = datetime(sessionDate + " 10:42:00", 'InputFormat', 'yyyy-MM-dd HH:mm:ss');
% desired_end_time = datetime(sessionDate + " 11:15:00", 'InputFormat', 'yyyy-MM-dd HH:mm:ss');


% Filter files based on timestamps
keep_files = (time_stamps >= desired_start_time) & (time_stamps <= desired_end_time);
files = files(keep_files);
time_stamps = time_stamps(keep_files);

% 🔍 DEBUG: Print out what files and times we're working with
disp(['Number of .DAT files after filtering: ' num2str(length(files))]);
fileNames = string({files.name}).';
fprintf('***********************\n')
fprintf('.DAT files:\n')
disp(fileNames)
fprintf('***********************\n')
disp(['Start time: ' datestr(min(time_stamps))]);
disp(['End time: ' datestr(max(time_stamps))]);
% raw=('/Users/mvallejomartelo/Documents/Suthana Lab/PTSD/Exposure/RNS B/s2_test');
% addpath(raw)
% files=dir(raw);

% files([1:2],:)=[];  % delete the first to elements (always empty files in the folder (. and ..))

N_files = size(files,1);
delta = zeros(N_files,1);

for cnt = 2:N_files % R019
% for cnt = 3:N_files % R037
    dt = time_stamps(cnt)- time_stamps(cnt-1);
    delta(cnt) = seconds(dt);
end

% exceeding 4 min chunks
boundary = find(delta > 250);

if ~isempty(boundary)

    for bnd = 1:length(boundary)
        
        if bnd ==1
            
            ix_session{bnd} = [1:(boundary(bnd)-1)];
        else
            ix_session{bnd} = [boundary(bnd-1):(boundary(bnd)-1)];
        end
    end

    ix_session{bnd+1} = boundary(bnd):N_files;
else

    ix_session{1} = 1:N_files;
end

N_sessions = length(ix_session);

%% loop for files. 
% unpacks and concatenates .dat files into a matlab matrix.
% chans X samples

ECoG.data = cell(1,N_sessions);

for cnt = 1:N_sessions

    ECoG.time{1,cnt} = time_stamps(ix_session{cnt}(1));
    ECoG.time{2,cnt} = time_stamps(ix_session{cnt}(end)) + seconds(240);

    for i = ix_session{cnt}
        
        tECoG_data=[];
        WaveformCount = 4;

        DATFile=files(i).name; % file for preloading
        
        % unpack and convert from .dat
        fid = fopen([directory filesep DATFile]); 
        
        dat = fread(fid,'int16');
        fclose(fid);
        
        % populate channels
        ChannelNum = 0;
        
        for ChannelIndex = 1:4
            ChannelNum = ChannelNum + 1;
            tECoG_data(ChannelIndex,:) = dat(ChannelNum:WaveformCount:end)'-512;
        end
        
        % concatenates loaded file to matrix
        
        ECoG.data{cnt} = horzcat(ECoG.data{cnt},tECoG_data);
    end

end
