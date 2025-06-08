%% Mauricio Vallejo, Martin Seeber
% RNS analysis 
% 01/01/22
% adapted to separate multiple sessions 07/14/2022
% added catalog time stamps 05/30/2025

%%%  IN
%%%% OUT
% EcoG_data = concatenated matrix. Chans x Samples. 
% 
        
function ECoG = concat_RNS(directory, catalog)
%% Select the folder with all .dat files for the session 

% directory=uigetdir;  % get directory
% addpath(directory) % add dir

files = dir([directory,'/*.dat']); % disp files in dir

id_data = contains( [catalog.Filename],{files.name});
id_real = contains( [catalog.ECoGTrigger], {'Real_Time'});

time_stamps = catalog.Timestamp(id_data & id_real);
files = files(id_real(id_data));

% raw=('/Users/mvallejomartelo/Documents/Suthana Lab/PTSD/Exposure/RNS B/s2_test');
% addpath(raw)
% files=dir(raw);

% files([1:2],:)=[];  % delete the first to elements (always empty files in the folder (. and ..))

N_files = size(files,1);
delta = zeros(N_files,1);

for cnt = 2:N_files

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
