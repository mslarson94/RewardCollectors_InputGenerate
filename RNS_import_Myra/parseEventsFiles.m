% Myra Saraí Larson, 2025
% Suthana Lab, UCLA

clearvars;
close all;
clc;

Subj_ID = 'R037';
session = 'Afternoon';
pairID = 'pair_200';
testing_date = '03_17_2025';
role = 'AN';
eventName = 'ChestMarks';  % e.g. 'CylinderWalks'

trueRootFolder = ['/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS' ]; 
eventPattern = [Subj_ID '_' role '_' eventName '_' session '.csv'];
eventFile = [trueRootFolder filesep 'SocialVsAsocial' filesep eventName filesep eventPattern];

% Detect import options
opts = detectImportOptions(eventFile);
opts = setvaropts(opts, {'Timestamp', 'start_Timestamp', 'end_Timestamp'}, 'Type', 'char');  % Force as text

% Read the table
eventData = readtable(eventFile, opts);

% Convert testing date string to datetime object
date_part = datetime(testing_date, 'InputFormat', 'MM_dd_yyyy');

% Initialize arrays
N_rows = height(eventData);
parsedTimestamps = datetime([],[],[]);
parsedStartTimestamps = datetime([],[],[]);
parsedEndTimestamps = datetime([],[],[]);

% Parse timestamps
for i = 1:N_rows
    % Timestamp
    time_str = eventData.Timestamp{i};
    parsedTimestamps(i,1) = parseTimeString(time_str, date_part);

    % start_Timestamp
    start_time_str = eventData.start_Timestamp{i};
    parsedStartTimestamps(i,1) = parseTimeString(start_time_str, date_part);

    % end_Timestamp
    end_time_str = eventData.end_Timestamp{i};
    parsedEndTimestamps(i,1) = parseTimeString(end_time_str, date_part);
end

% Add new columns to the table
eventData.ParsedTimestamp = parsedTimestamps;
eventData.ParsedStartTimestamp = parsedStartTimestamps;
eventData.ParsedEndTimestamp = parsedEndTimestamps;

% Preview
disp(eventData(1:5, :));

%% Helper function
function parsedTime = parseTimeString(time_str, date_part)
    % Helper function to parse time string to datetime
    if isempty(time_str) || any(strcmp(time_str, {'NaN', 'NaT'}))
        parsedTime = NaT;
        return;
    end

    time_parts = split(time_str, ':');
    if numel(time_parts) < 3
        parsedTime = NaT;
        return;
    end

    hour = str2double(time_parts{1});
    minute = str2double(time_parts{2});

    % Handle second + microseconds or milliseconds
    second_parts = split(time_parts{3}, '.');
    second = str2double(second_parts{1});
    if length(second_parts) > 1
        microsecond = str2double(second_parts{2});
    else
        microsecond = 0;
    end

    parsedTime = date_part + hours(hour) + minutes(minute) + ...
                 seconds(second) + milliseconds(microsecond / 1000);
end
