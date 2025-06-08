% Myra Saraí Larson, 2025
% Suthana Lab, UCLA

clearvars;
close all;
clc;

% -----------------------
% Main Script
% -----------------------
Subj_ID = 'R019';
session = 'Morning';
pairID = 'pair_200';
testing_date = '03_17_2025';
role = 'AN';
eventName = 'Marks';  % Change to 'Cylinders' or 'Chest' as needed

trueRootFolder = '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS'; 
eventPattern = [Subj_ID '_' role '_' eventName '_' session '.csv'];
eventFile = fullfile(trueRootFolder, 'SocialVsAsocial', eventName, eventPattern);

% Detect import options
opts = detectImportOptions(eventFile);
% Always set Timestamp as text
opts = setvaropts(opts, 'Timestamp', 'Type', 'char');

% If start_Timestamp and end_Timestamp exist, set them as text
timestampFields = {'start_Timestamp', 'end_Timestamp'};
for i = 1:length(timestampFields)
    if any(strcmp(opts.VariableNames, timestampFields{i}))
        opts = setvaropts(opts, timestampFields{i}, 'Type', 'char');
    end
end

% If details column exists, set it as text
if any(strcmp(opts.VariableNames, 'details'))
    opts = setvaropts(opts, 'details', 'Type', 'char');
end

% Read the table
eventData = readtable(eventFile, opts);

% Convert testing date string to datetime object
date_part = datetime(testing_date, 'InputFormat', 'MM_dd_yyyy');

% Initialize arrays
N_rows = height(eventData);
parsedTimestamps = datetime([],[],[]);
parsedStartTimestamps = datetime([],[],[]);
parsedEndTimestamps = datetime([],[],[]);
parsedDetails = cell(N_rows, 1);

% Parse timestamps
for i = 1:N_rows
    % Timestamp
    time_str = eventData.Timestamp{i};
    parsedTimestamps(i,1) = parseTimeString(time_str, date_part);

    % start_Timestamp (if exists)
    if any(strcmp(eventData.Properties.VariableNames, 'start_Timestamp'))
        start_time_str = eventData.start_Timestamp{i};
        parsedStartTimestamps(i,1) = parseTimeString(start_time_str, date_part);
    end

    % end_Timestamp (if exists)
    if any(strcmp(eventData.Properties.VariableNames, 'end_Timestamp'))
        end_time_str = eventData.end_Timestamp{i};
        parsedEndTimestamps(i,1) = parseTimeString(end_time_str, date_part);
    end

    % details (if exists)
    if any(strcmp(eventData.Properties.VariableNames, 'details'))
        rawDetails = eventData.details{i};
        if isempty(rawDetails)
            parsedDetails{i} = struct();
        else
            % Replace single quotes with double quotes for JSON compatibility
            jsonStr = strrep(rawDetails, '''', '"');
            % Parse with jsondecode
            try
                detailsStruct = jsondecode(jsonStr);
                parsedDetails{i} = detailsStruct;
            catch
                warning('Failed to parse details in row %d: %s', i, rawDetails);
                parsedDetails{i} = struct();
            end
        end
    end
end

% % Add the new columns to the table
% eventData.ParsedTimestamp = parsedTimestamps;
% 
% if exist('parsedStartTimestamps', 'var') && any(parsedStartTimestamps)
%     eventData.ParsedStartTimestamp = parsedStartTimestamps;
% end
% if exist('parsedEndTimestamps', 'var') && any(parsedEndTimestamps)
%     eventData.ParsedEndTimestamp = parsedEndTimestamps;
% end
% if exist('parsedDetails', 'var') && any(~cellfun(@isempty, parsedDetails))
%     eventData.ParsedDetails = parsedDetails;
% end

% Add the new columns to the table
eventData.ParsedTimestamp = parsedTimestamps;

if exist('parsedStartTimestamps', 'var') && any(~isnat(parsedStartTimestamps))
    eventData.ParsedStartTimestamp = parsedStartTimestamps;
end
if exist('parsedEndTimestamps', 'var') && any(~isnat(parsedEndTimestamps))
    eventData.ParsedEndTimestamp = parsedEndTimestamps;
end
if exist('parsedDetails', 'var') && any(~cellfun(@isempty, parsedDetails))
    eventData.ParsedDetails = parsedDetails;
end


% Preview
disp(eventData(1:5, :));

% -----------------------
% Helper Functions
% -----------------------

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
