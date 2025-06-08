% Myra Saraí Larson, 2025
% Suthana Lab, UCLA

clearvars;
close all;
clc;

% -----------------------
% Step 1: Load Chest Walk Data
% -----------------------
chestFiles = {
    '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Chest/R019_AN_Chest_Morning.csv',
    '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Chest/R037_AN_Chest_Afternoon.csv'};

chestData = [];
for i = 1:length(chestFiles)
    opts = detectImportOptions(chestFiles{i});
    opts = setvaropts(opts, {'start_Timestamp', 'end_Timestamp'}, 'Type', 'char');
    tmp = readtable(chestFiles{i}, opts);
    
    % Convert timestamps
    date_part = datetime('03_17_2025', 'InputFormat', 'MM_dd_yyyy');
    tmp.ParsedStartTimestamp = arrayfun(@(x) parseTimeString(x, date_part), tmp.start_Timestamp);
    tmp.ParsedEndTimestamp = arrayfun(@(x) parseTimeString(x, date_part), tmp.end_Timestamp);
    
    chestData = [chestData; tmp];
end

% Compute Chest durations
chestDurations = seconds(chestData.ParsedEndTimestamp - chestData.ParsedStartTimestamp);

% -----------------------
% Step 2: Load Cylinder Walk Data (across roles)
% -----------------------
cylinderFiles = { 
    '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Cylinder/R019_AN_Cylinders_Morning.csv', 
    '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Cylinder/R019_PO_Cylinders_Afternoon.csv', 
    '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Cylinder/R037_AN_Cylinders_Afternoon.csv', 
    '/Users/mairahmac/Desktop/RC_TestingNotes/SmallSelectedData/RNS/SocialVsAsocial/Cylinder/R037_PO_Cylinders_Morning.csv'};

cylinderData = [];
for i = 1:length(cylinderFiles)
    opts = detectImportOptions(cylinderFiles{i});
    opts = setvaropts(opts, {'start_Timestamp', 'end_Timestamp'}, 'Type', 'char');
    tmp = readtable(cylinderFiles{i}, opts);

    % Convert timestamps
    date_part = datetime('03_17_2025', 'InputFormat', 'MM_dd_yyyy');
    if any(strcmp(tmp.Properties.VariableNames, 'start_Timestamp'))
        tmp.ParsedStartTimestamp = arrayfun(@(x) parseTimeString(x, date_part), tmp.start_Timestamp);
    else
        tmp.ParsedStartTimestamp = NaT(height(tmp),1);
    end
    
    if any(strcmp(tmp.Properties.VariableNames, 'end_Timestamp'))
        tmp.ParsedEndTimestamp = arrayfun(@(x) parseTimeString(x, date_part), tmp.end_Timestamp);
    else
        tmp.ParsedEndTimestamp = NaT(height(tmp),1);
    end

    tmp.FileName = repmat(cylinderFiles(i), height(tmp), 1);

    % Handle AlignedTimestamp columns
    alignedFields = {'AlignedTimestamp', 'start_AlignedTimestamp', 'end_AlignedTimestamp'};
    for a = 1:length(alignedFields)
        varName = alignedFields{a};
        if any(strcmp(tmp.Properties.VariableNames, varName))
            % Convert from char to datetime
            tmp.(varName) = arrayfun(@(x) parseTimeString(x, date_part), tmp.(varName));
        else
            tmp.(varName) = NaT(height(tmp),1);
        end
    end

    cylinderData = [cylinderData; tmp];
end

% Compute Cylinder durations
cylinderDurations = seconds(cylinderData.ParsedEndTimestamp - cylinderData.ParsedStartTimestamp);

% -----------------------
% Step 3: Find Top 3 Matches for Each Chest Walk
% -----------------------
topMatches = cell(height(chestData), 3);
topDiffs = zeros(height(chestData), 3);

for i = 1:height(chestData)
    thisDuration = chestDurations(i);
    diffs = abs(cylinderDurations - thisDuration);
    
    [sortedDiffs, sortedIdx] = sort(diffs);
    topMatches(i,:) = cylinderData.FileName(sortedIdx(1:3));
    topDiffs(i,:) = sortedDiffs(1:3);
end

% Display results
for i = 1:height(chestData)
    fprintf('\nChest Walk %d (Duration: %.2f s):\n', i, chestDurations(i));
    for j = 1:3
        fprintf('  Match %d: File = %s, Diff = %.2f s\n', ...
            j, topMatches{i,j}, topDiffs(i,j));
    end
end

% -----------------------
% Helper Function
% -----------------------
% function parsedTime = parseTimeString(time_str, date_part)
%     if isempty(time_str) || any(strcmp(time_str, {'NaN', 'NaT'}))
%         parsedTime = NaT;
%         return;
%     end
%     time_parts = split(time_str, ':');
%     if numel(time_parts) < 3
%         parsedTime = NaT;
%         return;
%     end
%     hour = str2double(time_parts{1});
%     minute = str2double(time_parts{2});
%     second_parts = split(time_parts{3}, '.');
%     second = str2double(second_parts{1});
%     microsecond = 0;
%     if length(second_parts) > 1
%         microsecond = str2double(second_parts{2});
%     end
%     parsedTime = date_part + hours(hour) + minutes(minute) + ...
%                  seconds(second) + milliseconds(microsecond / 1000);
% end


function parsedTime = parseTimeString(time_str, date_part)
    % Check if time_str is already datetime or NaT
    if isdatetime(time_str)
        parsedTime = time_str;
        return;
    end

    % Check for missing or empty strings
    if isempty(time_str) || any(strcmp(time_str, {'NaN', 'NaT'}))
        parsedTime = NaT;
        return;
    end

    % Convert numeric input to string if needed
    if isnumeric(time_str)
        time_str = string(time_str);
    elseif iscell(time_str)
        time_str = string(time_str{1});
    end

    % Ensure time_str is a string
    time_str = string(time_str);

    % Split time string into hour, minute, second, microsecond
    time_parts = split(time_str, ':');
    if numel(time_parts) < 3
        parsedTime = NaT;
        return;
    end

    hour = str2double(time_parts{1});
    minute = str2double(time_parts{2});
    second_parts = split(time_parts{3}, '.');
    second = str2double(second_parts{1});
    microsecond = 0;
    if length(second_parts) > 1
        microsecond = str2double(second_parts{2});
    end

    parsedTime = date_part + hours(hour) + minutes(minute) + ...
                 seconds(second) + milliseconds(microsecond / 1000);
end
