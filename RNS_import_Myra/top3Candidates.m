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
    
    % Add file name and row number
    tmp.FileName = repmat(chestFiles(i), height(tmp), 1);
    if any(strcmp(tmp.Properties.VariableNames, 'original_row_start'))
        tmp.OriginalRow = tmp.original_row_start;
    else
        tmp.OriginalRow = (1:height(tmp))';
    end
    
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

    % Add file name and row number
    tmp.FileName = repmat(cylinderFiles(i), height(tmp), 1);
    if any(strcmp(tmp.Properties.VariableNames, 'original_row_start'))
        tmp.OriginalRow = tmp.original_row_start;
    else
        tmp.OriginalRow = (1:height(tmp))';
    end

    % Handle AlignedTimestamp columns
    alignedFields = {'AlignedTimestamp', 'start_AlignedTimestamp', 'end_AlignedTimestamp'};
    for a = 1:length(alignedFields)
        varName = alignedFields{a};
        if any(strcmp(tmp.Properties.VariableNames, varName))
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
% Step 3: Find Top 5 Matches for Each Chest Walk
% -----------------------
numMatchesToFind = 5;
topMatchesFile = cell(height(chestData), numMatchesToFind);
topMatchesRow = zeros(height(chestData), numMatchesToFind);
topDiffs = nan(height(chestData), numMatchesToFind);

for i = 1:height(chestData)
    thisDuration = chestDurations(i);
    
    % Extract participant ID from chest file name
    chestFileName = chestData.FileName{i};
    [~, chestName, ~] = fileparts(chestFileName);
    chestParts = split(chestName, '_');
    participantID = chestParts{1};
    
    % Filter Cylinder Data for this participant
    isParticipant = contains(cylinderData.FileName, participantID);
    relevantCylinderDurations = cylinderDurations(isParticipant);
    relevantCylinderFiles = cylinderData.FileName(isParticipant);
    relevantCylinderRows = cylinderData.OriginalRow(isParticipant);
    
    % Skip if no matches
    if isempty(relevantCylinderDurations)
        warning('No cylinder data found for participant %s', participantID);
        continue;
    end
    
    diffs = abs(relevantCylinderDurations - thisDuration);
    [sortedDiffs, sortedIdx] = sort(diffs);

    % Select unique file-row pairs
    selectedFiles = {};
    selectedRows = [];
    matchCount = 0;
    for k = 1:length(sortedIdx)
        currentFile = relevantCylinderFiles{sortedIdx(k)};
        currentRow = relevantCylinderRows(sortedIdx(k));
        % Check for duplicates
        isDuplicate = any(strcmp(selectedFiles, currentFile) & selectedRows == currentRow);
        if ~isDuplicate
            matchCount = matchCount + 1;
            topMatchesFile{i, matchCount} = currentFile;
            topMatchesRow(i, matchCount) = currentRow;
            topDiffs(i, matchCount) = sortedDiffs(k);
            selectedFiles{end+1} = currentFile;
            selectedRows(end+1) = currentRow;
        end
        if matchCount == numMatchesToFind
            break;
        end
    end
end

% -----------------------
% Display Results
% -----------------------
for i = 1:height(chestData)
    fprintf('\nChest Walk %d\n', i);
    fprintf('  File: %s\n', chestData.FileName{i});
    fprintf('  Original Row: %d\n', chestData.OriginalRow(i));
    fprintf('  Duration: %.2f s\n', chestDurations(i));
    fprintf('  Start Time: %s\n', datestr(chestData.ParsedStartTimestamp(i)));
    fprintf('  End Time: %s\n', datestr(chestData.ParsedEndTimestamp(i)));

    for j = 1:numMatchesToFind
        if ~isempty(topMatchesFile{i,j})
            fprintf('    Match %d:\n', j);
            fprintf('      File: %s\n', topMatchesFile{i,j});
            fprintf('      Row: %d\n', topMatchesRow(i,j));
            fprintf('      Duration Difference: %.2f s\n', topDiffs(i,j));
        else
            fprintf('    Match %d: No match found.\n', j);
        end
    end
end

% -----------------------
% Save Results to CSV
% -----------------------
outputTable = table;
outputTable.ChestFile = chestData.FileName;
outputTable.ChestRowNumber = chestData.OriginalRow;
outputTable.ChestDuration = chestDurations;

for j = 1:numMatchesToFind
    outputTable.(['Match' num2str(j) '_File']) = topMatchesFile(:,j);
    outputTable.(['Match' num2str(j) '_RowNumber']) = topMatchesRow(:,j);
    outputTable.(['Match' num2str(j) '_Diff']) = topDiffs(:,j);
end

outputFileName = '/Users/mairahmac/Desktop/ChestCylinderMatches.csv';
writetable(outputTable, outputFileName);
fprintf('\nResults saved to %s\n', outputFileName);

% -----------------------
% Helper Function
% -----------------------
function parsedTime = parseTimeString(time_str, date_part)
    if isdatetime(time_str)
        parsedTime = time_str;
        return;
    end
    if isempty(time_str) || any(strcmp(time_str, {'NaN', 'NaT'}))
        parsedTime = NaT;
        return;
    end
    if isnumeric(time_str)
        time_str = string(time_str);
    elseif iscell(time_str)
        time_str = string(time_str{1});
    end
    time_str = string(time_str);
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
