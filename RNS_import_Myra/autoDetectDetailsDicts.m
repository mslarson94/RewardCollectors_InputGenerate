% Auto-detect keys in ParsedDetails and create columns dynamically
N_rows = height(eventData);

% Collect all unique keys
allKeys = {};
for i = 1:N_rows
    if ~isempty(eventData.ParsedDetails{i})
        allKeys = union(allKeys, fieldnames(eventData.ParsedDetails{i}));
    end
end

% Preallocate columns with NaN or empty cells
for k = 1:length(allKeys)
    key = allKeys{k};
    % Check if the first non-empty value is numeric or string
    exampleValue = [];
    for i = 1:N_rows
        if isfield(eventData.ParsedDetails{i}, key)
            exampleValue = eventData.ParsedDetails{i}.(key);
            break;
        end
    end

    % Determine the type and create the column
    if isnumeric(exampleValue)
        eventData.(key) = NaN(N_rows,1);
    else
        eventData.(key) = repmat({''}, N_rows,1);
    end
end

% Populate the columns
for i = 1:N_rows
    detailsStruct = eventData.ParsedDetails{i};
    if isempty(detailsStruct)
        continue;
    end
    for k = 1:length(allKeys)
        key = allKeys{k};
        if isfield(detailsStruct, key)
            val = detailsStruct.(key);
            if isnumeric(val)
                eventData.(key)(i) = val;
            else
                eventData.(key){i} = val;
            end
        end
    end
end
