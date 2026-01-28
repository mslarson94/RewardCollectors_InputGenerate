function logdata = read_logfile(filename)

%% Set up the Import Options and import the data
opts = delimitedTextImportOptions("NumVariables", 1);

% Specify range and delimiter
opts.DataLines = [1, Inf];
opts.Delimiter = "";

% Specify column names and types
opts.VariableNames = "VarName1";
opts.VariableTypes = "char";

% Specify file level properties
opts.MissingRule = "omitrow";
opts.ExtraColumnsRule = "ignore";
opts.EmptyLineRule = "read";

% Specify variable properties
opts = setvaropts(opts, "VarName1", "WhitespaceRule", "preserve");
opts = setvaropts(opts, "VarName1", "EmptyFieldRule", "auto");

% Import the data
logdata = readtable(filename, opts);

%% Convert to output type
logdata = table2cell(logdata);
numIdx = cellfun(@(x) ~isnan(str2double(x)), logdata);
logdata(numIdx) = cellfun(@(x) {str2double(x)}, logdata(numIdx));

%% Clear temporary variables
clear numIdx opts