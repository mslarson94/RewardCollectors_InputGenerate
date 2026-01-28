function merge_tables(fileList, outputCSV, varargin)
% Vertically merge multiple CSV/TSV tables by UNION of columns.
% - Normalizes headers to valid, unique MATLAB identifiers (consistent)
% - Coerces every column to string (robust to mixed schemas)
% - Optionally adds a source column (auto-renamed if it collides)
%
% Usage:
%   merge_tables({'a.csv','b.csv'}, 'merged.csv');
%   merge_tables('paths.txt', 'merged.csv');  % paths.txt: one file path per line
%
% Options:
%   'AddSource'   (logical) : default true
%   'SourceColumn' (char)   : default 'source_file'

p = inputParser;
addParameter(p,'AddSource',true,@islogical);
addParameter(p,'SourceColumn','source_file_events',@(s)ischar(s)||isstring(s));
parse(p,varargin{:});
addSource = p.Results.AddSource;
srcName   = char(p.Results.SourceColumn);

% -------- normalize file list --------
if ischar(fileList) || isstring(fileList)
    paths = strtrim(splitlines(fileread(fileList)));
    paths = paths(paths~="");
    files = cellstr(paths);
else
    files = cellstr(fileList);
end
assert(~isempty(files),'No input files provided.');

merged = table();
allVars = {};   % cellstr header list we maintain

for k = 1:numel(files)
    f = files{k};

    % ---- read with headers preserved, then normalize names ----
    opts = detectImportOptions(f,'VariableNamingRule','preserve');
    T = readtable(f, opts);
    % valid + unique names (cellstr)
    vnames = matlab.lang.makeValidName(T.Properties.VariableNames, 'ReplacementStyle','delete');
    vnames = matlab.lang.makeUniqueStrings(vnames);
    T.Properties.VariableNames = vnames;

    % ---- coerce all variables to string ----
    for v = 1:width(T)
        T.(v) = string(T.(v));
    end

    % ---- optional: add source column (avoid collision) ----
    if addSource
        addName = matlab.lang.makeValidName(srcName);
        if any(strcmp(T.Properties.VariableNames, addName))
            % auto-rename to a unique variant
            addName = matlab.lang.makeUniqueStrings([T.Properties.VariableNames, {addName}]); 
            addName = addName{end};  % last suggestion is unique
            warning('Input already has "%s"; adding "%s" instead.', srcName, addName);
        end
        T.(addName) = repmat(string(f), height(T), 1);
    end

        % ---- union of columns with MERGED ----
    varsThis = T.Properties.VariableNames;    % cellstr
    if isempty(merged)
        merged = T;
        allVars = varsThis;
        continue;
    end

    newAll = union(allVars, varsThis);        % cellstr

    % add missing cols to MERGED
    missMerged = setdiff(newAll, merged.Properties.VariableNames);
    for i = 1:numel(missMerged)
        merged.(missMerged{i}) = strings(height(merged),1);
    end

    % add missing cols to T
    missT = setdiff(newAll, varsThis);
    for i = 1:numel(missT)
        T.(missT{i}) = strings(height(T),1);
    end

    % reorder to the same column order and **VERTICALLY** append
    merged = merged(:, newAll);
    T      = T(:, newAll);
    merged = [merged; T];   % <-- vertical concat
    allVars = newAll;

end

writetable(merged, outputCSV);
fprintf('✅ Wrote merged table to: %s  (rows=%d, cols=%d)\n', outputCSV, height(merged), width(merged));
end
