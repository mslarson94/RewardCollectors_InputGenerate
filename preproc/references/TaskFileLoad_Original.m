%{ 
Project: Reward-Collectors 
Date Updated: Feb 04, 2025 
Researcher: Bri 
Description: This .m file is intended to preprocess and analyize
eyetracking data taken from magicleap2 system. 

loads task files from csv/excel into matlab format from two headsets

requirements - csv files, TrialValueType variable (3+ columns), TClocations, TrialExclude
refer to Preprocessing Guide
%}

%% Load data files 
tic

%check these settings
folder=''; %Change to current working directory 
numUnityFile=2;
UnityMarks=1; %0 for pre-mark files
UnityA{1}='A_ApprchAvoid_A_01_21_2025_12_27.csv'; %update to incorporate pulling in files based on their subject ID and then deviding by condition 
UnityB{1}='B_ApprchAvoid_B_01_21_2025_12_26.csv';
UnityA{2}='A_ApprchAvoid_A_01_21_2025_14_18.csv';
UnityB{2}='B_ApprchAvoid_B_01_21_2025_14_18.csv';
LinetoExclude_A=[142951 143060 143088 143089 145960]; %Update with the current data output variables we would like to exclude 
LinetoExclude_B=[];
MotiveOptiExist=1;
MBreaks_Unity = {[60.5];[200.5]};
UBreaks=[140.5]; %#ok<*NBRAK2>
Motive{1}='Take 2025-01-21 12.22.36 PM.csv';% ID phases and update file names
Motive{2}='Take 2025-01-21 12.45.28 PM.csv';
Motive{3}='Take 2025-01-21 02.16.21 PM.csv';
Motive{4}='Take 2025-01-21 02.35.56 PM.csv';
MotiveARot=[9:11; 9:11; 9:11; 9:11]; 
MotiveAPos=[12:14; 12:14; 12:14; 12:14]; 
MotiveBRot=[3:5; 3:5; 3:5; 3:5]; 
MotiveBPos=[6:8; 6:8; 6:8; 6:8];
A_UseOptiFromMotive=[1; 1; 1; 1]; 
B_UseOptiFromMotive=[1; 1; 1; 1];
Opti2Unity_translate_A=[320 25 1595; 320 25 1595; 320 25 1595; 320 25 1595]; 
Opti2Unity_translate_B=[320 25 1595; 320 25 1595; 320 25 1595; 320 25 1595];
Opti2Unity_direction=[-1 1 1; -1 1 1; -1 1 1; -1 1 1];
OptiRot_offsetA=[1.5 -3.5 0; 1.5 -3.5 0; 1.5 -3.5 0; 1.5 -3.5 0];
OptiRot_offsetB=[0 0 0; 0 0 0; 0 0 0; 0 0 0];
OptiRot_direction=[-1 -1 1; -1 -1 1; -1 -1 1; -1 -1 1];
interp_column=1;
horizontalFOV=65;
verticalFOV=50;
AutoSave=1;
UseHeadA=1;
UseEye=1;
UseBiopac=1;
UseRNS=1;

if MotiveOptiExist==0
    clear Opti2Unity_translate_A Opti2Unity_direction OptiRot_direction %update with columns in reward collectors data 
    clear Motive MotiveBPos MotiveAPos MotiveARot MotiveBRot MBreaks_Unity
    clear interp_column OptiRot_offsetB OptiRot_offsetA Opti2Unity_translate_B
end

cd(folder)
warning('off', 'MATLAB:table:ModifiedAndSavedVarnames');

TrialValueType=load('TrialValueType.mat'); TrialValueType=TrialValueType.TrialValueType;
TClocations=load('TClocations.mat'); TClocations=TClocations.TClocations;
TrialExclude=load('TrialExclude.mat'); TrialExclude=TrialExclude.TrialExclude;

pre_table=struct(); %Create a table to store all data 
fignum=1; %initialize figure numbers


%% Importing and File concatenation
% if Unity recording was done in multiple files, this section combines the files

opts=detectImportOptions(UnityA{1}); opts.VariableTypes{1}='char'; %difficulty importing in its standard format
A_import=[]; B_import=[];
A_UnitySplits=[]; B_UnitySplits=[];

%Make AppTime on a continuous timescale
for filenum=1:numUnityFile
    import=readtable(UnityA{filenum},opts);
    if filenum>1
        time1=A_import(:,1); time1=time1{end,1}; %end time of previous file
        time1=datetime(time1,'InputFormat','HH:mm:ss:SSS');
        time1.Format='HH:mm:ss.SSS';
        time2=import(:,1); time2=time2{1,1}; %start time of new file
        time2=datetime(time2,'InputFormat','HH:mm:ss:SSS');
        time2.Format='HH:mm:ss.SSS';
        time_diff=time2-time1; time_diff=double(seconds(time_diff));
        if time_diff<10 || time_diff>36000
            error('Order of Unity files may be incorrect');
        end
        import{:,2}=A_import{end,2}+time_diff+import{:,2};
        A_UnitySplits=[A_UnitySplits; size(import,1)];
    end
    A_import=[A_import; import];

    import=readtable(UnityB{filenum},opts);
    if filenum>1
        time1=B_import(:,1); time1=time1{end,1}; %end time of previous file
        time1=datetime(time1,'InputFormat','HH:mm:ss:SSS');
        time1.Format='HH:mm:ss.SSS';
        time2=import(:,1); time2=time2{1,1}; %start time of new file
        time2=datetime(time2,'InputFormat','HH:mm:ss:SSS');
        time2.Format='HH:mm:ss.SSS';
        time_diff=time2-time1; time_diff=double(seconds(time_diff));
        if time_diff<10 || time_diff>36000
            error('Order of Unity files may be incorrect');
        end
        import{:,2}=B_import{end,2}+time_diff+import{:,2};
        B_UnitySplits=[B_UnitySplits; size(import,1)];
    end
    B_import=[B_import; import];     
    
    clear import time_diff time2 time1
end

A_import{LinetoExclude_A,end}={'Fake Message'};
B_import{LinetoExclude_B,end}={'Fake Message'};

clear filenum opts

%% Timestamp
for idx=1:size(A_import(:,1),1)
    datapt=A_import(:,1);
    datapt=datapt{idx,1}; %extract value as a string
    datapt=datetime(datapt, 'InputFormat', 'HH:mm:ss:SSS');
    datapt.Format='HH:mm:ss.SSS'; %technically the date is wrong, but who cares
    pre_table.A_timestamp{idx,1}=datapt;
end
for idx=1:size(B_import(:,1),1)
    datapt=B_import(:,1);
    datapt=datapt{idx,1};%extract value as a string
    datapt=datetime(datapt, 'InputFormat', 'HH:mm:ss:SSS');
    datapt.Format='HH:mm:ss.SSS'; %technically the date is wrong, but who cares
    pre_table.B_timestamp{idx,1}=datapt;
end
clear datapt

% AppTime (another Timestamp)
pre_table.A_AppTime=A_import{:,2};
pre_table.B_AppTime=B_import{:,2};

DerivedA_AppTime(1)=0; DerivedB_AppTime=0;
for idx=2:size(pre_table.A_timestamp,1)
    DerivedA_AppTime(idx)=double(seconds(pre_table.A_timestamp{idx}-pre_table.A_timestamp{1}));
end
for idx=2:size(pre_table.B_timestamp,1)
    DerivedB_AppTime(idx)=double(seconds(pre_table.B_timestamp{idx}-pre_table.B_timestamp{1}));
end

figure(fignum); 
subplot(1, 2, 1); plot(pre_table.A_AppTime-pre_table.A_AppTime(1));
title('Unity A - Time');
hold on; plot(DerivedA_AppTime);
subplot(1, 2, 2); plot(pre_table.B_AppTime-pre_table.B_AppTime(1));
title('Unity B - Time');
hold on; plot(DerivedB_AppTime);
fig = figure(fignum); currentPosition = get(fig, 'Position');
newPosition = currentPosition - [150, 150, 0, 0]; set(fig, 'Position', newPosition);
fignum=fignum+1; 

%AppTime is now replaced by TimeStamp
pre_table.A_AppTime=DerivedA_AppTime';
pre_table.B_AppTime=DerivedB_AppTime';

%check for jumps in the data
if abs(pre_table.A_AppTime(end)-DerivedA_AppTime(end))>0.5
    warning('Headset A - two timestamps shift/drift by >1 s'); end
if abs(pre_table.B_AppTime(end)-DerivedB_AppTime(end))>0.5
    warning('Headset B - two timestamps shift/drift by >1 s'); end
clear DerivedA_AppTime DerivedB_AppTime idx

%% GlobalBlock (not the same as task block)
pre_table.A_GlobalBlock=A_import{:,3};
pre_table.B_GlobalBlock=B_import{:,3};

%% LogType (is log row Event/Message or Data)
%converted to 0 (Event), 1 (Data), or 2 (Log Restart)
prelimA=A_import{:,4}; prelimB=B_import{:,4};
for idx=1:size(prelimA,1)
    if strcmp(prelimA{idx},'Event'), pre_table.A_LogType(idx,1)=0; %#ok<*ALIGN>
    elseif strcmp(prelimA{idx},'RTdata'), pre_table.A_LogType(idx,1)=1; 
    else, pre_table.A_LogType(idx,1)=2; end
end
for idx=1:size(prelimB,1)
    if strcmp(prelimB{idx},'Event'), pre_table.B_LogType(idx,1)=0;
    elseif strcmp(prelimB{idx},'RTdata'), pre_table.B_LogType(idx,1)=1; 
    else, pre_table.B_LogType(idx,1)=2; end
end
clear prelimA prelimB

%% EyesOpen (sum of Left/RightEyeOpen) - 0 can also indicate Event/Message Log
prelimAleft=A_import{:,8}; prelimAright=A_import{:,9};
prelimBleft=B_import{:,8}; prelimBright=B_import{:,9};
pre_table.A_EyesOpen=prelimAleft+prelimAright;
pre_table.B_EyesOpen=prelimBleft+prelimBright;
clear prelimAleft prelimAright prelimBleft prelimBright

%% EyeTarget (Categories of Gaze Target)
prelimA=A_import{:,10}; prelimB=B_import{:,10};
for idx=1:size(prelimA,1)
    if strcmp(prelimA{idx},'OtherParticipant'), pre_table.A_EyeTarget(idx,1)=1;
    elseif strcmp(prelimA{idx},'closedLock'), pre_table.A_EyeTarget(idx,1)=2; 
    elseif strcmp(prelimA{idx},'Game Controller'), pre_table.A_EyeTarget(idx,1)=3; 
    elseif strcmp(prelimA{idx},'fakeCeiling'), pre_table.A_EyeTarget(idx,1)=4;
    elseif strcmp(prelimA{idx},'fakeGround'), pre_table.A_EyeTarget(idx,1)=5; 
    elseif strcmp(prelimA{idx},'fakeWallEast'), pre_table.A_EyeTarget(idx,1)=6; 
    elseif strcmp(prelimA{idx},'fakeWallNorth'), pre_table.A_EyeTarget(idx,1)=7; 
    elseif strcmp(prelimA{idx},'fakeWallSouth'), pre_table.A_EyeTarget(idx,1)=8; 
    elseif strcmp(prelimA{idx},'fakeWallWest'), pre_table.A_EyeTarget(idx,1)=9; 
    elseif strcmp(prelimA{idx},'none'), pre_table.A_EyeTarget(idx,1)=10; 
    elseif strcmp(prelimA{idx},'nothing'), pre_table.A_EyeTarget(idx,1)=11; end
end

for idx=1:size(prelimB,1)
    if strcmp(prelimB{idx},'OtherParticipant'), pre_table.B_EyeTarget(idx,1)=1;
    elseif strcmp(prelimB{idx},'closedLock'), pre_table.B_EyeTarget(idx,1)=2; 
    elseif strcmp(prelimB{idx},'Game Controller'), pre_table.B_EyeTarget(idx,1)=3; 
    elseif strcmp(prelimB{idx},'fakeCeiling'), pre_table.B_EyeTarget(idx,1)=4;
    elseif strcmp(prelimB{idx},'fakeGround'), pre_table.B_EyeTarget(idx,1)=5; 
    elseif strcmp(prelimB{idx},'fakeWallEast'), pre_table.B_EyeTarget(idx,1)=6; 
    elseif strcmp(prelimB{idx},'fakeWallNorth'), pre_table.B_EyeTarget(idx,1)=7; 
    elseif strcmp(prelimB{idx},'fakeWallSouth'), pre_table.B_EyeTarget(idx,1)=8; 
    elseif strcmp(prelimB{idx},'fakeWallWest'), pre_table.B_EyeTarget(idx,1)=9; 
    elseif strcmp(prelimB{idx},'none'), pre_table.B_EyeTarget(idx,1)=10; 
    elseif strcmp(prelimB{idx},'nothing'), pre_table.B_EyeTarget(idx,1)=11; end
end
clear prelimA prelimB

%% Head/Eye/Gaze quantified

prelimA=A_import{:,11:14}; prelimB=B_import{:,11:14};
for idx=1:size(prelimA,1)
    %Head Position (Anchored)
    if isempty(prelimA{idx,1}), pre_table.UA_HeadPos(idx,1:3)=NaN;
    else, pre_table.UA_HeadPos(idx,1:3)=str2double(strsplit(prelimA{idx,1})); end
    %Head Forth (Anchored)
    if isempty(prelimA{idx,2}), pre_table.UA_HeadFrth(idx,1:3)=NaN;
    else, pre_table.UA_HeadFrth(idx,1:3)=str2double(strsplit(prelimA{idx,2})); end
    %Eye Direction (Anchored)
    if isempty(prelimA{idx,3}), pre_table.UA_EyeDir(idx,1:3)=NaN;
    else, pre_table.UA_EyeDir(idx,1:3)=str2double(strsplit(prelimA{idx,3})); end
    %Fixation Point (Anchored)
    if isempty(prelimA{idx,4}), pre_table.UA_FixPt(idx,1:3)=NaN;
    else, pre_table.UA_FixPt(idx,1:3)=str2double(strsplit(prelimA{idx,4})); end
end
for idx=1:size(prelimB,1)
    %Head Position (Anchored)
    if isempty(prelimB{idx,1}), pre_table.UB_HeadPos(idx,1:3)=NaN;
    else, pre_table.UB_HeadPos(idx,1:3)=str2double(strsplit(prelimB{idx,1})); end
    %Head Forth (Anchored)
    if isempty(prelimB{idx,2}), pre_table.UB_HeadFrth(idx,1:3)=NaN;
    else, pre_table.UB_HeadFrth(idx,1:3)=str2double(strsplit(prelimB{idx,2})); end
    %Eye Direction (Anchored)
    if isempty(prelimB{idx,3}), pre_table.UB_EyeDir(idx,1:3)=NaN;
    else, pre_table.UB_EyeDir(idx,1:3)=str2double(strsplit(prelimB{idx,3})); end
    %Fixation Point (Anchored)
    if isempty(prelimB{idx,4}), pre_table.UB_FixPt(idx,1:3)=NaN;
    else, pre_table.UB_FixPt(idx,1:3)=str2double(strsplit(prelimB{idx,4})); end
end
clear prelimA prelimB idx

pre_table.A_HeadPos=pre_table.UA_HeadPos;
pre_table.B_HeadPos=pre_table.UA_HeadPos; pre_table.B_HeadPos(:)=NaN;
pre_table.A_HeadFrth=pre_table.UA_HeadFrth;
pre_table.B_HeadFrth=pre_table.UA_HeadFrth; pre_table.B_HeadFrth(:)=NaN;
pre_table.A_HeadDir=pre_table.UA_HeadFrth; pre_table.A_HeadDir(:)=NaN;
pre_table.B_HeadDir=pre_table.A_HeadDir;


%% Task Messages (Raw and Coded)
pre_table.A_Message=A_import{:,end};
pre_table.B_Message=B_import{:,end};

for idx=1:size(pre_table.A_Message)
    if ~isempty(pre_table.A_Message{idx,1}) && pre_table.A_LogType(idx,1)==0
        SpString=strsplit(pre_table.A_Message{idx,1});
        if strcmp(SpString{1},'Started')
            if strcmp(SpString{2},'Experiment'), pre_table.A_MCode(idx,1)=1; %Start Task
            elseif strcmp(SpString{2},'collecting.'), pre_table.A_MCode(idx,1)=26; end %Start Block
        elseif strcmp(SpString{1},'Leaving'), pre_table.A_MCode(idx,1)=2; %End Intermission/Start Trial 
        elseif strcmp(SpString{1},'Chest'), pre_table.A_MCode(idx,1)=3; %Successful collection
        elseif strcmp(SpString{2},'penalized'), pre_table.A_MCode(idx,1)=4; %Penalty/Collision
        elseif strcmp(SpString{2},'lost'), pre_table.A_MCode(idx,1)=5; %Time expired
        elseif strcmp(SpString{1},'coin'), pre_table.A_MCode(idx,1)=6; %Coin collected
        elseif strcmp(SpString{1},'Log'), pre_table.A_MCode(idx,1)=7; %Log restart post coin collection
        elseif strcmp(SpString{1},'finished'), pre_table.A_MCode(idx,1)=8; %End of block
        elseif strcmp(SpString{1},'Sending'), pre_table.A_MCode(idx,1)=25; %Manual - Mark sent (for RPi)
        elseif strcmp(SpString{1},'Mark'), pre_table.A_MCode(idx,1)=24; %Auto - Mark sent (Inter-headset calibration)
        else
            if length(SpString{1})>=9
                FirstWord=SpString{1};
                if strcmp(FirstWord(1:9),'coinsetID'), pre_table.A_MCode(idx,1)=20; %ID of upcoming block
                elseif strcmp(FirstWord(1:9),'coinpoint'), pre_table.A_MCode(idx,1)=21; %upcoming chest location
                else, pre_table.A_MCode(idx,1)=999; warning('unknown log file message, A - line %d',idx); end %something else
            else
                pre_table.A_MCode(idx,1)=999; warning('unknown log file message, A - line %d',idx); %something else
            end
        end
    end
end

%sanity check
if sum(pre_table.A_MCode==3)~=sum(pre_table.A_MCode==6)
    warning('Number of chest approaches do not equal chest collections');
end

for idx=1:size(pre_table.B_Message)
    if ~isempty(pre_table.B_Message{idx,1})
        SpString=strsplit(pre_table.B_Message{idx,1});
        if strcmp(SpString{2},'Experiment'), pre_table.B_MCode(idx,1)=11; %Start Entire Task
        elseif strcmp(SpString{2},'watching'), pre_table.B_MCode(idx,1)=12; %Start of each block
        elseif strcmp(SpString{1},'Other')
            if strcmp(SpString{4},'collected'), pre_table.B_MCode(idx,1)=13; %Coin collected
            elseif strcmp(SpString{3},'missed'), pre_table.B_MCode(idx,1)=14; %Time ran out
            elseif strcmp(SpString{3},'collided'), pre_table.B_MCode(idx,1)=15; %Collision
            elseif strcmp(SpString{4},'penalyzed'), pre_table.B_MCode(idx,1)=16; %Penalty
            end
        elseif strcmp(SpString{1},'Log'), pre_table.B_MCode(idx,1)=17; %Log restart post coin collection
        elseif strcmp(SpString{2},'preventing'), pre_table.B_MCode(idx,1)=18; %finished preventing in block
        elseif strcmp(SpString{2},'current'), pre_table.B_MCode(idx,1)=19; %finished block
        elseif strcmp(SpString{1},'Sending'), pre_table.B_MCode(idx,1)=25; %Manual - Mark sent (for RPi)
        elseif strcmp(SpString{1},'Mark'), pre_table.B_MCode(idx,1)=24; %Auto - Mark sent (Inter-headset calibration)
        else
            if length(SpString{1})>=9
                FirstWord=SpString{1};
                if strcmp(FirstWord(1:9),'coinsetID'), pre_table.B_MCode(idx,1)=20; %ID of upcoming block
                elseif strcmp(FirstWord(1:9),'coinpoint'), pre_table.B_MCode(idx,1)=21; %upcoming chest location
                else, pre_table.B_MCode(idx,1)=999; warning('unknown log file message, B - line %d',idx); end %something else
            else
                pre_table.B_MCode(idx,1)=999; warning('unknown log file message, B - line %d',idx); %something else
            end
        end
    end
end
clear FirstWord SpString


%% Determine block/trial indexes and information for each trial

%Assign trial types to each trial (imported file TrialValueType)
for trialnum=1:size(TrialValueType,1)
    if TrialValueType(trialnum,2)==100 && TrialValueType(trialnum,3)==2000 %high penalty
        TrialValueType(trialnum,4)=1;
    elseif TrialValueType(trialnum,2)==300 && TrialValueType(trialnum,3)==500 %high conflict
        TrialValueType(trialnum,4)=2;
    elseif TrialValueType(trialnum,2)==1000 && TrialValueType(trialnum,3)==100 %high reward
        TrialValueType(trialnum,4)=3;
    elseif TrialValueType(trialnum,2)==500 && TrialValueType(trialnum,3)==0 %control
        TrialValueType(trialnum,4)=4;
    else, TrialValueType(trialnum,4)=NaN; end
end

%label blocks
numblocks=find(pre_table.A_MCode==26); numblocks = [numblocks; size(pre_table.A_AppTime,1)];
for blocknum=1:size(numblocks,1)-1
    pre_table.A_BlockIndex(numblocks(blocknum):numblocks(blocknum+1)-1,1)=blocknum;
    blocklist(blocknum,1)=numblocks(blocknum); blocklist(blocknum,2)=numblocks(blocknum+1)-1;
end

%Mark-based synchronization between headsets
if UnityMarks==1
    A_marks=find(pre_table.A_MCode==24); B_marks=find(pre_table.B_MCode==24);
    if length(A_marks)~=length(B_marks), error('Inconsistent mark notes between two headsets');
    else, pre_table.Mark_AB_diff=pre_table.B_AppTime(B_marks,1)-pre_table.A_AppTime(A_marks,1);
    end
end

%label trial starts and ends
trialstarts=find(pre_table.A_MCode==2);
trialend_collect=find(pre_table.A_MCode==3);
trialend_expire=find(pre_table.A_MCode==5);
trialend=sort([trialend_collect; trialend_expire]);
if ~isequal(length(trialstarts),length(trialend_collect)+length(trialend_expire))
    error('Counts of trial starts/ends do not align');
else
    pre_table.A_trialinfo=[trialstarts trialend];
end

%Assumption that the trials line up with the imported data file (TrialValueType)
warning('Ensure that trials in pre_table.A_trialinfo line up with reality (TrialValueType)');

%Merge TrialInfo(internal) with TrialValueType(external)
pre_table.A_trialinfo(:,3:6)=TrialValueType;

%exclude trials (manual determination)
if ~exist('TrialExclude','var')
    error('The TrialExclude variable includes trials that do not match the actual data');
else
    pre_table.A_trialinfo(TrialExclude,6)=NaN;
end

pre_table.A_TrialIndex(size(pre_table.A_timestamp,1),1)=0;

%Assign each trial to its particular block, if it fits one of the trial types
for trialnum=1:size(pre_table.A_trialinfo,1)
    trialstartblock=find(blocklist(:,1)<pre_table.A_trialinfo(trialnum,1), 1, 'last' );
    trialendblock=find(blocklist(:,2)>pre_table.A_trialinfo(trialnum,1), 1, 'first' );
    if ~isequal(trialstartblock,trialendblock)
        error('Trial starts/ends do not align to block, trial %d', trialnum);
    elseif ~isnan(pre_table.A_trialinfo(trialnum,6))
        pre_table.A_TrialIndex(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),1)=TrialValueType(trialnum,4);
    end
end

%Remove TrialIndex values for excluded trials
for trialnum=1:size(pre_table.A_trialinfo,1)
    if isnan(pre_table.A_trialinfo(trialnum,6))
        pre_table.A_TrialIndex(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2))=NaN;
    end
end

%list of possible intermission index start times
itm_start_idx=sort([blocklist(:,1); find(pre_table.A_MCode==1); find(pre_table.A_MCode==5); find(pre_table.A_MCode==6); find(pre_table.A_MCode==7); find(pre_table.A_MCode==24);]);

%Find each trial's preceding intermission index
for trialnum=1:size(pre_table.A_trialinfo,1)
    pre_table.A_trialinfo(trialnum,7)=itm_start_idx(find(itm_start_idx<pre_table.A_trialinfo(trialnum,1), 1, 'last'));
    if trialnum>1
        if pre_table.A_trialinfo(trialnum,7)<pre_table.A_trialinfo(trialnum-1,2)
            %sanity check
            error('intermission apparently does not start after end of previous trial, trial %d', trialnum);
        end
    end
end

%Label successful trials
collect_idx=find(pre_table.A_MCode==6);

%Find each successful trial's corresponding subsequent collection index
for trialnum=1:size(pre_table.A_trialinfo,1)
    if pre_table.A_MCode(pre_table.A_trialinfo(trialnum,2))~=3
        pre_table.A_trialinfo(trialnum,8)=NaN; pre_table.A_trialinfo(trialnum,9)=0;
    else
        pre_table.A_trialinfo(trialnum,8)=collect_idx(find(collect_idx>pre_table.A_trialinfo(trialnum,2), 1, 'first'));
        pre_table.A_trialinfo(trialnum,9)=1;
        if trialnum<size(pre_table.A_trialinfo,1)
            if pre_table.A_trialinfo(trialnum,8)>pre_table.A_trialinfo(trialnum+1,1)
                %sanity check
                error('collection apparently happens during the next trial, trial %d', trialnum);
            end
        end
    end
end

%How many collisions happened in each trial
for trialnum=1:size(pre_table.A_trialinfo,1)
    data_seg=pre_table.A_MCode(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2));
    pre_table.A_trialinfo(trialnum,10)=length(find(data_seg==4));
end


%% OptiTrack

if MotiveOptiExist==1
    if exist('MBreaks_Unity','var'), pre_table.MBreaks_Unity=MBreaks_Unity; clear MBreaks_Unity; end
    U_division=[0 UBreaks size(pre_table.A_trialinfo,1)+1]; %divides Unity file by trials with Motive break
    M1_filenum=1; M2_filenum=1; %Number ID for motive files

    for U_filenum=1:numUnityFile %Run through Unity files

        num_Mfile=1+size(pre_table.MBreaks_Unity{U_filenum,1},2); %how many motive files for this Unity file
        Mfile_breaks=pre_table.MBreaks_Unity(U_filenum,1); Mfile_breaks=Mfile_breaks{1:num_Mfile};
        M_division=[U_division(U_filenum) Mfile_breaks U_division(U_filenum+1)];
        clear UM_indexlist
        for U_Mfilenum=1:length(M_division)-1 %Run through Motive files within a Unity file
            M_triallist(U_Mfilenum,1)=floor(M_division(U_Mfilenum))+1;
            M_triallist(U_Mfilenum,2)=ceil(M_division(U_Mfilenum+1))-1;
            UM_indexlist(U_Mfilenum,1)=pre_table.A_trialinfo(M_triallist(U_Mfilenum,1),7);
            UM_indexlist(U_Mfilenum,2)=pre_table.A_trialinfo(M_triallist(U_Mfilenum,2),8);           
            if isnan(UM_indexlist(U_Mfilenum,2))
                UM_indexlist(U_Mfilenum,2)=pre_table.A_trialinfo(M_triallist(U_Mfilenum,2),2); 
            end
            M_indexlist(M2_filenum,1)=UM_indexlist(U_Mfilenum,1);
            M_indexlist(M2_filenum,2)=UM_indexlist(U_Mfilenum,2);
            M2_filenum=M2_filenum+1;
        end
        clear num_Mfiles Mfile_breaks M_division U_Mfilenum M_triallist

        for U_Mfilenum=1:size(UM_indexlist,1)
            Motive_import=readtable(Motive{M1_filenum});
            
            %Re-format Motive_import table
            Motive_import=Motive_import(~isnan(Motive_import{:,1}),:);
            Motive_import=[Motive_import{:,1:2} Motive_import{:,MotiveAPos(M1_filenum,:)} Motive_import{:,MotiveARot(M1_filenum,:)} Motive_import{:,MotiveBPos(M1_filenum,:)} Motive_import{:,MotiveBRot(M1_filenum,:)}];
        
            %X dimension
            Motive_import(:,3)=Opti2Unity_direction(M1_filenum,1)*(Motive_import(:,3)-Opti2Unity_translate_A(M1_filenum,1))/1000;
            Motive_import(:,9)=Opti2Unity_direction(M1_filenum,1)*(Motive_import(:,9)-Opti2Unity_translate_B(M1_filenum,1))/1000;
        
            %Height dimension
            Motive_import(:,4)=Opti2Unity_direction(M1_filenum,2)*(Motive_import(:,4)-Opti2Unity_translate_A(M1_filenum,2))/1000;
            Motive_import(:,10)=Opti2Unity_direction(M1_filenum,2)*(Motive_import(:,10)-Opti2Unity_translate_B(M1_filenum,2))/1000;
        
            %Z dimension
            Motive_import(:,5)=Opti2Unity_direction(M1_filenum,3)*(Motive_import(:,5)-Opti2Unity_translate_A(M1_filenum,3))/1000;
            Motive_import(:,11)=Opti2Unity_direction(M1_filenum,3)*(Motive_import(:,11)-Opti2Unity_translate_B(M1_filenum,3))/1000;
        
            %Yaw
            Motive_import(:,6)=OptiRot_direction(M1_filenum,1)*(Motive_import(:,6)-OptiRot_offsetA(M1_filenum,1));
            Motive_import(:,12)=OptiRot_direction(M1_filenum,1)*(Motive_import(:,12)-OptiRot_offsetB(M1_filenum,1));
            
            %Pitch
            Motive_import(:,7)=OptiRot_direction(M1_filenum,2)*(Motive_import(:,7)-OptiRot_offsetA(M1_filenum,2));
            Motive_import(:,13)=OptiRot_direction(M1_filenum,2)*(Motive_import(:,13)-OptiRot_offsetB(M1_filenum,2));
            
            %Roll
            Motive_import(:,8)=OptiRot_direction(M1_filenum,3)*(Motive_import(:,8)-OptiRot_offsetA(M1_filenum,3));
            Motive_import(:,14)=OptiRot_direction(M1_filenum,3)*(Motive_import(:,14)-OptiRot_offsetB(M1_filenum,3));
            
            idx_use=UM_indexlist(U_Mfilenum,1):UM_indexlist(U_Mfilenum,2);

            figure(fignum); 
            subplot(2, 3, 1); plot(Motive_import(:,3)); title('Motive (calibrated) - X');
            subplot(2, 3, 2); plot(Motive_import(:,4)); title('Motive (calibrated) - Y');
            subplot(2, 3, 3); plot(Motive_import(:,5)); title('Motive (calibrated) - Z');
            subplot(2, 3, 4); plot(pre_table.UA_HeadPos(idx_use,1)); title('Unity - X');
            subplot(2, 3, 5); plot(pre_table.UA_HeadPos(idx_use,2)); title('Unity - Y');
            subplot(2, 3, 6); plot(pre_table.UA_HeadPos(idx_use,3)); title('Unity - Z');
            fig = figure(fignum); currentPosition = get(fig, 'Position');
            newPosition = currentPosition - [150, 150, 0, 0]; set(fig, 'Position', newPosition);
            fignum=fignum+1;
        
            %Cross-correlate from Motive to Unity
            if A_UseOptiFromMotive(M1_filenum)==1
                MotiveSampleRate=Motive_import(end,1)/Motive_import(end,2);
                
                %Calls data of interest (for this Unity/Motive file combination)
                time=pre_table.A_AppTime(idx_use);
                data=pre_table.UA_HeadPos(idx_use,:);
                data=[data pre_table.UA_HeadFrth(idx_use,:)];

                %removes duplicates from Unity timeseries - needed for interp1
                [Unity_time_unique, unique_idx] = unique(time, 'stable');
                Unity_data_unique = data(unique_idx,1:6);

                %removes NaN
                nonan_idx=~isnan(Unity_data_unique(:,1)); 
                Unity_data_unique=Unity_data_unique(nonan_idx,:); Unity_time_unique=Unity_time_unique(nonan_idx);
                RecordingLength=Unity_time_unique(end)-Unity_time_unique(1);
        
                new_UnityTime=linspace(Unity_time_unique(1),Unity_time_unique(end),RecordingLength*MotiveSampleRate);
                new_UnityData=interp1(Unity_time_unique,Unity_data_unique(:,1),new_UnityTime,'linear');
                new_UnityData=new_UnityData';
                new_UnityData(:,2)=interp1(Unity_time_unique,Unity_data_unique(:,2),new_UnityTime,'linear');
                new_UnityData(:,3)=interp1(Unity_time_unique,Unity_data_unique(:,3),new_UnityTime,'linear');
                new_UnityData(:,4)=interp1(Unity_time_unique,Unity_data_unique(:,4),new_UnityTime,'linear');
                new_UnityData(:,5)=interp1(Unity_time_unique,Unity_data_unique(:,5),new_UnityTime,'linear');
                new_UnityData(:,6)=interp1(Unity_time_unique,Unity_data_unique(:,6),new_UnityTime,'linear');
        
                Motive_data = Motive_import(:,interp_column+2);
        
                [cross_corr, lags] = xcorr(Motive_data, new_UnityData(:,interp_column));
                
                figure(fignum); 
                subplot(2, 4, 1); hold on;
                plot(cross_corr); title('Xcorr','FontSize', 12);
                set(gca, 'XTickLabel', []);
                fig = figure(fignum); currentPosition = get(fig, 'Position');
                newPosition = currentPosition - [150, 150, 0, 0]; set(fig, 'Position', newPosition);

                [~, max_idx] = max(cross_corr);
                %sanity check
                if lags(max_idx)<0, error('Cross correlation algorithm not correct.'); end
                
                Motive_data = Motive_import((1+lags(max_idx)):(length(new_UnityTime)+lags(max_idx)),:); 
                Motive_data(:,2)=new_UnityTime;
                
                Motive_data_downsampled = interp1(Motive_data(:,2), Motive_data, Unity_time_unique, 'linear');

                %Determine Unity positions from aligned Motive data
                pre_table.A_HeadPos(idx_use,1)=interp1(Motive_data(:,2),Motive_data(:,3),time,'linear','extrap');
                pre_table.A_HeadPos(idx_use,2)=interp1(Motive_data(:,2),Motive_data(:,4),time,'linear','extrap');
                pre_table.A_HeadPos(idx_use,3)=interp1(Motive_data(:,2),Motive_data(:,5),time,'linear','extrap');
                pre_table.A_HeadDir(idx_use,1)=interp1(Motive_data(:,2),Motive_data(:,6),time,'linear','extrap');
                pre_table.A_HeadDir(idx_use,2)=interp1(Motive_data(:,2),Motive_data(:,7),time,'linear','extrap');
                pre_table.A_HeadDir(idx_use,3)=interp1(Motive_data(:,2),Motive_data(:,8),time,'linear','extrap');
                pre_table.B_HeadPos(idx_use,1)=interp1(Motive_data(:,2),Motive_data(:,9),time,'linear','extrap');
                pre_table.B_HeadPos(idx_use,2)=interp1(Motive_data(:,2),Motive_data(:,10),time,'linear','extrap');
                pre_table.B_HeadPos(idx_use,3)=interp1(Motive_data(:,2),Motive_data(:,11),time,'linear','extrap');
                pre_table.B_HeadDir(idx_use,1)=interp1(Motive_data(:,2),Motive_data(:,12),time,'linear','extrap');
                pre_table.B_HeadDir(idx_use,2)=interp1(Motive_data(:,2),Motive_data(:,13),time,'linear','extrap');
                pre_table.B_HeadDir(idx_use,3)=interp1(Motive_data(:,2),Motive_data(:,14),time,'linear','extrap');
        
                %Calculate HeadFrth (x/y/z projections) from HeadDir (yaw/pitch)
                pre_table.A_HeadFrth(idx_use,1)=cos(pre_table.A_HeadDir(idx_use,2)*pi/180).*sin(pre_table.A_HeadDir(idx_use,1)*pi/180);
                pre_table.A_HeadFrth(idx_use,2)=sin(pre_table.A_HeadDir(idx_use,2)*pi/180);
                pre_table.A_HeadFrth(idx_use,3)=cos(pre_table.A_HeadDir(idx_use,2)*pi/180).*cos(pre_table.A_HeadDir(idx_use,1)*pi/180);
                pre_table.B_HeadFrth(idx_use,1)=cos(pre_table.B_HeadDir(idx_use,2)*pi/180).*sin(pre_table.B_HeadDir(idx_use,1)*pi/180);
                pre_table.B_HeadFrth(idx_use,2)=sin(pre_table.B_HeadDir(idx_use,2)*pi/180);
                pre_table.B_HeadFrth(idx_use,3)=cos(pre_table.B_HeadDir(idx_use,2)*pi/180).*cos(pre_table.B_HeadDir(idx_use,1)*pi/180);                
                
                %sanity check
                if length(pre_table.A_HeadPos)~=length(pre_table.UA_HeadPos)
                    error('Head Position vectors between sources are not matching.');
                end
        
                for idx=idx_use
                    if isnan(pre_table.UA_HeadPos(idx,1))
                        pre_table.A_HeadPos(idx,1:3)=NaN; pre_table.A_HeadDir(idx,1:3)=NaN; pre_table.A_HeadFrth(idx,1:3)=NaN;
                        pre_table.B_HeadPos(idx,1:3)=NaN; pre_table.B_HeadDir(idx,1:3)=NaN; pre_table.B_HeadFrth(idx,1:3)=NaN;
                    end
                end
                
                %plot correlation overlays for X,Y,Z dimensions
                subplot(2, 4, 2); hold on;
                time_series=Motive_import((1+lags(max_idx)):(length(new_UnityTime)+lags(max_idx)),2);
                plot(Motive_import(:,2),Motive_import(:,3),'k'); 
                plot(time_series,new_UnityData(:,1),'r');

                string = ['Correlation Overlay (X)'];                
                title(string, 'FontSize', 12); xlabel('Time (s)');
        
                subplot(2, 4, 3); hold on;
                plot(Motive_import(:,2),Motive_import(:,4),'k'); 
                plot(time_series,new_UnityData(:,2),'r');
                string = ['Correlation Overlay (Y)'];                
                title(string, 'FontSize', 12); xlabel('Time (s)');

                subplot(2, 4, 4); hold on;
                plot(Motive_import(:,2),Motive_import(:,5),'k'); 
                plot(time_series,new_UnityData(:,3),'r');
                string = ['Correlation Overlay (Z)'];                
                title(string, 'FontSize', 12); xlabel('Time (s)');

                subplot(2, 4, 6); hold on;
                plot(Motive_import(:,2),Motive_import(:,6),'k'); 
                yaw=atan2(new_UnityData(:,4),new_UnityData(:,6))*180/pi;
                plot(time_series,yaw,'r');
                string = ['Correlation Overlay (yaw)'];                
                title(string, 'FontSize', 12); xlabel('Time (s)');

                subplot(2, 4, 7); hold on;
                plot(Motive_import(:,2),Motive_import(:,7),'k'); 
                pitch=asin(new_UnityData(:,5))*180/pi;
                plot(time_series,pitch,'r');

                string = ['Correlation Overlay (pitch)'];                
                title(string, 'FontSize', 12); xlabel('Time (s)');

                fignum=fignum+1;

                PosValues=get(gcf, 'Position'); 
                Width=PosValues(3)*2; Height=PosValues(4)*1;
                set(gcf,'Position',[PosValues(1), PosValues(2), Width, Height]);
                
                %calculate correlation values and RMS - by file
                Corr_R(M1_filenum,1) = corr(new_UnityData(:,1),Motive_data(:,3)); 
                RMS_Corr(M1_filenum,1) = sqrt(mean((new_UnityData(:,1) - Motive_data(:,3)).^2));

                Corr_R(M1_filenum,2) = corr(new_UnityData(:,2),Motive_data(:,4));
                RMS_Corr(M1_filenum,2) = sqrt(mean((new_UnityData(:,2) - Motive_data(:,4)).^2));

                Corr_R(M1_filenum,3) = corr(new_UnityData(:,3),Motive_data(:,5)); 
                RMS_Corr(M1_filenum,3) = sqrt(mean((new_UnityData(:,3) - Motive_data(:,5)).^2));

                Corr_R(M1_filenum,4) = corr(yaw,Motive_data(:,6)); 
                RMS_Corr(M1_filenum,4) = sqrt(mean((yaw - Motive_data(:,6)).^2));

                Corr_R(M1_filenum,5) = corr(pitch,Motive_data(:,7)); 
                RMS_Corr(M1_filenum,5) = sqrt(mean((pitch - Motive_data(:,7)).^2));               

                %calculate correlation values and RMS - by trial
                yaw=atan2(pre_table.UA_HeadFrth(:,1),pre_table.UA_HeadFrth(:,3))*180/pi;
                pitch=asin(pre_table.UA_HeadFrth(:,2))*180/pi;
                motive_trial_list=[];
                for trialnum=1:size(pre_table.A_trialinfo,1)
                    if ~isnan(pre_table.A_trialinfo(trialnum,6))
                        if pre_table.A_trialinfo(trialnum,1)>min(idx_use) && pre_table.A_trialinfo(trialnum,2)<max(idx_use)
                            trial_idx=pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2);
                            motive_trial_list=[motive_trial_list trialnum]; %list of trials relevant for this motive file

                            pre_table.trial_R(trialnum,1)=corr(pre_table.A_HeadPos(trial_idx,1),pre_table.UA_HeadPos(trial_idx,1),'Rows', 'complete');
                            pre_table.trial_R(trialnum,2)=corr(pre_table.A_HeadPos(trial_idx,2),pre_table.UA_HeadPos(trial_idx,2),'Rows', 'complete');
                            pre_table.trial_R(trialnum,3)=corr(pre_table.A_HeadPos(trial_idx,3),pre_table.UA_HeadPos(trial_idx,3),'Rows', 'complete');
                            pre_table.trial_R(trialnum,4)=corr(pre_table.A_HeadDir(trial_idx,1),yaw(trial_idx),'Rows', 'complete');
                            pre_table.trial_R(trialnum,5)=corr(pre_table.A_HeadDir(trial_idx,2),pitch(trial_idx),'Rows', 'complete');

                            pre_table.trial_RMS(trialnum,1)=sqrt(nanmean((pre_table.A_HeadPos(trial_idx,1)-pre_table.UA_HeadPos(trial_idx,1)).^2));
                            pre_table.trial_RMS(trialnum,2)=sqrt(nanmean((pre_table.A_HeadPos(trial_idx,2)-pre_table.UA_HeadPos(trial_idx,2)).^2));
                            pre_table.trial_RMS(trialnum,3)=sqrt(nanmean((pre_table.A_HeadPos(trial_idx,3)-pre_table.UA_HeadPos(trial_idx,3)).^2));
                            pre_table.trial_RMS(trialnum,4)=sqrt(nanmean((pre_table.A_HeadDir(trial_idx,1)-yaw(trial_idx)).^2));
                            pre_table.trial_RMS(trialnum,5)=sqrt(nanmean((pre_table.A_HeadDir(trial_idx,2)-pitch(trial_idx)).^2));
                        end
                    else
                        pre_table.trial_R(trialnum,1:5)=NaN; pre_table.trial_RMS(trialnum,1:5)=NaN;
                    end
                end

                %Plot difference between motive and unity
                figure(fignum); 
                subplot(2, 3, 1); hold on;
                plot(time_series,Motive_data(:,3)-new_UnityData(:,1),'b');
                string = ['Motive/Unity Diff. (X)']; title(string, 'FontSize', 12); xlabel('Time (s)');

                subplot(2, 3, 2); hold on;
                plot(time_series,Motive_data(:,4)-new_UnityData(:,2),'b');
                string = ['Motive/Unity Diff. (Y)']; title(string, 'FontSize', 12); xlabel('Time (s)');

                subplot(2, 3, 3); hold on;
                plot(time_series,Motive_data(:,5)-new_UnityData(:,3),'b');
                string = ['Motive/Unity Diff. (Z)']; title(string, 'FontSize', 12); xlabel('Time (s)');

                %Plot correlation changes over trials

                subplot(2, 3, 4); hold on;
                plot(motive_trial_list, pre_table.trial_R(motive_trial_list,1), 'o', 'Color','k');
                string = ['Correlation by Trial (X)']; title(string, 'FontSize', 12); xlabel('Trial');

                subplot(2, 3, 5); hold on;
                plot(motive_trial_list, pre_table.trial_R(motive_trial_list,2), 'o', 'Color','k');
                string = ['Correlation by Trial (Y)']; title(string, 'FontSize', 12); xlabel('Trial');

                subplot(2, 3, 6); hold on;
                plot(motive_trial_list, pre_table.trial_R(motive_trial_list,3), 'o', 'Color','k');
                string = ['Correlation by Trial (Z)']; title(string, 'FontSize', 12); xlabel('Trial');

                PosValues=get(gcf, 'Position'); 
                Width=PosValues(3)*2; Height=PosValues(4)*1.5;
                set(gcf,'Position',[50, 50, Width, Height]);

                fignum=fignum+1;

                clear PosValues Width Height bin_size bin trial_idx
                clear new_UnityTime unique_idx nonan_idx Motive_data_downsampled
                clear Unity_data_unique Unity_time_unique new_UnityData 
                clear lags cross_corr max_idx time_series string
                clear time data corr_per_bin unity_bin motive_bin
                clear start_idx end_idx yaw pitch motive_trial_list
            end
            M1_filenum=M1_filenum+1;
        end
    end
end

clear prelim idx M1_filenum M2_filenum

%% Head Velocity A (two dimensions)

%initial run - calculate all velocity values for which the current and previous matrix index position are known
for idx=2:size(pre_table.A_HeadPos,1)
    if ~isnan(pre_table.A_HeadPos(idx-1,1)) && ~isnan(pre_table.A_HeadPos(idx,1))
        pre_table.A_HeadVel(idx,1)=sqrt((pre_table.A_HeadPos(idx,1)-pre_table.A_HeadPos(idx-1,1))^2+(pre_table.A_HeadPos(idx,3)-pre_table.A_HeadPos(idx-1,3))^2)...
            /(pre_table.A_AppTime(idx,1)-pre_table.A_AppTime(idx-1,1));
    else
        pre_table.A_HeadVel(idx,1)=NaN;
    end
end

%first value in the column
if isnan(pre_table.A_HeadPos(2,1)), pre_table.A_HeadVel(1,1)=NaN;
else
    if isnan(pre_table.A_HeadPos(1,1)), pre_table.A_HeadVel(1,1)=NaN;
    else, pre_table.A_HeadVel(1,1)=pre_table.A_HeadVel(2,1); end
end

%establish velocity values where the previous matrix position is NaN (copies subsequent velocity)
for idx=2:size(pre_table.A_HeadPos,1)-1
    if isnan(pre_table.A_HeadVel(idx,1)) && ~isnan(pre_table.A_HeadVel(idx+1,1)) && ~isnan(isnan(pre_table.A_HeadPos(idx,1)))
        pre_table.A_HeadVel(idx,1)=pre_table.A_HeadVel(idx+1,1);
    end
end

%Calculate time differentials (relevant for all variables)

for idx=1:size(pre_table.A_AppTime,1)-1
    pre_table.A_TimeDiff(idx,1)=pre_table.A_AppTime(idx+1)-pre_table.A_AppTime(idx);
end
pre_table.A_TimeDiff=[pre_table.A_TimeDiff; pre_table.A_TimeDiff(end)];

%Data/time length
time_length=0; %initialize variables
for trialnum=1:size(pre_table.A_trialinfo,1)
    if ~isnan(pre_table.A_trialinfo(trialnum,6))
        for idx=pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2)
            time_length=time_length+pre_table.A_TimeDiff(idx);
end; end; end
clear trialnum idx

%% Trial collection-based synchronization between two headsets

%Find coin collections on Headset B
Bcol_idx=find(pre_table.B_MCode==13);
if ~isequal(length(Bcol_idx),length(find(pre_table.A_trialinfo(:,9)==1)))
    error('Collections in B do not line up with A');
end

%For each coin collection (successful trial), line up values
goodtrialcounter=1;
for trialnum=1:size(pre_table.A_trialinfo,1)
    if pre_table.A_trialinfo(trialnum,9)==1
        pre_table.A_trialinfo(trialnum,11)=pre_table.B_AppTime(Bcol_idx(goodtrialcounter))-pre_table.A_AppTime(pre_table.A_trialinfo(trialnum,8));
        goodtrialcounter=goodtrialcounter+1;
    else
        if trialnum==1, pre_table.A_trialinfo(trialnum,11)=NaN; 
        else, pre_table.A_trialinfo(trialnum,11)=pre_table.A_trialinfo(trialnum-1,11); end
    end
end

for trialnum=1:10
    if isnan(pre_table.A_trialinfo(trialnum,11))
        synch=find(~isnan(pre_table.A_trialinfo(:,11)),1,'first');
        pre_table.A_trialinfo(trialnum,11)=pre_table.A_trialinfo(synch,11);
    end
end


if UnityMarks==1
    %refined synchronization - mark/collection logs combined
    for blocknum=1:length(pre_table.Mark_AB_diff)
        blockstart=find(pre_table.A_BlockIndex==blocknum);
        blockend=find(pre_table.A_BlockIndex==blocknum); 
        if ~isempty(blockstart) && ~isempty(blockend)
            blockstart=blockstart(1); blockend=blockend(end);
            blockrows=pre_table.A_trialinfo(:,1)>=blockstart&pre_table.A_trialinfo(:,2)<=blockend;
            blockrows=find(blockrows);
            for trialnum=blockrows(1):blockrows(end)
                pre_table.A_trialinfo(trialnum,12)=pre_table.A_trialinfo(trialnum,11)+pre_table.Mark_AB_diff(blocknum)-pre_table.A_trialinfo(blockrows(1),11);
            end
        end
    end
elseif UnityMarks==0
    pre_table.A_trialinfo(:,12)=pre_table.A_trialinfo(:,11);
end


%Find all B position data
Bpos=[pre_table.B_AppTime pre_table.B_LogType pre_table.UB_HeadPos pre_table.UB_HeadFrth];
Bpos=Bpos(Bpos(:,2)==1,:);

%Go line by line for A's head position, find synchronized B position
for idx=1:size(pre_table.UA_HeadPos,1)
    if isnan(pre_table.UA_HeadPos(idx,1))
        pre_table.UB_SynchPos(idx,1:3)=NaN; pre_table.UB_SynchFrth(idx,1:3)=NaN;
    else
        synch=pre_table.A_trialinfo(find(pre_table.A_trialinfo(:,7)<idx,1,'last'),12);
        if isempty(synch), synch=pre_table.A_trialinfo(1,12); end
        UB_synchtime=pre_table.A_AppTime(idx)+synch;
        if UB_synchtime<min(Bpos(:,1))-1
            error('Bad synchronization or Loss of Data between A start and B start');
        elseif UB_synchtime<min(Bpos(:,1))
            pre_table.UB_SynchPos(idx,1:3)=Bpos(1,3:5);
            pre_table.UB_SynchFrth(idx,1:3)=Bpos(1,6:8);
        elseif UB_synchtime>max(Bpos(:,1))+1 && idx<pre_table.A_trialinfo(end,8)
            error('Bad synchronization or Loss of Data between A end and B end');
        elseif UB_synchtime>max(Bpos(:,1))
            pre_table.UB_SynchPos(idx,1:3)=Bpos(end,3:5);
            pre_table.UB_SynchFrth(idx,1:3)=Bpos(end,6:8);
        else
            prerow_idx=find(Bpos(:,1)<=UB_synchtime,1,'last');
            postrow_idx=find(Bpos(:,1)>UB_synchtime,1,'first');
            if postrow_idx~=prerow_idx+1
                error('Issue with interpolation, Bpos variable line %d', prerow_idx);
            elseif Bpos(postrow_idx,1)-Bpos(prerow_idx,1)>1
                pre_table.UB_SynchPos(idx,1:3)=NaN;
                pre_table.UB_SynchFrth(idx,1:3)=NaN;
            else
                pre_table.UB_SynchPos(idx,1)=interp1([Bpos(prerow_idx,1), Bpos(postrow_idx,1)], [Bpos(prerow_idx,3), Bpos(postrow_idx,3)], UB_synchtime);
                pre_table.UB_SynchPos(idx,2)=interp1([Bpos(prerow_idx,1), Bpos(postrow_idx,1)], [Bpos(prerow_idx,4), Bpos(postrow_idx,4)], UB_synchtime);
                pre_table.UB_SynchPos(idx,3)=interp1([Bpos(prerow_idx,1), Bpos(postrow_idx,1)], [Bpos(prerow_idx,5), Bpos(postrow_idx,5)], UB_synchtime);
                pre_table.UB_SynchFrth(idx,1)=interp1([Bpos(prerow_idx,1), Bpos(postrow_idx,1)], [Bpos(prerow_idx,6), Bpos(postrow_idx,6)], UB_synchtime);
                pre_table.UB_SynchFrth(idx,2)=interp1([Bpos(prerow_idx,1), Bpos(postrow_idx,1)], [Bpos(prerow_idx,7), Bpos(postrow_idx,7)], UB_synchtime);
                pre_table.UB_SynchFrth(idx,3)=interp1([Bpos(prerow_idx,1), Bpos(postrow_idx,1)], [Bpos(prerow_idx,8), Bpos(postrow_idx,8)], UB_synchtime);                
            end
        end
    end
end

warning('check that the time delays A vs B synchronization are reasonable');

%make an array of indices to assign B Position from Unity (Not Motive)
if any(A_UseOptiFromMotive)
    for filenum=1:size(M_indexlist,1)
        if B_UseOptiFromMotive(filenum)==0
            idx_use=M_indexlist(filenum,1):M_indexlist(filenum,2);
            pre_table.B_HeadPos(idx_use,:)=pre_table.UB_SynchPos(idx_use,:);
            pre_table.B_HeadFrth(idx_use,:)=pre_table.UB_SynchFrth(idx_use,:);
            pre_table.B_HeadDir(idx_use,1)=atan2(pre_table.B_HeadFrth(idx_use,1),pre_table.B_HeadFrth(idx_use,3))*180/pi;
            pre_table.B_HeadDir(idx_use,2)=asin(pre_table.B_HeadFrth(idx_use,2))*180/pi;
        end
    end
else
    pre_table.A_HeadDir(:,1)=atan2(pre_table.A_HeadFrth(:,1),pre_table.A_HeadFrth(:,3))*180/pi;
    pre_table.A_HeadDir(:,2)=asin(pre_table.A_HeadFrth(:,2))*180/pi;
    pre_table.B_HeadPos=pre_table.UB_SynchPos;
    pre_table.B_HeadFrth=pre_table.UB_SynchFrth;
    pre_table.B_HeadDir(:,1)=atan2(pre_table.B_HeadFrth(:,1),pre_table.B_HeadFrth(:,3))*180/pi;
    pre_table.B_HeadDir(:,2)=asin(pre_table.B_HeadFrth(:,2))*180/pi;
end

%% Head Velocity B (synchronized data - two dimensions)

%initial run - calculate all velocity values for which the current and previous matrix index position are known
for idx=2:size(pre_table.B_HeadPos,1)
    if ~isnan(pre_table.B_HeadPos(idx-1,1)) && ~isnan(pre_table.B_HeadPos(idx,1))
        pre_table.B_HeadVel(idx,1)=sqrt((pre_table.B_HeadPos(idx,1)-pre_table.B_HeadPos(idx-1,1))^2+(pre_table.B_HeadPos(idx,3)-pre_table.B_HeadPos(idx-1,3))^2)...
            /(pre_table.A_AppTime(idx,1)-pre_table.A_AppTime(idx-1,1));
    else
        pre_table.B_HeadVel(idx,1)=NaN;
    end
end

%first value in the column
if isnan(pre_table.B_HeadPos(2,1)), pre_table.B_HeadVel(1,1)=NaN;
else
    if isnan(pre_table.B_HeadPos(1,1)), pre_table.B_HeadVel(1,1)=NaN;
    else, pre_table.B_HeadVel(1,1)=pre_table.B_HeadVel(2,1); end
end

%establish velocity values where the previous matrix position is NaN (copies subsequent velocity)
for idx=2:size(pre_table.B_HeadPos,1)-1
    if isnan(pre_table.B_HeadVel(idx,1)) && ~isnan(pre_table.B_HeadVel(idx+1,1)) && ~isnan(isnan(pre_table.B_HeadPos(idx,1)))
        pre_table.B_HeadVel(idx,1)=pre_table.B_HeadVel(idx+1,1);
    end
end

%% Avoidance Angle and Distance Relations

pre_table.AvoidAngle(1:size(pre_table.A_timestamp,1),1)=NaN;
pre_table.P1_Dist_to_Chest(1:size(pre_table.A_timestamp,1),1)=NaN;
pre_table.P1_Dist_to_P2(1:size(pre_table.A_timestamp,1),1)=NaN;
pre_table.P2_Dist_to_Chest(1:size(pre_table.A_timestamp,1),1)=NaN;

for trialnum=1:size(pre_table.A_trialinfo,1)
    if ~isnan(pre_table.A_trialinfo(trialnum,6))
        TC_pos=TClocations(trialnum,[1 3]);
        for idx=pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2)
            if ~isnan(pre_table.A_HeadPos(idx,1)) && ~isnan(pre_table.B_HeadPos(idx,1)) 
                if pre_table.B_HeadVel(idx,1)>0.1 %Experimenter must be walking above certain speed
                    P2_forth=pre_table.B_HeadPos(idx,:)-pre_table.B_HeadPos(idx-1,:); P2_forth=P2_forth/norm(P2_forth);
                    P2_forth_XZ=P2_forth([1 3]); P2_forth_XZ=P2_forth_XZ/norm(P2_forth_XZ); %Experimenter direction
                    
                    P1_minus_P2=pre_table.A_HeadPos(idx,:)-pre_table.B_HeadPos(idx,:); %Vector between participant and experimenter
    
                    %Angle between vectors
                    pre_table.AvoidAngle(idx,1) = rad2deg(acos(dot(P2_forth_XZ,P1_minus_P2([1 3]))/norm(P1_minus_P2([1 3]))));
                end
                pre_table.P1_Dist_to_Chest(idx,1)=norm(TC_pos-pre_table.A_HeadPos(idx,[1 3]));  %P1 to Chest linear distance
                pre_table.P1_Dist_to_P2(idx,1)=norm(pre_table.B_HeadPos(idx,[1 3])-pre_table.A_HeadPos(idx,[1 3])); %P1 to P2 linear distance
                pre_table.P2_Dist_to_Chest(idx,1)=norm(TC_pos-pre_table.B_HeadPos(idx,[1 3]));  %P2 to Chest linear distance
            end
        end
        pre_table.A_trialinfo(trialnum,16)=nanmean(pre_table.P1_Dist_to_P2(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2)));
        pre_table.A_trialinfo(trialnum,17)=nanmean(pre_table.AvoidAngle(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2)));
        pre_table.A_trialinfo(trialnum,18)=nanmean(pre_table.P1_Dist_to_Chest(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2)));
        pre_table.A_trialinfo(trialnum,19)=nanmean(pre_table.P2_Dist_to_Chest(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2)));
    else
        pre_table.A_trialinfo(trialnum,16:19)=NaN;
    end
end

%Sanity check analysis - distance during each collision
if any(pre_table.A_MCode==4)
    collision_list=find(pre_table.A_MCode==4);
    for collision=1:size(collision_list,1)
        round_num=find(pre_table.A_trialinfo(:,2) > collision_list(collision),1,'first');
        if ~isnan(pre_table.A_trialinfo(round_num,6))
            n=0;
            while isnan(pre_table.P1_Dist_to_P2(collision_list(collision,1)+n))
                n=n+1;
            end
            collision_list(collision,2)=pre_table.P1_Dist_to_P2(collision_list(collision,1)+n);
        else
            collision_list(collision,1:2)=NaN;
        end
    end
end
clear collision round_num

figure(fignum); 
subplot(2, 2, 1); %Avoidance Angle histogram plot
histogram(pre_table.AvoidAngle); xlabel('Degrees'); xticks([0 45 90 135 180]);
title('Angle to Experimenter''s Trajectory'); 

subplot(2, 2, 2); hold on; %Avoidance Angle - each trial
for trialnum=1:size(pre_table.A_trialinfo,1)
    plot(pre_table.AvoidAngle(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),1));  
end
title('Angle to Experimenter''s Trajectory'); set(gca,'XtickLabel',[]); ylabel('Degrees')

subplot(2, 2, 3); hold on; %Distance to Chest - each trial
for trialnum=1:size(pre_table.A_trialinfo,1)
    plot(pre_table.P1_Dist_to_Chest(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),1));  
end
title('Distance to Chest (m)'); set(gca,'XtickLabel',[]); ylabel('Distance (m)');

subplot(2, 2, 4); %Distance from experimenter histogram plot
histogram(pre_table.P1_Dist_to_P2); xlabel('Distance (m)'); title('Distance from experimenter'); 

fig = figure(fignum); currentPosition = get(fig, 'Position');
newPosition = currentPosition - [200, 200, 0, 0]; set(fig, 'Position', newPosition);
PosValues=get(gcf, 'Position'); 
set(gcf,'Position',[PosValues(1), PosValues(2), PosValues(3)*1.5, PosValues(4)*1.5]);
fignum=fignum+1;

clear idx P1_minus_P2 P2_forth_XZ P2_forth TC_pos

%% Field of View

if UseHeadA==1
    % Clean up bugs/jumps in head direction
    dif=diff(pre_table.A_HeadDir)./pre_table.A_TimeDiff(1:end-1); dif=abs(dif(:,1))>1000; dif=[0; dif]; dif=logical(dif);
    pre_table.A_HeadDir(dif,:)=NaN; pre_table.A_HeadFrth(dif,:)=NaN;
    
    % Initialize variables for field of view analysis
    pre_table.TCinView(1:size(pre_table.A_timestamp,1),1)=NaN;
    pre_table.P2inView(1:size(pre_table.A_timestamp,1),1)=NaN;
    pre_table.TCDir(1:size(pre_table.A_timestamp,1),1)=NaN;
    pre_table.P2Dir(1:size(pre_table.A_timestamp,1),1)=NaN;
    
    for trialnum=1:size(pre_table.A_trialinfo,1)
        if ~isnan(pre_table.A_trialinfo(trialnum,6))
            TC_pos=TClocations(trialnum,:);
            for idx=pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2)
                if ~isnan(pre_table.A_HeadPos(idx,1))
                    P1_pos=pre_table.A_HeadPos(idx,:);
                    P2_pos=pre_table.B_HeadPos(idx,:);
                    P1_forth=pre_table.A_HeadFrth(idx,:); P1_forth=P1_forth/norm(P1_forth);
                    P1_forth_XZ=P1_forth([1 3]); P1_forth_XZ=P1_forth_XZ/norm(P1_forth_XZ);
                    TC_minus_P1=TC_pos-P1_pos; 
                    P2_minus_P1=P2_pos-P1_pos;
    
                    P1toTC_theta_horiz = rad2deg(acos(dot(P1_forth_XZ,TC_minus_P1([1 3]))/norm(TC_minus_P1([1 3]))));
                    if (P1_forth_XZ(1) * TC_minus_P1(3) - P1_forth_XZ(2) * TC_minus_P1(1)) > 0 %cross-product
                        P1toTC_theta_horiz = -1*P1toTC_theta_horiz;
                    end
    
                    P1toTC_theta_vert_top = rad2deg(atan((TC_minus_P1(2)+1)/sqrt(TC_minus_P1(1)^2+TC_minus_P1(3)^2)))-pre_table.A_HeadDir(idx,2);
                    P1toTC_theta_vert_bottom = rad2deg(atan(TC_minus_P1(2)/sqrt(TC_minus_P1(1)^2+TC_minus_P1(3)^2)))-pre_table.A_HeadDir(idx,2);
                    
                    P1toP2_theta_horiz = rad2deg(acos(dot(P1_forth_XZ,P2_minus_P1([1 3]))/norm(P2_minus_P1([1 3]))));
                    if (P1_forth_XZ(1) * P2_minus_P1(3) - P1_forth_XZ(2) * P2_minus_P1(1)) > 0 %cross-product
                        P1toP2_theta_horiz = -1*P1toP2_theta_horiz;
                    end
    
                    P1toP2_theta_vert_top = rad2deg(atan(P2_minus_P1(2)/sqrt(P2_minus_P1(1)^2+P2_minus_P1(3)^2)))-pre_table.A_HeadDir(idx,2);
                    P1toP2_theta_vert_bottom = rad2deg(atan((-1*P1_pos(2))/sqrt(P2_minus_P1(1)^2+P2_minus_P1(3)^2)))-pre_table.A_HeadDir(idx,2);
                    P1toP2_theta_vert_middle = rad2deg(atan((-0.5*P1_pos(2))/sqrt(P2_minus_P1(1)^2+P2_minus_P1(3)^2)))-pre_table.A_HeadDir(idx,2);
                    
                    if ((2*P1toTC_theta_horiz/horizontalFOV)^2 + (2*P1toTC_theta_vert_top/verticalFOV)^2) <= 1
                        pre_table.TCinView(idx,1)=1;
                    elseif ((2*P1toTC_theta_horiz/horizontalFOV)^2 + (2*P1toTC_theta_vert_bottom/verticalFOV)^2) <= 1
                        pre_table.TCinView(idx,1)=1;
                    else, pre_table.TCinView(idx,1)=0;
                    end
    
                    if ((2*P1toP2_theta_horiz/horizontalFOV)^2 + (2*P1toP2_theta_vert_top/verticalFOV)^2) <= 1
                        pre_table.P2inView(idx,1)=1;
                    elseif ((2*P1toP2_theta_horiz/horizontalFOV)^2 + (2*P1toP2_theta_vert_bottom/verticalFOV)^2) <= 1
                        pre_table.P2inView(idx,1)=1;
                    else, pre_table.P2inView(idx,1)=0;
                    end
    
                    pre_table.TCDir(idx,1:2)=[P1toTC_theta_horiz P1toTC_theta_vert_top];
                    pre_table.P2Dir(idx,1:2)=[P1toP2_theta_horiz P1toP2_theta_vert_middle];
    
                    clear P1_pos P2_pos P1_forth P1toP2_theta_vert_middle
                    clear P1toP2_theta_horiz P1toP2_theta_vert_bottom
                    clear P1toP2_theta_vert_top P1toTC_theta_vert_top P1toTC_theta_horiz 
                    clear P1toTC_theta_vert_bottom P2_minus_P1 TC_minus_P1
                    clear P1_forth_XZ P1_gaze_XZ dif
                else
                    pre_table.TCinView(idx,1)=NaN; pre_table.TCDir(idx,1:2)=NaN;
                    pre_table.P2inView(idx,1)=NaN; pre_table.P2Dir(idx,1:2)=NaN;
                end
            end
        end
    end
    
    figure(fignum); 
    subplot(2, 2, 1); hold on;
    for trialnum=1:size(pre_table.A_trialinfo,1)
        if ~isnan(pre_table.A_trialinfo(trialnum,6))
            plot(pre_table.TCDir(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),1));  
        end
    end
    ylim([-180 180]); title('Target vs Participant Head - Horizontal'); set(gca,'XtickLabel',[]);
    subplot(2, 2, 2); hold on; 
    for trialnum=1:size(pre_table.A_trialinfo,1)
        if ~isnan(pre_table.A_trialinfo(trialnum,6))
            plot(pre_table.P2Dir(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),1));  
        end
    end
    ylim([-180 180]); title('Experimenter vs Participant Head - Horizontal'); set(gca,'XtickLabel',[]);
    subplot(2, 2, 3); hold on;
    for trialnum=1:size(pre_table.A_trialinfo,1)
        if ~isnan(pre_table.A_trialinfo(trialnum,6))
            plot(pre_table.TCDir(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),2));  
        end
    end
    title('Target vs Participant Head - Vertical'); set(gca,'XtickLabel',[]);
    subplot(2, 2, 4); hold on; 
    for trialnum=1:size(pre_table.A_trialinfo,1)
        if ~isnan(pre_table.A_trialinfo(trialnum,6))
            plot(pre_table.P2Dir(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),2));  
        end
    end
    title('Experimenter vs Participant Head - Vertical'); set(gca,'XtickLabel',[]);
    
    fig = figure(fignum); currentPosition = get(fig, 'Position');
    newPosition = currentPosition - [300, 220, 0, 0]; set(fig, 'Position', newPosition);
    PosValues=get(gcf, 'Position'); 
    set(gcf,'Position',[PosValues(1), PosValues(2), PosValues(3)*1.8, PosValues(4)*1.8]);
    fignum=fignum+1; 
    
    clear TC_pos trialnum currentPosition newPosition PosValues
end

%% Gaze 

if any(pre_table.UA_EyeDir(:,1)>0) && UseHeadA==1 && UseEye==1
    pre_table.TC_Gaze(1:size(pre_table.A_timestamp,1),1)=NaN;
    pre_table.P2_Gaze(1:size(pre_table.A_timestamp,1),1)=NaN;
    pre_table.GazeDir(1:size(pre_table.A_timestamp,1),1)=NaN;
    
    %calculate gaze yaw/pitch from the 3D projection
    pre_table.GazeDir(:,1)=atan2(pre_table.UA_EyeDir(:,1),pre_table.UA_EyeDir(:,3))*180/pi;
    pre_table.GazeDir(:,2)=asin(pre_table.UA_EyeDir(:,2))*180/pi;
    
    for trialnum=1:size(pre_table.A_trialinfo,1)
        if ~isnan(pre_table.A_trialinfo(trialnum,6))
            TC_pos=TClocations(trialnum,:);
            for idx=pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2)
                if ~isnan(pre_table.A_HeadPos(idx,1))
                    P1_pos=pre_table.A_HeadPos(idx,:);
                    P2_pos=pre_table.B_HeadPos(idx,:);
                    P1_gaze=pre_table.UA_EyeDir(idx,:); P1_gaze=P1_gaze/norm(P1_gaze);
                    P1_gaze_XZ=P1_gaze([1 3]); P1_gaze_XZ=P1_gaze_XZ/norm(P1_gaze_XZ);
                    TC_minus_P1=TC_pos-P1_pos; 
                    P2_minus_P1=P2_pos-P1_pos;
    
                    P1toTC_theta_horiz = rad2deg(acos(dot(P1_gaze_XZ,TC_minus_P1([1 3]))/norm(TC_minus_P1([1 3]))));
                    if ~isreal(P1toTC_theta_horiz) % in case the trig attempts to do an acos of a value slightly greater than 1
                        P1toTC_theta_horiz=0;
                    end
                    if (P1_gaze_XZ(1) * TC_minus_P1(3) - P1_gaze_XZ(2) * TC_minus_P1(1)) > 0 %cross-product
                        P1toTC_theta_horiz = -1*P1toTC_theta_horiz;
                    end
    
                    P1toTC_theta_vert_top = rad2deg(atan((TC_minus_P1(2)+1)/sqrt(TC_minus_P1(1)^2+TC_minus_P1(3)^2)))-pre_table.GazeDir(idx,2); 
                    P1toTC_theta_vert_bottom = rad2deg(atan(TC_minus_P1(2)/sqrt(TC_minus_P1(1)^2+TC_minus_P1(3)^2)))-pre_table.GazeDir(idx,2); 
                    
                    P1toP2_theta_horiz = rad2deg(acos(dot(P1_gaze_XZ,P2_minus_P1([1 3]))/norm(P2_minus_P1([1 3]))));
                    if (P1_gaze_XZ(1) * P2_minus_P1(3) - P1_gaze_XZ(2) * P2_minus_P1(1)) > 0 %cross-product
                        P1toP2_theta_horiz = -1*P1toP2_theta_horiz;
                    end
    
                    P1toP2_theta_vert_top = rad2deg(atan(P2_minus_P1(2)/sqrt(P2_minus_P1(1)^2+P2_minus_P1(3)^2)))-pre_table.GazeDir(idx,2);
                    P1toP2_theta_vert_bottom = rad2deg(atan((-1*P1_pos(2))/sqrt(P2_minus_P1(1)^2+P2_minus_P1(3)^2)))-pre_table.GazeDir(idx,2);
                    P1toP2_theta_vert_middle = rad2deg(atan((-0.5*P1_pos(2))/sqrt(P2_minus_P1(1)^2+P2_minus_P1(3)^2)))-pre_table.GazeDir(idx,2);
                    
                    pre_table.TC_Gaze(idx,1:2)=[P1toTC_theta_horiz P1toTC_theta_vert_top];
                    pre_table.P2_Gaze(idx,1:2)=[P1toP2_theta_horiz P1toP2_theta_vert_middle];
    
                    clear P1_pos P2_pos P1_gaze P1toP2_theta_vert_middle
                    clear P1toP2_theta_horiz P1toP2_theta_vert_bottom
                    clear P1toP2_theta_vert_top P1toTC_theta_vert_top P1toTC_theta_horiz 
                    clear P1toTC_theta_vert_bottom P2_minus_P1 TC_minus_P1 P1_gaze_XZ
                else
                    pre_table.TC_Gaze(idx,:)=NaN; 
                    pre_table.P2_Gaze(idx,:)=NaN; 
                end
            end
        end
    end
    
    figure(fignum); 
    subplot(3, 2, 1); hold on;
    for trialnum=1:size(pre_table.A_trialinfo,1)
        if ~isnan(pre_table.A_trialinfo(trialnum,6))
            plot(pre_table.TC_Gaze(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),1));
        end
    end
    ylim([-180 180]); title('Target vs Participant Gaze - Horizontal'); set(gca,'XtickLabel',[]);
    subplot(3, 2, 2); hold on; 
    for trialnum=1:size(pre_table.A_trialinfo,1)
        if ~isnan(pre_table.A_trialinfo(trialnum,6))
            plot(pre_table.P2_Gaze(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),1));  
        end
    end
    ylim([-180 180]); title('Experimenter vs Participant Gaze - Horizontal'); set(gca,'XtickLabel',[]);
    subplot(3, 2, 3); hold on;
    for trialnum=1:size(pre_table.A_trialinfo,1)
        if ~isnan(pre_table.A_trialinfo(trialnum,6))
            plot(pre_table.TC_Gaze(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),2));
        end
    end
    title('Target vs Participant Gaze - Vertical'); set(gca,'XtickLabel',[]);
    subplot(3, 2, 4); hold on; 
    for trialnum=1:size(pre_table.A_trialinfo,1)
        if ~isnan(pre_table.A_trialinfo(trialnum,6))
            plot(pre_table.P2_Gaze(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),2));  
        end
    end
    title('Experimenter vs Participant Gaze - Vertical'); set(gca,'XtickLabel',[]);
    subplot(3, 2, 5); histogram(pre_table.TC_Gaze(:,1));
    title('Target vs Participant Gaze - Horizontal'); 
    subplot(3, 2, 6); histogram(pre_table.P2_Gaze(:,1));
    title('Experimenter vs Participant Gaze - Horizontal'); 
    
    fig = figure(fignum); currentPosition = get(fig, 'Position');
    newPosition = currentPosition - [350, 220, 0, 0]; set(fig, 'Position', newPosition);
    PosValues=get(gcf, 'Position'); 
    set(gcf,'Position',[PosValues(1), PosValues(2), PosValues(3)*1.8, PosValues(4)*1.8]);
    fignum=fignum+1; 
    
    
    pre_table.Derived_EyeXY=pre_table.GazeDir-pre_table.A_HeadDir(:,1:2);
    pre_table.Derived_EyeXY(:,1)=mod(pre_table.Derived_EyeXY(:,1)+180,360)-180;
    
    pre_table.Derived_EyeVel=[];
    pre_table.Derived_EyeVel(1:size(pre_table.Derived_EyeXY,1),1)=NaN; 
    for idx=2:size(pre_table.Derived_EyeXY,1)-1
        if ~any(isnan(pre_table.Derived_EyeXY(idx-1:idx+1,1)))
            diff=(pre_table.Derived_EyeXY(idx+1,:)-pre_table.Derived_EyeXY(idx-1,:))/(pre_table.A_AppTime(idx+1)-pre_table.A_AppTime(idx-1));
            pre_table.Derived_EyeVel(idx,1)=norm(diff);
        end
    end
    
    pre_table.BadEyeIdx=zeros([size(pre_table.Derived_EyeXY,1) 1]);
    for idx=1:size(pre_table.Derived_EyeXY,1)
        if abs(pre_table.Derived_EyeXY(idx,1))>50, pre_table.BadEyeIdx(idx)=1; end
        if abs(pre_table.Derived_EyeXY(idx,2))>50, pre_table.BadEyeIdx(idx)=1; end
        if abs(pre_table.Derived_EyeVel(idx))>1000, pre_table.BadEyeIdx(idx)=1; end
        if pre_table.A_EyesOpen(idx)<2, pre_table.BadEyeIdx(idx)=1; end
    end

    %count number of bad eye indexes in good trial
    BadEyeIdx_count=0; Total_count=0; %initialize variable
    for trialnum=1:size(pre_table.A_trialinfo,1)
        if ~isnan(pre_table.A_trialinfo(trialnum,6))
            for idx=pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2)
                if ~isnan(pre_table.A_HeadPos(idx,1))
                    if pre_table.BadEyeIdx(idx)==1, BadEyeIdx_count=BadEyeIdx_count+1; end
                    Total_count=Total_count+1;
                end
            end
        end
    end
    BadEyeIdx_prop=BadEyeIdx_count/Total_count;
else
    Total_count=0; %initialize variable
    for trialnum=1:size(pre_table.A_trialinfo,1)
        if ~isnan(pre_table.A_trialinfo(trialnum,6))
            for idx=pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2)
                if ~isnan(pre_table.A_HeadPos(idx,1))
                    Total_count=Total_count+1;
end; end; end; end; end

clear TC_pos trialnum currentPosition newPosition PosValues idx diff BadEyeIdx_count 

%% Behavioral Index v1 - v3

%for each trial, go index by index and determine behavioral code
% V1
% 1 = No Conflict 
% 2 = Passive Avoidance
% 3 = Active Avoidance
% 4 = Conflicted Pursuit

% V2
% 1 = No Movement (low speed)
% 2 = Approach (ap target + ap experimenter)
% 3 = Avoidance (av target + av experimenter)
% 4 = No Conflict (ap target + av experimenter)
% 5 = Sabotage (av target + ap experimenter)

% V3
% same as V2, but relationship with experimenter is based on angle rather than linear distance

% V4
% combines aspects of V2 and V3 to be very strict
% 1 = Approach
% 2 = Avoidance
% 3 = No Conflict
% 4 = Other

%each applicable data point assigned to behaviors for 4 behavior codes
pre_table.BehaviorIndex_v1(size(pre_table.A_timestamp,1),1)=0;
pre_table.BehaviorIndex_v2(size(pre_table.A_timestamp,1),1)=0;
pre_table.BehaviorIndex_v3(size(pre_table.A_timestamp,1),1)=0;
pre_table.BehaviorIndex_v4(size(pre_table.A_timestamp,1),1)=0;
%prevalence of each behavior for 4 behavior codes
pre_table.BehaviorSummaries.v1(1:size(pre_table.A_trialinfo,1),1:4)=NaN;
pre_table.BehaviorSummaries.v2(1:size(pre_table.A_trialinfo,1),1:5)=NaN;
pre_table.BehaviorSummaries.v3(1:size(pre_table.A_trialinfo,1),1:5)=NaN;
pre_table.BehaviorSummaries.v4(1:size(pre_table.A_trialinfo,1),1:4)=NaN;

for trialnum=1:size(pre_table.A_trialinfo,1)
    if ~isnan(pre_table.A_trialinfo(trialnum,6))
        A_pos=pre_table.A_HeadPos(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),[1 3]);
        A_vel=pre_table.A_HeadVel(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),1);
        B_pos=pre_table.B_HeadPos(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),[1 3]);
        B_vel=pre_table.B_HeadVel(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),1);
        Avoid_Angle=pre_table.AvoidAngle(pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2),1);
        A_trj=diff(A_pos); A_trj(1,:)=A_trj(2,:); A_trj(end,:)=A_trj(end-1,:); A_trj=[NaN NaN; A_trj];
        B_trj=diff(B_pos); B_trj(1,:)=B_trj(2,:); B_trj(end,:)=B_trj(end-1,:); B_trj=[NaN NaN; B_trj];
        Angle_trj=diff(Avoid_Angle); Angle_trj(1,1)=Angle_trj(2,1); Angle_trj(end,1)=Angle_trj(end-1,1); Angle_trj=[NaN; Angle_trj];
        TC=TClocations(trialnum,[1 3]);
        for idx=1:size(A_pos,1)
            if ~isnan(A_pos(idx,1))
                %calculate angle between people with participant as the vertex
                P1=A_pos(idx,:); P2=B_pos(idx,:); v1=TC-P1; v2=P2-P1;
                dotProduct = dot(v1, v2);
                magV1 = norm(v1);
                magV2 = norm(v2);
                cosTheta = dotProduct / (magV1 * magV2);
                thetaRadians = acos(cosTheta);
                angle1(idx,1) = rad2deg(thetaRadians);
    
                %determine if B is moving away from the participant-target line
                distanceToLine = abs((TC(2) - P1(2)) * P2(1) - (TC(1) - P1(1)) * P2(2) + TC(1) * P1(2) - TC(2) * P1(1)) / norm(TC - P1);
                newP3 = P2 + B_trj(idx,:);
                newDistanceToLine = abs((TC(2) - P1(2)) * newP3(1) - (TC(1) - P1(1)) * newP3(2) + TC(1) * P1(2) - TC(2) * P1(1)) / norm(TC - P1);
    
                %determine if A and B are diverging
                newP1 = P1 + A_trj(idx,:);
                AB_distance = sqrt((P2(2)-P1(2))^2+(P2(1)-P1(1))^2);
                newAB_distance = sqrt((newP3(2)-newP1(2))^2+(newP3(1)-newP1(1))^2);

                %determine if A and B angle of avoidance is increasing
                avoid_angle_trj=Angle_trj(idx);

                %determine if A and TC are diverging
                AT_distance = sqrt((TC(2)-P1(2))^2+(TC(1)-P1(1))^2);
                newAT_distance = sqrt((TC(2)-newP1(2))^2+(TC(1)-newP1(1))^2);
    
                % Behaviors - V1
                if angle1(idx)>90 || newDistanceToLine>distanceToLine
                    pre_table.BehaviorIndex_v1(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=1;
                elseif A_vel(idx)<B_vel(idx)
                    pre_table.BehaviorIndex_v1(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=2;
                elseif newAB_distance > AB_distance
                    pre_table.BehaviorIndex_v1(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=3;
                else
                    pre_table.BehaviorIndex_v1(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=4;
                end

                % Behaviors - V2-4
                if A_vel(idx)<0.2
                    pre_table.BehaviorIndex_v2(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=1; %no movement (slow)
                    pre_table.BehaviorIndex_v3(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=1; %no movement (slow)
                else
                    if newAT_distance < AT_distance %approaching the target
                        %V2 - distance
                        if newAB_distance < AB_distance %approaching the experimenter
                            pre_table.BehaviorIndex_v2(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=2; %approach
                        elseif newAB_distance > AB_distance %avoiding the experimenter
                            pre_table.BehaviorIndex_v2(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=4; %no conflict
                        end
                        %V3 - angle
                        if avoid_angle_trj < 0 %approaching the experimenter
                            pre_table.BehaviorIndex_v3(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=2; %approach
                        elseif avoid_angle_trj > 0 %avoiding the experimenter
                            pre_table.BehaviorIndex_v3(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=4; %no conflict
                        end
                        %V4 - distance + angle (strict)
                        if newAB_distance < AB_distance && avoid_angle_trj < 0
                            pre_table.BehaviorIndex_v4(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=1; %approach
                        elseif newAB_distance > AB_distance && avoid_angle_trj > 0
                            pre_table.BehaviorIndex_v4(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=3; %no conflict
                        end

                    elseif newAT_distance >= AT_distance %avoiding the target
                        %V2 - distance
                        if newAB_distance < AB_distance %approaching the experimenter
                            pre_table.BehaviorIndex_v2(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=5; %sabotage
                        elseif newAB_distance > AB_distance %avoiding the experimenter
                            pre_table.BehaviorIndex_v2(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=3; %avoidance
                        end
                        %V3 - angle
                        if avoid_angle_trj < 0 %approaching the experimenter
                            pre_table.BehaviorIndex_v3(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=5; %sabotage
                        elseif avoid_angle_trj > 0 %avoiding the experimenter
                            pre_table.BehaviorIndex_v3(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=3; %avoidance
                        end
                        %V4 - distance + angle (strict)
                        if newAB_distance > AB_distance && avoid_angle_trj > 0
                            pre_table.BehaviorIndex_v4(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=2; %avoidance
                        end
                    end
                end
                if pre_table.BehaviorIndex_v4(pre_table.A_trialinfo(trialnum,1)+idx-1,1)==0
                    pre_table.BehaviorIndex_v4(pre_table.A_trialinfo(trialnum,1)+idx-1,1)=4; %other
                end

            end
        end
        %count how many of each behavior are in each trial (and repeat for all four behavioral codes)
        idx_use=pre_table.A_trialinfo(trialnum,1):pre_table.A_trialinfo(trialnum,2);
        pre_table.BehaviorSummaries.v1(trialnum,1)=numel(find(pre_table.BehaviorIndex_v1(idx_use)==1));
        pre_table.BehaviorSummaries.v1(trialnum,2)=numel(find(pre_table.BehaviorIndex_v1(idx_use)==2));
        pre_table.BehaviorSummaries.v1(trialnum,3)=numel(find(pre_table.BehaviorIndex_v1(idx_use)==3));
        pre_table.BehaviorSummaries.v1(trialnum,4)=numel(find(pre_table.BehaviorIndex_v1(idx_use)==4));
        %convert to proportion
        pre_table.BehaviorSummaries.v1(trialnum,1:4)=pre_table.BehaviorSummaries.v1(trialnum,1:4)/sum(pre_table.BehaviorSummaries.v1(trialnum,1:4));

        pre_table.BehaviorSummaries.v2(trialnum,1)=numel(find(pre_table.BehaviorIndex_v2(idx_use)==1));
        pre_table.BehaviorSummaries.v2(trialnum,2)=numel(find(pre_table.BehaviorIndex_v2(idx_use)==2));
        pre_table.BehaviorSummaries.v2(trialnum,3)=numel(find(pre_table.BehaviorIndex_v2(idx_use)==3));
        pre_table.BehaviorSummaries.v2(trialnum,4)=numel(find(pre_table.BehaviorIndex_v2(idx_use)==4));
        pre_table.BehaviorSummaries.v2(trialnum,5)=numel(find(pre_table.BehaviorIndex_v2(idx_use)==5));
        %convert to proportion
        pre_table.BehaviorSummaries.v2(trialnum,1:5)=pre_table.BehaviorSummaries.v2(trialnum,1:5)/sum(pre_table.BehaviorSummaries.v2(trialnum,1:5));

        pre_table.BehaviorSummaries.v3(trialnum,1)=numel(find(pre_table.BehaviorIndex_v3(idx_use)==1));
        pre_table.BehaviorSummaries.v3(trialnum,2)=numel(find(pre_table.BehaviorIndex_v3(idx_use)==2));
        pre_table.BehaviorSummaries.v3(trialnum,3)=numel(find(pre_table.BehaviorIndex_v3(idx_use)==3));
        pre_table.BehaviorSummaries.v3(trialnum,4)=numel(find(pre_table.BehaviorIndex_v3(idx_use)==4));
        pre_table.BehaviorSummaries.v3(trialnum,5)=numel(find(pre_table.BehaviorIndex_v3(idx_use)==5));
        %convert to proportion
        pre_table.BehaviorSummaries.v3(trialnum,1:5)=pre_table.BehaviorSummaries.v3(trialnum,1:5)/sum(pre_table.BehaviorSummaries.v3(trialnum,1:5));

        pre_table.BehaviorSummaries.v4(trialnum,1)=numel(find(pre_table.BehaviorIndex_v4(idx_use)==1));
        pre_table.BehaviorSummaries.v4(trialnum,2)=numel(find(pre_table.BehaviorIndex_v4(idx_use)==2));
        pre_table.BehaviorSummaries.v4(trialnum,3)=numel(find(pre_table.BehaviorIndex_v4(idx_use)==3));
        pre_table.BehaviorSummaries.v4(trialnum,4)=numel(find(pre_table.BehaviorIndex_v4(idx_use)==4));
        %convert to proportion
        pre_table.BehaviorSummaries.v4(trialnum,1:4)=pre_table.BehaviorSummaries.v4(trialnum,1:4)/sum(pre_table.BehaviorSummaries.v4(trialnum,1:4));
    end
end

clear idx_use

%% detour calculation

TCvsEndSpot=[]; %used later as a sanity check

for trialnum=1:size(pre_table.A_trialinfo,1)
    if ~isnan(pre_table.A_trialinfo(trialnum,6)) && pre_table.A_trialinfo(trialnum,9)==1
        %establish initial participant position
        InitPos=pre_table.A_HeadPos(pre_table.A_trialinfo(trialnum,1),1:3);
        if isnan(InitPos(1))
            n=0;
            while isnan(InitPos(1))
                n=n+1;
                InitPos=pre_table.A_HeadPos(pre_table.A_trialinfo(trialnum,1)+n,1:3);
                if n>=(pre_table.A_trialinfo(trialnum,2)-pre_table.A_trialinfo(trialnum,1))
                    error('No position data for trial %d', trialnum);
                end
            end
        end

        %determine end location of each trial (target reached)
        EndPos=pre_table.A_HeadPos(pre_table.A_trialinfo(trialnum,2),1:3);
        if isnan(EndPos(1))
            n=0;
            while isnan(EndPos(1))
                n=n+1;
                EndPos=pre_table.A_HeadPos(pre_table.A_trialinfo(trialnum,2)-n,1:3);
                if n>=(pre_table.A_trialinfo(trialnum,2)-pre_table.A_trialinfo(trialnum,1))
                    error('No position data for trial %d', trialnum);
                end
            end
        end

        %sanity check - distance between treasure chest and end location
        TCvsEndSpot=[TCvsEndSpot; trialnum sqrt((TClocations(trialnum,3)-EndPos(3))^2+(TClocations(trialnum,1)-EndPos(1))^2)];

        %minimum distance between start point and end point
        MinDist=sqrt((EndPos(3)-InitPos(3))^2+(EndPos(1)-InitPos(1))^2);

        %actual distance
        RealDist=pre_table.A_HeadPos(pre_table.A_trialinfo(trialnum,1)+1:pre_table.A_trialinfo(trialnum,2)-1,:);
        RealDist=diff(RealDist,1); RealDist=abs(RealDist);
        RealDist=nansum(RealDist,1); RealDist=sqrt(RealDist(1)^2+RealDist(3)^2); %#ok<*NANSUM>

        pre_table.A_trialinfo(trialnum,13)=100*(RealDist-MinDist)/MinDist;

        if MinDist<=0.5 %trial started at the treasure chest
            TrialExclude=[TrialExclude trialnum]; TrialExclude=sort(TrialExclude);
            warning('Warning: Bad trial at trial %d. TrialExclude has been updated', trialnum);
            pre_table.A_trialinfo(trialnum,6)=NaN; pre_table.A_trialinfo(trialnum,13)=NaN; 
        end

        if pre_table.A_trialinfo(trialnum,13)<0 && pre_table.A_trialinfo(trialnum,13)>-5
            pre_table.A_trialinfo(trialnum,13)=0; %Possibility that position is slightly off
        elseif pre_table.A_trialinfo(trialnum,13)<-5
            error('One of the detour distances is way off, trial %d',trialnum);
        end
    else, pre_table.A_trialinfo(trialnum,13)=NaN;
    end
end

clear trialnum InitPos n MinDist RealDist EndPos

% trial speed calculation

All_ASpeed=[]; All_BSpeed=[]; %every participant/experimenter velocity data point (under valid trials)
for trialnum=1:size(pre_table.A_trialinfo,1)
    if ~isnan(pre_table.A_trialinfo(trialnum,6))
        %Participant speed calculation
        SpeedList=pre_table.A_HeadVel(pre_table.A_trialinfo(trialnum,1)+1:pre_table.A_trialinfo(trialnum,2)-1,:);
        pre_table.A_trialinfo(trialnum,14)=nanmean(SpeedList); %#ok<*NANMEAN>
        All_ASpeed=[All_ASpeed; SpeedList];

        %Experimenter speed calculation
        SpeedList=pre_table.B_HeadVel(pre_table.A_trialinfo(trialnum,1)+1:pre_table.A_trialinfo(trialnum,2)-1,:);
        pre_table.A_trialinfo(trialnum,15)=nanmean(SpeedList);
        All_BSpeed=[All_BSpeed; SpeedList];
    else
        pre_table.A_trialinfo(trialnum,14:15)=NaN;
    end
end

clear SpeedList

%% Plot positions of participant, experimenter, treasure chest

figure(fignum); 
subplot(1, 3, 1); hold on; 

%Participant
for trialnum=1:size(pre_table.A_trialinfo,1)
    if ~isnan(pre_table.A_trialinfo(trialnum,6))
        trialstart=pre_table.A_trialinfo(trialnum,1);
        trialend=pre_table.A_trialinfo(trialnum,2);
        scatter(pre_table.A_HeadPos(trialstart:trialend,1),pre_table.A_HeadPos(trialstart:trialend,3),20,'b','filled');
    end
end

% Define and plot the circle
plot(3*cos(linspace(0, 2*pi, 100)), 3*sin(linspace(0, 2*pi, 100)), 'LineWidth', 4, 'Color', 'k'); % Circle with thick outline
axis equal; xlim([-3, 3]);  ylim([-3, 3]);  xlabel('X-axis (m)'); ylabel('Y-axis (m)'); title('Participant Positions','FontSize',16);


%Treasure Chests
subplot(1, 3, 2); hold on; 

for trialnum=1:size(pre_table.A_trialinfo,1)
    if ~isnan(pre_table.A_trialinfo(trialnum,6))
        scatter(TClocations(trialnum,1),TClocations(trialnum,3),100,[0.4940 0.1840 0.5560],'filled');
    end
end

% Define and plot the circle
plot(3*cos(linspace(0, 2*pi, 100)), 3*sin(linspace(0, 2*pi, 100)), 'LineWidth', 4, 'Color', 'k'); % Circle with thick outline
axis equal; xlim([-3, 3]);  ylim([-3, 3]);  xlabel('X-axis (m)'); ylabel('Y-axis (m)'); title('Treasure Chests','FontSize',16);

%Experimenter
subplot(1, 3, 3); hold on; 

for trialnum=1:size(pre_table.A_trialinfo,1)
    if ~isnan(pre_table.A_trialinfo(trialnum,6))
        trialstart=pre_table.A_trialinfo(trialnum,1);
        trialend=pre_table.A_trialinfo(trialnum,2);
        scatter(pre_table.B_HeadPos(trialstart:trialend,1),pre_table.B_HeadPos(trialstart:trialend,3),20,'r','filled');
    end
end

% Define and plot the circle
plot(3*cos(linspace(0, 2*pi, 100)), 3*sin(linspace(0, 2*pi, 100)), 'LineWidth', 4, 'Color', 'k'); % Circle with thick outline
axis equal; xlim([-3, 3]);  ylim([-3, 3]);  xlabel('X-axis (m)'); ylabel('Y-axis (m)'); title('Experimenter Positions','FontSize',16);

PosValues=get(gcf, 'Position'); 
set(gcf,'Position',[PosValues(1)-450, PosValues(2)-150, PosValues(3)*2.2, PosValues(4)]);
fignum=fignum+1; 

clear PosValues x_circle y_circle trialstart trialend

%% summary information

analyzed_trials=pre_table.A_trialinfo(~isnan(pre_table.A_trialinfo(:,6)),:);
pre_table.Summary=[]; 

pre_table.Summary{1,1}='Blocks';
pre_table.Summary{1,2}=length(find(diff(pre_table.A_trialinfo(:,3)) ~= 0)) + 1;

pre_table.Summary{2,1}='Blocks (Analyzed)';
pre_table.Summary{2,2}=length(find(diff(analyzed_trials(:,3)) ~= 0)) + 1;

pre_table.Summary{3,1}='Trials';
pre_table.Summary{3,2}=size(pre_table.A_trialinfo,1);

pre_table.Summary{4,1}='Trials (Analyzed)';
pre_table.Summary{4,2}=size(analyzed_trials,1);

pre_table.Summary{5,1}='List of Analyzed Trials';
pre_table.Summary{5,2}=find(~isnan(pre_table.A_trialinfo(:,6)));

pre_table.Summary{6,1}='Analyzed - Type 1 (100/2000)';
pre_table.Summary{6,2}=sum(analyzed_trials(:,6)==1);

pre_table.Summary{7,1}='Analyzed - Type 2 (300/500)';
pre_table.Summary{7,2}=sum(analyzed_trials(:,6)==2);

pre_table.Summary{8,1}='Analyzed - Type 3 (1000/100)';
pre_table.Summary{8,2}=sum(analyzed_trials(:,6)==3);

pre_table.Summary{9,1}='Analyzed - Type 4 (500/0)';
pre_table.Summary{9,2}=sum(analyzed_trials(:,6)==4);

pre_table.Summary{10,1}='Collections';
pre_table.Summary{10,2}=sum(pre_table.A_trialinfo(:,9)==1);

pre_table.Summary{11,1}='Collections (Analyzed)';
pre_table.Summary{11,2}=sum(analyzed_trials(:,9)==1);

pre_table.Summary{12,1}='Sync - Marks';
if UnityMarks==1, pre_table.Summary{12,2}=pre_table.Mark_AB_diff; else, pre_table.Summary{12,2}=NaN; end

pre_table.Summary{13,1}='Sync - Collections';
pre_table.Summary{13,2}=pre_table.A_trialinfo(:,11);

pre_table.Summary{14,1}='Rounds with Collision (%)';
pre_table.Summary{14,2}=100*length(find(analyzed_trials(:,10)==1))/length(analyzed_trials(:,10));

pre_table.Summary{15,1}='Rounds - Target Reached (%)';
pre_table.Summary{15,2}=100*length(find(analyzed_trials(:,9)==1))/length(analyzed_trials(:,9));

pre_table.Summary{16,1}='End Location vs Treasure Chest discrepancy (m)';
pre_table.Summary{16,2}=TCvsEndSpot;

pre_table.Summary{17,1}='Participant Speed (m/s)';
pre_table.Summary{17,2}=nanmean(All_ASpeed);

pre_table.Summary{18,1}='Experimenter Speed (m/s)';
pre_table.Summary{18,2}=nanmean(All_BSpeed);

pre_table.Summary{19,1}='Collision Distances (m)';
if exist('collision_list','var'), pre_table.Summary{19,2}=collision_list(:,2); else, pre_table.Summary{19,2}=NaN; end

pre_table.Summary{20,1}='Total amount of data (seconds, data points)';
pre_table.Summary{20,2}=[time_length Total_count];

pre_table.Summary{21,1}='Bad eye indexes (%)';
if exist('BadEyeIdx_prop','var'), pre_table.Summary{21,2}=[100*BadEyeIdx_prop]; else, pre_table.Summary{21,2}=NaN; end

pre_table.Summary{22,1}='Treasure Chest in View (%)';
if UseHeadA==1, pre_table.Summary{22,2}=[100*numel(find(pre_table.TCinView(:,1)==1))/(numel(find(pre_table.TCinView(:,1)==1))+numel(find(pre_table.TCinView(:,1)==0)))]; else, pre_table.Summary{22,2}=NaN; end

pre_table.Summary{23,1}='Experimenter in View (%)';
if any(pre_table.UA_EyeDir(:,1)>0) && UseHeadA==1 && UseEye==1
    pre_table.Summary{23,2}=[100*numel(find(pre_table.P2inView(:,1)==1))/(numel(find(pre_table.P2inView(:,1)==1))+numel(find(pre_table.P2inView(:,1)==0)))];
else, pre_table.Summary{23,2}=NaN; end

pre_table.Summary{24,1}='Unity indexes for Motive Files';
if exist('M_indexlist','var'), pre_table.Summary{24,2}=M_indexlist; else, pre_table.Summary{24,2}=NaN; end

pre_table.Summary{25,1}='Unity vs Motive Correspondence Strength - R value';
if exist('Corr_R','var'), pre_table.Summary{25,2}=Corr_R; else, pre_table.Summary{25,2}=NaN; end

pre_table.Summary{26,1}='Unity vs Motive Correspondence Strength - RMS';
if exist('RMS_Corr','var'), pre_table.Summary{26,2}=RMS_Corr; else, pre_table.Summary{26,2}=NaN; end

toc

%% Garbage clean-up

clear idx ans numblocks blocknum trialstarts trialend trialend_collect trialend_expire
clear trialstartblock trialnum trialendblock blocklist itm_start_idx collect_idx data_seg
clear Bcol_idx goodtrialcounter synch UB_synchtime Bpos first_synch postrow_idx prerow_idx
clear A_pos B_pos A_vel B_vel P1 P2 v1 v2 dotProduct magV1 magV2 cosTheta row
clear thetaRadians TC newP1 newP3 newPoint newAB_distance newDistanceToLine magMovementVector_P3
clear magMovementVector_P1 magMovementVector magLineVector AB_distance distanceToLine crossProduct
clear B_trj A_trj angle angle1 A_marks B_marks analyzed_trials blockend blockrows blockstart
clear A_import B_import Motive_import Motive_data TCvsEndSpot UM_indexlist U_Mfilenum U_filenum
clear U_division RecordingLength num_Mfile MotiveSampleRate list filenum A_UnitySplits B_UnitySplits
clear M_indexlist AT_distance newAT_distance Corr_R RMS_Corr fig newPosition currentPosition
clear idx_use All_BSpeed All_ASpeed Angle_trj Avoid_Angle avoid_angle_trj collision_list
clear time_length BadEyeIdx_prop BadEyeIdx_count Total_count

%% Save Output

cd(folder)
if AutoSave==1
    filename = ['Preprocessing Output' '_' datestr(now, 'yyyy-mm-dd_HH-MM-SS')]; %#ok<*TNOW1,*DATST>
    figHandles = findall(0, 'Type', 'figure');
    save(filename);
end

