'''
taskBlocks_generator.py
Created on March, 14 2024
@author: myra

generate TaskBlocks.csv files from given x,y,z coordinates
'''

import csv
import random
import pandas as pd
import math
import os
from helper_functions.path_check import path_check
from dataConfigs_3Coins import *
def generate_balanced_segments(trial_ratio, ev_types, segment_count=5, segment_size=24):
    """
    Generate balanced and randomized segments for trialType_list and pathIDs.

    Args:
        trial_ratio (list): Actual counts of trial types in one segment (e.g., [16, 4, 4]).
        ev_types (list): List of trial types, e.g., ["normal", "PPE", "NPE"].
        segment_count (int): Number of segments.
        segment_size (int): Total size of each segment.

    Returns:
        df (DataFrame): Balanced DataFrame with columns 'TrialType', 'Segment', 'PathID'.
    """
    # Validate trial ratio
    if sum(trial_ratio) != segment_size:
        raise ValueError(
            f"The trial ratio ({sum(trial_ratio)}) must sum up to the segment size ({segment_size})."
        )

    path_id_map = {"normal": 1, "PPE": 2, "NPE": 3}  # Map trial types to PathIDs

    # Create a DataFrame for all segments
    all_segments = []
    for seg in range(1, segment_count + 1):
        segment = []
        for ev_type, count in zip(ev_types, trial_ratio):
            path_id = path_id_map[ev_type]  # Get PathID for the trial type
            segment.extend([(ev_type, seg, path_id)] * count)

        # Shuffle trials within the segment
        random.shuffle(segment)

        # Create a DataFrame for this segment
        segment_df = pd.DataFrame(segment, columns=["TrialType", "Segment", "set"])
        all_segments.append(segment_df)

    # Concatenate all segments into a single DataFrame
    df = pd.concat(all_segments, ignore_index=True)

    # Debugging: Ensure all PathIDs and segments are populated correctly
    segment_summary = df.groupby(["Segment", "set"]).size().reset_index(name="TrialCount")
    print("Segment Summary:")
    print(segment_summary)

    return df




######################################################################################
################################## Basic Stuff #######################################

fileEnding = '_' + whichCoinSet + '.csv'
setListOrder = ['normal', 'collectionOrder', 'PPE', 'NPE', 'tutorial']

initial_text = """//pathID,initialposA,initialposB,mode,perfectRoundsTarget,resetpositionsA,resetpositionsB (mode options: AvtraceBwatch,AitraceBwatch,Avoting,Bvoting,bothVoting,BvtraceAwatch,BitraceAwatch,AvtraceBfollow,BvtraceAfollow)"""

AN_positions = AN_positions[:-1]
PO_positions = PO_positions[:-1]
position_list = position_list[:-1]
single_list = []
for i in position_list:
    AN_tmp = i[0]
    PO_tmp = i[1]
    single_str = str(AN_tmp[0]) + ' 0.0 ' + str(AN_tmp[1]) + ',' + str(PO_tmp[0]) + ' 0.0 ' + str(PO_tmp[1]) + ','
    single_list.append(single_str)

pos_dict = {
    "position": ["1", "2", "3", "4", "5", "6", "7", "8"],
    "AN_vals": AN_positions,
    "PO_vals": PO_positions,
    "strPositions": single_list
}
positions = pd.DataFrame.from_dict(pos_dict)

# Parameters
trial_ratio = [16, 4, 4]  # Ratio of Normal:PPE:NPE for 80:20:20
ev_types = ["normal", "PPE", "NPE"]  # Event types
segment_count = 5  # Number of segments
segment_size = 24  # Number of items in each segment

# Generate balanced and randomized segments
df_balanced = generate_balanced_segments(trial_ratio, ev_types, segment_count, segment_size)

# Save the generated trials for verification
balanced_trials_file = os.path.join(troubleshootingFolder, f"balanced_trials_{fileEnding}")
df_balanced.to_csv(balanced_trials_file, index=False)
print(f"Saved balanced trials to: {balanced_trials_file}")
######################################################################################
################## Generating the long multi line for the TP1 phase ##################

######################################################################################
################## Generating the long multi line for the TP1 phase ##################

shuffling_AN_pos = AN_positions
shuffling_PO_pos = PO_positions

tutorial_AN_pos = str(tutorial_pos[0][0]) + ' 0.0 ' + str(tutorial_pos[0][1])
tutorial_PO_pos = str(tutorial_pos[1][0]) + ' 0.0 ' + str(tutorial_pos[1][1])
tutorial_AN_tp1_pos = str(tutorial_pos[0][0]) + '|0.0|' + str(tutorial_pos[0][1])
tutorial_PO_tp1_pos = str(tutorial_pos[1][0]) + '|0.0|' + str(tutorial_pos[1][1])
print('tutorial AN position', tutorial_AN_pos, '\n')
print('tutorial PO position', tutorial_PO_pos)

print('myra check check  check ')
tutorial_ie_block = '4,' + tutorial_AN_pos + ',' + tutorial_PO_pos + ',AcollectBwatch'
tutorial_tp1_blocka = '4,' + tutorial_AN_pos + ',' + tutorial_PO_pos + ',ApindropBwatch,' + str(criterion) + ','
tutorial_tp1_block = tutorial_tp1_blocka + tutorial_AN_tp1_pos + ',' + tutorial_PO_tp1_pos
tutorial_tp2_block = '5,' + tutorial_AN_pos + ',' + tutorial_PO_pos + ',ApindropBwatch, 0,' + tutorial_AN_tp1_pos + ',' + tutorial_PO_tp1_pos

print(firstpos)
firstpos = int(firstpos)

print(single_list[firstpos-1], type(single_list[firstpos-1]))
ie_block = '1,' + str(single_list[firstpos-1]) + 'AcollectBwatch' ## Initial Encoding Block

### possibly add tutorial blocks here 
new_list = list(range(1,8))

random.shuffle(new_list)

AN_firstpos = AN_positions[firstpos-1]
PO_firstpos = PO_positions[firstpos-1]

shuffling_AN_pos.pop(firstpos-1)
shuffling_PO_pos.pop(firstpos-1)

shuffling_AN_pos = [x for _, x in sorted(zip(new_list, shuffling_AN_pos), key=lambda pair: pair[0])]
shuffling_PO_pos = [x for _, x in sorted(zip(new_list, shuffling_PO_pos), key=lambda pair: pair[0])]

AN_secondpos = shuffling_AN_pos[0] 
PO_secondpos = shuffling_PO_pos[0]

AN_secondpos_str = str(AN_secondpos[0]) + ' 0.0 ' + str(AN_secondpos[1])
PO_secondpos_str = str(PO_secondpos[0]) + ' 0.0 ' + str(PO_secondpos[1])

shuffling_AN_pos.pop(0)
shuffling_PO_pos.pop(0)

tp1_block_a = '1,' + str(AN_secondpos_str) + ',' +str(PO_secondpos_str) + ',' + 'ApindropBwatch,' + str(criterion) + ','

shuffling_AN_pos.append(AN_firstpos)
shuffling_PO_pos.append(PO_firstpos)

shuffling_AN_pos.append(AN_secondpos)
shuffling_PO_pos.append(PO_secondpos)
# multi_AN_str = ""
# multi_PO_str = ""

multi_AN_str = ' | '.join(f"{an[0]}|0.0|{an[1]}" for an in shuffling_AN_pos)
multi_PO_str = ' | '.join(f"{po[0]}|0.0|{po[1]}" for po in shuffling_PO_pos)

# for an in shuffling_AN_pos[:-1]:
# 	multi_AN_str += str(an[0]) + '|0.0|' + str(an[1]) + ' |'

# last_AN_item = shuffling_AN_pos[-1]
# multi_AN_str += str(last_AN_item[0]) + '|0.0|' + str(last_AN_item[1])

# for po in shuffling_PO_pos[:-1]:
# 	multi_PO_str += str(po[0]) + '|0.0|' + str(po[1]) + ' |'

# last_PO_item = shuffling_PO_pos[-1]
# multi_PO_str += str(last_PO_item[0]) + '|0.0|' + str(last_PO_item[1])


# print(tp1_block_a)

tp1_block = tp1_block_a + multi_AN_str + ',' + multi_PO_str

#######################################################################################
################################## Generating Data ###################################
#trialType_list = ['normal', 'normal', 'normal', 'normal', 'PPE', 'NPE']
trialType_list = ['normal', 'PPE', 'NPE']
# Read the balanced trials file
df_balanced = pd.read_csv(balanced_trials_file)

# Debugging: Check the loaded balanced trials
print("Loaded Balanced Trials:")
print(df_balanced.head())

# Map trial data to positions
positions_div = math.ceil(len(df_balanced) / len(position_list))
position_data = pd.concat([positions] * positions_div, ignore_index=True).sample(
    n=len(df_balanced), random_state=42).reset_index(drop=True)

# Ensure lengths match
assert len(df_balanced) == len(position_data), "Mismatch between trial data and positions!"

# Merge balanced trials with positions
big_df = df_balanced.copy()
big_df["strPositions"] = position_data["strPositions"]


######################################################################################
#################### Generating and Saving Additional DataFrames #####################
######################################################################################

# Generate RR data (role-reversal trials)
roleReversalTrials = 20
rr_trialType_list = ["rr"] * roleReversalTrials
rr_set_list = [1] * roleReversalTrials
rr_positions = pd.concat([positions] * math.ceil(roleReversalTrials / len(positions)), ignore_index=True)
rr_positions = rr_positions.sample(n=roleReversalTrials, random_state=42).reset_index(drop=True)

rr_big_df = pd.DataFrame({
    "TrialType": rr_trialType_list,
    "set": rr_set_list,
    "strPositions": rr_positions["strPositions"]
})

# Save RR DataFrames
rr_big_df_file = os.path.join(troubleshootingFolder, f"rr_big_df_{fileEnding}")
rr_big_df.to_csv(rr_big_df_file, index=False)
print(f"Saved rr_big_df to: {rr_big_df_file}")

######################################################################################
########################## Generating TP2 and Role-Reversal Blocks ###################
######################################################################################

temp_str_AN = '0.0|0.0|5.0,1.75|0.0|4.25'

# Generate tp2_block for big_df
tp2_block = [
    f"{row['set']},{row['strPositions']}ApindropBwatch,0,{temp_str_AN}"
    for _, row in big_df.iterrows()
]
big_df["tp2_block"] = tp2_block

# Save biggest_df
biggest_df_file = os.path.join(troubleshootingFolder, f"biggest_df_{fileEnding}")
big_df.to_csv(biggest_df_file, index=False)
print(f"Saved biggest_df to: {biggest_df_file}")

# Generate rr_block for rr_big_df
rr_block = [
    f"{row['set']},{row['strPositions']}ApindropBwatch,0,{temp_str_AN}"
    for _, row in rr_big_df.iterrows()
]
rr_big_df["rr_block"] = rr_block

# Save rr_biggest_df
rr_biggest_df_file = os.path.join(troubleshootingFolder, f"rr_biggest_df_{fileEnding}")
rr_big_df.to_csv(rr_biggest_df_file, index=False)
print(f"Saved rr_biggest_df to: {rr_biggest_df_file}")


nearly_there = [initial_text]
nearly_there.append(tutorial_ie_block)
nearly_there.append(tutorial_tp1_block)
nearly_there.append(ie_block)
nearly_there.append(tp1_block)
nearly_there.extend(tp2_block)


withAddTut = [initial_text]
withAddTut.append(tutorial_ie_block)
withAddTut.append(tutorial_tp1_block)
withAddTut.append(ie_block)
withAddTut.append(tp1_block)
withAddTut.append(tutorial_tp2_block)
withAddTut.extend(tp2_block)

roleReversal = [initial_text]
roleReversal.append(tutorial_tp1_block)
roleReversal.extend(rr_block)


print('Troubleshooting file order: large_positions, big_df, bigger_df')
#taskBlocks = pd.DataFrame(nearly_there)

taskBlocksFileName = 'taskBlocks' + fileEnding

path = os.path.join(outPath, taskBlocksFileName)

roleReversalFile = 'taskBlocks_2' + fileEnding
rolepath = os.path.join(outPath, roleReversalFile)

tuttaskBlocksFileName = 'withXtraTut_taskBlocks' + fileEnding

tutpath = os.path.join(outPath, tuttaskBlocksFileName)

with open(path, "w+") as csvFile:
	for line in nearly_there[:-1]:
		csvFile.write(line)
		csvFile.write('\n')
	csvFile.write(nearly_there[-1])

with open(tutpath, "w+") as csvFile:
	for line in withAddTut[:-1]:
		csvFile.write(line)
		csvFile.write('\n')
	csvFile.write(withAddTut[-1])

with open(rolepath, "w+") as csvFile:
	for line in roleReversal[:-1]:
		csvFile.write(line)
		csvFile.write('\n')
	csvFile.write(roleReversal[-1])
