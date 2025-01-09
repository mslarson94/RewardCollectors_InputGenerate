'''
taskBlocks_generator.py
Created on March, 14 2024
@author: myra

generate TaskBlocks.csv files from given x,y,z coordinates
'''
#from collections import namedtuple
import csv
import random
import pandas as pd
import math
import os
################### inputs #################

#### Positions ######
# position values are in x,z format
# pos_ = [AN, PO]
pos1 = [[0.0, 5.0],		[1.75, 4.25]] 
pos2 = [[3.5, 3.5],		[4.25, 1.75]]
pos3 = [[5.0, 0.0],		[4.25, -1.75]]
pos4 = [[3.5, -3.5],	[1.75, -4.25]]
pos5 = [[0.0, -5.0],	[-1.75, -4.25]]
pos6 = [[-3.5, -3.5],	[-4.25, -1.75]]
pos7 = [[-5.0, 0.0],	[-4.25, 1.75]]
pos8 = [[-3.5, 3.5],	[-1.75, 4.25]]

tutorial_pos = [[-2, -5], [0, -5]]

#first position set
firstpos = 1

#criterion is the number of consecutive perfect rounds to hit in TP1 to reach TP2
criterion = 3

# ratio of total number of normal trials to EV trials. e.g. ratio = 3 is 3:1 normal:EV 
ratio = 3

# total number of trials for a EV sub-swap type e.g. swap type = HV-NV, sub-swap types are : HV1-NV1, HV2-NV2, HV1-NV2, HV2-NV1
EV_trials = 4

# outpath 
outpath = 'Desktop/task_test/'

###################################################################################################################
###################################################################################################################



######################################################################################
################################## Basic Stuff #######################################

setListOrder = ['normal', 'collectionOrder',
						  'HV1_NV1', 'HV1_NV2', 'HV2_NV1', 'HV2_NV2', 
                          'LV1_NV1', 'LV1_NV2', 'LV2_NV1', 'LV2_NV2',
                          'HV1_LV1', 'HV1_LV2', 'HV2_LV1', 'HV2_LV2', 'tutorial']

initial_text = """//pathID,initialposA,initialposB,mode,perfectRoundsTarget,resetpositionsA,resetpositionsB (mode options: AvtraceBwatch,AitraceBwatch,Avoting,Bvoting,bothVoting,BvtraceAwatch,BitraceAwatch,AvtraceBfollow,BvtraceAfollow)"""
position_list = [pos1, pos2, pos3, pos4, pos5, pos6, pos7, pos8]
AN_positions = list(list(zip(*position_list))[0])
PO_positions = list(list(zip(*position_list))[1])

single_list =[]
for i in position_list:
	AN_tmp = i[0]
	PO_tmp = i[1]
	single_str = str(AN_tmp[0]) + ' 0.0 ' + str(AN_tmp[1]) + ',' + str(PO_tmp[0]) + ' 0.0 ' + str(PO_tmp[1]) + ','
	single_list.append(single_str)

pos_dict = {
	"position" : ["1", "2", "3", "4", "5", "6", "7", "8"],
	"AN_vals": AN_positions,
	"PO_vals": PO_positions,
	"strPositions": single_list
	}

positions = pd.DataFrame.from_dict(pos_dict)

######################################################################################
################## Generating the long multi line for the TP1 phase ##################

shuffling_AN_pos = AN_positions
shuffling_PO_pos = PO_positions

block1 = '2,' + str(single_list[firstpos-1]) + 'AcollectBwatch' ## 1st actual block
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

block2_a = '1,' + str(AN_secondpos_str) + ',' +str(PO_secondpos_str) + ',ApindropBwatch,' + str(criterion) + ','

shuffling_AN_pos.append(AN_firstpos)
shuffling_PO_pos.append(PO_firstpos)

shuffling_AN_pos.append(AN_secondpos)
shuffling_PO_pos.append(PO_secondpos)
multi_AN_str = ""
multi_PO_str = ""
for an in shuffling_AN_pos[:-1]:
	multi_AN_str += str(an[0]) + '|0.0|' + str(an[1]) + ' |'

last_AN_item = shuffling_AN_pos[-1]
multi_AN_str += str(last_AN_item[0]) + '|0.0|' + str(last_AN_item[1])

for po in shuffling_PO_pos[:-1]:
	multi_PO_str += str(po[0]) + '|0.0|' + str(po[1]) + ' |'

last_PO_item = shuffling_PO_pos[-1]
multi_PO_str += str(last_PO_item[0]) + '|0.0|' + str(last_PO_item[1])


print(block2_a)

block2 = block2_a + multi_AN_str + ',' + multi_PO_str

######################################################################################
######################################################################################


subType_EV_trials = setListOrder[2:]
print(subType_EV_trials)
total_EV_trials = 12 * EV_trials

#print('length of positions list', len(subType_EV_trials))
normal_trials = ratio * total_EV_trials
total_trials = (ratio + 1) * total_EV_trials

positions_div = math.ceil(total_trials/8)
print(total_trials, 8*positions_div)
rows_to_remove = (positions_div*8) - total_trials
print('rows to remove', rows_to_remove)
#print('length of total trials', len(total_trials))
new_EV_list = []
#set_list = []
#trialType_list = []
set_list = [1]*normal_trials
trialType_list = ['normal']*normal_trials

step = 1
for ev in subType_EV_trials:
	step += 1
	num_EV = [ev]*EV_trials
	new_step = [step]*EV_trials
	set_list.extend(new_step)
	trialType_list.extend(num_EV)

print(len(trialType_list), len(set_list))
print(set_list)
#big_df = pd.concat([positions]*total_trials)
big_df = pd.DataFrame(list(zip(trialType_list, set_list)), columns = ['TrialType', 'set'])

big_df_file = outpath + 'big_df.csv'
big_df.to_csv(big_df_file, index=False)


large_positions = pd.concat([positions]*positions_div, ignore_index=True)

large_positions = large_positions.sample(frac=1).reset_index(drop=True)
big_df = big_df.sample(frac=1).reset_index(drop=True)
print(len(large_positions))

large_positions = large_positions.head(-1*(rows_to_remove))
print(len(large_positions))

big_df = pd.concat([big_df,large_positions], axis = 1)

bigger_df_file = outpath + 'bigger_df.csv'
big_df.to_csv(bigger_df_file, index=False)
temp_str_AN = '0.0|0.0|5.0,1.75|0.0|4.25'
actual_txt = []
for index, row in big_df.iterrows():
    block_text = str(row['set']) + ',' + str(row['strPositions']) + 'ApindropBwatch,0' 
    actual_txt.append(block_text)

big_df['actual_txt'] = actual_txt
biggest_df_file = outpath + 'biggest_df.csv'
big_df.to_csv(biggest_df_file, index=False)

nearly_there = [initial_text]
nearly_there.append(block1)
nearly_there.append(block2)
nearly_there.extend(actual_txt)

#taskBlocks = pd.DataFrame(nearly_there)
taskBlocks_file = outpath + 'taskBlocks.csv'


path = os.path.join(outpath, 'taskBlocks.csv')
# os.chdir('..')
# print(os.getcwd())
# os.chdir('..')
# print(os.getcwd())
# os.chdir('..')
# print(os.getcwd())
# os.chdir('..')
# print(os.getcwd())
# os.chdir(outpath)
with open(path, "w+") as csvFile:
	for line in nearly_there[:-1]:
		csvFile.write(line)
		csvFile.write('\n')
	csvFile.write(nearly_there[-1])
# with taskBlocks_file.open(mode='w+') as csv_file:
#         writer = csv.writer(csv_file, delimiter=',')
#         for line in nearly_there:
#             writer.writerow(line)

#taskBlocks_file = outpath + 'nearly_there.csv'
#taskBlocks.to_csv(taskBlocks_file, index = False, mode='a',doublequote=False,escapechar='"',quoting=csv.QUOTE_NONE)


