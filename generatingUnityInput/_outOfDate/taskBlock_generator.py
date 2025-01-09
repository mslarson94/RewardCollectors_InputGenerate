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
from helper_functions.path_check import path_check
from dataConfigs import *

######################################################################################
################################## Basic Stuff #######################################

fileEnding = '_ratio' + str(ratio) + 'to1.csv'
setListOrder = ['normal', 'collectionOrder',
						  'HV1_NV1', 'HV1_NV2', 'HV2_NV1', 'HV2_NV2', 'tutorial']

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
# print('8'*20)
positions = pd.DataFrame.from_dict(pos_dict)
# print(positions.strPositions)
######################################################################################
################## Generating the long multi line for the TP1 phase ##################

shuffling_AN_pos = AN_positions
shuffling_PO_pos = PO_positions

tutorial_AN_pos = str(tutorial_pos[0][0]) + ' 0.0 ' + str(tutorial_pos[0][1])
tutorial_PO_pos = str(tutorial_pos[1][0]) + ' 0.0 ' + str(tutorial_pos[1][1])
tutorial_AN_tp1_pos = str(tutorial_pos[0][0]) + '|0.0|' + str(tutorial_pos[0][1])
tutorial_PO_tp1_pos = str(tutorial_pos[1][0]) + '|0.0|' + str(tutorial_pos[1][1])
# print('tutorial AN position', tutorial_AN_pos, '\n')
# print('tutorial PO position', tutorial_PO_pos)
tutorial_ie_block = '6,'+ tutorial_AN_pos + ',' + tutorial_PO_pos + ',AcollectBwatch'
tutorial_tp1_blocka = '6,' + tutorial_AN_pos + ',' + tutorial_PO_pos + ',ApindropBwatch,'+ str(criterion) + ','
tutorial_tp1_block = tutorial_tp1_blocka + tutorial_AN_tp1_pos + ',' + tutorial_PO_tp1_pos
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


print(tp1_block_a)

tp1_block = tp1_block_a + multi_AN_str + ',' + multi_PO_str

######################################################################################
######################################################################################


subType_EV_trials = setListOrder[2:-1]
print(subType_EV_trials)
total_EV_trials = 4 * EV_trials

#print('length of positions list', len(subType_EV_trials))
normal_trials = ratio * total_EV_trials
total_trials = (ratio + 1) * total_EV_trials

positions_div = math.ceil(total_trials/8)
print(total_trials, 8*positions_div)
rows_to_remove = (positions_div*8) - total_trials
print('rows to remove', rows_to_remove)
print('length of total trials', total_trials)
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
print('set list', len(set_list), set_list)

big_df = pd.DataFrame(list(zip(trialType_list, set_list)), columns = ['TrialType', 'set'])

big_df_file = troubleshootingFolder + '/big_df' + fileEnding
big_df.to_csv(big_df_file, index=False)


large_positions = pd.concat([positions]*positions_div, ignore_index=True)

large_positions = large_positions.sample(frac=1).reset_index(drop=True)
#print(positions)
big_df = big_df.sample(frac=1).reset_index(drop=True)
print('length of big_df')
print(len(big_df))
print(len(large_positions))
print('large positions')
#print(large_positions)
if rows_to_remove == 0: 
	pass
else:
	large_positions = large_positions.head(-1*(rows_to_remove))
print(len(large_positions))
print('new large positions')
#print(large_positions)
large_pos_df_file = troubleshootingFolder + '/large_pos_df' + fileEnding
large_positions.to_csv(large_pos_df_file, index=False)

big_df = pd.concat([big_df,large_positions], axis = 1)

bigger_df_file = troubleshootingFolder + '/bigger_df' + fileEnding
big_df.to_csv(bigger_df_file, index=False)
temp_str_AN = '0.0|0.0|5.0,1.75|0.0|4.25'
tp2_block = []
for index, row in big_df.iterrows():
	block_text = str(row['set']) + ',' + str(row['strPositions']) + 'ApindropBwatch,0,' + temp_str_AN
	print(block_text)
	tp2_block.append(block_text)

big_df['tp2_block'] = tp2_block
biggest_df_file = troubleshootingFolder + '/biggest_df' + fileEnding
big_df.to_csv(biggest_df_file, index=False)

nearly_there = [initial_text]
nearly_there.append(tutorial_ie_block)
nearly_there.append(tutorial_tp1_block)
nearly_there.append(ie_block)
nearly_there.append(tp1_block)
nearly_there.extend(tp2_block)

print('Troubleshooting file order: large_positions, big_df, bigger_df')
#taskBlocks = pd.DataFrame(nearly_there)

taskBlocksFileName = 'taskBlocks' + fileEnding

path = os.path.join(outPath, taskBlocksFileName)

with open(path, "w+") as csvFile:
	for line in nearly_there[:-1]:
		csvFile.write(line)
		csvFile.write('\n')
	csvFile.write(nearly_there[-1])



