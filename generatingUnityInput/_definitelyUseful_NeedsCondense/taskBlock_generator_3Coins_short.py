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
from dataConfigs_3Coins import *

######################################################################################
################################## Basic Stuff #######################################

fileEnding = '_' + whichCoinSet + '.csv'
setListOrder = ['normal', 'collectionOrder',
						  'PPE', 'NPE', 'tutorial']

initial_text = """//pathID,initialposA,initialposB,mode,perfectRoundsTarget,resetpositionsA,resetpositionsB (mode options: AvtraceBwatch,AitraceBwatch,Avoting,Bvoting,bothVoting,BvtraceAwatch,BitraceAwatch,AvtraceBfollow,BvtraceAfollow)"""

AN_positions = AN_positions[:-1]
PO_positions = PO_positions[:-1]
position_list = position_list[:-1]
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
print('tutorial AN position', tutorial_AN_pos, '\n')
print('tutorial PO position', tutorial_PO_pos)


print('myra check check  check ')
tutorial_ie_block = '4,'+ tutorial_AN_pos + ',' + tutorial_PO_pos + ',AcollectBwatch'
tutorial_tp1_blocka = '4,' + tutorial_AN_pos + ',' + tutorial_PO_pos + ',ApindropBwatch,'+ str(criterion) + ','
tutorial_tp1_block = tutorial_tp1_blocka + tutorial_AN_tp1_pos + ',' + tutorial_PO_tp1_pos
tutorial_tp2_block = '5,' + tutorial_AN_pos + ',' + tutorial_PO_pos + ',ApindropBwatch, 0,' + tutorial_AN_tp1_pos + ',' + tutorial_PO_tp1_pos
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


subType_EV_trials = len(EV_types)
print(subType_EV_trials)
#total_EV_trials = 2 * EV_trials # medium
total_EV_trials = subType_EV_trials * EV_trials # short


#print('length of positions list', len(subType_EV_trials))
normal_trials = ratio * total_EV_trials
total_trials = normal_trials + total_EV_trials

positions_div = math.ceil(total_trials/8)
print(positions_div, 'OG psoitions div')
rr_div = math.ceil(roleReversalTrials/8)
print(rr_div, 'RR positions div')

print(total_trials, 8*positions_div)
rows_to_remove = (positions_div*8) - total_trials
rr_rows_to_remove = (rr_div*8) - roleReversalTrials
print('rows to remove', rows_to_remove)
print('length of total trials', total_trials)
print('8'*20)
print('rr rows to remove', rr_rows_to_remove)
print('length of rr trials', roleReversalTrials )
new_EV_list = []
#set_list = []
#trialType_list = []
set_list = [1]*normal_trials
trialType_list = ['normal']*normal_trials
rr_set_list = [1]*roleReversalTrials
rrType_list = ['rr']*roleReversalTrials
step = 1
for ev in EV_types:
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

rr_big_df = pd.DataFrame(list(zip(rrType_list, rr_set_list)), columns = ['TrialType', 'set'])
rr_big_df_file = troubleshootingFolder + '/rr_big_df' + fileEnding
rr_big_df.to_csv(rr_big_df_file, index=False)

large_positions = pd.concat([positions]*positions_div, ignore_index=True)
large_positions = large_positions.sample(frac=1).reset_index(drop=True)


rr_large_positions = pd.concat([positions]*rr_div, ignore_index=True)
rr_large_positions = rr_large_positions.sample(frac=1).reset_index(drop=True)

#print(positions)
big_df = big_df.sample(frac=1).reset_index(drop=True)
rr_big_df = rr_big_df.sample(frac=1).reset_index(drop=True)
print('length of big_df')
print(len(big_df))
print(len(large_positions))
print('large positions')
print('kj;'*35)
print('length of rr big_df')
print(len(rr_big_df))
print(len(rr_large_positions))
print('rr large positions')
#print(large_positions)
if rows_to_remove == 0: 
	pass
else:
	large_positions = large_positions.head(-1*(rows_to_remove))

if rr_rows_to_remove == 0: 
	pass
else:
	rr_large_positions = rr_large_positions.head(-1*(rr_rows_to_remove))
print(len(large_positions))
print('new large positions')
#print(large_positions)
large_pos_df_file = troubleshootingFolder + '/large_pos_df' + fileEnding
large_positions.to_csv(large_pos_df_file, index=False)

rr_large_pos_df_file = troubleshootingFolder + '/rr_large_pos_df' + fileEnding
rr_large_positions.to_csv(rr_large_pos_df_file, index=False)

big_df = pd.concat([big_df,large_positions], axis = 1)

rr_big_df = pd.concat([rr_big_df,rr_large_positions], axis = 1)

bigger_df_file = troubleshootingFolder + '/bigger_df' + fileEnding
rr_bigger_df_file = troubleshootingFolder + '/rr_bigger_df' + fileEnding

big_df.to_csv(bigger_df_file, index=False)
rr_big_df.to_csv(rr_bigger_df_file, index=False)

temp_str_AN = '0.0|0.0|5.0,1.75|0.0|4.25'
tp2_block = []
for index, row in big_df.iterrows():
	block_text = str(row['set']) + ',' + str(row['strPositions']) + 'ApindropBwatch,0,' + temp_str_AN
	print(block_text)
	tp2_block.append(block_text)

big_df['tp2_block'] = tp2_block
biggest_df_file = troubleshootingFolder + '/biggest_df' + fileEnding
big_df.to_csv(biggest_df_file, index=False)

rr_block = []
for index, row in rr_big_df.iterrows():
	block_text = str(row['set']) + ',' + str(row['strPositions']) + 'ApindropBwatch,0,' + temp_str_AN
	print(block_text)
	rr_block.append(block_text)

rr_big_df['rr_block'] = rr_block
rr_biggest_df_file = troubleshootingFolder + '/rr_biggest_df' + fileEnding
rr_big_df.to_csv(rr_biggest_df_file, index=False)


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
