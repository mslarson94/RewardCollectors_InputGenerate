'''
TaskBlocks_generator.py
Created on March, 14 2024
@author: myra

generate TaskBlocks.csv files from given x,y,z coordinates
'''
from collections import namedtuple
import csv
import random
################### inputs #################
AN_pos1 = [0.0, 4.0]
AN_pos2 = [2.8, 2.8]
AN_pos3 = [4.0, 0.0]
AN_pos4 = [2.8, -2.8]
AN_pos5 = [0.0, -4.0]
AN_pos6 = [-2.8, -2.8]
AN_pos7 = [-4.0, 0.0]
AN_pos8 = [-2.8, 2.8]

PO_pos1 = [1.4, 3.4]
PO_pos2 = [3.4, 1.4]
PO_pos3 = [1.4, -3.4]
PO_pos4 = [3.4, -1.4]
PO_pos5 = [-1.4, -3.4]
PO_pos6 = [-3.4, -1.4]
PO_pos7 = [-1.4, 3.4]
PO_pos8 = [-3.4, 1.4]

#first position set
firstpos = 8

#criterion is the number of consecutive perfect rounds to hit in TP1 to reach TP2
criterion = 3

# ratio of total number of normal trials to EV trials. e.g. ratio = 3 is 3:1 normal:EV 
ratio = 3

# total number of trials for a EV sub-swap type e.g. swap type = HV-NV, sub-swap types are : HV1-NV1, HV2-NV2, HV1-NV2, HV2-NV1
EV_trials = 16

# outpath 
outpath = '/Desktop/task_test'

############################################
#tp2_round = namedtuple('tp2_round', 'block_num, pathID, initialposA, initialposB')
setListOrder = ['normal', 'collectionOrder',
						  'HV1_NV1', 'HV1_NV2', 'HV2_NV1', 'HV2_NV2', 
                          'LV1_NV1', 'LV1_NV2', 'LV2_NV1', 'LV2_NV2',
                          'HV1_LV1', 'HV1_LV2', 'HV2_LV1', 'HV2_LV2']

initial_text = """//pathID,initialposA,initialposB,mode,perfectRoundsTarget,resetpositionsA,resetpositionsB (mode options: AvtraceBwatch,AitraceBwatch,Avoting,Bvoting,bothVoting,BvtraceAwatch,BitraceAwatch,AvtraceBfollow,BvtraceAfollow)\n"""

AN_pos1_txt = str(AN_pos1[0]) + ' 0.0 ' + str(AN_pos1[1])
AN_pos2_txt = str(AN_pos2[0]) + ' 0.0 ' + str(AN_pos2[1])
AN_pos3_txt = str(AN_pos3[0]) + ' 0.0 ' + str(AN_pos3[1])
AN_pos4_txt = str(AN_pos4[0]) + ' 0.0 ' + str(AN_pos4[1])
AN_pos5_txt = str(AN_pos5[0]) + ' 0.0 ' + str(AN_pos5[1])
AN_pos6_txt = str(AN_pos6[0]) + ' 0.0 ' + str(AN_pos6[1])
AN_pos7_txt = str(AN_pos7[0]) + ' 0.0 ' + str(AN_pos7[1])
AN_pos8_txt = str(AN_pos8[0]) + ' 0.0 ' + str(AN_pos8[1])

PO_pos1_txt = str(PO_pos1[0]) + ' 0.0 ' + str(PO_pos1[1])
PO_pos2_txt = str(PO_pos2[0]) + ' 0.0 ' + str(PO_pos2[1])
PO_pos3_txt = str(PO_pos3[0]) + ' 0.0 ' + str(PO_pos3[1])
PO_pos4_txt = str(PO_pos4[0]) + ' 0.0 ' + str(PO_pos4[1])
PO_pos5_txt = str(PO_pos5[0]) + ' 0.0 ' + str(PO_pos5[1])
PO_pos6_txt = str(PO_pos6[0]) + ' 0.0 ' + str(PO_pos6[1])
PO_pos7_txt = str(PO_pos7[0]) + ' 0.0 ' + str(PO_pos7[1])
PO_pos8_txt = str(PO_pos8[0]) + ' 0.0 ' + str(PO_pos8[1])

AN_pos_txt = [AN_pos1_txt, AN_pos2_txt, AN_pos3_txt, AN_pos4_txt, AN_pos5_txt, AN_pos6_txt, AN_pos7_txt, AN_pos8_txt]
PO_pos_txt = [PO_pos1_txt, PO_pos2_txt, PO_pos3_txt, PO_pos4_txt, PO_pos5_txt, PO_pos6_txt, PO_pos7_txt, PO_pos8_txt]

AN_pos = [AN_pos1, AN_pos2, AN_pos3, AN_pos4, AN_pos5, AN_pos6, AN_pos7, AN_pos8]
PO_pos = [PO_pos1, PO_pos2, PO_pos3, PO_pos4, PO_pos5, PO_pos6, PO_pos7, PO_pos8]

shuffling_AN_pos = AN_pos
shuffling_AN_pos_txt = AN_pos_txt

shuffling_PO_pos = PO_pos
shuffling_PO_pos_txt = PO_pos_txt

block1 = '1, ' + str(AN_pos_txt[firstpos-1]) + ', ' + str(PO_pos_txt[firstpos-1]) + ', AcollectBwatch\n'
new_list = list(range(1,8))

random.shuffle(new_list)

AN_firstpos = AN_pos[firstpos-1]
AN_firstpos_txt = AN_pos_txt[firstpos-1]

PO_firstpos = PO_pos[firstpos-1]
PO_firstpos_txt = PO_pos_txt[firstpos-1]

AN_pos.pop(firstpos-1)
AN_pos_txt.pop(firstpos-1)

PO_pos.pop(firstpos-1)
PO_pos_txt.pop(firstpos-1)

AN_pos = [x for _, x in sorted(zip(new_list, AN_pos), key=lambda pair: pair[0])]
AN_pos_txt = [x for _, x in sorted(zip(new_list, AN_pos_txt), key=lambda pair: pair[0])]
PO_pos = [x for _, x in sorted(zip(new_list, PO_pos), key=lambda pair: pair[0])]
PO_pos_txt = [x for _, x in sorted(zip(new_list, PO_pos_txt), key=lambda pair: pair[0])]

AN_secondpos = AN_pos[0] 
PO_secondpos = PO_pos[0]
AN_secondpos_txt = AN_pos_txt[0] 
PO_secondpos_txt = PO_pos_txt[0]

AN_pos.pop(0)
AN_pos_txt.pop(0)
PO_pos.pop(0)
PO_pos_txt.pop(0)

block2_a = '1, ' + str(AN_secondpos) + ', ' +str(PO_secondpos) + ', ApindropBwatch, ' + str(criterion) + ', '

AN_pos.append(AN_firstpos)
PO_pos.append(PO_firstpos)
AN_pos_txt.append(AN_firstpos_txt)
PO_pos_txt.append(PO_firstpos_txt)

AN_pos.append(AN_secondpos)
AN_pos_txt.append(AN_secondpos_txt)
PO_pos.append(PO_secondpos)
PO_pos_txt.append(PO_secondpos_txt)


print(block2_a)
all_AN_pipe = (str(AN_pos[0][0]) + '|0.0|' + str(AN_pos[0][1]) + '|' + 
	           str(AN_pos[1][0]) + '|0.0|' + str(AN_pos[1][1]) + '|' +
	           str(AN_pos[2][0]) + '|0.0|' + str(AN_pos[2][1]) + '|' +
	           str(AN_pos[3][0]) + '|0.0|' + str(AN_pos[3][1]) + '|' +
	           str(AN_pos[4][0]) + '|0.0|' + str(AN_pos[4][1]) + '|' +
	           str(AN_pos[5][0]) + '|0.0|' + str(AN_pos[5][1]) + '|' +
	           str(AN_pos[6][0]) + '|0.0|' + str(AN_pos[6][1]) + '|' +
	           str(AN_pos[7][0]) + '|0.0|' + str(AN_pos[7][1])
	           )

all_PO_pipe = (str(PO_pos[0][0]) + '|0.0|' + str(PO_pos[0][1]) + '|' + 
	           str(PO_pos[1][0]) + '|0.0|' + str(PO_pos[1][1]) + '|' +
	           str(PO_pos[2][0]) + '|0.0|' + str(PO_pos[2][1]) + '|' +
	           str(PO_pos[3][0]) + '|0.0|' + str(PO_pos[3][1]) + '|' +
	           str(PO_pos[4][0]) + '|0.0|' + str(PO_pos[4][1]) + '|' +
	           str(PO_pos[5][0]) + '|0.0|' + str(PO_pos[5][1]) + '|' +
	           str(PO_pos[6][0]) + '|0.0|' + str(PO_pos[6][1]) + '|' +
	           str(PO_pos[7][0]) + '|0.0|' + str(PO_pos[7][1])
	           )



block2 = block2_a + all_AN_pipe + ', ' + all_PO_pipe
#print(block2)

subType_EV_trials = EV_trials

print('length of AN_pos_txt', len(AN_pos_txt))
total_trials = AN_pos_txt * ((ratio + 1)*16)
print('length of total trials', len(total_trials))


AN_x = [AN_pos2[0], AN_pos3[0], AN_pos4[0], AN_pos5[0], AN_pos6[0], AN_pos7[0], AN_pos8[0], AN_pos1[0]]
AN_y = [AN_pos2[1], AN_pos3[1], AN_pos4[1], AN_pos5[1], AN_pos6[1], AN_pos7[1], AN_pos8[1], AN_pos1[1]]

PO_x = [PO_pos2[0], PO_pos3[0], PO_pos4[0], PO_pos5[0], PO_pos6[0], PO_pos7[0], PO_pos8[0], PO_pos1[0]]
PO_y = [PO_pos2[1], PO_pos3[1], PO_pos4[1], PO_pos5[1], PO_pos6[1], PO_pos7[1], PO_pos8[1], PO_pos1[1]]

all_z = [0.0]*8

