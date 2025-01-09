'''
dataConfigs.py
Created on March, 14 2024
@author: myra

generate CoinLocations.csv files from given x,y,z coordinates
'''
from helper_functions.path_check import path_check
########### Files & Coin Set ###### 
whichDevice_AN = 'A' # Possible Values (str): A, E, F?
whichDevice_PO = 'D' # Possible Values (str): D, B, C?

whichCoinSet = 'A' # Possible Values (str): A, B, C, C
outPath = '/Users/mairahmac/Desktop/RC_Data_Analysis/task_test'
outFile_pre = 'CoinLocations'
path_check(outPath)
troubleshootingFolder = outPath + '/troubleshootingFiles'
path_check(troubleshootingFolder)


#criterion is the number of consecutive perfect rounds to hit in TP1 to reach TP2
criterion = 3

# ratio of total number of normal trials to EV trials. e.g. ratio = 3 is 3:1 normal:EV 
ratio = 2

# total number of trials for a EV sub-swap type e.g. swap type = HV-NV, sub-swap types are : HV1-NV1, HV2-NV2, HV1-NV2, HV2-NV1
EV_trials = 20

EV_types = 2
# Which Djikstra Calculation? 
whichWeight = 'PureUnweighted' # PureUnweighted, PureWeighted, InitialPath, WeightedUnweighted, WeightedInitialPath
############# Start Positions ###########
# pos_ = [AN, PO]
pos1 = [(0.0, 5.0),     (4.25, 1.75)] 
pos2 = [(3.5, 3.5),     (4.25, -1.75)]
pos3 = [(5.0, 0.0),     (1.75, -4.25)]
pos4 = [(3.5, -3.5),    (-1.75, -4.25)]
pos5 = [(0.0, -5.0),    (-4.25, -1.75)]
pos6 = [(-3.5, -3.5),   (-4.25, 1.75)]
pos7 = [(-5.0, 0.0),    (-1.75, 4.25)]
pos8 = [(-3.5, 3.5),    (1.75, 4.25)]

tutorial_pos = [[0, -5], [2, -5]]

position_list = [pos1, pos2, pos3, pos4, pos5, pos6, pos7, pos8, tutorial_pos]
pos_strList= ['pos1', 'pos2', 'pos3', 'pos4', 'pos5', 'pos6', 'pos7', 'pos8', 'tutorial_pos']

AN_positions = list(list(zip(*position_list))[0])
PO_positions = list(list(zip(*position_list))[1])

############# Tutorial Coins ############
tutorial_1 = [-2.0, -6.0, 0.0]
tutorial_2 = [-1.0, -6.0, 5.0]
tutorial_3 = [0.0, -6.0, 10.0]

if whichCoinSet == 'A':
        ############# Set A Coins ###############
        # [x, z, orig value]

        #High Value
        HV_1 = [-2.5,1.9, 10.0]
        HV_2=  [1.8,-3.1, 10.0]

        #Low Value
        LV_1 = [2.8,1.2, 5.0]
        LV_2 = [-2.1,-2.2, 5.0]

        #Null Value
        NV_1 = [0.75, 3.0, 0.0]
        NV_2 = [0.2, -1.3, 0.0]

        #High Value coords
        HV_1_coor = (-2.5,1.9)
        HV_2_coor=  (1.8,-3.1)

        #Low Value coords
        LV_1_coor = (2.8,1.2)
        LV_2_coor = (-2.1,-2.2)

        #Null Value coords
        NV_1_coor = (0.75, 3.0)
        NV_2_coor = (0.2, -1.3)

        collectionOrder_List = [LV_1, HV_1, NV_2, LV_2, HV_2, NV_1]
        collectionOrder_List_str = ['LV_1', 'HV_1', 'NV_2', 'LV_2', 'HV_2', 'NV_1']
        actual_startPos = tuple(pos2[0])
        firstpos = 2


elif whichCoinSet == 'B':
        ############# Set B Coins ###############
        # [x, z, orig value]

        #High Value
        HV_1 = [2.0, 2.0, 10.0]
        HV_2=  [-1.5, -2.7, 10.0]

        #Low Value
        LV_1 = [-0.4, 3.1, 5.0]
        LV_2 = [3.0, -1.5, 5.0]


        #Null Value
        NV_1 = [-2.7, 1.3, 0.0]
        NV_2 = [-0.5, -0.5, 0.0]
        collectionOrder_List = [LV_1, NV_2, HV_1, NV_1, HV_2, LV_2]
        collectionOrder_List_str = ['LV_1', 'NV_2', 'HV_1', 'NV_1', 'HV_2', 'LV_2']
        actual_startPos = tuple(pos6[0])
        firstpos = 6

elif whichCoinSet == 'C':
        ############# Set C Coins ###############
        # [x, z, orig value]

        #High Value
        HV_1 = [-2.8, -1.2, 10.0] 
        HV_2 = [3.0, 0.5, 10.0]

        #Low Value
        LV_1 = [-1.6, 1.8, 5.0] 
        LV_2 = [1.5, -2.5, 5.0] 

        #Null Value
        NV_1 = [1.75, 2.8, 0.0]
        NV_2 = [0.5, -0.5, 0.0]
        collectionOrder_List = [LV_1, NV_2, HV_1, NV_1, HV_2, LV_2]
        collectionOrder_List_str = ['LV_1', 'NV_2', 'HV_1', 'NV_1', 'HV_2', 'LV_2']
        actual_startPos = tuple(pos7[0])
        firstpos = 7

elif whichCoinSet == 'D':
        ############# Set D Coins ###############
        # [x, z, orig value]

        #High Value
        HV_1 = [2.9, 1.5, 10.0]
        HV_2=  [-2.7, -2.0, 10.0]

        #Low Value
        LV_1 = [-2.0, 2.5, 5.0]
        LV_2 = [1.2, -1.0, 5.0]

        #Null Value
        NV_1 = [0.75, 1.0, 0.0]
        NV_2 = [2.3, -2.5, 0.0]
        collectionOrder_List = [LV_1, NV_1, HV_1, NV_2, HV_2, LV_2]
        collectionOrder_List_str = ['LV_1', 'NV_1', 'HV_1', 'NV_2', 'HV_2', 'LV_2']
        actual_startPos = tuple(pos7[0])
        firstpos = 7

elif whichCoinSet == 'E':
        ############# Blank Coin Set ###############
        # [x, z, orig value]

        #High Value
        HV_1 = [-2, 2.5, 10.0]
        HV_2=  [0.2, -3.3, 10.0]

        #Low Value
        LV_1 = [2.9, 1.5, 5.0]
        LV_2 = [-3.4, -0.5, 5.0]

        #Null Value
        NV_1 = [0.8, 3.1, 0.0]
        NV_2 = [3.2, -1.6, 0.0]
        collectionOrder_List = [HV_1, LV_2, NV_2, LV_1, HV_2, NV_1]
        collectionOrder_List_str = ['HV_1', 'LV_2', 'NV_2', 'LV_1', 'HV_2', 'NV_1']
        actual_startPos = tuple(pos6[0])
        firstpos = 6

elif whichCoinSet == 'Dummy':
        ############# Blank Coin Set ###############
        # [x, z, orig value]

        #High Value
        HV_1 = [1.0, 1.0, 10.0]
        HV_2=  [1.0, -1.0, 10.0]

        #Low Value
        LV_1 = [0.0, 1.0, 5.0]
        LV_2 = [0.0, -1.0, 5.0]

        #Null Value
        NV_1 = [-1.0, 1.0, 0.0]
        NV_2 = [-1.0, -1.0, 0.0]
        collectionOrder_List = [NV_1, NV_2, LV_1, LV_2, HV_1, HV_2]
        collectionOrder_List_str = ['NV_1', 'NV_2', 'LV_1', 'LV_2', 'HV_1', 'HV_2']
        actual_startPos = tuple(pos1[0])
        firstpos = 1

# elif whichCoinSet == 'Blank':
#         ############# Blank Coin Set ###############
#         # [x, z, orig value]

#         #High Value
#         HV_1 = [, 10.0]
#         HV_2=  [, 10.0]

#         #Low Value
#         LV_1 = [, 5.0]
#         LV_2 = [, 5.0]

#         #Null Value
#         NV_1 = [, 0.0]
#         NV_2 = [, 0.0]
#         collectionOrder_List = []
#         collectionOrder_List_str = []
#         actual_startPos = tuple(posX[0])

