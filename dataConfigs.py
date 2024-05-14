'''
dataConfigs.py
Created on March, 14 2024
@author: myra

generate CoinLocations.csv files from given x,y,z coordinates
'''
########### Files & Coin Set ###### 
whichCoinSet = 'B' # Possible Values (str): A, B, C, D
outPath = '/Users/mairahmac/Desktop/task_test'
outFile_pre = 'CoinLocations'


############# Start Positions ###########
# pos_ = [AN, PO]
pos1 = [[0.0, 5.0],     [1.75, 4.25]] 
pos2 = [[3.5, 3.5],     [4.25, 1.75]]
pos3 = [[5.0, 0.0],     [4.25, -1.75]]
pos4 = [[3.5, -3.5],    [1.75, -4.25]]
pos5 = [[0.0, -5.0],    [-1.75, -4.25]]
pos6 = [[-3.5, -3.5],   [-4.25, -1.75]]
pos7 = [[-5.0, 0.0],    [-4.25, 1.75]]
pos8 = [[-3.5, 3.5],    [-1.75, 4.25]]

tutorial_pos = [[5, 1], [5, -1]]


############# Tutorial Coins ############
tutorial_1 = [-2.0, -6.0, 0.0]
tutorial_2 = [-1.0, -6.0, 2.0]
tutorial_3 = [0.0, -6.0, 10.0]

if whichCoinSet == 'A':
        ############# Set A Coins ###############
        # [x, z, orig value]

        #High Value
        HV_1 = [-2.5,1.9, 10.0]
        HV_2=  [1.8,-3.1, 10.0]

        #Low Value
        LV_1 = [2.8,1.2, 2.0]
        LV_2 = [-2.1,-2.2, 2.0]

        #Null Value
        NV_1 = [0.75, 3.0, 0.0]
        NV_2 = [0.2, -1.3, 0.0]

        collectionOrder_List = [HV_2, NV_1, LV_2, NV_2, LV_1, HV_1]

elif whichCoinSet == 'B':
        ############# Set B Coins ###############
        # [x, z, orig value]

        #High Value
        HV_1 = [2.0, 2.0, 10.0]
        HV_2=  [-1.5, -2.7, 10.0]

        #Low Value
        LV_1 = [-0.4, 3.1, 2.0]
        LV_2 = [0.5, -0.5, 2.0]


        #Null Value
        NV_1 = [-2.7, 1.3, 0.0]
        NV_2 = [3.0, -1.5, 0.0]
        collectionOrder_List = [HV_2, NV_1, LV_2, NV_2, LV_1, HV_1]

elif whichCoinSet == 'C':
        ############# Set C Coins ###############
        # [x, z, orig value]

        #High Value
        HV_1 = [-2.8, -1.2, 10.0] 
        HV_2 = [3.0, 0.5, 10.0]

        #Low Value
        LV_1 = [-1.6, 1.8, 2.0] 
        LV_2 = [1.5, -2.5, 2.0] 

        #Null Value
        NV_1 = [1.75, 2.8, 0.0]
        NV_2 = [0.5, -0.5, 0.0]
        collectionOrder_List = [HV_2, NV_1, LV_2, NV_2, LV_1, HV_1]

elif whichCoinSet == 'D':
        ############# Set D Coins ###############
        # [x, z, orig value]

        #High Value
        HV_1 = [2.9, 1.5, 10.0]
        HV_2=  [-2.7, -2.0, 10.0]

        #Low Value
        LV_1 = [-2.0, 2.5, 2.0]
        LV_2 = [1.2, -1.0, 2.0]

        #Null Value
        NV_1 = [0.75, 1.0, 0.0]
        NV_2 = [2.3, -2.5, 0.0]
        collectionOrder_List = [HV_2, NV_1, LV_2, NV_2, LV_1, HV_1]
# elif whichCoinSet == 'Blank':
#         ############# Blank Coin Set ###############
#         # [x, z, orig value]

#         #High Value
#         HV_1 = [, 10.0]
#         HV_2=  [, 10.0]

#         #Low Value
#         LV_1 = [, 2.0]
#         LV_2 = [, 2.0]

#         #Null Value
#         NV_1 = [, 0.0]
#         NV_2 = [, 0.0]
#         collectionOrder_List = []
