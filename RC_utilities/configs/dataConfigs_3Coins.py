'''
dataConfigs_3Coins.py
Created on March, 14 2024
@author: myra

Config file used to generate coinLoc & taskBlock files and in path analyses 
'''
from RC_utilities.helper_functions.path_check import *

###################################################
################## Config Section #################
###################################################

###################    Paths   ####################
outPath = '/Users/mairahmac/Desktop/RC_Data_Analysis/3Coins'
outFile_pre = 'CoinLocations'
path_check(outPath)
troubleshootingFolder = outPath + '/troubleshootingFiles'
path_check(troubleshootingFolder)

#################    Hardware   ####################
whichWifi = 'SuthanaLabResearch' # 'SuthanaLab' 'SuthanaLabResearch'
whichDevice_AN = 'A' # Possible Values (str): A,  G
whichDevice_PO = 'D' # Possible Values (str): D,  C

#################  Task Design  ####################
whichCoinSet = 'A'              # Possible Values (str): 'A', 'B', 'Conf', 'Dummy'
criterion = 3                   #criterion is the number of consecutive perfect rounds to hit in TP1 to reach TP2
overall_ratio = [2, 1, 1]       # ratio of total number of normal trials to a single EV type of trials. 
                                #       e.g. ratio = [2, 1, 1] is 2:1:1 for Normal:PPE:NPE
# Parameters
trial_ratio = [10, 5, 5]        # Ratio of Normal:PPE:NPE that totals to the segment size defined below
segment_count = 2               # Number of segments
segment_size = 20               # Number of trials in each segment


EV_types = ['PPE', 'NPE']       # total number of EV types (e.g. PPE, NPE)
EV_trials = 20                  # total number of trials for a single EV type
roleReversalTrials = 10         # total number of Role Reversal Trials 
roleReversal_EV = False         # Toggle true or false to allow for a replicated rate of EV trials for the role reveral phase 

## coin values
HV_pts = 10
LV_pts = 5
NV_pts = 0
PPE_pts = 20
NPE_pts = 0

# Which Djikstra Calculation? 
whichWeight = 'PureUnweighted' # PureUnweighted, PureWeighted, InitialPath, WeightedUnweighted, WeightedInitialPath


####################################################
################ Start Positions ###################
####################################################
# pos_ = [AN, PO]
pos1 = [(0.0, 5.0),     (5.0, 0.0)] 
pos2 = [(3.5, 3.5),     (3.5, -3.5)]
pos3 = [(5.0, 0.0),     (0.0, -5.0)]
pos4 = [(3.5, -3.5),    (-3.5, -3.5)]
pos5 = [(0.0, -5.0),    (-5.0, 0.0)]
pos6 = [(-3.5, -3.5),   (-3.5, 3.5)]
pos7 = [(-5.0, 0.0),    (0.0, 5.0)]
pos8 = [(-3.5, 3.5),    (3.5, 3.5)]
tutorial_pos = [(0, -5), (2, -5)]
dummy = [(0, 0), (0, 0)]

position_list = [pos1, pos2, pos3, pos4, pos5, pos6, pos7, pos8, tutorial_pos]
pos_strList= ['pos1', 'pos2', 'pos3', 'pos4', 'pos5', 'pos6', 'pos7', 'pos8', 'tutorial_pos']

AN_positions = list(list(zip(*position_list))[0])
PO_positions = list(list(zip(*position_list))[1])

####################################################
################# Tutorial Coins ###################
####################################################
tut_CoinSet = {
'HV':   {'coords': (-2,  -6),   'pts': 10, 'weight': 0},
'LV':   {'coords': (-1,  -6),   'pts': 5,  'weight': 8}, 
'NV':   {'coords': ( 0,  -6),   'pts': 0,  'weight': 20},
'PPE':  {'coords': ( 0,  -6),   'pts': 20, 'weight': 0},
'NPE':  {'coords': (-2,  -6),   'pts': 0,  'weight': 0}
}
tutorialStart_pos = tutorial_pos[0]
firstpos = 'tutorial'

####################################################
################### Coin Sets ######################
####################################################

############# Set A Coins ###############
if whichCoinSet == 'A_old':
        CoinSet = {
        'HV':   {'coords': ( 3.00,  -0.50),   'pts': 10,  'weight': 0},
        'LV':   {'coords': (-0.50,   2.50),   'pts': 5,   'weight': 10}, 
        'NV':   {'coords': (-2.50,  -2.70),   'pts': 0,   'weight': 30},
        'PPE':  {'coords': (-2.50,  -2.70),   'pts': 20,  'weight': 0},
        'NPE':  {'coords': ( 3.00,  -0.50),   'pts': 0,   'weight': 0}
        }
        actual_startPos = tuple(pos4[0])
        firstpos = 4

############# Set B Coins ###############
elif whichCoinSet == 'B_old':
        CoinSet = {
        'HV':   {'coords': ( 1.50,  -3.00),  'pts': 10,  'weight': 0},
        'LV':   {'coords': ( 2.00,   1.75),  'pts': 5,   'weight': 10}, 
        'NV':   {'coords': (-2.50,   2.50),  'pts': 0,   'weight': 30},
        'PPE':  {'coords': (-2.50,   2.50),  'pts': 20,  'weight': 0},
        'NPE':  {'coords': ( 1.50,  -3.00),  'pts': 0,   'weight': 0}
        }
        actual_startPos = tuple(pos1[0])
        firstpos = 1

############# Set A Coins ###############
elif whichCoinSet == 'A':
        CoinSet = {
        'HV':   {'coords': (1.36, -3.04),   'pts': 10,  'weight': 0},
        'LV':   {'coords': (-3.76, -0.1),   'pts': 5,   'weight': 10}, 
        'NV':   {'coords': (-0.57, 2.4),   'pts': 0,   'weight': 20},
        'PPE':  {'coords': (-0.57, 2.4),   'pts': 20,  'weight': 0},
        'NPE':  {'coords': (1.36, -3.04),   'pts': 0,   'weight': 0}
        }
        actual_startPos = tuple(pos2[0])
        firstpos = 2


############# Set B Coins ###############
elif whichCoinSet == 'B':
        CoinSet = {
        'HV':   {'coords': (2.5, 1.49),   'pts': 10,  'weight': 0},
        'LV':   {'coords': (-1.51, 2.71),   'pts': 5,   'weight': 10}, 
        'NV':   {'coords': (-1.4, -2.67),   'pts': 0,   'weight': 20},
        'PPE':  {'coords': (-1.4, -2.67),   'pts': 20,  'weight': 0},
        'NPE':  {'coords': (2.5, 1.49),   'pts': 0,   'weight': 0}
        }
        actual_startPos = tuple(pos5[0])
        firstpos = 5

############# Set C Coins ###############
elif whichCoinSet == 'C':
        CoinSet = {
        'HV':   {'coords': (-0.82, -2.91),   'pts': 10,  'weight': 0},
        'LV':   {'coords': (2.45, 1.17),   'pts': 5,   'weight': 10}, 
        'NV':   {'coords': (-2.39, 2.31),   'pts': 0,   'weight': 20},
        'PPE':  {'coords': (-2.39, 2.31),   'pts': 20,  'weight': 0},
        'NPE':  {'coords': (-0.82, -2.91),   'pts': 0,   'weight': 0}
        }
        actual_startPos= tuple(pos4[0])
        firstpos = 4


############# Set D Coins ###############
elif whichCoinSet == 'D':
        CoinSet = {
        'HV':   {'coords': (-2.32, 0.55),   'pts': 10,  'weight': 0},
        'LV':   {'coords': (1.89, -2.52),   'pts': 5,   'weight': 10}, 
        'NV':   {'coords': (1.99, 2.23),   'pts': 0,   'weight': 20},
        'PPE':  {'coords': (1.99, 2.23),   'pts': 20,  'weight': 0},
        'NPE':  {'coords': (-2.32, 0.55),   'pts': 0,   'weight': 0}
        }
        actual_startPos = tuple(pos6[0])
        firstpos = 6

# ############# Dummy Coin Set ###############
# elif whichCoinSet == 'C':
#         CoinSet = {
#         'HV':   {'coords': (1, 0),   'pts': 10,  'weight': 0},
#         'LV':   {'coords': (2, 0),   'pts': 5,   'weight': 10}, 
#         'NV':   {'coords': (3, 0),   'pts': 0,   'weight': 20},
#         'PPE':  {'coords': (3, 0),   'pts': 20,  'weight': 0},
#         'NPE':  {'coords': (1, 0),   'pts': 0,   'weight': 0}
#         }
#         actual_startPos= tuple(pos1[0])
#         firstpos = 1


# ############# Conference Room Coin Set ###############
# elif whichCoinSet == 'D':
#         CoinSet = {
#         'HV':   {'coords': (0.5, 1),   'pts': 10,  'weight': 0},
#         'LV':   {'coords': (0.5, 2),   'pts': 5,   'weight': 10}, 
#         'NV':   {'coords': (0.5, 3),   'pts': 0,   'weight': 20},
#         'PPE':  {'coords': (0.5, 3),   'pts': 20,  'weight': 0},
#         'NPE':  {'coords': (0.5, 1),   'pts': 0,   'weight': 0}
#         }
#         actual_startPos = tuple(pos1[0])
#         firstpos = 1



collectionOrder_List = [CoinSet['LV']['coords'], CoinSet['NV']['coords'], CoinSet['HV']['coords']]
collectionOrder_List_str = ['LV', 'NV', 'HV']
####################################################
################## IP Addresses ####################
####################################################

ipAddress_AN = ''
ipAddress_PO = ''

###################    AN    #######################
if whichDevice_AN == 'G':
        if whichWifi == "SuthanaLabResearch":
                ipAddress_AN = '192.168.50.128'
        elif whichWifi == 'SuthanaLab':
                ipAddress_AN = '192.168.1.XX'
elif whichDevice_AN == 'A':
        if whichWifi == 'SuthanaLabResearch':
                ipAddress_AN = '192.168.50.109'
        elif whichWifi == 'SuthanaLab': 
                ipAddress_AN = '192.168.1.84'

###################    PO    #######################

if whichDevice_PO == 'D':
        if whichWifi == 'SuthanaLabResearch':
                ipAddress_PO = '192.168.50.127'
        elif whichWifi == 'SuthanaLab': 
                ipAddress_PO = '192.168.1.146'
elif whichDevice_PO == 'C':
        if whichWifi == "SuthanaLabResearch":
                ipAddress_PO = '192.168.50.156'
        elif whichWifi == 'SuthanaLab': 
                ipAddress_PO = '192.168.50.XX'
