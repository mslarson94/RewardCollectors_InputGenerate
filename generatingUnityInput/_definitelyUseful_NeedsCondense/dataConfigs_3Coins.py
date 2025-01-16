'''
dataConfigs_3Coins.py
Created on March, 14 2024
@author: myra

Config file used to generate coinLoc & taskBlock files and in path analyses 
'''
from helper_functions.path_check import path_check

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
whichWifi = 'SuthanaLab' # 'SuthanaLab' 'SuthanaLabResearch'
whichDevice_AN = 'F' # Possible Values (str): A, E, F?
whichDevice_PO = 'D' # Possible Values (str): D, B, G?

#################  Task Design  ####################
whichCoinSet = 'Conf'      # Possible Values (str): 'A', 'B', 'Conf', 'Dummy'
criterion = 3           #criterion is the number of consecutive perfect rounds to hit in TP1 to reach TP2
ratio = 2               # ratio of total number of normal trials to a single EV type of trials. 
                        #       e.g. ratio = 3 is 3:1:1 for Normal:PPE:NPE
EV_types = ['PPE', 'NPE']            # total number of EV types (e.g. PPE, NPE)
EV_trials = 20          # total number of trials for a single EV type
roleReversalTrials = 20

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
'HV':   ((-2,  -6),  10),
'LV':   ((-1,  -6),   5), 
'NV':   (( 0,  -6),   0),
'PPE':  (( 0,  -6),  20),
'NPE':  ((-2,  -6),   0)
}
tutorialStart_pos = tutorial_pos[0]
firstpos = 'tutorial'

####################################################
################### Coin Sets ######################
####################################################

############# Set A Coins ###############
if whichCoinSet == 'A':
        CoinSet = {
        'HV':   (( 3.00,  -0.50),  10),
        'LV':   ((-0.50,   2.50),   5), 
        'NV':   ((-2.50,  -2.70),   0),
        'PPE':  ((-2.50,  -2.70),  20),
        'NPE':  (( 3.00,  -0.50),   0)
        }
        actual_startPos = tuple(pos4[0])
        firstpos = 4

############# Set B Coins ###############
elif whichCoinSet == 'B':
        CoinSet = {
        'HV':   (( 1.50,  -3.00),  10),
        'LV':   (( 2.00,   1.75),   5), 
        'NV':   ((-2.50,   2.50),   0),
        'PPE':  ((-2.50,   2.50),  20),
        'NPE':  (( 1.50,  -3.00),   0)
        }
        actual_startPos = tuple(pos1[0])
        firstpos = 1

############# Dummy Coin Set ###############
elif whichCoinSet == 'Dummy':
        CoinSet = {
        'HV':   ((1, 0),  10),
        'LV':   ((2, 0),   5), 
        'NV':   ((3, 0),   0),
        'PPE':  ((3, 0),  20),
        'NPE':  ((1, 0),   0)
        }
        dummy_startPos = tuple(dummy[0])
        firstpos = 'dummy'


############# Conference Room Coin Set ###############
elif whichCoinSet == 'Conf':
        CoinSet = {
        'HV':   ((0.5, 1),  10),
        'LV':   ((0.5, 2),   5), 
        'NV':   ((0.5, 3),   0),
        'PPE':  ((0.5, 3),  20),
        'NPE':  ((0.5, 1),   0)
        }
        conf_startPos = (0,0)
        firstpos = 'conf'



collectionOrder_List = [CoinSet['LV'][0], CoinSet['NV'][0], CoinSet['HV'][0]]
collectionOrder_List_str = ['LV', 'NV', 'HV']
####################################################
################## IP Addresses ####################
####################################################

ipAddress_AN = ''
ipAddress_PO = ''

###################    AN    #######################

if whichDevice_AN == 'A':
        if whichWifi == 'SuthanaLabResearch':
                ipAddress_AN = '192.168.50.109'
        elif whichWifi == 'SuthanaLab': 
                ipAddress_AN = '192.168.1.84'
elif whichDevice_AN == 'F':
        if whichWifi == "SuthanaLabResearch":
                ipAddress_AN = '192.168.50.XX'
        elif whichWifi == 'SuthanaLab':
                ipAddress_AN = '192.168.1.218'
                print("hey y'all")

###################    PO    #######################

if whichDevice_PO == 'D':
        if whichWifi == 'SuthanaLabResearch':
                ipAddress_PO = '192.168.50.127'
        elif whichWifi == 'SuthanaLab': 
                ipAddress_PO = '192.168.1.146'

elif whichDevice_PO == 'B':
        if whichWifi == "SuthanaLabResearch":
                ipAddress_PO = '192.168.50.133'
        elif whichWifi == 'SuthanaLab': 
                ipAddress_PO = '192.168.1.XX'
elif whichDevice_PO == 'C':
        if whichWifi == "SuthanaLabResearch":
                ipAddress_PO = '192.168.50.XX'
        elif whichWifi == 'SuthanaLab': 
                ipAddress_PO = '192.168.50.XX'
