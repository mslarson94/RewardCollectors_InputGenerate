'''
CoinLocations_generator.py
Created on March, 14 2024
@author: myra

generate CoinLocations.csv files from given x,y,z coordinates & which Magic Leap Devices are being used

'''
#from .coinLoc_generator_test_cfg.py import *
import sys
import os
import csv
import pandas as pd 
import numpy as np
from RC_utilities.configs.dataConfigs_3Coins import * # Import from the package

## Outpaths & Out File Names
outFile = outPath + '/' + outFile_pre + '_' + whichCoinSet + '.csv'
outFile_type = troubleshootingFolder + '/' + outFile_pre + '_Annotated_' + whichCoinSet + '.csv'

header_string = 'coinsetID,coinposx,coinposy,coinposz,originalreward,reward,swapsound,' + ipAddress_AN + '|' + ipAddress_PO + '\n'
header_string_type = 'Coin Set ID, X, Y, Z, ActualValue, ExpectedValue, Orig(1) or Special (2), IP Address AN|IP Address PO \n' + header_string

with open(outFile, 'w') as file:
        file.write(header_string )
with open(outFile_type, 'w') as file2:
        file2.write(header_string_type )

## All Coins Dataframe
x_vals = [list(CoinSet['HV']['coords'])[0], list(CoinSet['LV']['coords'])[0], list(CoinSet['NV']['coords'])[0]]
y_vals = [0.0]*3
z_vals = [list(CoinSet['HV']['coords'])[1], list(CoinSet['LV']['coords'])[1], list(CoinSet['NV']['coords'])[1]]

tutorial_x_vals = [list(tut_CoinSet['HV']['coords'])[0], list(tut_CoinSet['LV']['coords'])[0], list(tut_CoinSet['NV']['coords'])[0]]

tutorial_z_vals = [list(tut_CoinSet['HV']['coords'])[1], list(tut_CoinSet['LV']['coords'])[1], list(tut_CoinSet['NV']['coords'])[1]]


tut2_x_vals = [list(tut_CoinSet['NPE']['coords'])[0], list(tut_CoinSet['LV']['coords'])[0], list(tut_CoinSet['PPE']['coords'])[0]]
tut2_z_vals = [list(tut_CoinSet['NPE']['coords'])[1], list(tut_CoinSet['LV']['coords'])[1], list(tut_CoinSet['PPE']['coords'])[1]]


## Orig Values
normal_vals  = [HV_pts, LV_pts, NV_pts]
collectionOrder_vals = [LV_pts, NV_pts, HV_pts]
PPE_vals = [HV_pts, LV_pts, PPE_pts]
NPE_vals = [NPE_pts, LV_pts, NV_pts]
tut2_vals = [NPE_pts, LV_pts, PPE_pts]

collectionOrder_x = [list(CoinSet['LV']['coords'])[0], list(CoinSet['NV']['coords'])[0], list(CoinSet['HV']['coords'])[0]]
collectionOrder_z = [list(CoinSet['LV']['coords'])[1], list(CoinSet['NV']['coords'])[1], list(CoinSet['HV']['coords'])[1]]

PPE_x_vals = list(CoinSet['HV']['coords'])[0], list(CoinSet['LV']['coords'])[0], list(CoinSet['PPE']['coords'])[0]
PPE_z_vals = list(CoinSet['HV']['coords'])[1], list(CoinSet['LV']['coords'])[1], list(CoinSet['PPE']['coords'])[1]


NPE_x_vals = list(CoinSet['NPE']['coords'])[0], list(CoinSet['LV']['coords'])[0], list(CoinSet['NV']['coords'])[0]
NPE_z_vals = list(CoinSet['NPE']['coords'])[1], list(CoinSet['LV']['coords'])[1], list(CoinSet['NV']['coords'])[1]

print(collectionOrder_List[0])

#print(collectionOrder_vals)

## Orig Values
normal_vals  = [10, 5, 0]

## Normal
normal = {'coinsetID':[0]*3, 
        'coinposx':x_vals, 
        'coinposy': y_vals,
        'coinposz': z_vals,
        'originalreward': normal_vals,
        'reward': normal_vals,
        'roundType': ['normal']*3,
        'swapsound': [1]*3
       } 

normal = pd.DataFrame(normal) 
  
print(normal) 

#print('#'*15)

############## Collection Order ############

collectionOrder = {'coinsetID':[1]*3, 
        'coinposx':collectionOrder_x, 
        'coinposy': y_vals,
        'coinposz': collectionOrder_z,
        'originalreward': collectionOrder_vals,
        'reward': collectionOrder_vals,
        'roundType': ['collectionOrder']*3,
        'swapsound': [1]*3
       } 

collectionOrder = pd.DataFrame(collectionOrder) 
############## Tutorial ###################
tutorial = {'coinsetID':[4]*3, 
        'coinposx':tutorial_x_vals, 
        'coinposy': y_vals,
        'coinposz': tutorial_z_vals,
        'originalreward': normal_vals,
        'reward': normal_vals,
        'roundType': ['tutorial']*3,
        'swapsound': [1]*3
       } 
tutorial = pd.DataFrame(tutorial) 

############## Tutorial ###################
tutorial_TP2 = {'coinsetID':[5]*3, 
        'coinposx':tut2_x_vals, 
        'coinposy': y_vals,
        'coinposz': tut2_z_vals,
        'originalreward': normal_vals,
        'reward': tut2_vals,
        'roundType': ['tutorial_TP2']*3,
        'swapsound': [2, 1, 2]
       } 
tutorial_TP2 = pd.DataFrame(tutorial_TP2) 

############## PPE Swaps ################

PPE = {'coinsetID':[2]*3, 
        'coinposx':PPE_x_vals, 
        'coinposy': y_vals,
        'coinposz': PPE_z_vals,
        'originalreward': normal_vals,
        'reward': PPE_vals,
        'roundType': ['PPE']*3,
        'swapsound': [1, 1, 2]
        } 

PPE = pd.DataFrame(PPE) 
############## NPE Swaps ################

NPE = {'coinsetID':[3]*3, 
        'coinposx':NPE_x_vals, 
        'coinposy': y_vals,
        'coinposz': NPE_z_vals,
        'originalreward': normal_vals,
        'reward': NPE_vals,
        'roundType': ['NPE']*3,
        'swapsound': [2, 1 , 1]
       } 


NPE = pd.DataFrame(NPE) 

############# Concatenating the dataframes ###############

AllCoinLocs_withType = pd.concat([normal, collectionOrder, 
                                 PPE, NPE, 
                                 tutorial, tutorial_TP2], axis=0)

AllCoinLocs_withType.to_csv(outFile_type, mode='a', index=False)

AllCoinLocs = AllCoinLocs_withType.drop('roundType', axis=1)
AllCoinLocs.to_csv(outFile, mode='a', index=False, header=False)

with open(outFile, 'r+') as f:
    f.seek(0, 2)  # Move to the end of the file
    size = f.tell()
    f.truncate(size - 1)  # Remove the last character if it's a newline

with open(outFile_type, 'r+') as f2:
    f2.seek(0, 2)  # Move to the end of the file
    size = f2.tell()
    f2.truncate(size - 1)  # Remove the last character if it's a newline