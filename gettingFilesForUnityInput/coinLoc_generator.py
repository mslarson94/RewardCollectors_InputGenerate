'''
CoinLocations_generator.py
Created on March, 14 2024
@author: myra

generate CoinLocations.csv files from given x,y,z coordinates & which Magic Leap Devices are being used

'''
#from .coinLoc_generator_test_cfg.py import *
from dataConfigs import *
import csv
import pandas as pd 
import numpy as np

## Outpaths & Out File Names
outFile = outPath + '/' + outFile_pre + '_' + whichCoinSet + '.csv'
outFile_type = troubleshootingFolder + '/' + outFile_pre + '_Annotated_' + whichCoinSet + '.csv'

## Which Devices & IP Addresses
ipAddress_AN = ''
ipAddress_PO = ''

# AN
if whichDevice_AN == 'A':
        ipAddress_AN = '192.168.50.109'
elif whichDevice_AN == 'E':
        ipAddress_AN = '192.168.50.144'

# PO
if whichDevice_PO == 'D':
        ipAddress_PO = '192.168.50.127'
elif whichDevice_PO == 'B':
         ipAddress_PO = '192.168.50.133'
elif whichDevice_PO == 'C':
        ipAddress_PO = '192.168.50.'

header_string = ipAddress_AN + '|' + ipAddress_PO
header_string_type = 'IP Address AN | IP Address PO \n' + header_string

with open(outFile, 'w') as file:
        file.write(header_string + '\n')
with open(outFile_type, 'w') as file2:
        file2.write(header_string_type + '\n')

## All Coins Dataframe
x_vals = [HV_1[0], HV_2[0], LV_1[0], LV_2[0], NV_1[0], NV_2[0]]
y_vals = [0.0]*6
z_vals = [HV_1[1], HV_2[1], LV_1[1], LV_2[1], NV_1[1], NV_2[1]]

tutorial_x_vals = [tutorial_1[0], tutorial_2[0],tutorial_3[0]]
tutorial_y_vals = [tutorial_1[1], tutorial_2[1], tutorial_3[1]]
tutorial_vals = [tutorial_1[2], tutorial_2[2], tutorial_3[2]]

## Orig Values
normal_vals  = [10.0, 10.0, 5.0, 5.0, 0.0, 0.0]

print(collectionOrder_List[0])
## Collection Values
collectionOrder_vals = [collectionOrder_List[0][2], collectionOrder_List[1][2], 
                        collectionOrder_List[2][2], collectionOrder_List[3][2],
                        collectionOrder_List[4][2], collectionOrder_List[5][2]]

#print(collectionOrder_vals)

## Coin Values 
normal_vals  = [10.0, 10.0, 5.0, 5.0, 0.0, 0.0]

#HV-NV swaps
HV1_NV1_vals = [0.0, 10.0, 5.0, 5.0, 10.0, 0.0]
HV1_NV2_vals = [0.0, 10.0, 5.0, 5.0, 0.0, 10.0]
HV2_NV1_vals = [10.0, 0.0, 5.0, 5.0, 10.0, 0.0]
HV2_NV2_vals = [10.0, 0.0, 5.0, 5.0, 0.0, 10.0]

## Normal
normal = {'Set':[0]*6, 
        'x':x_vals, 
        'y': y_vals,
        'z': z_vals,
        'value': normal_vals,
        'roundType': ['normal']*6
       } 

normal = pd.DataFrame(normal) 
  
print(normal) 

#print('#'*15)

############## Collection Order ############
collectionOrder_x = [collectionOrder_List[0][0], collectionOrder_List[1][0], 
                     collectionOrder_List[2][0], collectionOrder_List[3][0],
                     collectionOrder_List[4][0], collectionOrder_List[5][0]]

collectionOrder_z = [collectionOrder_List[0][1], collectionOrder_List[1][1], 
                     collectionOrder_List[2][1], collectionOrder_List[3][1],
                     collectionOrder_List[4][1], collectionOrder_List[5][1]]


collectionOrder_vals = [collectionOrder_List[0][2], collectionOrder_List[1][2], 
                        collectionOrder_List[2][2], collectionOrder_List[3][2],
                        collectionOrder_List[4][2], collectionOrder_List[5][2]]

collectionOrder = {'Set':[1]*6, 
        'x':collectionOrder_x, 
        'y': y_vals,
        'z': collectionOrder_z,
        'value': collectionOrder_vals,
        'roundType': ['collectionOrder']*6
       } 

collectionOrder = pd.DataFrame(collectionOrder) 
############## Tutorial ###################
tutorial = {'Set':[6]*3, 
        'x':tutorial_x_vals, 
        'y': [0.0]*3,
        'z': tutorial_y_vals,
        'value': tutorial_vals,
        'roundType': ['tutorial']*3
       } 
tutorial = pd.DataFrame(tutorial) 
############## HV NV Swaps ################
## HV1_NV1
HV1_NV1 = {'Set':[2]*6, 
        'x':x_vals, 
        'y': y_vals,
        'z': z_vals,
        'value': HV1_NV1_vals,
        'roundType': ['HV1_NV1']*6
       } 

HV1_NV1 = pd.DataFrame(HV1_NV1) 

## HV1_NV2
HV1_NV2 = {'Set':[3]*6, 
        'x':x_vals, 
        'y': y_vals,
        'z': z_vals,
        'value': HV1_NV2_vals,
        'roundType': ['HV1_NV2']*6
       } 

HV1_NV2 = pd.DataFrame(HV1_NV2) 

## HV2_NV1
HV2_NV1 = {'Set':[4]*6, 
        'x':x_vals, 
        'y': y_vals,
        'z': z_vals,
        'value': HV2_NV1_vals,
        'roundType': ['HV2_NV1']*6
       } 

HV2_NV1 = pd.DataFrame(HV2_NV1)

## HV2_NV2
HV2_NV2 = {'Set':[5]*6, 
        'x':x_vals, 
        'y': y_vals,
        'z': z_vals,
        'value': HV2_NV2_vals,
        'roundType': ['HV2_NV2']*6
       } 

HV2_NV2 = pd.DataFrame(HV2_NV2)

############# Concatenating the dataframes ###############

AllCoinLocs_withType = pd.concat([normal, collectionOrder, 
                                 HV1_NV1, HV1_NV2, HV2_NV1, HV2_NV2, 
                                 tutorial], axis=0)

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