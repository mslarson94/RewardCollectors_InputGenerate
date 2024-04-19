import pandas as pd
import numpy as np
import os
import sys
#sys.path.append(r"C:\Users\Myra\Dropbox\RemoteAnalyses\scripts\helper_functions") # Windows
sys.path.append(r"/Users/myralarson/Dropbox/RemoteAnalyses/scripts/helper_functions") # Mac
# Import the libraries from that folder
from path_check import *
from save_xls import *

###########################################################################################

def check_equality(Group1, Group2, OutPath):
    ''' 
    Simple function to check whether two dataframes are exactly the same.
    Group1    = Your first group of data 
    Group2    = Your second group of data
    OutPath   = Your desired out directory
    '''
    
    group1name = Group1.replace('.csv', '')
    group2name = Group2.replace('.csv', '')
    
    path_check(OutPath)
    
    df1= pd.read_csv(Group1, index_col=0)
    df2= pd.read_csv(Group2, index_col=0)
    df_check = pd.DataFrame()
    df_check['Subject'] = df1.index
    df1 = df1.apply(pd.to_numeric, errors='coerce')
    df2 = df2.apply(pd.to_numeric, errors='coerce')
    cols = list(df1.columns)
    for i in cols:
        df_check[i] = np.where(df1[i] == df2[i], 0, df1[i] - df2[i])
    df_check = df_check.set_index('Subject')
    outname = OutPath+'/equality_check.csv'
    df_check.to_csv(outname)

###########################################################################################

myra_df = r'/Users/myralarson/Dropbox/RemoteAnalyses/Round6_ravlt_mri_plotting/john_vs_myra_mdt/myra_all.csv'
john_df = r'/Users/myralarson/Dropbox/RemoteAnalyses/Round6_ravlt_mri_plotting/john_vs_myra_mdt/john_all_blanks.csv'
out = r'/Users/myralarson/Dropbox/RemoteAnalyses/Round6_ravlt_mri_plotting/john_vs_myra_mdt'

###########################################################################################


check_equality(myra_df, john_df, out)


    
    
