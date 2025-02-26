import pandas as pd
import numpy as np

def list_comp_help_keep(List1, List2):
    '''
    Helper function that just removes items in one list from another list. 
    List1 = your list of items that you want removed from List2
    List2 = list of all items you have 
    '''
    # removes items in one list from another list.

    lerp = [x for x in List2 if x not in List1]

    # returns your amended List2
    return lerp

def add_blank_rows(DataFrame, SubjectRange):
    '''
    Helper function that adds in blank subject rows (for people that didn't complete MRI  
    InFile = your csv file
    '''
    # read in your csv file
    
    cols = DataFrame.index.values.tolist()

    for i in cols:
        i = str(i)
    
    # removes duplicates
    blank_vals = list_comp_help_keep(cols, SubjectRange)

    a = np.empty((1,len(list(DataFrame.columns))))
    a[:] = np.nan
    
    for b in blank_vals:
        s2 = pd.Series(a[0], index=list(DataFrame.columns), name=b)
        DataFrame = DataFrame.append(s2)

    return DataFrame, blank_vals
