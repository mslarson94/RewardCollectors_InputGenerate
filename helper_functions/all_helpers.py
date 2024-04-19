# Import Calls
from __future__ import division
import os
import numpy as np
from numpy import trapz
import pandas as pd
from datetime import date
# ExcelWriter has the highest chance of deprecation of these calls. It is only called in function 'save_xls'
from pandas import ExcelWriter


def save_xls(dict_df, path):
    """
    Helper function that saves a dictionary of dataframes to an excel file, with each dataframe as a seperate page
    dict_df = your dictionary of dataframes, Keys are the names, Values are the dataframes
    path = your desired outpath/file name. 
    """

    writer = ExcelWriter(path)
    for key in dict_df:
        dict_df[key].to_excel(writer, key)

    writer.save()

def disclaimer():
    '''
    Helper function that returns a disclaimer text string. 
    '''
    disclaimer ='''
    ********************* DISCLAIMER ****************************
    These analyses are ***not*** quality checked by this script.
    YOU, the experimenter, will need to go through this file and
    determine whether and how you will be excluding subjects from
    your analyses. Some suggestions:
        - A negative d’ Foil value indicates sampling error or
          response confusion (responding ‘New’ when intending to
          respond ‘Old’) at the easiest level of discrimination
          (Target from Novel Foil)
        - A d’ Foil value of 0 indicates inability to distinguish
          signals from noise at the easiest level of discrimination
          (Target from Novel Foil)
        - Values of -1 in criterion Foil indicate complete bias
          toward responding ‘New’ to both Target & Foil stimuli.
        - Conversely, values of 1 in criterion Foil indicate
          complete bias towards responding ‘Old’ to both Target
          & Foil stimuli.
        - Additionally, one could set a total test response rate
          threshold (for example: exclude less response rates of
          less than 75% of all presented stimuli)
    An additional note:
        For all Ratcliff-Diffusion model measures, you ***must***
        determine whether the subject's fingers were given clear
        instructions to keep their fingers on their response keys
        throughout the entire task. For all tasks administered on
        version 1.1 or later, you are guaranteed to have those
        instructions + practice tests administered as a part of the
        task. For all tasks administered on version 1.0, see
        your experiment documentation for further information.
    **************************************************************
    '''
    return disclaimer 
def path_check(Path):
    ''' Simple function to check whether a path
    exists or not. If not, will create that
    directory
    Path: path string.
    '''
    if not os.path.exists(Path):
        os.mkdir(Path)
        print("Directory " , Path ,  " Created ")
    else:    
        print("Directory " , Path ,  " already exists")
        
def list_str(item_list):
    """
    Helper function that converts lists into a single string
    item_list = your list that you want converted into strings separated by commas
    """
    return_str = ''
    if len(item_list) > 1:
        for item in item_list:
            return_str +=  str(item) + ", "
        return_str = return_str[:-2]
    else:
        return_str = str(item_list[0])
    return return_str

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
