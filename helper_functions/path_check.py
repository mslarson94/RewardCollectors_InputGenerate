# Import Calls
from __future__ import division
import os


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
        
