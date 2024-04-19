import glob, os
import fnmatch
def glob_help(InPath, Ending):
    return_list = []
    for filename in fnmatch.filter( glob.glob(InPath), ('*' + Ending)):
        return_list.append(filename)
    return return_list

specific_sample = 
