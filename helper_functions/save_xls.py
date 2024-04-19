import pandas as pd
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

