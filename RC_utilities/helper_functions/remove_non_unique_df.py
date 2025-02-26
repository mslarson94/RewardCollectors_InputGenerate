import pandas as pd

def remove_nonunique_df(InFile):
    '''
    Helper function that drops duplicate rows from a csv file 
    InFile = your csv file
    '''
    # read in your csv file
    df = pd.read_csv(InFile, index_col=0)
    
    # removes duplicates
    df.drop_duplicates(inplace=True)

    # sending new file out to csv
    filename = InFile.replace(".csv", "")
    new_filename = filename + '_duplicatepurged.csv'
    df.to_csv(new_filename)
