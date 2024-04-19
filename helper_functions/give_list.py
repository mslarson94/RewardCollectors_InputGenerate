import pandas as pd
def give_list(InFile):
    '''
    Helper function that makes list of lists or a single list from a csv file
    InFile = your csv file
    '''
    # reading in your csv file
    df = pd.read_csv(InFile)

    # turning your dataframe into list or list of lists & returning that list
    df_list = df.tolist()
    return df_list
