## Grabbing data & initializing plotting parameters 
# @title Data retrieval
import os, requests
import numpy as np

def grabData(fname, url):
    '''
    fname = file name
    url = the place you are downloading your data from
    '''
    if not os.path.isfile(fname):
        try:
            r = requests.get(url)
        except requests.ConnectionError:
            print("!!! Failed to download data !!!")
        else:
            if r.status_code != requests.codes.ok:
                print("!!! Failed to download data !!!")
            else:
                with open(fname, "wb") as fid:
                    fid.write(r.content)
    
    alldata = np.load(fname, allow_pickle=True)['datum']
    
    return alldata

