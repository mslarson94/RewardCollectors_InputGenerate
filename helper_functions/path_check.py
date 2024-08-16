# Import Calls
from pathlib import Path


def path_check(myPath):
    ''' Simple function to check whether a path
    exists or not. If not, will create that
    directory
    Path: path string.
    '''
    Path(myPath).mkdir(parents=True, exist_ok=True)