def list_comp_help_subtract(List1, List2):
    '''
    Helper function that just removes items in one list from another list. 
    List1 = your list of items that you want removed from List2
    List2 = list of all items you have 
    '''
    # removes items in one list from another list.
    for i in List1:
        [s if s != i else List2.remove(s) for s in List2]
        
    # returns your amended List2
    return List2

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
