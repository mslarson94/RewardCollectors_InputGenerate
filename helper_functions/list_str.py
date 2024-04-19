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

