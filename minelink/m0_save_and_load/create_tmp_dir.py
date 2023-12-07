import os

def check_dir(path_dir, additional=''):
    """
    check_dir function checkes whether the inputted directory path exists and creates one if it does not exist
    
    input: path_dir=path to directory
    """
    path_dir = os.path.join(path_dir, additional)

    if not os.path.exists(path_dir):
        os.makedirs(path_dir)