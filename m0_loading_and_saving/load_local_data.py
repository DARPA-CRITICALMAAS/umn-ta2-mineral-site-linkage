import os

def open_local_files():
    return 0

def prompt_user_for_source_name():
    file_name = 'MRDS.csv'
    name_estimate = 'MRDS'

    print(f'Is {name_estimate} an appropriate source name of {file_name}?')
    source_name = input('Enter Y for yes, or a different source name: ').lower()

    if source_name != 'y':
        name_estiamte = source_name

    print(source_name)

    return 0

def open_local_directory(path_directory:str):
    """
    
    : param: path_directory = 
    """
    list_dir_items = os.listdir(path_directory)

    for i in list_dir_items:
        file_name, file_extension = os.path.splitext(i)

        print(file_name, file_extension)
        
    return 0

open_local_directory('/home/yaoyi/pyo00005/CriticalMAAS/src/data/raw')