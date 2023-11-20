#!/usr/bin/python

import os
import sys
import shutil

from minelink.loadsave_modules.file_related import *
from minelink.params import *

def check_dir(path_dir):
    if not os.path.exists(path_dir):
        os.makedirs(path_dir)

def load_dir_items(path_dir):
    list_dir_items = os.listdir(path_dir)
    list_files = ['',]
    
    bool_confirm = False
    while not bool_confirm:
        for l in list_dir_items:
            file_name, file_extension = os.path.splitext(l)
            if file_extension == '':
                pass
            elif file_extension in ACCEPTABLE_INPUT_FILE:
                list_files.append(l)

        print("\nModel running with the following files: " + "\n|-- ".join(list_files))

        if len(list_files) == 1:
            break

        user_confirm = str(input("> Confirmed (Y/N)?: ")).upper()
        if user_confirm == 'Y':
            bool_confirm = True
        elif user_confirm == 'N':
            user_remove = str(input("> Which file should be removed (Enter the exact name, case sensitive)?: "))
            list_files.remove(user_remove)

            list_dir_items = list_files
            list_files = ['',]

        #TODO: Deal with case when other stuffs are inputted in

    # TODO: Do something that will print out {filename} AS {extension}
        
    folder_temporary = './temporary'
    check_dir(folder_temporary)

    list_files.pop(0)

    for l in list_files:
        file_name, file_extension = os.path.splitext(l)
        dataframe = loader(path_dir, file_name, file_extension)
        dumper(dataframe, folder_temporary, file_name, 'PICKLE')

    return len(list_files)

def close_dir(path_dir):
    try:
        shutil.rmtree(path_dir, ignore_errors=False)
    except:
        pass