#!/usr/bin/python

import os
import sys
import shutil
from tabulate import tabulate

from minelink.params import *
from minelink.m0_SaveAndLoad.save_load_file import *
from minelink.m1_PreProcessing.dataframe_preprocessing import separate_dataframe

def check_dir(path_dir):
    """
    check_dir function checkes whether the inputted directory path exists and creates one if it does not exist
    
    input: path_dir=path to directory
    """
    if not os.path.exists(path_dir):
        os.makedirs(path_dir)

# def load_dir_items(path_dir):
#     list_dir_items = os.listdir(path_dir)
#     list_files = ['',]
#     list_dict = []
    
#     bool_confirm = True
#     while not bool_confirm:
#         for l in list_dir_items:
#             file_name, file_extension = os.path.splitext(l)
#             if file_extension == '':
#                 list_dict.append(file_name)

#             elif re.match('dict', file_name):
#                 list_dict.append(l)

#             elif file_extension in ACCEPTABLE_INPUT_FILE:
#                 list_files.append(l)

#         print("\nModel running with the following files: " + "\n|-- ".join(list_files))

#         if len(list_files) == 1:
#             break

#         # user_confirm = str(input("> Confirmed (Y/N)?: ")).upper()
#         # if user_confirm == 'Y':
#         #     bool_confirm = True
#         # elif user_confirm == 'N':
#         #     user_remove = str(input("> Which file should be removed (Enter the exact name, case sensitive)?: "))
#         #     list_files.remove(user_remove)

#         #     list_dir_items = list_files
#         #     list_files = ['',]

#         #TODO: Deal with case when other stuffs are inputted in

#     print(list_dict)

#     # TODO: Do something that will print out {filename} AS {extension}
        
#     folder_temporary = './temporary'
#     check_dir(folder_temporary)

#     list_files.pop(0)

#     for l in list_files:
#         file_name, file_extension = os.path.splitext(l)
#         dataframe = loader(path_dir, file_name, file_extension)
#         dumper(dataframe, folder_temporary, file_name, 'PICKLE')

#     return len(list_files)

def open_directory(path_dir):
    check_dir(PATH_TMP_DIR)

    list_dir_items = os.listdir(path_dir)
    list_files = ['',]
    list_dict = []

    dict_tmp_alias = {}
    over_char = 'a'
    alias_char = 'a'

    for l in list_dir_items:
        file_name, file_extension = os.path.splitext(l)
        # TODO: Find dictionary file and save it to format dict_overchar+aliaschar
        if file_extension == '':
            list_dict.append(file_name)

        elif re.match('dict', file_name):
            list_dict.append(l)

        elif file_extension in ACCEPTABLE_INPUT_FILE:
            try:
                source_name = re.sub('_', '/', file_name)
            except:
                source_name = file_name

            tmp_file_folder = os.path.join(PATH_TMP_DIR, over_char+alias_char)
            check_dir(tmp_file_folder)

            list_files.append(l)
            dict_tmp_alias[over_char+alias_char] = source_name

            df = load_file(path_dir, file_name, file_extension)
            dump_file(df, tmp_file_folder, 'raw', 'PICKLE')

            dict_sameas, dict_loc, dict_geo = separate_dataframe(df, over_char+alias_char, source_name)
            dump_file(dict_sameas, tmp_file_folder, 'same_as', 'PICKLE')
            dump_file(dict_loc, tmp_file_folder, 'location_info', 'PICKLE')
            dump_file(dict_geo, tmp_file_folder, 'geometry', 'PICKLE')

            alias_char = chr(ord(alias_char) + 1) 
            over_char = chr(ord(over_char) + 1) if alias_char == 'a' else over_char

    print("\nModel running with the following files: " + "\n|-- ".join(list_files))
    list_files.pop(0)

    dump_file(dict_tmp_alias, PATH_TMP_DIR, 'code_alias', 'PICKLE')

    return len(list_files)

def remove_tmp_dir(path_dir=PATH_TMP_DIR):
    try:
        shutil.rmtree(path_dir, ignore_errors=False)
    except:
        pass