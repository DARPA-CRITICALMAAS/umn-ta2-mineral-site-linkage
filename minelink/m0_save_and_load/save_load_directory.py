#!/usr/bin/python

import os
import shutil

from minelink.params import *
from minelink.m0_save_and_load.save_load_file import *
from minelink.m1_preprocessing.dataframe_preprocessing import separate_dataframe

def check_dir(path_dir, additional=''):
    """
    check_dir function checkes whether the inputted directory path exists and creates one if it does not exist
    
    input: path_dir=path to directory
    """
    path_dir = os.path.join(path_dir, additional)

    if not os.path.exists(path_dir):
        os.makedirs(path_dir)

def open_dir(path_dir, bool_dict=False):
    list_dir_items = os.listdir(path_dir)

    list_files = []
    list_dict = []

    if bool_dict==False:
        return list_dir_items, len(list_dir_items)

    for l in list_dir_items:
        file_name, file_extension = os.path.splitext(l)

        if file_extension != '':
            if re.search('dict', file_name):
                list_dict.append(l)
            else:
                list_files.append(l)
        else:
            list_dict_items = os.listdir(os.path.join(path_dir, l))
            list_dict_items = [l+'/'+i for i in list_dict_items]
            list_dict.extend(list_dict_items)

    return list_files, len(list_files), list_dict

def remove_dir(path_dir=PATH_TMP_DIR, additional=''):
    path_dir = os.path.join(path_dir, additional)
    
    try:
        shutil.rmtree(path_dir, ignore_errors=False)
    except:
        pass